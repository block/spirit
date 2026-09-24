package change

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/block/spirit/pkg/table"
)

// This file makes a map-mode drain's batches disjoint *by unique secondary
// index range* rather than only by primary key.
//
// The problem it solves. Two concurrent REPLACE statements on PK-disjoint rows
// can still deadlock, and in production (block/spirit#1168) they did: the
// InnoDB cycle inverted between the clustered index and a UNIQUE secondary
// index. The reason is that a REPLACE's duplicate detection takes a *next-key*
// lock on each unique secondary index — the gap below the record included — so
// two batches collide whenever any of their rows land in adjacent slots of any
// such index. Primary-key disjointness says nothing about that, because
// secondary key order is unrelated to PK order.
//
// The existing defence is an AIMD controller that narrows the flush until the
// deadlocks stop. It works, but it pays for safety in throughput and it never
// stops paying: each contention step costs 4x (both concurrency and batch size
// halve), and a table whose unique key correlates with anything keeps
// re-triggering it. On the table that motivated this, the drain never converged.
//
// The approach. Sort the drain by one unique secondary index, cut *contiguous*
// batches, and never run two adjacent batches at the same time. Then rows that
// are close together in that index land in the same statement — where they
// cannot conflict, since a statement does not deadlock against itself — and
// statements that do run together are separated by at least one whole batch of
// intervening rows.
//
// Three things make this work that are worth stating plainly, because each one
// is a claim that could have gone the other way:
//
//   - It must be *range* partitioning, not hashing the key into buckets.
//     Hashing spreads equal-ish values across buckets, which is precisely the
//     arrangement that collides. The risk is gap adjacency, not shared records.
//
//   - Separation must be by *rows*, not by value distance. Whether two values
//     are adjacent in the B-tree depends on the 8.5-billion-row table, not on
//     the 50,000 rows in hand, so no value-space margin is meaningful. One
//     whole batch of intervening rows from this drain is a margin that is,
//     because those rows are themselves records sitting between the two.
//
//   - Being wrong is a throughput cost, never a correctness one. Batches remain
//     disjoint by key and map mode makes no cross-key ordering promises, so a
//     misordered sort or a badly chosen index produces the *old* collision
//     behaviour and the AIMD controller still catches it. Nothing here is
//     load-bearing for data integrity. That is what makes it safe to sort row
//     images with a best-effort comparator, which an earlier attempt at
//     PK-sorting could not safely do.
//
// What it does not cover, and why the AIMD controller stays:
//
//   - Deletes. A buffered delete keeps only its PK (`LogicalRow{IsDeleted:
//     true}` — the before image is discarded at buffer time), so there is no
//     way to know where it sits in a unique secondary index. Deleted rows are
//     grouped together at the tail and keep today's behaviour.
//   - Second and subsequent unique indexes. Sorting by one index says nothing
//     about the others. Choosing the *most clustered* index is what makes this
//     worthwhile: an index whose values are uniformly distributed has an
//     adjacency probability of about n^2/N per drain (~0.3 for 50,000 rows in
//     8.5 billion), while a correlated one is a near-certainty.
//   - REPLACE's out-of-partition deletion cascade. A REPLACE deletes any row
//     conflicting on any unique index, including PKs not in the batch, whose
//     other unique values are unknowable from here. This is documented at
//     applier.UpsertRows and is irreducible.

// partitionIndex is a resolved unique secondary index: its columns' positions
// in the source row image, so extracting a row's key is a slice index rather
// than a name lookup per row.
type partitionIndex struct {
	name string
	// ordinals are positions in bufferedChange.logicalRow.RowImage, in key
	// order. Empty is impossible — resolvePartitionIndexes drops such an index.
	ordinals []int
}

// flushPartitioner holds the candidate indexes for a subscription. Resolution
// needs a query, and NewBufferedSubscription has no context, so it happens
// lazily on the first drain that could use it.
type flushPartitioner struct {
	once       sync.Once
	candidates []partitionIndex
}

// resolvePartitionIndexes reads the *new* table's unique secondary indexes and
// maps each one's columns onto positions in the source row image.
//
// The new table is the right side to read: the REPLACE runs there, so its
// indexes are the locks being taken. An ALTER may have added or dropped one,
// which is exactly why this cannot be read from the source. But the row image
// comes from the source's binlog, so an index is only usable if every one of
// its columns also exists on the source — an index on a column the ALTER *adds*
// has no value to sort by here. Those are dropped rather than partially used.
func resolvePartitionIndexes(ctx context.Context, sourceTable, newTable *table.TableInfo) ([]partitionIndex, error) {
	// Both handles are optional as far as this file is concerned. Queue-mode
	// subscriptions and the package's own bare test maps carry no new table, and
	// a metadata-only TableInfo has no server to ask. None of those is a fault:
	// they just mean the drain batches as it did before.
	if sourceTable == nil || newTable == nil {
		return nil, nil
	}
	unique, err := newTable.UniqueSecondaryIndexes(ctx)
	if err != nil {
		return nil, err
	}
	var out []partitionIndex
	for _, idx := range unique {
		ordinals := make([]int, 0, len(idx.Columns))
		usable := true
		for _, col := range idx.Columns {
			ord := slices.Index(sourceTable.Columns, col)
			if ord < 0 {
				usable = false
				break
			}
			ordinals = append(ordinals, ord)
		}
		if usable {
			out = append(out, partitionIndex{name: idx.Name, ordinals: ordinals})
		}
	}
	return out, nil
}

// partitionIndexes returns the resolved candidates, resolving them on first
// use. A failure is cached as "no candidates": the query is against
// information_schema on the same connection the drain is about to use, so if it
// fails the drain has bigger problems, and partitioning is an optimization that
// must never be the reason a flush fails.
func (s *bufferedMap) partitionIndexes(ctx context.Context) []partitionIndex {
	s.partitioner.once.Do(func() {
		candidates, err := resolvePartitionIndexes(ctx, s.table, s.newTable)
		if err != nil {
			s.logger.Warn("could not read unique indexes for flush partitioning; flush batches will be partitioned by primary key only",
				"table", s.table.SchemaName+"."+s.table.TableName,
				"error", err.Error())
			return
		}
		s.partitioner.candidates = candidates
		switch len(candidates) {
		case 0:
			// Not a warning. No unique secondary index means there is no
			// conflict surface to partition: the clustered index takes record
			// locks without gaps, so PK-disjoint batches already cannot collide.
			s.logger.Debug("flush partitioning inactive: no usable unique secondary index",
				"table", s.table.SchemaName+"."+s.table.TableName)
		default:
			names := make([]string, len(candidates))
			for i, c := range candidates {
				names[i] = c.name
			}
			s.logger.Info("flush partitioning enabled: batches will be cut into contiguous ranges of a unique secondary index",
				"table", s.table.SchemaName+"."+s.table.TableName,
				"candidate_indexes", names)
		}
	})
	return s.partitioner.candidates
}

// drainRow is one snapshot entry with its map key, so the sort can carry both.
type drainRow struct {
	key    string
	change bufferedChange
}

// keyOf extracts a row's value for the leading column of idx, or nil when the
// row has no usable image (a delete, or a truncated image).
func (idx partitionIndex) leadingValue(change bufferedChange) (any, bool) {
	img := change.logicalRow.RowImage
	if change.logicalRow.IsDeleted || len(img) <= idx.ordinals[0] {
		return nil, false
	}
	return img[idx.ordinals[0]], true
}

// choosePartitionIndex picks the candidate whose values are most clustered
// across this drain's rows, or nil when there is nothing to choose from.
//
// The score is the number of rows that share a leading-column value with some
// other row: `len(rows) - distinct(leading values)`. That is the signal that
// matters, and it is measured from the data in hand rather than configured —
// which is the only way to get it right, since whether a key correlates with
// anything is a property of the workload and not of the schema.
//
// A correlated index (sibling rows sharing a leading value, so they are
// physically adjacent and *guaranteed* to collide once random map iteration
// scatters them across batches) scores near len(rows). An opaque, uniformly
// distributed key scores 0, because every value is distinct. Sorting by the
// former is what buys something.
//
// One consequence is worth being explicit about, because it looks like a bug
// otherwise: a *single-column* unique index always scores exactly 0. It has to
// — its values cannot repeat, that is what unique means. Clustering can only
// ever show up as a repeating *leading* column of a composite unique key, with
// a discriminator after it. That is not a limitation so much as a description
// of the production shape: a correlated leading column is precisely how a
// composite unique key ends up with adjacent sibling records.
//
// So an all-zero score does not mean "don't partition", and the tie is not
// arbitrary neglect:
//
//   - A uniform key still has B-tree-adjacent pairs by chance (~n^2/N per
//     drain), and sorting brings any such pair into one batch.
//   - A single-column key can be *sequentially allocated* — 'tok-0001',
//     'tok-0002' — which is adjacency this score cannot see but the sort
//     handles anyway.
//
// Both are served by sorting at all, so the first candidate wins ties and the
// score only decides *which* index when one of them is measurably clustered.
// Scoring lexical closeness rather than equality would also catch the
// sequential-allocation case in the choice, at the cost of sorting once per
// candidate; it has not been needed, and this note is here so the option is
// visible if it ever is.
func choosePartitionIndex(candidates []partitionIndex, rows []drainRow) *partitionIndex {
	if len(candidates) == 0 || len(rows) == 0 {
		return nil
	}
	best, bestScore := -1, -1
	seen := make(map[any]struct{}, len(rows))
	for i, idx := range candidates {
		clear(seen)
		counted := 0
		for _, r := range rows {
			v, ok := idx.leadingValue(r.change)
			if !ok {
				continue
			}
			counted++
			seen[normalizeForSet(v)] = struct{}{}
		}
		if counted == 0 {
			continue // every row is a delete, or the image is too short
		}
		if score := counted - len(seen); score > bestScore {
			best, bestScore = i, score
		}
	}
	if best < 0 {
		return nil
	}
	return &candidates[best]
}

// normalizeForSet makes a row-image value usable as a map key. []byte is not
// comparable, so it becomes a string; everything else already is. Numeric
// widths are deliberately *not* unified here — this only counts distinct
// values, and go-mysql decodes a given column to one consistent Go type, so
// int32(1) and int64(1) never both appear for the same column.
func normalizeForSet(v any) any {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

// sortRowsByIndex orders rows by idx's key values, rows with no usable key
// last. Rows are compared component by component in key order.
//
// The ordering only has to *cluster*; it does not have to match InnoDB's
// collation. Where it disagrees — a non-binary collation, a locale-specific
// ordering — the affected rows land in a neighbouring batch instead of this
// one, which costs a little separation and nothing else. See this file's header
// for why that is a throughput property rather than a correctness one.
func sortRowsByIndex(rows []drainRow, idx *partitionIndex) {
	slices.SortStableFunc(rows, func(a, b drainRow) int {
		aImg, aOK := usableImage(a.change, idx)
		bImg, bOK := usableImage(b.change, idx)
		switch {
		case !aOK && !bOK:
			return 0
		case !aOK:
			return 1 // no key sorts last
		case !bOK:
			return -1
		}
		for _, ord := range idx.ordinals {
			if c := compareRowValues(aImg[ord], bImg[ord]); c != 0 {
				return c
			}
		}
		return 0
	})
}

func usableImage(change bufferedChange, idx *partitionIndex) ([]any, bool) {
	img := change.logicalRow.RowImage
	if change.logicalRow.IsDeleted {
		return nil, false
	}
	for _, ord := range idx.ordinals {
		if ord >= len(img) {
			return nil, false
		}
	}
	return img, true
}

// valueKind ranks the type families so a column that somehow yields mixed types
// still produces a total order instead of an arbitrary one. Within a real
// column this never fires — go-mysql decodes a column to one Go type — but the
// comparator must be total regardless, because sort.Interface's contract is
// not "total when the data is well-formed".
type valueKind int

const (
	kindNull valueKind = iota
	kindNumber
	kindBytes
	kindOther
)

func kindOf(v any) valueKind {
	switch v.(type) {
	case nil:
		return kindNull
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return kindNumber
	case string, []byte:
		return kindBytes
	default:
		return kindOther
	}
}

// compareRowValues orders two row-image values of the same column.
//
// Every integer width go-mysql can produce is handled explicitly. That is not
// defensive boilerplate: an earlier PK-sorting attempt in this package had arms
// for int64/uint64/float64/string/[]byte only, and because go-mysql decodes
// MYSQL_TYPE_LONG to int32, TINY to int8, SHORT to int16 and INT24 to int32,
// an ordinary INT column fell through to a string comparison and sorted 100
// before 9 — the exact inversion the sort existed to prevent. Falling through
// silently is the failure mode to design against here.
func compareRowValues(a, b any) int {
	ka, kb := kindOf(a), kindOf(b)
	if ka != kb {
		return int(ka) - int(kb)
	}
	switch ka {
	case kindNull:
		return 0
	case kindNumber:
		return compareNumbers(a, b)
	case kindBytes:
		return bytes.Compare(asBytes(a), asBytes(b))
	case kindOther:
		// time.Time, decimal types rendered as a custom type, anything a future
		// go-mysql adds. Comparing the rendered form is a poor order but a
		// consistent one, and it is reached only by types no MySQL column
		// currently decodes to.
		return bytes.Compare(fmt.Append(nil, a), fmt.Append(nil, b))
	}
	return 0
}

func asBytes(v any) []byte {
	switch t := v.(type) {
	case string:
		return []byte(t)
	case []byte:
		return t
	}
	return nil
}

// compareNumbers compares two numeric row-image values without going through
// float64 for the integer cases: a bigint PK or unique key past 2^53 would lose
// its low bits, and adjacent values differing only there are exactly the ones
// this whole mechanism cares about keeping together.
func compareNumbers(a, b any) int {
	if ai, ok := asInt64(a); ok {
		if bi, ok := asInt64(b); ok {
			return cmpOrdered(ai, bi)
		}
		if bu, ok := asUint64(b); ok {
			if ai < 0 {
				return -1
			}
			return cmpOrdered(uint64(ai), bu)
		}
	}
	if au, ok := asUint64(a); ok {
		if bu, ok := asUint64(b); ok {
			return cmpOrdered(au, bu)
		}
		if bi, ok := asInt64(b); ok {
			if bi < 0 {
				return 1
			}
			return cmpOrdered(au, uint64(bi))
		}
	}
	// At least one side is a float; compare in float space. Precision loss is
	// acceptable here in a way it is not for integers, because a float column's
	// adjacent values are not meaningfully orderable at the ulp level anyway.
	return cmpOrdered(asFloat64(a), asFloat64(b))
}

func cmpOrdered[T int64 | uint64 | float64](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func asInt64(v any) (int64, bool) {
	switch t := v.(type) {
	case int:
		return int64(t), true
	case int8:
		return int64(t), true
	case int16:
		return int64(t), true
	case int32:
		return int64(t), true
	case int64:
		return t, true
	}
	return 0, false
}

func asUint64(v any) (uint64, bool) {
	switch t := v.(type) {
	case uint:
		return uint64(t), true
	case uint8:
		return uint64(t), true
	case uint16:
		return uint64(t), true
	case uint32:
		return uint64(t), true
	case uint64:
		return t, true
	}
	return 0, false
}

func asFloat64(v any) float64 {
	switch t := v.(type) {
	case float32:
		return float64(t)
	case float64:
		return t
	}
	if i, ok := asInt64(v); ok {
		return float64(i)
	}
	if u, ok := asUint64(v); ok {
		return float64(u)
	}
	return 0
}

// minCutFill is the fraction of a full batch below which a cut point will not
// be pulled back in search of a leading-value change. Without a floor, a table
// whose leading value repeats every few rows would let every batch shrink to
// almost nothing and multiply the statement count.
const minCutFill = 2

// cutAtValueBoundary returns where to end a batch that started at `start` and
// would otherwise end at `hardEnd`, preferring a position where the leading key
// value changes.
//
// Splitting a run of rows that share a leading value is the one thing worth
// avoiding: those rows are physically adjacent in the index, so a split puts a
// guaranteed-adjacent pair in two different batches — the production failure
// exactly. Walking back to the last value change instead keeps the run whole.
//
// The walk-back is bounded to half a batch. A run longer than that cannot be
// kept whole without unbounded batches, so it is split at the hard limit and
// left to the stripe scheme below, which at least guarantees the two halves do
// not run concurrently.
//
// Given hardEnd > start, the result is always strictly greater than start: the
// walk-back is floored at start+1, and both early returns (hardEnd, len(rows))
// are past it. buildBatches advances its cursor to the cut, so a cut at start
// would be a batch of no rows and a loop that never terminates. It keeps its
// own guard rather than relying on this — but a change here that could return
// start is a bug on this side of the boundary, and
// TestCutAtValueBoundaryAlwaysAdvances is what says so.
func cutAtValueBoundary(rows []drainRow, idx *partitionIndex, start, hardEnd int) int {
	if hardEnd >= len(rows) {
		return len(rows)
	}
	floor := start + max(1, (hardEnd-start)/minCutFill)
	prev, prevOK := idx.leadingValue(rows[hardEnd-1].change)
	if !prevOK {
		return hardEnd // in the delete tail; nothing to align to
	}
	for i := hardEnd - 1; i > floor; i-- {
		v, ok := idx.leadingValue(rows[i-1].change)
		if !ok {
			return hardEnd
		}
		if compareRowValues(v, prev) != 0 {
			return i // rows[i-1] and rows[i] differ: a real boundary
		}
	}
	return hardEnd
}

// stripeBatches splits a list of *contiguous, sorted* batches into groups that
// may each be run with full concurrency, such that no two batches in the same
// group are adjacent in the sort order.
//
// Two groups is all it takes: evens, then odds. Any two batches in one group
// have at least one whole batch of this drain's rows between them in unique-key
// order, and those rows are themselves index records — so the two batches
// cannot be holding adjacent slots. That is a structural guarantee rather than
// a probabilistic one, and it is why the batches are not simply handed to the
// existing limiter in sorted order: with a limiter, the in-flight window is
// roughly contiguous, so neighbours run together and every batch boundary
// becomes a candidate collision.
//
// The cost is one barrier: the slowest batch of the even group delays the odd
// group. That is real, and it is much cheaper than what it replaces — an AIMD
// contention step costs 4x and persists for several drains, while this costs
// the tail of one group, once.
func stripeBatches(batches []*mapFlushBatch) [][]*mapFlushBatch {
	if len(batches) < 2 {
		return [][]*mapFlushBatch{batches}
	}
	stripes := make([][]*mapFlushBatch, 2)
	for i, b := range batches {
		stripes[i%2] = append(stripes[i%2], b)
	}
	return stripes
}

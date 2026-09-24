package change

import (
	"fmt"
	"math"
	"testing"

	"github.com/block/spirit/pkg/applier"
	"github.com/stretchr/testify/require"
)

// upsertRow builds a drainRow whose row image is [id, leading, trailing], which
// matches the partition indexes the tests below construct.
func upsertRow(key string, id int64, leading, trailing any) drainRow {
	return drainRow{
		key: key,
		change: bufferedChange{
			logicalRow:  applier.LogicalRow{RowImage: []any{id, leading, trailing}},
			originalKey: []any{id},
		},
	}
}

func deleteRow(key string, id int64) drainRow {
	return drainRow{
		key: key,
		change: bufferedChange{
			logicalRow:  applier.LogicalRow{IsDeleted: true},
			originalKey: []any{id},
		},
	}
}

// leadingIdx partitions on the row image's column 1 alone; compositeIdx on
// columns 1 and 2, so only the leading one decides coarse placement.
var (
	leadingIdx   = partitionIndex{name: "unq_leading", ordinals: []int{1}}
	compositeIdx = partitionIndex{name: "unq_composite", ordinals: []int{1, 2}}
)

// TestCompareRowValuesHandlesEveryIntegerWidth is the regression guard for the
// bug that killed the previous sorting attempt in this package. go-mysql decodes
// MYSQL_TYPE_LONG to int32, TINY to int8, SHORT to int16 and INT24 to int32, so
// a comparator with only an int64 arm sends an ordinary INT column through a
// string comparison — where "100" sorts before "9" and the sort actively
// inverts the order it was added to establish.
func TestCompareRowValuesHandlesEveryIntegerWidth(t *testing.T) {
	// Every width go-mysql can produce, each asserting 9 < 100 numerically.
	widths := []struct {
		name  string
		small any
		large any
	}{
		{"int", int(9), int(100)},
		{"int8", int8(9), int8(100)},
		{"int16", int16(9), int16(100)},
		{"int32", int32(9), int32(100)},
		{"int64", int64(9), int64(100)},
		{"uint", uint(9), uint(100)},
		{"uint8", uint8(9), uint8(100)},
		{"uint16", uint16(9), uint16(100)},
		{"uint32", uint32(9), uint32(100)},
		{"uint64", uint64(9), uint64(100)},
		{"float32", float32(9), float32(100)},
		{"float64", float64(9), float64(100)},
	}
	for _, w := range widths {
		t.Run(w.name, func(t *testing.T) {
			require.Negative(t, compareRowValues(w.small, w.large),
				"9 must sort before 100 — a string comparison would invert this")
			require.Positive(t, compareRowValues(w.large, w.small))
			require.Zero(t, compareRowValues(w.small, w.small))
		})
	}

	// Integers must not be compared through float64: past 2^53 that loses the
	// low bits, and values differing only there are exactly the adjacent ones
	// this mechanism exists to keep in one batch.
	a, b := int64(1<<53), int64(1<<53)+1
	require.Negative(t, compareRowValues(a, b), "bigint precision must survive the comparison")
	require.InDelta(t, float64(a), float64(b), 0,
		"the premise: these are indistinguishable as float64, which is why the integer path matters")

	ua, ub := uint64(math.MaxUint64), uint64(math.MaxUint64)-1
	require.Positive(t, compareRowValues(ua, ub), "a uint64 past MaxInt64 must not wrap")

	// A signed/unsigned mix cannot arise from one column, but the comparator has
	// to stay total rather than returning a wrong answer if it ever does.
	require.Negative(t, compareRowValues(int64(-1), uint64(1)))
	require.Positive(t, compareRowValues(uint64(1), int64(-1)))
}

// TestCompareRowValuesIsTotal pins the properties sort.Interface requires. A
// comparator that is merely usually-right produces an arbitrary permutation
// rather than a slightly worse one, so the ordering must be total across type
// families too — not only within a well-formed column.
func TestCompareRowValuesIsTotal(t *testing.T) {
	values := []any{
		nil, int64(-5), int64(0), int64(7), float64(1.5),
		"", "a", "b", []byte("a"), []byte("z"),
		struct{ X int }{1},
	}
	for _, a := range values {
		for _, b := range values {
			ab, ba := compareRowValues(a, b), compareRowValues(b, a)
			require.Equal(t, ab, -ba, "antisymmetry for (%v, %v)", a, b)
		}
		require.Zero(t, compareRowValues(a, a), "reflexivity for %v", a)
	}
	// NULL sorts first and string/[]byte compare as the same family, so a
	// varchar column that go-mysql sometimes hands back as []byte still orders
	// with its string neighbours instead of forming a second cluster.
	require.Negative(t, compareRowValues(nil, int64(0)))
	require.Zero(t, compareRowValues("abc", []byte("abc")))
	require.Negative(t, compareRowValues([]byte("abc"), "abd"))
}

// TestChoosePartitionIndexPrefersTheClusteredKey is the heart of the design's
// self-configuration: with two unique indexes, the one whose values repeat is
// the one worth sorting by, because repeated leading values mean physically
// adjacent records and therefore guaranteed collisions once map iteration
// scatters them. An opaque, uniformly distributed key has an adjacency
// probability of roughly n^2/N per drain and needs no help.
func TestChoosePartitionIndexPrefersTheClusteredKey(t *testing.T) {
	// column 1 is correlated (10 rows per value); column 2 is unique per row.
	var rows []drainRow
	for i := range int64(100) {
		rows = append(rows, upsertRow(fmt.Sprintf("k%d", i), i,
			fmt.Sprintf("group-%02d", i/10), fmt.Sprintf("opaque-%04d", i)))
	}
	clustered := partitionIndex{name: "unq_clustered", ordinals: []int{1}}
	opaque := partitionIndex{name: "unq_opaque", ordinals: []int{2}}

	// Offered in either order, the clustered index wins on its own merits
	// rather than on its position in the candidate list.
	for _, candidates := range [][]partitionIndex{
		{clustered, opaque},
		{opaque, clustered},
	} {
		chosen := choosePartitionIndex(candidates, rows)
		require.NotNil(t, chosen)
		require.Equal(t, "unq_clustered", chosen.name)
	}

	// With only the opaque key on offer it is still chosen: every value being
	// distinct scores zero, but sorting by it costs one sort and still brings
	// any chance-adjacent pair into a single batch.
	chosen := choosePartitionIndex([]partitionIndex{opaque}, rows)
	require.NotNil(t, chosen)
	require.Equal(t, "unq_opaque", chosen.name)

	// Degenerate inputs must yield nil rather than a partitioning that cannot
	// be computed.
	require.Nil(t, choosePartitionIndex(nil, rows), "no candidates")
	require.Nil(t, choosePartitionIndex([]partitionIndex{clustered}, nil), "no rows")
	deletes := []drainRow{deleteRow("d1", 1), deleteRow("d2", 2)}
	require.Nil(t, choosePartitionIndex([]partitionIndex{clustered}, deletes),
		"a delete keeps no row image, so there is no value to cluster on")
}

// TestSortRowsByIndexClustersAndTailsDeletes pins the two things the sort has
// to get right: the composite key is compared component by component, and rows
// with no usable key go to the end rather than being interleaved (which would
// split otherwise-contiguous ranges around them).
func TestSortRowsByIndexClustersAndTailsDeletes(t *testing.T) {
	rows := []drainRow{
		upsertRow("e", 5, "b", "2"),
		deleteRow("del1", 90),
		upsertRow("c", 3, "a", "3"),
		upsertRow("a", 1, "a", "1"),
		upsertRow("d", 4, "b", "1"),
		deleteRow("del2", 91),
		upsertRow("b", 2, "a", "2"),
	}
	sortRowsByIndex(rows, &compositeIdx)

	var got []string
	for _, r := range rows {
		got = append(got, r.key)
	}
	require.Equal(t, []string{"a", "b", "c", "d", "e", "del1", "del2"}, got,
		"sorted by (col1, col2) with the image-less rows tailed")
}

// TestCutAtValueBoundaryKeepsRunsWhole covers the cut-point nudge. Splitting a
// run of rows that share a leading value is the production failure in
// miniature: those records are adjacent, so a split guarantees an adjacent pair
// in two different batches.
func TestCutAtValueBoundaryKeepsRunsWhole(t *testing.T) {
	// 12 rows: values "a" x5, "b" x4, "c" x3, already sorted.
	var rows []drainRow
	for i, v := range []string{"a", "a", "a", "a", "a", "b", "b", "b", "b", "c", "c", "c"} {
		rows = append(rows, upsertRow(fmt.Sprintf("k%d", i), int64(i), v, ""))
	}

	// A hard end of 7 falls inside the "b" run; the cut walks back to 5, where
	// "a" becomes "b".
	require.Equal(t, 5, cutAtValueBoundary(rows, &leadingIdx, 0, 7))

	// A hard end that already sits on a boundary is left alone.
	require.Equal(t, 9, cutAtValueBoundary(rows, &leadingIdx, 5, 9))

	// The walk-back is floored at half a batch. Here the whole span is one
	// value, so there is no boundary to find and the hard limit stands — a run
	// longer than a batch cannot be kept whole without unbounded batches, and
	// the stripe scheme is what protects it instead.
	var oneValue []drainRow
	for i := range 20 {
		oneValue = append(oneValue, upsertRow(fmt.Sprintf("s%d", i), int64(i), "same", ""))
	}
	require.Equal(t, 10, cutAtValueBoundary(oneValue, &leadingIdx, 0, 10))

	// Past the end of the rows the cut is the end of the rows.
	require.Equal(t, len(rows), cutAtValueBoundary(rows, &leadingIdx, 0, 99))
}

// TestBuildBatchesProducesContiguousRanges pins that a partitioned drain's
// batches are contiguous in the sorted order and that no leading-value run is
// split across two of them.
func TestBuildBatchesProducesContiguousRanges(t *testing.T) {
	sub := newByteCapBufferedMap(&countingApplier{}, false)
	sub.batchSize = 10

	// 200 rows in runs of 4 sharing a leading value.
	var rows []drainRow
	for i := range int64(200) {
		rows = append(rows, upsertRow(fmt.Sprintf("k%03d", i), i,
			fmt.Sprintf("g%03d", i/4), fmt.Sprintf("t%03d", i)))
	}
	sortRowsByIndex(rows, &leadingIdx)
	batches := sub.buildBatches(rows, &leadingIdx)
	require.Greater(t, len(batches), 1)

	// Every row appears exactly once, and batches tile the sorted order in
	// order — the property the stripe scheme's separation argument rests on.
	seen := 0
	for _, b := range batches {
		for i, key := range b.keys {
			require.Equal(t, rows[seen+i].key, key, "batches must tile the sorted order")
		}
		seen += len(b.keys)
		require.LessOrEqual(t, len(b.keys), 10, "the row cap still binds")
		require.NotEmpty(t, b.keys, "an empty batch would spin the build loop")
	}
	require.Equal(t, len(rows), seen, "every row must be batched exactly once")

	// No leading value spans two batches.
	owner := make(map[string]int)
	for bi, b := range batches {
		for _, key := range b.keys {
			idx := 0
			for i, r := range rows {
				if r.key == key {
					idx = i
					break
				}
			}
			v, _ := leadingIdx.leadingValue(rows[idx].change)
			val := v.(string)
			if prev, ok := owner[val]; ok {
				require.Equal(t, prev, bi,
					"leading value %q was split across batches %d and %d", val, prev, bi)
			}
			owner[val] = bi
		}
	}
}

// TestStripeBatchesSeparatesNeighbours is the invariant the whole design
// reduces to: two batches that may be in flight together must never be
// neighbours in the sorted order, because neighbours share a boundary and a
// boundary is where a next-key lock on a unique index can overlap.
func TestStripeBatchesSeparatesNeighbours(t *testing.T) {
	for _, n := range []int{2, 3, 4, 5, 16, 33} {
		batches := make([]*mapFlushBatch, n)
		position := make(map[*mapFlushBatch]int, n)
		for i := range batches {
			batches[i] = &mapFlushBatch{keys: []string{fmt.Sprintf("b%d", i)}}
			position[batches[i]] = i
		}
		stripes := stripeBatches(batches)

		total := 0
		for _, stripe := range stripes {
			total += len(stripe)
			for i, a := range stripe {
				for _, b := range stripe[i+1:] {
					require.Greater(t, abs(position[a]-position[b]), 1,
						"batches %d and %d may run concurrently but are adjacent (n=%d)",
						position[a], position[b], n)
				}
			}
		}
		require.Equal(t, n, total, "striping must not drop or duplicate a batch (n=%d)", n)
	}

	// A single batch has no neighbour, so it is not worth a barrier.
	one := []*mapFlushBatch{{keys: []string{"only"}}}
	require.Len(t, stripeBatches(one), 1)
	require.Empty(t, stripeBatches(nil)[0])
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// TestCutAtValueBoundaryAlwaysAdvances sweeps the cut over adversarial row
// shapes and asserts the one property buildBatches' loop termination depends
// on. The cut is the loop's only source of progress, so a return at `start`
// would not be an off-by-one — it would be a hang that allocates an empty
// batch per turn until the process dies.
//
// The shapes are chosen to press on every branch: one value everywhere (no
// boundary to walk back to, so the floor must hold), all values distinct
// (a boundary at every position), long runs (the walk-back travels), and a
// delete tail (leadingValue stops reporting partway through).
func TestCutAtValueBoundaryAlwaysAdvances(t *testing.T) {
	shapes := map[string][]drainRow{}

	var oneValue, distinct, runs, withDeletes []drainRow
	for i := range int64(40) {
		key := fmt.Sprintf("k%03d", i)
		oneValue = append(oneValue, upsertRow(key, i, "same", ""))
		distinct = append(distinct, upsertRow(key, i, fmt.Sprintf("v%03d", i), ""))
		runs = append(runs, upsertRow(key, i, fmt.Sprintf("g%03d", i/7), ""))
		if i < 25 {
			withDeletes = append(withDeletes, upsertRow(key, i, fmt.Sprintf("g%03d", i/7), ""))
		} else {
			withDeletes = append(withDeletes, deleteRow(key, i))
		}
	}
	shapes["one value"] = oneValue
	shapes["all distinct"] = distinct
	shapes["runs of 7"] = runs
	shapes["delete tail"] = withDeletes
	shapes["single row"] = oneValue[:1]

	for name, rows := range shapes {
		t.Run(name, func(t *testing.T) {
			for _, idx := range []*partitionIndex{&leadingIdx, &compositeIdx} {
				for start := range len(rows) {
					// Every batch size, including 1 — the size an AIMD-floored
					// drain of narrow batches reaches, and the tightest case
					// for the walk-back floor.
					for _, batchSize := range []int{1, 2, 3, 7, 10, 64} {
						hardEnd := min(start+batchSize, len(rows))
						got := cutAtValueBoundary(rows, idx, start, hardEnd)
						require.Greater(t, got, start,
							"idx=%s start=%d batchSize=%d: a cut at or before the start "+
								"is an infinite loop in buildBatches", idx.name, start, batchSize)
						require.LessOrEqual(t, got, len(rows),
							"idx=%s start=%d batchSize=%d: cut past the rows", idx.name, start, batchSize)
					}
				}
			}
		})
	}
}

// TestBuildBatchesAlwaysConsumesRows is the same property one level up, where
// it actually bites. Termination is asserted by the test completing: the loop
// under test is the one that would hang.
func TestBuildBatchesAlwaysConsumesRows(t *testing.T) {
	for _, batchSize := range []int{1, 2, minAdaptiveBatchSize, DefaultBatchSize} {
		for _, partitioned := range []bool{false, true} {
			sub := newByteCapBufferedMap(&countingApplier{}, false)
			sub.batchSize = batchSize

			// One leading value across the whole set: the shape with no
			// boundary to align to, so the walk-back finds nothing and the
			// floor is the only thing keeping the cut ahead of the cursor.
			var rows []drainRow
			for i := range int64(120) {
				rows = append(rows, upsertRow(fmt.Sprintf("k%03d", i), i, "same", ""))
			}

			var idx *partitionIndex
			if partitioned {
				idx = &leadingIdx
			}
			batches := sub.buildBatches(rows, idx)

			total := 0
			for _, b := range batches {
				require.NotEmpty(t, b.keys,
					"batchSize=%d partitioned=%t: an empty batch means the cursor did not advance",
					batchSize, partitioned)
				total += len(b.keys)
			}
			require.Equal(t, len(rows), total,
				"batchSize=%d partitioned=%t: every row batched exactly once", batchSize, partitioned)
		}
	}
}

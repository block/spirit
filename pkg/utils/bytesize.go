package utils

import "time"

// EstimateRenderedChunkSize estimates the size of a chunk's rows, for the
// memory-based dynamic chunker: the sum of EstimateRenderedRowSize over every
// row. It is not exact Go heap accounting (it is the payload plus quotes and
// separators), but it is good enough to servo chunk row-count toward a byte
// budget: the servo cares about the relative size of chunks, and any
// consistent measure converges. It is cheap because the rows are already in
// hand.
//
// Every row counts as at least 2 bytes (its parentheses), even one whose
// values are all empty. This keeps the sum non-zero for any chunk that has
// rows, so a zero-byte total unambiguously means an empty (gap) chunk (see
// dynamicChunkSizer.feedbackBytes).
func EstimateRenderedChunkSize(rows [][]any) uint64 {
	var total uint64
	for _, row := range rows {
		total += uint64(EstimateRenderedRowSize(row))
	}
	return total
}

// EstimateRenderedRowSize estimates the size in bytes of a row's values as
// they will be rendered into a VALUES clause. It does not need to be precise:
// the statement budget it feeds (applier.MaxStatementSizeBytes, 1 MiB) sits
// ~64x below a typical max_allowed_packet, so the estimate only has to be the
// right order of magnitude to keep a statement well clear of the wire limit.
//
// The applier uses it to cut chunklets, the copier (via
// EstimateRenderedChunkSize) to size chunks, and callers that batch rows
// before handing them to Apply to bound a batch by the same measure — the
// checksum's chunk repair does this.
//
// It does need to be cheap. It runs on every value of every copied row, once
// per row on top of the rendering writeChunklet does anyway, so it is pure
// overhead on the hottest client-side path. The previous implementation
// measured len(fmt.Sprintf("%v", value)), which was neither cheap nor
// accurate: a text-protocol Scan into *any hands back []byte for string,
// temporal and DECIMAL columns, and %v renders a []byte as "[49 50 51 …]" —
// roughly four characters per byte. That cost ~2.2us and ~12 allocations per row and
// over-estimated by ~2.7x, so chunklets were being cut well short of the
// budget they were supposed to fill. A type switch is ~290x cheaper, allocates
// nothing, and lands much closer to what datum.String() actually emits.
func EstimateRenderedRowSize(values []any) int {
	size := 2 // the tuple's parentheses
	for _, value := range values {
		// +2 for the ", " separator. That over-counts by one separator per
		// row (values are joined, not terminated), which exactly covers the
		// ", " between this row's tuple and the next in the statement. Quote
		// characters are estimateRenderedValueSize's job, not counted here.
		size += estimateRenderedValueSize(value) + 2
	}
	return size
}

// estimateRenderedValueSize approximates the rendered length of one value.
//
// Two cases deliberately under-estimate rather than pad: a []byte bound to a
// binary column renders as 0x-hex (two characters per byte), and a string
// containing quotes or backslashes grows by escaping. Both are bounded by the
// 64x headroom above, and padding for them would penalise the common text case
// the way the old %v behaviour did — an over-estimate is not free, it shrinks
// every chunklet.
func estimateRenderedValueSize(value any) int {
	switch v := value.(type) {
	case nil:
		return 4 // NULL
	case []byte:
		// +2 for the surrounding quotes. Slightly over for DECIMAL (which the
		// text protocol delivers as []byte but which renders unquoted);
		// telling them apart would need the column type, which this
		// deliberately doesn't take.
		return len(v) + 2
	case string:
		return len(v) + 2 // +2 for the surrounding quotes
	case time.Time:
		return 28 // '2026-07-30 15:12:27.123456' — quoted, unlike numerics
	case float32, float64:
		// Typical rather than worst case: a float64 can render as wide as 24
		// ("-1.7976931348623157e+308") but usually lands around 6-9 ("3.14159",
		// "-2.71828"). Same bias as the two cases above — under-estimating is
		// covered by the headroom, over-estimating shrinks every chunklet on a
		// table of DOUBLE columns. FLOAT and DOUBLE arrive as float64 on both
		// the copy path and the binlog path; DECIMAL arrives as []byte and is
		// measured by the branch above.
		return 8
	case bool:
		return 1
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		// Typical, not worst case, on the same bias as above: 10 digits covers
		// an ordinary ID exactly, and a full-width int64 (19-20 characters)
		// under-estimates by about 2x. Integers arrive as native Go integers on
		// both the copy path (the driver parses them on the text protocol) and
		// the binlog path, so this branch runs on every integer column of
		// every copied row. Counting the digits would be exact but costs ~22ns
		// per row there, and it buys nothing either consumer needs: the
		// statement cut is covered by the headroom above, and the chunk sizer
		// servos on the relative size of chunks, which a flat width per
		// integer column keeps consistent.
		return 10
	default:
		// Not produced by either the SQL driver or the binlog reader today.
		// Cheap and slightly generous rather than reflective.
		return 32
	}
}

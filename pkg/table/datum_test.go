package table

import (
	"log/slog"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDatum(t *testing.T) {
	signed, err := NewDatum(1, signedType)
	require.NoError(t, err)
	unsigned, err := NewDatum(uint(1), unsignedType)
	require.NoError(t, err)

	require.Equal(t, "1", signed.String())
	require.Equal(t, "1", unsigned.String())

	require.Equal(t, strconv.Itoa(math.MinInt64), signed.MinValue().String())
	require.Equal(t, strconv.Itoa(math.MaxInt64), signed.MaxValue().String())
	require.Equal(t, "0", unsigned.MinValue().String())
	require.Equal(t, "18446744073709551615", unsigned.MaxValue().String())

	newsigned, err := signed.Add(10)
	require.NoError(t, err)
	newunsigned, err := unsigned.Add(10)
	require.NoError(t, err)
	require.Equal(t, "11", newsigned.String())
	require.Equal(t, "11", newunsigned.String())

	// Helpers that splat the (bool, error) tuple from a comparison into a
	// single assertion, so call sites read like the original boolean form.
	requireTrue := func(got bool, err error) {
		t.Helper()
		require.NoError(t, err)
		require.True(t, got)
	}
	requireFalse := func(got bool, err error) {
		t.Helper()
		require.NoError(t, err)
		require.False(t, got)
	}
	requireTrue(newsigned.GreaterThanOrEqual(signed))
	requireTrue(newunsigned.GreaterThanOrEqual(unsigned))
	requireTrue(newsigned.GreaterThan(signed))
	requireTrue(newunsigned.GreaterThan(unsigned))
	requireTrue(signed.LessThan(newsigned))
	requireTrue(unsigned.LessThan(newunsigned))
	requireTrue(signed.LessThanOrEqual(newsigned))
	requireTrue(unsigned.LessThanOrEqual(newunsigned))

	// Test that add operations do not overflow. i.e.
	// We initialize the values to max-10 of the range, but then add 100 to each.
	// The add operation truncates: so both should equal the maxValue exactly.
	overflowSigned, err := NewDatum(uint64(math.MaxInt64)-10, signedType) // wrong type, converts.
	require.NoError(t, err)
	overflowUnsigned, err := NewDatum(uint64(math.MaxUint64)-10, unsignedType)
	require.NoError(t, err)
	overflowSignedResult, err := overflowSigned.Add(100)
	require.NoError(t, err)
	require.Equal(t, strconv.Itoa(math.MaxInt64), overflowSignedResult.String())
	overflowUnsignedResult, err := overflowUnsigned.Add(100)
	require.NoError(t, err)
	require.Equal(t, "18446744073709551615", overflowUnsignedResult.String())

	// Test unsigned with signed input
	unsigned, err = NewDatum(int(1), unsignedType)
	require.NoError(t, err)
	require.Equal(t, "1", unsigned.String())

	// Test binary type. Binary datums always render as 0x-hex literals —
	// even valid-UTF-8 values — so that checkpoint JSON round-trips through
	// datumValFromString (which hex-decodes any binaryType value with a
	// "0x" prefix).
	binary, err := NewDatum("0", binaryType)
	require.NoError(t, err)
	require.Equal(t, `0x30`, binary.String())

	// Test string comparisons (VARCHAR/TEXT)
	str1, err := NewDatumFromValue("apple", "VARCHAR(255)")
	require.NoError(t, err)
	str2, err := NewDatumFromValue("banana", "VARCHAR(255)")
	require.NoError(t, err)
	requireTrue(str2.GreaterThan(str1))        // "banana" > "apple"
	requireTrue(str2.GreaterThanOrEqual(str1)) // "banana" >= "apple"
	requireTrue(str1.LessThan(str2))           // "apple" < "banana"
	requireTrue(str1.LessThanOrEqual(str2))    // "apple" <= "banana"
	str3, err := NewDatumFromValue("apple", "VARCHAR(255)")
	require.NoError(t, err)
	requireTrue(str1.GreaterThanOrEqual(str3)) // equal values
	requireTrue(str1.LessThanOrEqual(str3))    // equal values
	requireFalse(str1.GreaterThan(str3))       // equal values
	requireFalse(str1.LessThan(str3))          // equal values

	// Test temporal comparisons (DATETIME)
	datetime1, err := NewDatumFromValue("2024-01-01 10:00:00", "DATETIME")
	require.NoError(t, err)
	datetime2, err := NewDatumFromValue("2024-01-02 10:00:00", "DATETIME")
	require.NoError(t, err)
	requireTrue(datetime2.GreaterThan(datetime1))
	requireTrue(datetime1.LessThan(datetime2))

	// Comparing different types returns an error rather than panicking,
	// so a single malformed event during a long migration is recoverable.
	_, err = signed.GreaterThan(unsigned)
	require.Error(t, err)
	_, err = signed.LessThan(str1)
	require.Error(t, err)
}

func TestDatumInt32ToUnsigned(t *testing.T) {
	// Test that int32 values are correctly converted to unsigned uint64
	// This simulates MySQL binlog sending unsigned INT columns as signed int32

	// Positive int32 value
	positiveInt32 := int32(123456)
	d1, err := NewDatum(positiveInt32, unsignedType)
	require.NoError(t, err)
	require.Equal(t, uint64(123456), d1.Val)
	require.Equal(t, "123456", d1.String())

	// Negative int32 value (large unsigned value)
	// -840443956 as int32 = 3454523340 as uint32
	negativeInt32 := int32(-840443956)
	d2, err := NewDatum(negativeInt32, unsignedType)
	require.NoError(t, err)
	expectedUint32 := uint32(negativeInt32) // Reinterpret bits as unsigned
	require.Equal(t, uint64(expectedUint32), d2.Val)
	require.Equal(t, uint64(3454523340), d2.Val)
	require.Equal(t, "3454523340", d2.String())

	// Edge case: int32 max value
	maxInt32 := int32(math.MaxInt32)
	d3, err := NewDatum(maxInt32, unsignedType)
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxInt32), d3.Val)
	require.Equal(t, "2147483647", d3.String())

	// Edge case: int32 min value (becomes max uint32 range)
	minInt32 := int32(math.MinInt32)
	d4, err := NewDatum(minInt32, unsignedType)
	require.NoError(t, err)
	expectedUint32Min := uint32(minInt32)
	require.Equal(t, uint64(expectedUint32Min), d4.Val)
	require.Equal(t, uint64(2147483648), d4.Val)
	require.Equal(t, "2147483648", d4.String())

	// Test uint32 values pass through correctly
	positiveUint32 := uint32(3454523340)
	d5, err := NewDatum(positiveUint32, unsignedType)
	require.NoError(t, err)
	require.Equal(t, uint64(3454523340), d5.Val)
	require.Equal(t, "3454523340", d5.String())
}

func TestDatumInt64ToUnsigned(t *testing.T) {
	// Test that int64 values are correctly converted to unsigned uint64
	// This simulates MySQL binlog sending unsigned BIGINT columns as signed int64

	// Positive int64 value
	positiveInt64 := int64(123456789012345)
	d1, err := NewDatum(positiveInt64, unsignedType)
	require.NoError(t, err)
	require.Equal(t, uint64(123456789012345), d1.Val)
	require.Equal(t, "123456789012345", d1.String())

	// Negative int64 value (large unsigned value)
	// -1 as int64 = max uint64
	negativeInt64 := int64(-1)
	d2, err := NewDatum(negativeInt64, unsignedType)
	require.NoError(t, err)
	require.Equal(t, uint64(negativeInt64), d2.Val)
	require.Equal(t, uint64(math.MaxUint64), d2.Val)

	// Edge case: int64 max value
	maxInt64 := int64(math.MaxInt64)
	d3, err := NewDatum(maxInt64, unsignedType)
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxInt64), d3.Val)
}

func TestKeyBelowLowWatermarkWithNegativeInt32(t *testing.T) {
	ti := &TableInfo{
		SchemaName:        "test",
		TableName:         "t1",
		QuotedTableName:   "`t1`",
		KeyColumns:        []string{"id"},
		keyColumnsMySQLTp: []string{"int unsigned"},
		KeyIsAutoInc:      true,
		minValue:          Datum{Val: uint64(0), Tp: unsignedType},
		maxValue:          Datum{Val: uint64(1000), Tp: unsignedType},
	}

	// Create the optimistic chunker directly
	chk := &chunkerOptimistic{
		Ti:                ti,
		dynamicChunkSizer: dynamicChunkSizer{ChunkerTarget: 100 * time.Millisecond},
		watermarkTracker: watermarkTracker{
			watermark: &Chunk{
				UpperBound: &Boundary{Value: []Datum{{Val: uint64(100), Tp: unsignedType}}},
				LowerBound: &Boundary{Value: []Datum{{Val: uint64(0), Tp: unsignedType}}},
			},
		},
		logger: slog.Default(),
		// We need chunkPtr to have a type for NewDatum call
		chunkPtr: Datum{Val: uint64(0), Tp: unsignedType},
	}

	// KeyBelowLowWatermark receives the original typed value
	// Value arrives as int32(-12345) (representing a large unsigned int)
	// 4294954951 as uint32 is -12345 as int32
	originalVal := int32(-12345)

	// KeyBelowLowWatermark should correctly handle int32 values for unsigned columns
	// The int32 will be reinterpreted as uint32, giving us 4294954951
	// Since 4294954951 > 100 (watermark), it should return false
	require.NotPanics(t, func() {
		result := chk.KeyBelowLowWatermark(originalVal)
		require.False(t, result, "4294954951 should not be below watermark of 100")
	}, "KeyBelowLowWatermark should handle int32 values for unsigned columns")
}

// TestNewDatumFromValue tests that NewDatumFromValue properly handles various MySQL types
// This ensures compatibility with the old EscapeMySQLType function
func TestNewDatumFromValue(t *testing.T) {
	// Test NULL values
	d, err := NewDatumFromValue(nil, "INT")
	require.NoError(t, err)
	require.Equal(t, "NULL", d.String())
	d, err = NewDatumFromValue(nil, "VARCHAR(255)")
	require.NoError(t, err)
	require.Equal(t, "NULL", d.String())
	d, err = NewDatumFromValue(nil, "JSON")
	require.NoError(t, err)
	require.Equal(t, "NULL", d.String())

	// Test integer types
	intBytes := []byte("123")
	d, err = NewDatumFromValue(intBytes, "INT")
	require.NoError(t, err)
	require.Equal(t, "123", d.String())
	d, err = NewDatumFromValue(intBytes, "BIGINT")
	require.NoError(t, err)
	require.Equal(t, "123", d.String())
	d, err = NewDatumFromValue(456, "INT")
	require.NoError(t, err)
	require.Equal(t, "456", d.String())
	d, err = NewDatumFromValue(intBytes, "INT(11)")
	require.NoError(t, err)
	require.Equal(t, "123", d.String())

	// Test VARCHAR/TEXT types
	textBytes := []byte("hello world")
	d, err = NewDatumFromValue(textBytes, "VARCHAR(255)")
	require.NoError(t, err)
	require.Equal(t, "\"hello world\"", d.String())
	d, err = NewDatumFromValue("hello world", "TEXT")
	require.NoError(t, err)
	require.Equal(t, "\"hello world\"", d.String())
	d, err = NewDatumFromValue(textBytes, "CHAR(50)")
	require.NoError(t, err)
	require.Equal(t, "\"hello world\"", d.String())

	// Test with quotes that need escaping
	textWithQuotes := []byte("hello 'world'")
	d, err = NewDatumFromValue(textWithQuotes, "VARCHAR(255)")
	require.NoError(t, err)
	require.Equal(t, "\"hello \\'world\\'\"", d.String())

	// Test DATETIME/TIMESTAMP types
	timeBytes := []byte("2023-01-01 12:00:00")
	d, err = NewDatumFromValue(timeBytes, "DATETIME")
	require.NoError(t, err)
	require.Equal(t, "\"2023-01-01 12:00:00\"", d.String())
	d, err = NewDatumFromValue(timeBytes, "TIMESTAMP")
	require.NoError(t, err)
	require.Equal(t, "\"2023-01-01 12:00:00\"", d.String())

	// Test float types
	floatBytes := []byte("123.45")
	d, err = NewDatumFromValue(floatBytes, "FLOAT")
	require.NoError(t, err)
	require.Equal(t, "\"123.45\"", d.String())
	d, err = NewDatumFromValue(floatBytes, "DOUBLE")
	require.NoError(t, err)
	require.Equal(t, "\"123.45\"", d.String())

	// Test VARBINARY/BLOB types - should use hex encoding
	binaryData := []byte{0x01, 0x02, 0x03}
	d, err = NewDatumFromValue(binaryData, "VARBINARY(255)")
	require.NoError(t, err)
	require.Equal(t, "0x010203", d.String())

	// Test BLOB types
	blobData := []byte{0xFF, 0xFE, 0xFD}
	d, err = NewDatumFromValue(blobData, "BLOB")
	require.NoError(t, err)
	require.Equal(t, "0xfffefd", d.String())

	// A one-byte NUL serializes as its hex value.
	d, err = NewDatumFromValue([]byte{0x00}, "MEDIUMBLOB")
	require.NoError(t, err)
	require.Equal(t, "0x00", d.String())
	// Empty binary values must NOT serialize as "0x" (MySQL parses that as
	// an identifier, not a hex literal) — and must NOT serialize as "0x00"
	// either: a one-byte NUL is a different value, and emitting it here
	// silently corrupted empty blobs applied via the binlog feed.
	d, err = NewDatumFromValue([]byte{}, "VARBINARY(255)")
	require.NoError(t, err)
	require.Equal(t, "x''", d.String())
	d, err = NewDatumFromValue("", "BLOB")
	require.NoError(t, err)
	require.Equal(t, "x''", d.String())
	d, err = NewDatumFromValue(nil, "BLOB")
	require.NoError(t, err)
	require.Equal(t, "NULL", d.String())

	// Test JSON types - should be quoted like text
	jsonBytes := []byte(`[1, 2, 3]`)
	d, err = NewDatumFromValue(jsonBytes, "JSON")
	require.NoError(t, err)
	require.Equal(t, "\"[1, 2, 3]\"", d.String())
	d, err = NewDatumFromValue("test", "json")
	require.NoError(t, err)
	require.Equal(t, "\"test\"", d.String())

	// Test case insensitivity
	d, err = NewDatumFromValue(intBytes, "int")
	require.NoError(t, err)
	require.Equal(t, "123", d.String())
	d, err = NewDatumFromValue(intBytes, "Int")
	require.NoError(t, err)
	require.Equal(t, "123", d.String())
	d, err = NewDatumFromValue([]byte("hello"), "varchar(100)")
	require.NoError(t, err)
	require.Equal(t, "\"hello\"", d.String())

	// Test unknown/default types
	unknownBytes := []byte("unknown data")
	d, err = NewDatumFromValue(unknownBytes, "UNKNOWN_TYPE")
	require.NoError(t, err)
	require.Equal(t, "\"unknown data\"", d.String())
	d, err = NewDatumFromValue("test", "CUSTOM_TYPE")
	require.NoError(t, err)
	require.Equal(t, "\"test\"", d.String())

	// BIT(N) values: emitted as bare numeric literals so MySQL coerces them
	// to bit patterns, not as quoted strings (which MySQL would interpret
	// byte-by-byte as bits). The Go MySQL driver returns BIT as a raw
	// big-endian byte slice; the binlog reader returns int64. Both must
	// land on the same numeric literal.
	//
	// Driver path: []byte → uint64 (big-endian).
	d, err = NewDatumFromValue([]byte{0x01}, "BIT(1)")
	require.NoError(t, err)
	require.Equal(t, "1", d.String())
	d, err = NewDatumFromValue([]byte{0x35}, "BIT(8)")
	require.NoError(t, err)
	require.Equal(t, "53", d.String())
	d, err = NewDatumFromValue([]byte{0x01, 0x02}, "BIT(16)")
	require.NoError(t, err)
	require.Equal(t, "258", d.String())
	// BIT(64) with top bit set: bytes round-trip through uint64 without
	// the sign-extension that an int64 path would introduce.
	d, err = NewDatumFromValue(
		[]byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, "BIT(64)")
	require.NoError(t, err)
	require.Equal(t, "9223372036854775808", d.String())

	// Binlog path: int64 from go-mysql's decodeBit. Positive values are
	// trivial; negative int64 (BIT(64) high bit set) must reinterpret bits
	// as uint64 to round-trip correctly.
	d, err = NewDatumFromValue(int64(5), "BIT(8)")
	require.NoError(t, err)
	require.Equal(t, "5", d.String())
	d, err = NewDatumFromValue(int64(-1), "BIT(64)")
	require.NoError(t, err)
	require.Equal(t, "18446744073709551615", d.String())

	// NULL BIT survives.
	d, err = NewDatumFromValue(nil, "BIT(8)")
	require.NoError(t, err)
	require.Equal(t, "NULL", d.String())

	// Case-insensitive on the type spelling.
	d, err = NewDatumFromValue([]byte{0x07}, "bit(3)")
	require.NoError(t, err)
	require.Equal(t, "7", d.String())
}

// TestNewDatumFromValueBinaryString tests that binary strings are hex-encoded
func TestNewDatumFromValueBinaryString(t *testing.T) {
	// Test binary data that's not valid UTF-8
	binaryData := []byte{0x00, 0x01, 0x02, 0xFF}
	d, err := NewDatumFromValue(binaryData, "VARBINARY(255)")
	require.NoError(t, err)
	require.Equal(t, "0x000102ff", d.String())

	// Test string that starts with 0x - for VARCHAR (unknownType), this is just a normal string.
	// It should NOT be hex-encoded because datumValFromString only hex-decodes for binaryType,
	// so the JSON checkpoint round-trip is safe without encoding.
	jsonLikeString := "0x123"
	d, err = NewDatumFromValue(jsonLikeString, "VARCHAR(255)")
	require.NoError(t, err)
	require.Equal(t, "\"0x123\"", d.String())

	// Test normal UTF-8 string
	normalString := "hello"
	d, err = NewDatumFromValue(normalString, "VARCHAR(255)")
	require.NoError(t, err)
	require.Equal(t, "\"hello\"", d.String())
}

// TestNewDatumFromValueWithType pins that resolving the column type once
// (NewColumnType) and reusing it via NewDatumFromValueWithType produces a
// Datum identical to NewDatumFromValue, across the type classifications
// and wire forms that matter: numeric, binary (forceHexEncode), text, the
// BIT []byte decode path, and nil. This is the fast path used by the
// applier's DELETE builder (block/spirit#948); it must not diverge from
// the per-value parse it replaces.
func TestNewDatumFromValueWithType(t *testing.T) {
	cases := []struct {
		name    string
		value   any
		mysqlTp string
	}{
		{"signed int64", int64(42), "int"},
		{"signed from string", "42", "bigint"},
		{"binary []byte non-utf8", []byte{0xbb, 0x00, 0x10}, "binary(3)"},
		{"binary string utf8", "abc", "varbinary(8)"},
		{"varchar", "hello", "varchar(255)"},
		{"bit from bytes", []byte{0x01, 0x00}, "bit(16)"},
		{"nil binary", nil, "binary(8)"},
		{"nil int", nil, "int"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want, errWant := NewDatumFromValue(tc.value, tc.mysqlTp)
			got, errGot := NewDatumFromValueWithType(tc.value, NewColumnType(tc.mysqlTp))
			require.Equal(t, errWant, errGot)
			require.Equal(t, want, got)
			require.Equal(t, want.String(), got.String())
		})
	}
}

// TestDatumCompareTypeMismatch covers the checked type-assertion error paths in
// (Datum).compare: a Datum whose Tp claims signed/unsigned but whose Val holds
// the wrong concrete type must yield a clean error rather than panic. This can
// only happen for a hand-built Datum (NewDatum always normalizes Val), but the
// comparison wrappers are reachable from the chunker so the error path matters.
func TestDatumCompareTypeMismatch(t *testing.T) {
	// signedType but Val is a string, not int64.
	badSigned := Datum{Val: "not-an-int", Tp: signedType}
	goodSigned := Datum{Val: int64(1), Tp: signedType}

	_, err := badSigned.GreaterThan(goodSigned)
	require.ErrorContains(t, err, "expected int64")
	// Mismatch on the second operand is detected too.
	_, err = goodSigned.LessThan(badSigned)
	require.ErrorContains(t, err, "expected int64")

	// unsignedType but Val is a string, not uint64.
	badUnsigned := Datum{Val: "not-a-uint", Tp: unsignedType}
	goodUnsigned := Datum{Val: uint64(1), Tp: unsignedType}

	_, err = badUnsigned.GreaterThanOrEqual(goodUnsigned)
	require.ErrorContains(t, err, "expected uint64")
	_, err = goodUnsigned.LessThanOrEqual(badUnsigned)
	require.ErrorContains(t, err, "expected uint64")

	// Sanity: well-formed datums of the same type still compare without error.
	gt, err := goodSigned.GreaterThan(Datum{Val: int64(0), Tp: signedType})
	require.NoError(t, err)
	require.True(t, gt)
}

// TestEmptyBinaryDatumRoundTrip is a regression test for empty (zero-length,
// non-NULL) binary values. These used to serialize as 0x00 — a one-byte NUL,
// a *different* value — so every binlog-applied REPLACE rewrote empty blobs
// as \x00 in the target. On tables where the application stores empty strings
// (e.g. zero-length serialized protos), the checksum then flagged every
// feed-touched row, recopied it (which fixed it, byte-exact), and the next
// application update re-minted the difference: a checksum that never
// converges, plus real corruption for any row not re-verified after its last
// update. The empty value must round-trip through both serialization paths:
// Datum.String() (SQL literals for the applier and WHERE clauses) and
// jsonQuoteDatum/datumValFromString (checkpoint watermarks).
func TestEmptyBinaryDatumRoundTrip(t *testing.T) {
	empty, err := NewDatumFromValue([]byte{}, "BLOB")
	require.NoError(t, err)

	// SQL literal path: x'' is MySQL's zero-length binary literal.
	require.Equal(t, "x''", empty.String())

	// Checkpoint JSON path: emit, then decode back to the empty string.
	require.Equal(t, `"x''"`, jsonQuoteDatum(empty))
	val, err := datumValFromString("x''", binaryType)
	require.NoError(t, err)
	decoded, ok := val.(string)
	require.True(t, ok, "decoded empty binary should be a string, got %T", val)
	require.Empty(t, decoded)

	// A one-byte NUL is a distinct value and must stay 0x00 on both paths.
	nul, err := NewDatumFromValue([]byte{0x00}, "BLOB")
	require.NoError(t, err)
	require.Equal(t, "0x00", nul.String())
	require.Equal(t, `"0x00"`, jsonQuoteDatum(nul))
	val, err = datumValFromString("0x00", binaryType)
	require.NoError(t, err)
	require.Equal(t, "\x00", val)

	// NULL is also distinct from empty.
	require.Equal(t, "NULL", NewNilDatum(binaryType).String())
}

// TestVectorDatumIsBinary covers the VECTOR type (MySQL 9.7+). A VECTOR value
// arrives from both the driver and the binlog as raw bytes — a packed array of
// 4-byte little-endian floats — and MySQL will only take it back as a
// binary-charset literal: a character-set string of the correct byte length is
// rejected with "Value of type 'string, size: 12' cannot be converted to
// 'vector' type".
//
// So the type must classify as binaryType rather than fall through to the
// unknownType path, whose IsBinaryString() heuristic only hex-encodes values
// that are *not* valid UTF-8. The all-zeros vector [0,0,0] is the counterexample
// that broke the copier and the binlog applier: 12 NUL bytes are perfectly
// valid UTF-8, so it was emitted as a quoted string and the write failed.
func TestVectorDatumIsBinary(t *testing.T) {
	require.Equal(t, binaryType, mySQLTypeToDatumTp("vector"))
	require.Equal(t, binaryType, mySQLTypeToDatumTp("vector(3)"))
	require.Equal(t, binaryType, mySQLTypeToDatumTp("VECTOR(2048)"))

	// [0,0,0]: valid UTF-8, and must still be emitted as a hex literal.
	zeros, err := NewDatumFromValue(make([]byte, 12), "vector(3)")
	require.NoError(t, err)
	require.True(t, zeros.IsBinaryString())
	require.Equal(t, "0x000000000000000000000000", zeros.String())

	// [1.5,-2,0.0003]: high bytes, not valid UTF-8, same literal form.
	v, err := NewDatumFromValue([]byte{
		0x00, 0x00, 0xc0, 0x3f,
		0x00, 0x00, 0x00, 0xc0,
		0x52, 0x49, 0x9d, 0x39,
	}, "vector(3)")
	require.NoError(t, err)
	require.Equal(t, "0x0000c03f000000c052499d39", v.String())

	// NULL round-trips as NULL, not as an empty vector.
	require.Equal(t, "NULL", NewNilDatum(mySQLTypeToDatumTp("vector(3)")).String())
}

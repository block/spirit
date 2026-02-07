package table

import (
	"log/slog"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDatum(t *testing.T) {
	signed, err := NewDatum(1, signedType)
	assert.NoError(t, err)
	unsigned, err := NewDatum(uint(1), unsignedType)
	assert.NoError(t, err)

	assert.Equal(t, "1", signed.String())
	assert.Equal(t, "1", unsigned.String())

	assert.Equal(t, strconv.Itoa(math.MinInt64), signed.MinValue().String())
	assert.Equal(t, strconv.Itoa(math.MaxInt64), signed.MaxValue().String())
	assert.Equal(t, "0", unsigned.MinValue().String())
	assert.Equal(t, "18446744073709551615", unsigned.MaxValue().String())

	newsigned := signed.Add(10)
	newunsigned := unsigned.Add(10)
	assert.Equal(t, "11", newsigned.String())
	assert.Equal(t, "11", newunsigned.String())

	assert.True(t, newsigned.GreaterThanOrEqual(signed))
	assert.True(t, newunsigned.GreaterThanOrEqual(unsigned))
	assert.True(t, newsigned.GreaterThan(signed))
	assert.True(t, newunsigned.GreaterThan(unsigned))
	assert.True(t, signed.LessThan(newsigned))
	assert.True(t, unsigned.LessThan(newunsigned))
	assert.True(t, signed.LessThanOrEqual(newsigned))
	assert.True(t, unsigned.LessThanOrEqual(newunsigned))

	// Test that add operations do not overflow. i.e.
	// We initialize the values to max-10 of the range, but then add 100 to each.
	// The add operation truncates: so both should equal the maxValue exactly.
	overflowSigned, err := NewDatum(uint64(math.MaxInt64)-10, signedType) // wrong type, converts.
	assert.NoError(t, err)
	overflowUnsigned, err := NewDatum(uint64(math.MaxUint64)-10, unsignedType)
	assert.NoError(t, err)
	assert.Equal(t, strconv.Itoa(math.MaxInt64), overflowSigned.Add(100).String())
	assert.Equal(t, "18446744073709551615", overflowUnsigned.Add(100).String())

	// Test unsigned with signed input
	unsigned, err = NewDatum(int(1), unsignedType)
	assert.NoError(t, err)
	assert.Equal(t, "1", unsigned.String())

	// Test binary type.
	binary, err := NewDatum("0", binaryType)
	assert.NoError(t, err)
	assert.Equal(t, `"0"`, binary.String())

	// Test string comparisons (VARCHAR/TEXT)
	str1, err := NewDatumFromValue("apple", "VARCHAR(255)")
	assert.NoError(t, err)
	str2, err := NewDatumFromValue("banana", "VARCHAR(255)")
	assert.NoError(t, err)
	assert.True(t, str2.GreaterThan(str1))        // "banana" > "apple"
	assert.True(t, str2.GreaterThanOrEqual(str1)) // "banana" >= "apple"
	assert.True(t, str1.LessThan(str2))           // "apple" < "banana"
	assert.True(t, str1.LessThanOrEqual(str2))    // "apple" <= "banana"
	str3, err := NewDatumFromValue("apple", "VARCHAR(255)")
	assert.NoError(t, err)
	assert.True(t, str1.GreaterThanOrEqual(str3)) // equal values
	assert.True(t, str1.LessThanOrEqual(str3))    // equal values
	assert.False(t, str1.GreaterThan(str3))       // equal values
	assert.False(t, str1.LessThan(str3))          // equal values

	// Test temporal comparisons (DATETIME)
	datetime1, err := NewDatumFromValue("2024-01-01 10:00:00", "DATETIME")
	assert.NoError(t, err)
	datetime2, err := NewDatumFromValue("2024-01-02 10:00:00", "DATETIME")
	assert.NoError(t, err)
	assert.True(t, datetime2.GreaterThan(datetime1))
	assert.True(t, datetime1.LessThan(datetime2))

	// Test that comparing different types panics
	assert.Panics(t, func() {
		signed.GreaterThan(unsigned)
	})
	assert.Panics(t, func() {
		signed.LessThan(str1)
	})
}

func TestDatumInt32ToUnsigned(t *testing.T) {
	// Test that int32 values are correctly converted to unsigned uint64
	// This simulates MySQL binlog sending unsigned INT columns as signed int32

	// Positive int32 value
	positiveInt32 := int32(123456)
	d1, err := NewDatum(positiveInt32, unsignedType)
	assert.NoError(t, err)
	assert.Equal(t, uint64(123456), d1.Val)
	assert.Equal(t, "123456", d1.String())

	// Negative int32 value (large unsigned value)
	// -840443956 as int32 = 3454523340 as uint32
	negativeInt32 := int32(-840443956)
	d2, err := NewDatum(negativeInt32, unsignedType)
	assert.NoError(t, err)
	expectedUint32 := uint32(negativeInt32) // Reinterpret bits as unsigned
	assert.Equal(t, uint64(expectedUint32), d2.Val)
	assert.Equal(t, uint64(3454523340), d2.Val)
	assert.Equal(t, "3454523340", d2.String())

	// Edge case: int32 max value
	maxInt32 := int32(math.MaxInt32)
	d3, err := NewDatum(maxInt32, unsignedType)
	assert.NoError(t, err)
	assert.Equal(t, uint64(math.MaxInt32), d3.Val)
	assert.Equal(t, "2147483647", d3.String())

	// Edge case: int32 min value (becomes max uint32 range)
	minInt32 := int32(math.MinInt32)
	d4, err := NewDatum(minInt32, unsignedType)
	assert.NoError(t, err)
	expectedUint32Min := uint32(minInt32)
	assert.Equal(t, uint64(expectedUint32Min), d4.Val)
	assert.Equal(t, uint64(2147483648), d4.Val)
	assert.Equal(t, "2147483648", d4.String())

	// Test uint32 values pass through correctly
	positiveUint32 := uint32(3454523340)
	d5, err := NewDatum(positiveUint32, unsignedType)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3454523340), d5.Val)
	assert.Equal(t, "3454523340", d5.String())
}

func TestDatumInt64ToUnsigned(t *testing.T) {
	// Test that int64 values are correctly converted to unsigned uint64
	// This simulates MySQL binlog sending unsigned BIGINT columns as signed int64

	// Positive int64 value
	positiveInt64 := int64(123456789012345)
	d1, err := NewDatum(positiveInt64, unsignedType)
	assert.NoError(t, err)
	assert.Equal(t, uint64(123456789012345), d1.Val)
	assert.Equal(t, "123456789012345", d1.String())

	// Negative int64 value (large unsigned value)
	// -1 as int64 = max uint64
	negativeInt64 := int64(-1)
	d2, err := NewDatum(negativeInt64, unsignedType)
	assert.NoError(t, err)
	assert.Equal(t, uint64(negativeInt64), d2.Val)
	assert.Equal(t, uint64(math.MaxUint64), d2.Val)

	// Edge case: int64 max value
	maxInt64 := int64(math.MaxInt64)
	d3, err := NewDatum(maxInt64, unsignedType)
	assert.NoError(t, err)
	assert.Equal(t, uint64(math.MaxInt64), d3.Val)
}

func TestKeyBelowLowWatermarkWithNegativeInt32(t *testing.T) {
	ti := &TableInfo{
		SchemaName:        "test",
		TableName:         "t1",
		QuotedName:        "`test`.`t1`",
		KeyColumns:        []string{"id"},
		keyColumnsMySQLTp: []string{"int unsigned"},
		KeyIsAutoInc:      true,
		minValue:          Datum{Val: uint64(0), Tp: unsignedType},
		maxValue:          Datum{Val: uint64(1000), Tp: unsignedType},
	}

	// Create the optimistic chunker directly
	chk := &chunkerOptimistic{
		Ti:            ti,
		ChunkerTarget: 100 * time.Millisecond,
		logger:        slog.Default(),
		watermark: &Chunk{
			UpperBound: &Boundary{Value: []Datum{{Val: uint64(100), Tp: unsignedType}}},
			LowerBound: &Boundary{Value: []Datum{{Val: uint64(0), Tp: unsignedType}}},
		},
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
	assert.NotPanics(t, func() {
		result := chk.KeyBelowLowWatermark(originalVal)
		assert.False(t, result, "4294954951 should not be below watermark of 100")
	}, "KeyBelowLowWatermark should handle int32 values for unsigned columns")
}

// TestNewDatumFromValue tests that NewDatumFromValue properly handles various MySQL types
// This ensures compatibility with the old EscapeMySQLType function
func TestNewDatumFromValue(t *testing.T) {
	// Test NULL values
	d, err := NewDatumFromValue(nil, "INT")
	assert.NoError(t, err)
	assert.Equal(t, "NULL", d.String())
	d, err = NewDatumFromValue(nil, "VARCHAR(255)")
	assert.NoError(t, err)
	assert.Equal(t, "NULL", d.String())
	d, err = NewDatumFromValue(nil, "JSON")
	assert.NoError(t, err)
	assert.Equal(t, "NULL", d.String())

	// Test integer types
	intBytes := []byte("123")
	d, err = NewDatumFromValue(intBytes, "INT")
	assert.NoError(t, err)
	assert.Equal(t, "123", d.String())
	d, err = NewDatumFromValue(intBytes, "BIGINT")
	assert.NoError(t, err)
	assert.Equal(t, "123", d.String())
	d, err = NewDatumFromValue(456, "INT")
	assert.NoError(t, err)
	assert.Equal(t, "456", d.String())
	d, err = NewDatumFromValue(intBytes, "INT(11)")
	assert.NoError(t, err)
	assert.Equal(t, "123", d.String())

	// Test VARCHAR/TEXT types
	textBytes := []byte("hello world")
	d, err = NewDatumFromValue(textBytes, "VARCHAR(255)")
	assert.NoError(t, err)
	assert.Equal(t, "\"hello world\"", d.String())
	d, err = NewDatumFromValue("hello world", "TEXT")
	assert.NoError(t, err)
	assert.Equal(t, "\"hello world\"", d.String())
	d, err = NewDatumFromValue(textBytes, "CHAR(50)")
	assert.NoError(t, err)
	assert.Equal(t, "\"hello world\"", d.String())

	// Test with quotes that need escaping
	textWithQuotes := []byte("hello 'world'")
	d, err = NewDatumFromValue(textWithQuotes, "VARCHAR(255)")
	assert.NoError(t, err)
	assert.Equal(t, "\"hello \\'world\\'\"", d.String())

	// Test DATETIME/TIMESTAMP types
	timeBytes := []byte("2023-01-01 12:00:00")
	d, err = NewDatumFromValue(timeBytes, "DATETIME")
	assert.NoError(t, err)
	assert.Equal(t, "\"2023-01-01 12:00:00\"", d.String())
	d, err = NewDatumFromValue(timeBytes, "TIMESTAMP")
	assert.NoError(t, err)
	assert.Equal(t, "\"2023-01-01 12:00:00\"", d.String())

	// Test float types
	floatBytes := []byte("123.45")
	d, err = NewDatumFromValue(floatBytes, "FLOAT")
	assert.NoError(t, err)
	assert.Equal(t, "\"123.45\"", d.String())
	d, err = NewDatumFromValue(floatBytes, "DOUBLE")
	assert.NoError(t, err)
	assert.Equal(t, "\"123.45\"", d.String())

	// Test VARBINARY/BLOB types - should use hex encoding
	binaryData := []byte{0x01, 0x02, 0x03}
	d, err = NewDatumFromValue(binaryData, "VARBINARY(255)")
	assert.NoError(t, err)
	assert.Equal(t, "0x010203", d.String())

	// Test BLOB types
	blobData := []byte{0xFF, 0xFE, 0xFD}
	d, err = NewDatumFromValue(blobData, "BLOB")
	assert.NoError(t, err)
	assert.Equal(t, "0xfffefd", d.String())

	// Test JSON types - should be quoted like text
	jsonBytes := []byte(`[1, 2, 3]`)
	d, err = NewDatumFromValue(jsonBytes, "JSON")
	assert.NoError(t, err)
	assert.Equal(t, "\"[1, 2, 3]\"", d.String())
	d, err = NewDatumFromValue("test", "json")
	assert.NoError(t, err)
	assert.Equal(t, "\"test\"", d.String())

	// Test case insensitivity
	d, err = NewDatumFromValue(intBytes, "int")
	assert.NoError(t, err)
	assert.Equal(t, "123", d.String())
	d, err = NewDatumFromValue(intBytes, "Int")
	assert.NoError(t, err)
	assert.Equal(t, "123", d.String())
	d, err = NewDatumFromValue([]byte("hello"), "varchar(100)")
	assert.NoError(t, err)
	assert.Equal(t, "\"hello\"", d.String())

	// Test unknown/default types
	unknownBytes := []byte("unknown data")
	d, err = NewDatumFromValue(unknownBytes, "UNKNOWN_TYPE")
	assert.NoError(t, err)
	assert.Equal(t, "\"unknown data\"", d.String())
	d, err = NewDatumFromValue("test", "CUSTOM_TYPE")
	assert.NoError(t, err)
	assert.Equal(t, "\"test\"", d.String())
}

// TestNewDatumFromValueBinaryString tests that binary strings are hex-encoded
func TestNewDatumFromValueBinaryString(t *testing.T) {
	// Test binary data that's not valid UTF-8
	binaryData := []byte{0x00, 0x01, 0x02, 0xFF}
	d, err := NewDatumFromValue(binaryData, "VARBINARY(255)")
	assert.NoError(t, err)
	assert.Equal(t, "0x000102ff", d.String())

	// Test string that starts with 0x (should be hex encoded to avoid JSON corruption)
	jsonLikeString := "0x123"
	d, err = NewDatumFromValue(jsonLikeString, "VARCHAR(255)")
	assert.NoError(t, err)
	// This should be hex encoded because it starts with 0x
	assert.Equal(t, "0x3078313233", d.String()) // hex of "0x123"

	// Test normal UTF-8 string
	normalString := "hello"
	d, err = NewDatumFromValue(normalString, "VARCHAR(255)")
	assert.NoError(t, err)
	assert.Equal(t, "\"hello\"", d.String())
}

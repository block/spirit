package table

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatum(t *testing.T) {
	signed := NewDatum(1, signedType)
	unsigned := NewDatum(uint(1), unsignedType)

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

	// Test that add operations do not overflow. i.e.
	// We initialize the values to max-10 of the range, but then add 100 to each.
	// The add operation truncates: so both should equal the maxValue exactly.
	overflowSigned := NewDatum(uint64(math.MaxInt64)-10, signedType) // wrong type, converts.
	overflowUnsigned := NewDatum(uint64(math.MaxUint64)-10, unsignedType)
	assert.Equal(t, strconv.Itoa(math.MaxInt64), overflowSigned.Add(100).String())
	assert.Equal(t, "18446744073709551615", overflowUnsigned.Add(100).String())

	// Test unsigned with signed input
	unsigned = NewDatum(int(1), unsignedType)
	assert.Equal(t, "1", unsigned.String())

	// Test binary type.
	binary := NewDatum("0", binaryType)
	assert.Equal(t, `"0"`, binary.String())
}

func TestDatumInt32ToUnsigned(t *testing.T) {
	// Test that int32 values are correctly converted to unsigned uint64
	// This simulates MySQL binlog sending unsigned INT columns as signed int32

	// Positive int32 value
	positiveInt32 := int32(123456)
	d1 := NewDatum(positiveInt32, unsignedType)
	assert.Equal(t, uint64(123456), d1.Val)
	assert.Equal(t, "123456", d1.String())

	// Negative int32 value (large unsigned value)
	// -840443956 as int32 = 3454523340 as uint32
	negativeInt32 := int32(-840443956)
	d2 := NewDatum(negativeInt32, unsignedType)
	expectedUint32 := uint32(negativeInt32) // Reinterpret bits as unsigned
	assert.Equal(t, uint64(expectedUint32), d2.Val)
	assert.Equal(t, uint64(3454523340), d2.Val)
	assert.Equal(t, "3454523340", d2.String())

	// Edge case: int32 max value
	maxInt32 := int32(math.MaxInt32)
	d3 := NewDatum(maxInt32, unsignedType)
	assert.Equal(t, uint64(math.MaxInt32), d3.Val)
	assert.Equal(t, "2147483647", d3.String())

	// Edge case: int32 min value (becomes max uint32 range)
	minInt32 := int32(math.MinInt32)
	d4 := NewDatum(minInt32, unsignedType)
	expectedUint32Min := uint32(minInt32)
	assert.Equal(t, uint64(expectedUint32Min), d4.Val)
	assert.Equal(t, uint64(2147483648), d4.Val)
	assert.Equal(t, "2147483648", d4.String())

	// Test uint32 values pass through correctly
	positiveUint32 := uint32(3454523340)
	d5 := NewDatum(positiveUint32, unsignedType)
	assert.Equal(t, uint64(3454523340), d5.Val)
	assert.Equal(t, "3454523340", d5.String())
}

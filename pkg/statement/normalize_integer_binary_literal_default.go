package statement

import (
	"encoding/binary"
	"math"
	"strconv"
	"strings"
)

func init() { registerNormalizer(integerBinaryLiteralDefaultNormalizer{}) }

// integerBinaryLiteralDefaultNormalizer rewrites a hex or bit literal DEFAULT
// on an integer or unscaled decimal column to the decimal integer MySQL
// stores. MySQL reads the
// literal's bytes as a big-endian unsigned integer and SHOW CREATE TABLE
// reports the result as a number, so `b int DEFAULT 0x1A` comes back as
// `b int DEFAULT '26'`. Without the rule the literal diffs against the live
// column and emits a MODIFY COLUMN that MySQL stores as the number again, so
// the next diff emits it again. Verified against MySQL 8.0.43:
//
//	int DEFAULT 0x1A                              -> '26'
//	int DEFAULT b'1010'                           -> '10'
//	int DEFAULT b'0'                              -> '0'
//	int unsigned DEFAULT 0xFFFFFFFF               -> '4294967295'
//	bigint unsigned DEFAULT 0xFFFFFFFFFFFFFFFF    -> '18446744073709551615'
//	int DEFAULT 0xFFFFFFFF                        -> error 1067 (out of range)
//	int DEFAULT x'' / b''                         -> error 1067
//	bigint unsigned DEFAULT 0x00FFFFFFFFFFFFFFFF  -> error 1067 (9 bytes, even with a leading zero)
//	int DEFAULT (0x1A)                            -> (0x1a)  (an expression is stored as written)
//	decimal(5) DEFAULT 0x1A                       -> '26'
//	decimal(3,0) DEFAULT 0x3E8                    -> error 1067 (more digits than the precision)
//	decimal(20,0) DEFAULT 0x8000000000000000      -> error 1067 (2^63 and above)
//
// The value is recorded as a [DefaultKindNumber], which is emitted bare.
// Quotedness is not part of column identity on a numeric column (see
// [columnsEqual]), so it compares equal to the live '26'.
//
// A value out of range for the column's type is converted anyway. MySQL
// rejects it in either spelling, so the MODIFY fails the same way it would
// have with the literal. An empty literal and one longer than 8 bytes are left
// alone, because they have no integer value (see [binaryLiteralValue]).
//
// An unscaled decimal stores the plain integer too (decimal(5) DEFAULT 0x1A
// stores '26'), but only below 2^63: decimal(20,0) DEFAULT 0x8000000000000000
// is error 1067, though the same value written in decimal is accepted. A
// literal at or above 2^63 is left alone there, so the rule never turns DDL
// MySQL rejects into DDL it accepts.
//
// Left alone otherwise: a scaled decimal pads to its scale (decimal(5,2)
// DEFAULT 0x1A stores '26.00') and year puts the value through its own
// interpretation (year DEFAULT 0x07 stores '2007', which
// [yearDefaultNormalizer] folds), so neither stores the plain integer. float and
// double store it only while it fits their precision (double DEFAULT 0x1A
// stores '26'). Past that MySQL rounds it and formats it in its own notation,
// which this rule does not reproduce: float DEFAULT 0x01000001 stores
// 16777216 (reported as '16777200') and double DEFAULT 0x20000000000001
// stores '9.007199254740992e15'. numericDefaultNormalizer folds the scaled decimal,
// float and double cases.
type integerBinaryLiteralDefaultNormalizer struct{}

func (integerBinaryLiteralDefaultNormalizer) Name() string {
	return "integer-binary-literal-default"
}

func (integerBinaryLiteralDefaultNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if c.Default == nil || c.DefaultIsExpr {
			continue
		}
		unscaledDecimal := strings.EqualFold(c.Type, "decimal") && (c.Scale == nil || *c.Scale == 0)
		if !isIntegerColumnType(c.Type) && !unscaledDecimal {
			continue
		}
		value, ok := binaryLiteralValue(c)
		if !ok || (unscaledDecimal && value > math.MaxInt64) {
			continue
		}
		stored := strconv.FormatUint(value, 10)
		c.Default, c.DefaultKind = &stored, DefaultKindNumber
	}
	return ct
}

// binaryLiteralValue returns a column's hex or bit literal DEFAULT as the
// big-endian unsigned integer MySQL reads it as in a numeric context, or false
// for any other default. MySQL rejects an empty literal and one longer than 8
// bytes as a default on an integer or bit column, even when the extra bytes are
// leading zeros, so those are false too.
func binaryLiteralValue(c *Column) (uint64, bool) {
	b, ok := binaryLiteralBytes(c)
	if !ok || len(b) == 0 {
		return 0, false
	}
	return bigEndianUint64(b)
}

// bigEndianUint64 returns up to 8 bytes as a big-endian unsigned integer, and
// false for more than 8. No bytes is 0.
func bigEndianUint64(b string) (uint64, bool) {
	if len(b) > 8 {
		return 0, false
	}
	var buf [8]byte
	copy(buf[8-len(b):], b)
	return binary.BigEndian.Uint64(buf[:]), true
}

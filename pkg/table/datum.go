package table

import (
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
)

type datumTp int

const (
	unknownType datumTp = iota
	signedType
	unsignedType
	binaryType
)

// Datum could be a binary string, uint64 or int64.
type Datum struct {
	Val any
	Tp  datumTp // signed, unsigned, binary
}

func mySQLTypeToDatumTp(mysqlTp string) datumTp {
	// Normalize to uppercase and remove width specifications
	normalized := strings.ToUpper(removeWidth(mysqlTp))

	// Extract base type (remove size specifications like (255))
	baseType := normalized
	if before, _, found := strings.Cut(normalized, "("); found {
		baseType = before
	}

	switch baseType {
	case "INT", "BIGINT", "SMALLINT", "TINYINT", "MEDIUMINT":
		return signedType
	case "INT UNSIGNED", "BIGINT UNSIGNED", "SMALLINT UNSIGNED", "TINYINT UNSIGNED", "MEDIUMINT UNSIGNED":
		return unsignedType
	case "FLOAT", "DOUBLE", "DECIMAL":
		// Treat floats as unknownType so they get formatted as-is
		return unknownType
	case "VARBINARY", "BLOB", "BINARY", "LONGBLOB", "MEDIUMBLOB", "TINYBLOB":
		return binaryType
	case "VARCHAR", "CHAR", "TEXT", "LONGTEXT", "MEDIUMTEXT", "TINYTEXT", "JSON":
		return unknownType
	case "DATETIME", "TIMESTAMP", "DATE", "TIME":
		return unknownType
	}
	return unknownType
}

func NewDatum(val any, tp datumTp) (Datum, error) {
	var err error
	switch tp {
	case signedType:
		// We expect the value to be an int64, but it could be an int.
		// Anything else we attempt to convert it
		switch v := val.(type) {
		case int64:
			// do nothing
		case int:
			val = int64(v)
		default:
			val, err = strconv.ParseInt(fmt.Sprint(val), 10, 64)
			if err != nil {
				return Datum{}, fmt.Errorf("could not convert datum to int64: value=%v, error=%w", val, err)
			}
		}
	case unsignedType:
		// We expect uint64, but it could be uint.
		// We convert anything else.
		switch v := val.(type) {
		case uint64:
			// do nothing
		case uint:
			val = uint64(v)
		case uint32:
			val = uint64(v)
		case int32:
			// MySQL binlog sometimes sends unsigned int columns as signed int32.
			// We need to reinterpret the bits as unsigned.
			val = uint64(uint32(v))
		case int64:
			// For int64, a direct cast to uint64 is safe because both are 64-bit types
			// and the underlying bit pattern is preserved without additional sign extension.
			val = uint64(v)
		default:
			val, err = strconv.ParseUint(fmt.Sprint(val), 10, 64)
			if err != nil {
				return Datum{}, fmt.Errorf("could not convert datum to uint64: value=%v, error=%w", val, err)
			}
		}
	case binaryType, unknownType:
		// For binary and unknown types, convert to string if not already
		switch v := val.(type) {
		case string:
			// Already a string, keep as-is
		case []byte:
			val = string(v)
		default:
			// Convert other types to string using fmt.Sprint
			val = fmt.Sprint(v)
		}
	}
	return Datum{
		Val: val,
		Tp:  tp,
	}, nil
}

func datumValFromString(val string, tp datumTp) (any, error) {
	switch tp { //nolint:exhaustive
	case signedType:
		i, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, err
		}
		return i, nil
	case unsignedType:
		return strconv.ParseUint(val, 10, 64)
	}
	// If it starts with 0x then it's a binary string. If it was actually
	// a string that contained 0x, then we have encoded it as hex anyway.
	// see pkg/table/chunk.go:valuesString()
	if strings.HasPrefix(val, "0x") {
		tmp, err := hex.DecodeString(val[2:])
		if err != nil {
			return nil, err
		}
		return string(tmp), nil
	}
	return val, nil
}

func newDatumFromMySQL(val string, mysqlTp string) (Datum, error) {
	// Figure out the matching simplified type (signed, unsigned, binary)
	// We also have to simplify the value to the type.
	tp := mySQLTypeToDatumTp(mysqlTp)
	sVal, err := datumValFromString(val, tp)
	if err != nil {
		return Datum{}, err
	}
	return Datum{
		Val: sVal,
		Tp:  tp,
	}, nil
}

// NewDatumFromValue creates a Datum from a value and MySQL column type.
// This is useful for converting values from the database driver (which may be []byte, int, string, etc.)
// into a Datum that can be formatted as SQL.
func NewDatumFromValue(value any, mysqlType string) (Datum, error) {
	if value == nil {
		tp := mySQLTypeToDatumTp(mysqlType)
		return NewNilDatum(tp), nil
	}

	tp := mySQLTypeToDatumTp(mysqlType)

	// Convert []byte to string for non-numeric types
	if b, ok := value.([]byte); ok {
		switch tp {
		case signedType, unsignedType:
			// For numeric types, convert []byte to string then parse
			value = string(b)
		case binaryType:
			// For binary types, we want to always hex encode
			// If the data is valid UTF-8 and doesn't already trigger hex encoding,
			// prepend a special marker to force it
			s := string(b)
			// Only add marker if it's valid UTF-8 and doesn't start with "0x"
			if utf8.ValidString(s) && (len(s) <= 2 || s[0:2] != "0x") {
				// Prepend a special marker that's invalid UTF-8
				// Use a sequence that's very unlikely to appear in real data
				value = "\xFF\xFF\xFE" + s
			} else {
				value = s
			}
		default:
			// For unknown types (text, datetime, json, etc), convert to string
			value = string(b)
		}
	}

	return NewDatum(value, tp)
}

func NewNilDatum(tp datumTp) Datum {
	return Datum{
		Val: nil,
		Tp:  tp,
	}
}

func (d Datum) MaxValue() Datum {
	if d.Tp == signedType {
		return Datum{
			Val: int64(math.MaxInt64),
			Tp:  signedType,
		}
	}
	return Datum{
		Val: uint64(math.MaxUint64),
		Tp:  d.Tp,
	}
}

func (d Datum) MinValue() Datum {
	if d.Tp == signedType {
		return Datum{
			Val: int64(math.MinInt64),
			Tp:  signedType,
		}
	}
	return Datum{
		Val: uint64(0),
		Tp:  d.Tp,
	}
}

func (d Datum) Add(addVal uint64) Datum {
	if !d.IsNumeric() {
		panic("not supported on binary type")
	}
	ret := d
	if d.Tp == signedType {
		returnVal := d.Val.(int64) + int64(addVal)
		if returnVal < d.Val.(int64) {
			returnVal = int64(math.MaxInt64) // overflow
		}
		ret.Val = returnVal
		return ret
	}
	returnVal := d.Val.(uint64) + addVal
	if returnVal < d.Val.(uint64) {
		returnVal = uint64(math.MaxUint64) // overflow
	}
	ret.Val = returnVal
	return ret
}

// Range returns the diff between 2 datums as an uint64.
func (d Datum) Range(d2 Datum) uint64 {
	if !d.IsNumeric() {
		panic("not supported on binary type")
	}
	if d.Tp == signedType {
		return uint64(d.Val.(int64) - d2.Val.(int64))
	}
	return d.Val.(uint64) - d2.Val.(uint64)
}

// String returns the datum as a SQL escaped string
func (d Datum) String() string {
	if d.IsNil() {
		return "NULL"
	}
	if d.IsNumeric() {
		return fmt.Sprintf("%v", d.Val)
	}
	s, ok := d.Val.(string)
	if !ok {
		panic("can not convert datum to string")
	}
	// Check if it should be hex encoded
	if d.IsBinaryString() {
		// If the string starts with our marker (\xFF\xFF\xFE), strip it before hex encoding
		if len(s) >= 3 && s[0] == '\xFF' && s[1] == '\xFF' && s[2] == '\xFE' {
			return fmt.Sprintf("0x%x", s[3:])
		}
		return fmt.Sprintf("0x%x", s)
	}
	return "\"" + sqlescape.EscapeString(s) + "\""
}

// IsNumeric checks if it's signed or unsigned
func (d Datum) IsNumeric() bool {
	return d.Tp == signedType || d.Tp == unsignedType
}

func (d Datum) IsBinaryString() bool {
	s, ok := d.Val.(string)
	if !ok {
		return false
	}
	// Hex encode if:
	// 1. Starts with null byte (our marker for forced hex encoding)
	// 2. Not valid UTF-8
	// 3. Starts with "0x" (to avoid corruption when restoring JSON)
	return (len(s) > 0 && s[0] == '\x00') || !utf8.ValidString(s) || (len(s) > 2 && s[0:2] == "0x")
}

func (d Datum) IsNil() bool {
	return d.Val == nil
}

func (d Datum) GreaterThanOrEqual(d2 Datum) bool {
	if d.Tp != d2.Tp {
		panic("cannot compare different datum types")
	}

	switch d.Tp {
	case signedType:
		return d.Val.(int64) >= d2.Val.(int64)
	case unsignedType:
		return d.Val.(uint64) >= d2.Val.(uint64)
	case binaryType, unknownType:
		// For binary, string, and temporal types, use native Go string comparison
		// This uses lexicographic byte-by-byte comparison which is deterministic and consistent.
		// It may differ from MySQL collation but is safe for watermark optimizations since they
		// are disabled before the checksum phase.
		return fmt.Sprint(d.Val) >= fmt.Sprint(d2.Val)
	default:
		panic(fmt.Sprintf("unsupported datum type for comparison: %v", d.Tp))
	}
}

func (d Datum) GreaterThan(d2 Datum) bool {
	if d.Tp != d2.Tp {
		panic("cannot compare different datum types")
	}

	switch d.Tp {
	case signedType:
		return d.Val.(int64) > d2.Val.(int64)
	case unsignedType:
		return d.Val.(uint64) > d2.Val.(uint64)
	case binaryType, unknownType:
		// For binary, string, and temporal types, use native Go string comparison
		// This uses lexicographic byte-by-byte comparison which is deterministic and consistent.
		// It may differ from MySQL collation but is safe for watermark optimizations since they
		// are disabled before the checksum phase.
		return fmt.Sprint(d.Val) > fmt.Sprint(d2.Val)
	default:
		panic(fmt.Sprintf("unsupported datum type for comparison: %v", d.Tp))
	}
}

// LessThanOrEqual performs a comparison between two Datum values.
// Works with all comparable types including numeric, strings, binary, and temporal.
// Provided for completeness.
func (d Datum) LessThanOrEqual(d2 Datum) bool {
	if d.Tp != d2.Tp {
		panic("cannot compare different datum types")
	}

	switch d.Tp {
	case signedType:
		return d.Val.(int64) <= d2.Val.(int64)
	case unsignedType:
		return d.Val.(uint64) <= d2.Val.(uint64)
	case binaryType, unknownType:
		// For binary, string, and temporal types, use native Go string comparison
		// This uses lexicographic byte-by-byte comparison which is deterministic and consistent.
		// It may differ from MySQL collation but is safe for watermark optimizations since they
		// are disabled before the checksum phase.
		return fmt.Sprint(d.Val) <= fmt.Sprint(d2.Val)
	default:
		panic(fmt.Sprintf("unsupported datum type for comparison: %v", d.Tp))
	}
}

// LessThan performs a comparison between two Datum values.
// Works with all comparable types including numeric, strings, binary, and temporal.
// Provided for completeness.
func (d Datum) LessThan(d2 Datum) bool {
	if d.Tp != d2.Tp {
		panic("cannot compare different datum types")
	}

	switch d.Tp {
	case signedType:
		return d.Val.(int64) < d2.Val.(int64)
	case unsignedType:
		return d.Val.(uint64) < d2.Val.(uint64)
	case binaryType, unknownType:
		// For binary, string, and temporal types, use native Go string comparison
		// This uses lexicographic byte-by-byte comparison which is deterministic and consistent.
		// It may differ from MySQL collation but is safe for watermark optimizations since they
		// are disabled before the checksum phase.
		return fmt.Sprint(d.Val) < fmt.Sprint(d2.Val)
	default:
		panic(fmt.Sprintf("unsupported datum type for comparison: %v", d.Tp))
	}
}

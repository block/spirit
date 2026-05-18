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
	Val            any
	Tp             datumTp // signed, unsigned, binary
	forceHexEncode bool    // when true, always hex-encode the value in String()
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
	case binaryType:
		// Binary types are always hex-encoded in checkpoint JSON via Datum.String().
		// Decode the hex back to raw binary bytes.
		if strings.HasPrefix(val, "0x") {
			tmp, err := hex.DecodeString(val[2:])
			if err != nil {
				return nil, err
			}
			return string(tmp), nil
		}
		return val, nil
	}
	// For unknownType (VARCHAR, TEXT, etc), the value is stored as-is.
	// No hex decoding is needed because unknownType values are never hex-encoded.
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
	d := Datum{
		Val: sVal,
		Tp:  tp,
	}
	// Binary types should always be hex-encoded when serialized.
	if tp == binaryType {
		d.forceHexEncode = true
	}
	return d, nil
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
		switch tp { //nolint:exhaustive
		case signedType, unsignedType:
			// For numeric types, convert []byte to string then parse
			value = string(b)
		case binaryType:
			// For binary types, convert to string and set forceHexEncode.
			// We always want to hex-encode binary data in SQL output.
			// The forceHexEncode flag ensures this happens even for data
			// that is valid UTF-8 (which IsBinaryString() would not catch).
			d, err := NewDatum(string(b), tp)
			if err != nil {
				return Datum{}, err
			}
			d.forceHexEncode = true
			return d, nil
		default:
			// For unknown types (text, datetime, json, etc), convert to string
			value = string(b)
		}
	}

	d, err := NewDatum(value, tp)
	if err != nil {
		return Datum{}, err
	}
	// Ensure binary types are always hex-encoded, even when the input value
	// is not []byte (e.g. a string). This is consistent with the []byte path above.
	if tp == binaryType {
		d.forceHexEncode = true
	}
	return d, nil
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

// Add returns d + addVal. Returns an error if d is not numeric — callers
// that previously crashed on a binary-PK migration via the optimistic
// chunker's prefetch path now get a recoverable error and can checkpoint
// and exit cleanly.
func (d Datum) Add(addVal uint64) (Datum, error) {
	if !d.IsNumeric() {
		return Datum{}, fmt.Errorf("Datum.Add: not supported on non-numeric type %v", d.Tp)
	}
	ret := d
	if d.Tp == signedType {
		returnVal := d.Val.(int64) + int64(addVal)
		if returnVal < d.Val.(int64) {
			returnVal = int64(math.MaxInt64) // overflow
		}
		ret.Val = returnVal
		return ret, nil
	}
	returnVal := d.Val.(uint64) + addVal
	if returnVal < d.Val.(uint64) {
		returnVal = uint64(math.MaxUint64) // overflow
	}
	ret.Val = returnVal
	return ret, nil
}

// Range returns the diff between two datums as a uint64. Returns an
// error on non-numeric types for the same reason Add does.
func (d Datum) Range(d2 Datum) (uint64, error) {
	if !d.IsNumeric() {
		return 0, fmt.Errorf("Datum.Range: not supported on non-numeric type %v", d.Tp)
	}
	if d.Tp == signedType {
		return uint64(d.Val.(int64) - d2.Val.(int64)), nil
	}
	return d.Val.(uint64) - d2.Val.(uint64), nil
}

// String returns the datum as a complete, self-contained SQL literal.
// Every return path is safe to inline directly into a SQL statement
// without further quoting or escaping by the caller:
//
//   - NULL                            for IsNil()
//   - the numeric literal (e.g. 42)   for IsNumeric()
//   - 0x... hex literal               for IsBinaryString()
//   - "..." with backslash escapes    for everything else
//
// The string-literal path runs sqlescape.EscapeString on the contents
// and wraps in double quotes, so callers like Chunk.String /
// expandRowConstructorComparison / applier UpsertRows can construct
// SQL by simple fmt.Sprintf concatenation. New code that has explicit
// error handling available may prefer a typed accessor, but the
// pre-escaped contract here is load-bearing for the migration's SQL
// emission paths.
//
// It is also fmt.Stringer for log / debug output. The previous form
// panicked when a non-numeric datum's Val was not a string; this form
// coerces via %v so a misconstructed datum still produces a valid SQL
// fragment rather than crashing the migration. NewDatum always
// normalizes Val to string for binaryType/unknownType, so this
// coercion only fires for datums built by hand with an unexpected Val
// type.
func (d Datum) String() string {
	if d.IsNil() {
		return "NULL"
	}
	if d.IsNumeric() {
		return fmt.Sprintf("%v", d.Val)
	}
	s, ok := d.Val.(string)
	if !ok {
		s = fmt.Sprintf("%v", d.Val)
	}
	if d.IsBinaryString() {
		// MySQL binary string needs at least one character
		if len(s) == 0 {
			return "0x00"
		}
		return fmt.Sprintf("%#x", s)
	}
	return "\"" + sqlescape.EscapeString(s) + "\""
}

// IsNumeric checks if it's signed or unsigned
func (d Datum) IsNumeric() bool {
	return d.Tp == signedType || d.Tp == unsignedType
}

func (d Datum) IsBinaryString() bool {
	if d.forceHexEncode {
		return true
	}
	s, ok := d.Val.(string)
	if !ok {
		return false
	}
	// Hex encode if not valid UTF-8 (binary data that wasn't explicitly marked)
	return !utf8.ValidString(s)
}

func (d Datum) IsNil() bool {
	return d.Val == nil
}

// compare reduces the four ordering operators to a single (-1, 0, +1)
// result so the type-dispatch logic isn't duplicated four times. Returns
// an error on type mismatch or an unrecognized Datum type — the four
// public wrappers below convert that into a (bool, error) result that
// callers can either propagate or, in the chunker watermark paths,
// swallow with a safe-default false.
func (d Datum) compare(d2 Datum) (int, error) {
	if d.Tp != d2.Tp {
		return 0, fmt.Errorf("cannot compare datums of different types: %v vs %v", d.Tp, d2.Tp)
	}
	switch d.Tp {
	case signedType:
		a, b := d.Val.(int64), d2.Val.(int64)
		switch {
		case a < b:
			return -1, nil
		case a > b:
			return 1, nil
		default:
			return 0, nil
		}
	case unsignedType:
		a, b := d.Val.(uint64), d2.Val.(uint64)
		switch {
		case a < b:
			return -1, nil
		case a > b:
			return 1, nil
		default:
			return 0, nil
		}
	case binaryType, unknownType:
		// Native Go string comparison: lexicographic byte-by-byte,
		// deterministic and consistent. May differ from MySQL collation
		// but safe for watermark optimizations since they are disabled
		// before the checksum phase.
		a, b := fmt.Sprint(d.Val), fmt.Sprint(d2.Val)
		switch {
		case a < b:
			return -1, nil
		case a > b:
			return 1, nil
		default:
			return 0, nil
		}
	default:
		return 0, fmt.Errorf("unsupported datum type for comparison: %v", d.Tp)
	}
}

func (d Datum) GreaterThanOrEqual(d2 Datum) (bool, error) {
	c, err := d.compare(d2)
	return c >= 0, err
}

func (d Datum) GreaterThan(d2 Datum) (bool, error) {
	c, err := d.compare(d2)
	return c > 0, err
}

func (d Datum) LessThanOrEqual(d2 Datum) (bool, error) {
	c, err := d.compare(d2)
	return c <= 0, err
}

func (d Datum) LessThan(d2 Datum) (bool, error) {
	c, err := d.compare(d2)
	return c < 0, err
}

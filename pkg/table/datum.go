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
	charset        string  // when set, Val holds bytes in this charset, and String() emits them with its introducer; see ColumnType
}

// isBITType reports whether the given MySQL column type is a BIT(N) type.
// Used to gate the BIT-specific []byte → uint64 decoding in NewDatumFromValue.
func isBITType(mysqlTp string) bool {
	baseType := strings.ToUpper(removeWidth(mysqlTp))
	if before, _, found := strings.Cut(baseType, "("); found {
		baseType = before
	}
	return baseType == "BIT"
}

// isFloatColumnType reports whether mysqlTp is a FLOAT column type, in any of
// its spellings: "float", "float unsigned", "float(7,4) zerofill". FLOAT(p)
// with p > 24 is created as a DOUBLE, so information_schema never reports it.
func isFloatColumnType(mysqlTp string) bool {
	base := strings.ToLower(strings.TrimSpace(mysqlTp))
	if before, _, found := strings.Cut(base, "("); found {
		base = before
	}
	base, _, _ = strings.Cut(base, " ")
	return base == "float"
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
	case "BIT":
		// BIT(N) arrives from the binlog as int64 (go-mysql's decodeBit).
		// Classifying as unsignedType makes NewDatum reinterpret the int64
		// bit pattern as uint64 — preserving BIT(64) values with the high
		// bit set — and Datum.String() emits a bare numeric literal that
		// MySQL coerces to a bit pattern, rather than a quoted string
		// (which MySQL would otherwise interpret byte-by-byte as bits).
		return unsignedType
	case "YEAR":
		// YEAR must be emitted as a bare number. MySQL stores the string
		// '0' in a YEAR column as 2000 but the number 0 as 0000, and both
		// the driver and the binlog deliver YEAR 0000 as the integer 0.
		// As an unknownType it was quoted ("0") and silently became 2000.
		return signedType
	case "FLOAT", "DOUBLE", "DECIMAL":
		// Treat floats as unknownType so they get formatted as-is
		return unknownType
	case "VARBINARY", "BLOB", "BINARY", "LONGBLOB", "MEDIUMBLOB", "TINYBLOB":
		return binaryType
	case "VECTOR":
		// VECTOR (MySQL 9.7+) is a packed array of 4-byte little-endian
		// floats. Both the driver and the binlog surface it as []byte, and
		// MySQL only accepts it back as a binary-charset literal: a
		// character-set string of the right length is rejected outright
		// ("Value of type 'string, size: 12' cannot be converted to
		// 'vector' type"). Classifying it as binaryType forces the 0x-hex
		// literal in Datum.String() even for byte images that happen to be
		// valid UTF-8 — e.g. the all-zeros vector [0,0,0], which the
		// IsBinaryString() UTF-8 heuristic alone would emit as a quoted
		// string and the server would reject.
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
			original := val
			val, err = strconv.ParseInt(fmt.Sprint(original), 10, 64)
			if err != nil {
				return Datum{}, fmt.Errorf("could not convert datum to int64: value=%v, error=%w", original, err)
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
			original := val
			val, err = strconv.ParseUint(fmt.Sprint(original), 10, 64)
			if err != nil {
				return Datum{}, fmt.Errorf("could not convert datum to uint64: value=%v, error=%w", original, err)
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
		// Binary values must always serialize as 0x-hex literals, even when
		// they happen to be valid UTF-8 (e.g. a VARBINARY key holding the
		// ASCII string "0xab"). The checkpoint-restore path
		// (datumValFromString) unconditionally hex-decodes binaryType values
		// with a "0x" prefix, so serializing such a value as a plain string
		// would corrupt the watermark boundary on resume.
		forceHexEncode: tp == binaryType,
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
		// Binary types are hex-encoded ("0x...") in checkpoint JSON via
		// jsonDatumString, except the empty value, which is serialized as
		// x'' (there is no zero-digit 0x literal). Decode both back to a
		// Go string holding the raw bytes: Datum.Val stores binary values
		// as string, never []byte.
		if val == "x''" {
			return "", nil
		}
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

// ColumnType is a MySQL column type pre-resolved for datum construction.
// Resolving it (parsing the type string) is the dominant cost of
// NewDatumFromValue, so callers that convert many values of the same
// column type should resolve once with NewColumnType and reuse it via
// NewDatumFromValueWithType.
type ColumnType struct {
	tp    datumTp
	isBit bool
	// charset is the character set a string value's bytes are in, when
	// that is not one MySQL can read as the connection's utf8mb4. It is
	// only set by TableInfo.BinlogColumnType: the driver returns every
	// string converted to the connection charset, but a binlog row image
	// carries the column's own bytes.
	charset string
}

// NewColumnType resolves a MySQL column type string (e.g. "int",
// "binary(8)", "bit(8)") into a reusable ColumnType.
func NewColumnType(mysqlType string) ColumnType {
	return ColumnType{
		tp:    mySQLTypeToDatumTp(mysqlType),
		isBit: isBITType(mysqlType),
	}
}

// NewDatumFromValue creates a Datum from a value and MySQL column type.
// This is useful for converting values from the database driver (which may be []byte, int, string, etc.)
// into a Datum that can be formatted as SQL.
func NewDatumFromValue(value any, mysqlType string) (Datum, error) {
	return NewDatumFromValueWithType(value, NewColumnType(mysqlType))
}

// NewDatumFromValueWithType is NewDatumFromValue with the column type
// already resolved (see ColumnType), for hot paths that convert many
// values of the same column type.
func NewDatumFromValueWithType(value any, ct ColumnType) (Datum, error) {
	if value == nil {
		return NewNilDatum(ct.tp), nil
	}

	tp := ct.tp

	// BIT(N) is classified as unsignedType, but its wire form depends on
	// the source: the Go MySQL driver returns []byte (big-endian bit
	// pattern); the binlog reader returns int64. Decode []byte to uint64
	// here so the generic unsignedType path below sees a numeric value
	// and doesn't try to ParseUint on raw bit bytes (which would fail on
	// any byte that isn't an ASCII decimal digit).
	if b, ok := value.([]byte); ok && ct.isBit {
		var u uint64
		for _, by := range b {
			u = (u << 8) | uint64(by)
		}
		value = u
	}

	// Convert []byte to string. NewDatum parses numeric types from strings,
	// and marks binaryType datums with forceHexEncode so binary data is
	// always hex-encoded in SQL output — even data that is valid UTF-8
	// (which IsBinaryString() would not catch).
	isString := false
	switch v := value.(type) {
	case []byte:
		value = string(v)
		isString = true
	case string:
		isString = true
	}
	d, err := NewDatum(value, tp)
	if err != nil {
		return Datum{}, err
	}
	if isString && tp == unknownType {
		d.charset = ct.charset
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
//   - _charset 0x... literal          for a string whose bytes are in a
//     charset other than utf8mb4/utf8mb3 (see ColumnType.charset)
//   - 0x... hex literal               for IsBinaryString()
//     (a zero-length value uses the empty binary literal instead — see below)
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
	if d.charset != "" {
		// The bytes are in the column's charset (a binlog row image of a
		// latin1, gbk, utf16, ... column). A quoted string would be read
		// in the connection charset (utf8mb4) and converted: latin1 C3 A9
		// ("Ã©") is valid UTF-8 for "é" and would be stored as latin1 E9,
		// and utf16 00 4D ('M') would become two characters. A binary hex
		// literal is not right either: a latin1 -> utf8mb4 change would
		// reject latin1 E9 as an invalid utf8mb4 string. The introducer
		// labels the bytes with their charset, so MySQL stores them
		// unchanged in a column of that charset and converts them
		// correctly into any other.
		if len(s) == 0 {
			return "_" + d.charset + " x''"
		}
		return fmt.Sprintf("_%s %#x", d.charset, s)
	}
	if d.IsBinaryString() {
		// The empty value still needs a valid SQL literal: %#x renders ""
		// as "" and a bare "0x" parses as an identifier, so emit the
		// standard zero-length hex literal instead. It must NOT be 0x00 —
		// that is a one-byte NUL, a different value, and emitting it here
		// made every binlog-applied REPLACE corrupt empty blobs (minting
		// endless checksum mismatches on tables that store empty strings,
		// e.g. zero-length serialized protos).
		if len(s) == 0 {
			return "x''"
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
		a, ok := d.Val.(int64)
		if !ok {
			return 0, fmt.Errorf("datum compare: expected int64, got %T", d.Val)
		}
		b, ok := d2.Val.(int64)
		if !ok {
			return 0, fmt.Errorf("datum compare: expected int64, got %T", d2.Val)
		}
		switch {
		case a < b:
			return -1, nil
		case a > b:
			return 1, nil
		default:
			return 0, nil
		}
	case unsignedType:
		a, ok := d.Val.(uint64)
		if !ok {
			return 0, fmt.Errorf("datum compare: expected uint64, got %T", d.Val)
		}
		b, ok := d2.Val.(uint64)
		if !ok {
			return 0, fmt.Errorf("datum compare: expected uint64, got %T", d2.Val)
		}
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

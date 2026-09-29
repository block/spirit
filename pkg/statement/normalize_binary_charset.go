package statement

import (
	"strings"

	"github.com/block/spirit/pkg/parser/charset"
)

func init() { registerNormalizer(binaryCharsetNormalizer{}) }

// binaryCharsetNormalizer rewrites a character column whose charset resolves
// to binary into its binary type, the way MySQL does. The parser already does
// this for a column that writes `CHARACTER SET binary` itself (see
// parseColumn); this rule covers the two routes it does not see, because they
// need the rest of the definition:
//
//   - the column declares no charset or collation and inherits a binary table
//     default, written as either DEFAULT CHARSET=binary or DEFAULT
//     COLLATE=binary;
//   - the column writes `COLLATE binary`, which implies the binary charset.
//
// Verified against MySQL 8.0.43:
//
//	a varchar(3)                           (table charset binary)
//	  -> varbinary(3)
//	b char(3)                              -> binary(3)
//	c text                                 -> blob (tinytext, mediumtext, longtext likewise)
//	j varchar(3) BINARY                    -> varbinary(3)
//	a varchar(3) COLLATE binary            (table charset utf8mb4)
//	  -> varbinary(3)
//	g enum('x','y')                        -> enum('x','y')  (not rewritten)
//	i varchar(3) CHARACTER SET latin1      -> varchar(3) CHARACTER SET latin1 COLLATE latin1_swedish_ci
//	k varchar(3) COLLATE utf8mb4_bin       -> varchar(3) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin
//	n nvarchar(3)                          -> varchar(3) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci
//	DEFAULT COLLATE=binary                 -> DEFAULT CHARSET=binary
//	DEFAULT CHARSET=binary COLLATE=binary  -> DEFAULT CHARSET=binary
//
// Without this rule the written character type survives into the diff, which
// emits `MODIFY COLUMN a varchar(3)` against the live varbinary(3). MySQL
// applies it, stores varbinary(3) again, and the next diff emits the same
// statement.
//
// A rewritten column is given the binary charset and collation, as the parser
// gives an explicit `CHARACTER SET binary` column. Because the column then
// declares a charset and a collation, binaryAttributeNormalizer (which also
// skips a binary charset) and defaultCollationNormalizer (which skips a column
// that has a collation) leave it alone in either order.
//
// The table default is canonicalized to the form SHOW CREATE TABLE reports,
// DEFAULT CHARSET=binary with no COLLATE, so that DEFAULT COLLATE=binary and
// DEFAULT CHARSET=binary COLLATE=binary do not diff against it.
//
// booleanKeywordDefaultNormalizer folds a TRUE/FALSE default by column type,
// and treats char and binary differently, so it reads the type through
// storedColumnType to stay independent of the order the two rules run in.
type binaryCharsetNormalizer struct{}

func (binaryCharsetNormalizer) Name() string { return "binary-charset" }

func (binaryCharsetNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		col := &ct.Columns[i]
		binType, ok := resolvedBinaryType(col, ct)
		if !ok {
			continue
		}
		col.Type = binType
		cs, coll := charset.CharsetBin, charset.CollationBin
		col.Charset, col.Collation = &cs, &coll
	}
	if tableDefaultIsBinary(ct.TableOptions) {
		cs := charset.CharsetBin
		ct.TableOptions.Charset = &cs
		ct.TableOptions.Collation = nil
	}
	return ct
}

// storedColumnType returns the type MySQL stores col as: its binary type when
// binaryCharsetNormalizer rewrites it, and its written type otherwise. A rule
// whose outcome depends on the column type reads it through this, so that it
// sees the same type whether it runs before or after binaryCharsetNormalizer.
func storedColumnType(col *Column, ct *CreateTable) string {
	if binType, ok := resolvedBinaryType(col, ct); ok {
		return binType
	}
	return col.Type
}

// resolvedBinaryType returns the binary type of a character column whose
// charset resolves to binary through the table default or its own COLLATE
// binary, and false for every other column. A column that declares a charset
// is false: the parser has already converted an explicit binary charset, and
// a column this rule has rewritten declares one too, which keeps the rule
// idempotent.
func resolvedBinaryType(col *Column, ct *CreateTable) (string, bool) {
	var binary bool
	switch {
	case col.Charset != nil:
		return "", false
	case col.Collation != nil:
		binary = strings.EqualFold(*col.Collation, charset.CollationBin)
	default:
		binary = tableDefaultIsBinary(ct.TableOptions)
	}
	if !binary {
		return "", false
	}
	return binaryTypeOf(col.Type) // enum and set keep their type
}

// tableDefaultIsBinary reports whether the table's default charset is binary.
// The binary collation belongs only to the binary charset, so either option
// decides it.
func tableDefaultIsBinary(opts *TableOptions) bool {
	if opts == nil {
		return false
	}
	return (opts.Charset != nil && strings.EqualFold(*opts.Charset, charset.CharsetBin)) ||
		(opts.Collation != nil && strings.EqualFold(*opts.Collation, charset.CollationBin))
}

// binaryTypeOf returns the binary type MySQL stores a character type as when
// its charset is binary. It reports false for a type that has no binary
// counterpart, including enum and set, which keep their type.
func binaryTypeOf(typ string) (string, bool) {
	switch typ {
	case "varchar":
		return "varbinary", true
	case "char":
		return "binary", true
	case "text":
		return "blob", true
	case "tinytext":
		return "tinyblob", true
	case "mediumtext":
		return "mediumblob", true
	case "longtext":
		return "longblob", true
	}
	return "", false
}

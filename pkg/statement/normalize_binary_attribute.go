package statement

import (
	"strings"

	"github.com/block/spirit/pkg/parser/mysql"
)

func init() { registerNormalizer(binaryAttributeNormalizer{}) }

// binaryAttributeNormalizer resolves the legacy BINARY column attribute
// (e.g. `c varchar(100) BINARY`) the way MySQL does: BINARY does not change
// the data type, it selects the binary (_bin) collation of the column's
// charset. Verified against MySQL 8.0.45, the canonical forms are:
//
//	c varchar(100) BINARY                        (table charset utf8mb4)
//	  -> varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin
//	c varchar(100) CHARACTER SET latin1 BINARY
//	  -> varchar(100) CHARACTER SET latin1 COLLATE latin1_bin
//	c varchar(100) BINARY COLLATE latin1_swedish_ci  (table charset utf8mb4)
//	  -> varchar(100) CHARACTER SET latin1 COLLATE latin1_bin
//	c varchar(100) BINARY                        (table collation latin1_swedish_ci)
//	  -> varchar(100) CHARACTER SET latin1 COLLATE latin1_bin
//
// The TiDB parser surfaces the attribute as the binary type flag with a
// non-"binary" (usually empty) charset — distinct from true binary types
// (VARBINARY/BINARY/BLOB), which carry the "binary" charset and are
// converted to their binary type names in parseColumn. Without this
// normalization, diffing a live `varchar ... COLLATE utf8mb4_bin` column
// against a desired file written with the BINARY attribute would emit a
// destructive MODIFY to varbinary.
//
// When the column declares no charset of its own, MySQL lets BINARY win over
// an explicit COLLATE in the same column definition (varchar(100) BINARY
// COLLATE utf8mb4_general_ci resolves to utf8mb4_bin), so the parsed
// collation is overridden. When the column does declare a charset, the
// COLLATE wins instead, and is kept. NCHAR/NVARCHAR always declare one
// (utf8mb3). Verified against MySQL 8.0.43:
//
//	c varchar(100) CHARACTER SET latin1 BINARY COLLATE latin1_general_ci
//	  -> varchar(100) CHARACTER SET latin1 COLLATE latin1_general_ci
//	c NCHAR(5) BINARY COLLATE utf8mb3_unicode_ci
//	  -> char(5) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci
//
// If neither the column nor the table declares a charset or a collation the
// attribute cannot be resolved (the effective charset is a server default only
// known at runtime); the column keeps its character type and no collation is
// invented.
type binaryAttributeNormalizer struct{}

func (binaryAttributeNormalizer) Name() string { return "binary-attribute" }

func (binaryAttributeNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		col := &ct.Columns[i]
		if col.Raw == nil || !mysql.HasBinaryFlag(col.Raw.Tp.GetFlag()) {
			continue
		}
		if col.Raw.Tp.GetCharset() == "binary" {
			continue // true binary type (VARBINARY et al.), converted in parseColumn
		}
		if col.Charset != nil && col.Collation != nil {
			continue // an explicit charset lets the COLLATE win over BINARY
		}
		charsetName := binaryAttributeCharset(col, ct)
		if charsetName == "" || charsetName == "binary" {
			continue
		}
		// Every MySQL character set has a <charset>_bin collation.
		collation := charsetName + "_bin"
		col.Collation = &collation
	}
	return ct
}

// binaryAttributeCharset returns the charset whose _bin collation a BINARY
// attribute selects, or "" when the definition does not determine it. A
// collation names its charset, so the column's own charset or collation
// decides first, then the table's default charset or collation.
func binaryAttributeCharset(col *Column, ct *CreateTable) string {
	switch {
	case col.Charset != nil:
		return *col.Charset
	case col.Collation != nil:
		return charsetOfCollation(strings.ToLower(*col.Collation))
	case ct.TableOptions == nil:
		return ""
	case ct.TableOptions.Charset != nil:
		return *ct.TableOptions.Charset
	case ct.TableOptions.Collation != nil:
		return charsetOfCollation(strings.ToLower(*ct.TableOptions.Collation))
	}
	return ""
}

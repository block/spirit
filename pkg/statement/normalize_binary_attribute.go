package statement

import "github.com/block/spirit/pkg/parser/mysql"

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
//
// The TiDB parser surfaces the attribute as the binary type flag with a
// non-"binary" (usually empty) charset — distinct from true binary types
// (VARBINARY/BINARY/BLOB), which carry the "binary" charset and are
// converted to their binary type names in parseColumn. Without this
// normalization, diffing a live `varchar ... COLLATE utf8mb4_bin` column
// against a desired file written with the BINARY attribute would emit a
// destructive MODIFY to varbinary.
//
// MySQL lets BINARY win over an explicit COLLATE in the same column
// definition (varchar(100) BINARY COLLATE utf8mb4_general_ci resolves to
// utf8mb4_bin), so any parsed collation is overridden here. If neither the
// column nor the table declares a charset the attribute cannot be resolved
// (the effective charset is a server default only known at runtime); the
// column keeps its character type and no collation is invented.
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
		// Resolve the effective charset: explicit column charset first,
		// then the table default charset.
		charsetName := ""
		if col.Charset != nil {
			charsetName = *col.Charset
		} else if ct.TableOptions != nil && ct.TableOptions.Charset != nil {
			charsetName = *ct.TableOptions.Charset
		}
		if charsetName == "" || charsetName == "binary" {
			continue
		}
		// Every MySQL character set has a <charset>_bin collation.
		collation := charsetName + "_bin"
		col.Collation = &collation
	}
	return ct
}

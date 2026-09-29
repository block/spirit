package statement

import (
	"strings"

	"github.com/block/spirit/pkg/parser/charset"
	"github.com/block/spirit/pkg/parser/mysql"
)

func init() { registerNormalizer(defaultCollationNormalizer{}) }

// defaultCollationNormalizer fills in a charset's default collation on a
// column or table that declares the charset without a COLLATE, for every
// charset whose default collation is fixed. MySQL applies that default when
// the table is created, and SHOW CREATE TABLE writes it out on a column whose
// charset was written explicitly. Verified against MySQL 8.0.28, 8.0.43, 8.4
// and 9.7:
//
//	c varchar(3) CHARACTER SET latin1    (table charset utf8mb4)
//	  -> varchar(3) CHARACTER SET latin1 COLLATE latin1_swedish_ci
//	c varchar(3) CHARACTER SET latin1    (table charset latin1)
//	  -> varchar(3) CHARACTER SET latin1 COLLATE latin1_swedish_ci
//	c NVARCHAR(10)                       (table charset utf8mb4)
//	  -> varchar(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci
//	     (8.0.28: CHARACTER SET utf8 COLLATE utf8_general_ci)
//	DEFAULT CHARSET=latin1
//	  -> DEFAULT CHARSET=latin1          (COLLATE omitted: it is the default)
//
// Without this rule a charset with no collation is underdetermined to the
// diff (see resolvedCharsetCollation), which then compares the written values
// and emits a spurious MODIFY against the live form above. NCHAR/NVARCHAR and
// their NATIONAL aliases take this path too: they always use utf8mb3 and do not
// accept a charset clause.
//
// The table default is filled in as well, so that a column which inherits the
// table default and a column which declares the same charset explicitly
// resolve to the same collation and still compare equal. It also makes a
// table-level `DEFAULT CHARSET=latin1` mean latin1_swedish_ci, as it does in
// MySQL, rather than "any latin1 collation".
//
// utf8mb4 is excluded: its default depends on the server version and on
// default_collation_for_utf8mb4, so it stays underdetermined. binary is left
// alone: the parser already turns `CHAR(3) CHARACTER SET binary` into BINARY(3)
// with the binary collation, as MySQL does, binaryCharsetNormalizer does the
// same for a column that inherits a binary table default, and a binary table
// default has no other collation to be confused with.
//
// A column with the BINARY attribute is left to binaryAttributeNormalizer,
// which selects the charset's _bin collation for it. Filling in the default
// here would make a collation this rule invented indistinguishable from a
// written COLLATE, which that rule keeps.
type defaultCollationNormalizer struct{}

func (defaultCollationNormalizer) Name() string { return "default-collation" }

func (defaultCollationNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		col := &ct.Columns[i]
		if col.Charset == nil || col.Collation != nil {
			continue
		}
		if col.Raw != nil && mysql.HasBinaryFlag(col.Raw.Tp.GetFlag()) {
			continue
		}
		if collation, ok := fixedDefaultCollation(*col.Charset); ok {
			col.Collation = &collation
		}
	}
	if opts := ct.TableOptions; opts != nil && opts.Charset != nil && opts.Collation == nil {
		if collation, ok := fixedDefaultCollation(*opts.Charset); ok {
			opts.Collation = &collation
		}
	}
	return ct
}

// fixedDefaultCollation returns the collation MySQL applies to cs when no
// COLLATE is written, if that collation does not depend on the server. It uses
// MySQLDefaultCollation, not GetDefaultCollation, which returns the parser
// upstream's *_bin defaults. The 3-byte UTF-8 collations come back in the
// parser's legacy utf8_* spelling, which is how it parses both utf8mb3_* and
// utf8_*.
func fixedDefaultCollation(cs string) (string, bool) {
	switch strings.ToLower(cs) {
	case charset.CharsetUTF8MB4, charset.CharsetBin:
		return "", false
	}
	return charset.MySQLDefaultCollation(cs)
}

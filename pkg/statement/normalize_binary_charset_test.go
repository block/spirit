package statement

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBinaryCharsetNormalizer checks the type, charset and collation each
// column resolves to under the binary charset. This rule,
// binaryAttributeNormalizer and defaultCollationNormalizer all write a
// column's collation, and normalizers must not depend on registration order,
// so each case is parsed with the registry in its normal order and reversed.
func TestBinaryCharsetNormalizer(t *testing.T) {
	registered := normalizers
	t.Cleanup(func() { normalizers = registered })

	type want struct{ typ, charset, collation string }
	binary := func(typ string) want { return want{typ, "binary", "binary"} }
	for _, tc := range []struct {
		sql  string
		want want
	}{
		{"CREATE TABLE t (c varchar(3)) DEFAULT CHARSET=binary", binary("varbinary")},
		{"CREATE TABLE t (c char(3)) DEFAULT CHARSET=binary", binary("binary")},
		{"CREATE TABLE t (c text) DEFAULT CHARSET=binary", binary("blob")},
		{"CREATE TABLE t (c tinytext) DEFAULT CHARSET=binary", binary("tinyblob")},
		{"CREATE TABLE t (c mediumtext) DEFAULT CHARSET=binary", binary("mediumblob")},
		{"CREATE TABLE t (c longtext) DEFAULT CHARSET=binary", binary("longblob")},
		{"CREATE TABLE t (c varchar(3) BINARY) DEFAULT CHARSET=binary", binary("varbinary")},
		{"CREATE TABLE t (c varchar(3)) DEFAULT COLLATE=binary", binary("varbinary")},
		{"CREATE TABLE t (c varchar(3)) DEFAULT CHARSET=BINARY COLLATE=BINARY", binary("varbinary")},
		{"CREATE TABLE t (c varchar(3) COLLATE binary) DEFAULT CHARSET=utf8mb4", binary("varbinary")},
		{"CREATE TABLE t (c varchar(3) CHARACTER SET binary) DEFAULT CHARSET=utf8mb4", binary("varbinary")},
		{"CREATE TABLE t (c varchar(3) CHARACTER SET latin1) DEFAULT CHARSET=binary", want{"varchar", "latin1", "latin1_swedish_ci"}},
		{"CREATE TABLE t (c varchar(3) COLLATE utf8mb4_bin) DEFAULT CHARSET=binary", want{"varchar", "", "utf8mb4_bin"}},
		{"CREATE TABLE t (c varchar(3) BINARY COLLATE latin1_general_ci) DEFAULT CHARSET=binary", want{"varchar", "", "latin1_bin"}},
		{"CREATE TABLE t (c NVARCHAR(3)) DEFAULT CHARSET=binary", want{"varchar", "utf8", "utf8_general_ci"}},
		{"CREATE TABLE t (c enum('x','y')) DEFAULT CHARSET=binary", want{"enum", "", ""}},
		{"CREATE TABLE t (c set('x')) DEFAULT CHARSET=binary", want{"set", "", ""}},
		{"CREATE TABLE t (c varchar(3)) DEFAULT CHARSET=utf8mb4", want{"varchar", "", ""}},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			for _, order := range [][]Normalizer{registered, reversed(registered)} {
				normalizers = order
				ct, err := ParseCreateTable(tc.sql)
				require.NoError(t, err)
				require.Len(t, ct.Columns, 1)
				col := ct.Columns[0]
				require.Equal(t, tc.want.typ, col.Type)
				require.Equal(t, tc.want.charset, deref(col.Charset))
				require.Equal(t, tc.want.collation, deref(col.Collation))
			}
		})
	}
}

// TestBinaryCharsetBooleanDefaultOrderIndependent: booleanKeywordDefaultNormalizer
// folds a TRUE/FALSE default on char but not on binary, which pads it with
// NULs. Under a binary table default char(3) is stored as binary(3), so the
// keyword must be left alone whether that rule runs before or after the type
// rewrite. varchar is stored as varbinary, which folds either way.
func TestBinaryCharsetBooleanDefaultOrderIndependent(t *testing.T) {
	registered := normalizers
	t.Cleanup(func() { normalizers = registered })

	for _, tc := range []struct {
		sql, typ, dflt string
	}{
		{"CREATE TABLE t (c char(3) DEFAULT TRUE) DEFAULT CHARSET=binary", "binary", "TRUE"},
		{"CREATE TABLE t (c char(3) COLLATE binary DEFAULT FALSE) DEFAULT CHARSET=utf8mb4", "binary", "FALSE"},
		{"CREATE TABLE t (c varchar(3) DEFAULT TRUE) DEFAULT CHARSET=binary", "varbinary", "1"},
		{"CREATE TABLE t (c char(3) DEFAULT TRUE) DEFAULT CHARSET=utf8mb4", "char", "1"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			for _, order := range [][]Normalizer{registered, reversed(registered)} {
				normalizers = order
				ct, err := ParseCreateTable(tc.sql)
				require.NoError(t, err)
				require.Len(t, ct.Columns, 1)
				require.Equal(t, tc.typ, ct.Columns[0].Type)
				require.Equal(t, tc.dflt, deref(ct.Columns[0].Default))
			}
		})
	}
}

// TestBinaryCharsetNormalizerTableDefault checks that every spelling of a
// binary table default resolves to the form SHOW CREATE TABLE reports,
// DEFAULT CHARSET=binary with no COLLATE.
func TestBinaryCharsetNormalizerTableDefault(t *testing.T) {
	for _, sql := range []string{
		"CREATE TABLE t (id int) DEFAULT CHARSET=binary",
		"CREATE TABLE t (id int) DEFAULT COLLATE=binary",
		"CREATE TABLE t (id int) DEFAULT CHARSET=BINARY COLLATE=BINARY",
	} {
		t.Run(sql, func(t *testing.T) {
			ct, err := ParseCreateTable(sql)
			require.NoError(t, err)
			require.NotNil(t, ct.TableOptions)
			require.Equal(t, "binary", deref(ct.TableOptions.Charset))
			require.Nil(t, ct.TableOptions.Collation)
		})
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

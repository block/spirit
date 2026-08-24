package statement

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCarriesCharset(t *testing.T) {
	ct, err := ParseCreateTable(`CREATE TABLE t (
		a VARCHAR(10),
		b CHAR(2),
		c TEXT,
		d ENUM('x','y'),
		e SET('x','y'),
		f INT,
		g DATETIME,
		h VARBINARY(10),
		i BLOB,
		j JSON
	)`)
	require.NoError(t, err)

	for _, name := range []string{"a", "b", "c", "d", "e"} {
		require.True(t, ct.Columns.ByName(name).CarriesCharset(), "column %s should carry a charset", name)
	}
	for _, name := range []string{"f", "g", "h", "i", "j"} {
		require.False(t, ct.Columns.ByName(name).CarriesCharset(), "column %s should not carry a charset", name)
	}
}

func TestEffectiveCharsetCollation(t *testing.T) {
	tests := []struct {
		name          string
		createTable   string
		column        string
		wantCharset   string
		wantCollation string
	}{
		{
			// The case the diff deliberately leaves alone but a linter must
			// resolve: MySQL 8.0's utf8mb4 default is utf8mb4_0900_ai_ci, and
			// SHOW CREATE TABLE omits it precisely because it is the default.
			name:          "table charset only fills in the charset default",
			createTable:   "CREATE TABLE t (c VARCHAR(10)) DEFAULT CHARSET=utf8mb4",
			column:        "c",
			wantCharset:   "utf8mb4",
			wantCollation: "utf8mb4_0900_ai_ci",
		},
		{
			name:          "explicit table collation wins over the charset default",
			createTable:   "CREATE TABLE t (c VARCHAR(10)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
			column:        "c",
			wantCharset:   "utf8mb4",
			wantCollation: "utf8mb4_general_ci",
		},
		{
			name:          "column collation wins over the table default",
			createTable:   "CREATE TABLE t (c VARCHAR(10) COLLATE utf8mb4_bin) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
			column:        "c",
			wantCharset:   "utf8mb4",
			wantCollation: "utf8mb4_bin",
		},
		{
			// A column charset selects that charset's default collation, not
			// the table's collation.
			name:          "column charset only takes the charset default, not the table collation",
			createTable:   "CREATE TABLE t (c VARCHAR(10) CHARACTER SET latin1) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
			column:        "c",
			wantCharset:   "latin1",
			wantCollation: "latin1_swedish_ci",
		},
		{
			name:          "no charset anywhere is undeterminable",
			createTable:   "CREATE TABLE t (c VARCHAR(10))",
			column:        "c",
			wantCharset:   "",
			wantCollation: "",
		},
		{
			name:          "legacy utf8 spelling folds onto utf8mb3",
			createTable:   "CREATE TABLE t (c VARCHAR(10)) DEFAULT CHARSET=utf8",
			column:        "c",
			wantCharset:   "utf8mb3",
			wantCollation: "utf8mb3_general_ci",
		},
		{
			name:          "utf8mb3 spelling resolves identically",
			createTable:   "CREATE TABLE t (c VARCHAR(10)) DEFAULT CHARSET=utf8mb3",
			column:        "c",
			wantCharset:   "utf8mb3",
			wantCollation: "utf8mb3_general_ci",
		},
		{
			name:          "explicit utf8_ collation folds onto utf8mb3_",
			createTable:   "CREATE TABLE t (c VARCHAR(10) COLLATE utf8_bin) DEFAULT CHARSET=utf8mb3",
			column:        "c",
			wantCharset:   "utf8mb3",
			wantCollation: "utf8mb3_bin",
		},
		{
			name:          "table collation implies its charset",
			createTable:   "CREATE TABLE t (c VARCHAR(10)) COLLATE=latin1_general_cs",
			column:        "c",
			wantCharset:   "latin1",
			wantCollation: "latin1_general_cs",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ct, err := ParseCreateTable(tc.createTable)
			require.NoError(t, err)
			col := ct.Columns.ByName(tc.column)
			require.NotNil(t, col)
			charset, collation := col.EffectiveCharsetCollation(ct)
			require.Equal(t, tc.wantCharset, charset)
			require.Equal(t, tc.wantCollation, collation)
		})
	}
}

func TestDefaultCollationForCharset(t *testing.T) {
	tests := []struct {
		name          string
		wantCharset   string
		wantCollation string
		wantOK        bool
	}{
		{name: "utf8mb4", wantCharset: "utf8mb4", wantCollation: "utf8mb4_0900_ai_ci", wantOK: true},
		{name: "UTF8MB4", wantCharset: "utf8mb4", wantCollation: "utf8mb4_0900_ai_ci", wantOK: true},
		{name: "latin1", wantCharset: "latin1", wantCollation: "latin1_swedish_ci", wantOK: true},
		// Both spellings normalize the same way, so a value from here compares
		// equal to one from EffectiveCharsetCollation.
		{name: "utf8", wantCharset: "utf8mb3", wantCollation: "utf8mb3_general_ci", wantOK: true},
		{name: "utf8mb3", wantCharset: "utf8mb3", wantCollation: "utf8mb3_general_ci", wantOK: true},
		{name: "binary", wantCharset: "binary", wantCollation: "binary", wantOK: true},
		{name: "utf8mb5", wantOK: false},
		{name: "", wantOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cs, collation, ok := DefaultCollationForCharset(tc.name)
			require.Equal(t, tc.wantOK, ok)
			require.Equal(t, tc.wantCharset, cs)
			require.Equal(t, tc.wantCollation, collation)
		})
	}
}

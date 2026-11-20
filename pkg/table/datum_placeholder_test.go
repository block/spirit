package table

import (
	"database/sql"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// TestDatumPlaceholderMatchesLiteral checks, on the server, that a datum's
// placeholder expression with its bound argument is the same value as the
// datum's literal: the same bytes, character set and collation. The applier
// binds large values through Placeholder and writes every other value as a
// literal, so the two must store the same thing for every kind of datum
// String renders, including values that are not valid UTF-8 and strings in a
// charset other than the connection's.
func TestDatumPlaceholderMatchesLiteral(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	// The applier's sessions: utf8mb4 with a binary collation.
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["charset"] = "utf8mb4"
	cfg.Collation = "utf8mb4_bin"
	db, err := sql.Open("block-mysql", cfg.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	ti, err := NewTableInfoFromMeta("mydb", "t1", []ColumnMeta{
		{Name: "id", MySQLType: "int"},
		{Name: "blob", MySQLType: "longblob"},
		{Name: "text", MySQLType: "longtext", Collation: "utf8mb4_0900_ai_ci"},
		{Name: "l1", MySQLType: "varchar(20)", Collation: "latin1_swedish_ci"},
		{Name: "gb", MySQLType: "char(4)", Collation: "gbk_chinese_ci"},
		{Name: "u16", MySQLType: "varchar(20)", Collation: "utf16_general_ci"},
	}, []string{"id"})
	require.NoError(t, err)

	for _, tc := range []struct {
		name     string
		col      string
		value    any
		wantExpr string
	}{
		{"binary", "blob", []byte{0x00, 0xff, 0x27, 0x5c, 0x3f}, "CAST(? AS BINARY)"},
		{"empty binary", "blob", []byte{}, "CAST(? AS BINARY)"},
		{"binary that is valid UTF-8", "blob", []byte("é?"), "CAST(? AS BINARY)"},
		{"text that is not valid UTF-8", "text", []byte{0xc3, 0x28, 0x80}, "CAST(? AS BINARY)"},
		{"text with quotes and placeholders", "text", `it's "a" \ ? é 😀`, "?"},
		{"latin1", "l1", []byte("caf\xe9"), "CONVERT(CAST(? AS BINARY) USING latin1)"},
		{"latin1 bytes that are valid UTF-8", "l1", "\xc3\xa9", "CONVERT(CAST(? AS BINARY) USING latin1)"},
		{"gbk", "gb", "\xd0\xb0", "CONVERT(CAST(? AS BINARY) USING gbk)"},
		{"utf16", "u16", "\x00M\xd8\x3d\xde\x00", "CONVERT(CAST(? AS BINARY) USING utf16)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ct, err := ti.BinlogColumnType(tc.col)
			require.NoError(t, err)
			datum, err := NewDatumFromValueWithType(tc.value, ct)
			require.NoError(t, err)
			expr, arg, ok := datum.Placeholder()
			require.True(t, ok)
			require.Equal(t, tc.wantExpr, expr)

			literal := datum.String()
			var (
				literalHex, literalCharset, literalCollation string
				boundHex, boundCharset, boundCollation       string
			)
			query := "SELECT HEX(" + literal + "), CHARSET(" + literal + "), COLLATION(" + literal + "), " +
				"HEX(" + expr + "), CHARSET(" + expr + "), COLLATION(" + expr + ")"
			require.NoError(t, db.QueryRowContext(t.Context(), query, arg, arg, arg).Scan(
				&literalHex, &literalCharset, &literalCollation, &boundHex, &boundCharset, &boundCollation))
			require.Equal(t, literalHex, boundHex, "bytes")
			require.Equal(t, literalCharset, boundCharset, "charset")
			require.Equal(t, literalCollation, boundCollation, "collation")
		})
	}

	for _, value := range []any{nil, int64(7)} {
		datum, err := NewDatumFromValueWithType(value, NewColumnType("int"))
		require.NoError(t, err)
		_, _, ok := datum.Placeholder()
		require.False(t, ok, "%v has a short literal and no placeholder", value)
	}
}

package table

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"testing"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBinlogColumnType renders values the way a binlog row image delivers
// them. A string column in a charset other than utf8mb4/utf8mb3 carries its own
// bytes, which must be emitted with the charset's introducer: quoted, latin1
// C3 A9 ('Ã©') would be read as utf8mb4 'é'. Every other column renders as
// NewColumnType renders it, and so does the same latin1 column on the copy
// path, where the driver has already converted the value to utf8mb4.
func TestBinlogColumnType(t *testing.T) {
	ti, err := NewTableInfoFromMeta("mydb", "t1", []ColumnMeta{
		{Name: "id", MySQLType: "int"},
		{Name: "l1", MySQLType: "varchar(20)", Collation: "latin1_swedish_ci"},
		{Name: "l1_text", MySQLType: "text", Charset: "latin1", Collation: "latin1_bin"},
		{Name: "gb", MySQLType: "char(4)", Collation: "gbk_chinese_ci"},
		{Name: "u16", MySQLType: "varchar(20)", Collation: "utf16_general_ci"},
		{Name: "u8", MySQLType: "varchar(20)", Collation: "utf8mb4_0900_ai_ci"},
		{Name: "u3", MySQLType: "varchar(20)", Collation: "utf8_general_ci"},
		{Name: "e", MySQLType: "enum('café','thé')", Collation: "latin1_swedish_ci"},
		{Name: "s", MySQLType: "set('é','ü')", Collation: "latin1_swedish_ci"},
		{Name: "b", MySQLType: "varbinary(20)"},
		{Name: "unknown", MySQLType: "varchar(20)", CollationUnknown: true},
	}, []string{"id"})
	require.NoError(t, err)

	for col, want := range map[string]string{
		"id": "", "l1": "latin1", "l1_text": "latin1", "gb": "gbk", "u16": "utf16",
		"u8": "", "u3": "", "e": "", "s": "", "b": "", "unknown": "",
	} {
		assert.Equal(t, want, ti.BinlogCharset(col), col)
	}

	for _, tc := range []struct {
		col   string
		value any
		want  string
	}{
		{"id", int32(7), "7"},
		{"l1", "\xc3\xa9", "_latin1 0xc3a9"},            // valid UTF-8, used to be quoted
		{"l1", []byte("caf\xe9"), "_latin1 0x636166e9"}, // not UTF-8, used to be a binary literal
		{"l1", "a\"b", "_latin1 0x612262"},
		{"l1", "", "_latin1 x''"},
		{"l1", nil, "NULL"},
		{"l1_text", []byte("\xc3\xa9"), "_latin1 0xc3a9"},
		{"gb", "\xd0\xb0", "_gbk 0xd0b0"},
		{"u16", "\x00M", "_utf16 0x004d"},
		{"u8", "é", `"é"`},
		{"u3", "é", `"é"`},
		{"e", "café", `"café"`}, // DecodeBinlogRow's element text, from information_schema
		{"s", "é,ü", `"é,ü"`},
		{"b", []byte("abc"), "0x616263"},
		{"unknown", "é", `"é"`},
	} {
		ct, err := ti.BinlogColumnType(tc.col)
		require.NoError(t, err)
		d, err := NewDatumFromValueWithType(tc.value, ct)
		require.NoError(t, err)
		assert.Equal(t, tc.want, d.String(), "%s %q", tc.col, tc.value)
	}

	// The copy path reads values over the utf8mb4 connection, so the same
	// column's values are utf8mb4 there and stay quoted.
	tp, ok := ti.GetColumnMySQLType("l1")
	require.True(t, ok)
	d, err := NewDatumFromValueWithType("é", NewColumnType(tp))
	require.NoError(t, err)
	assert.Equal(t, `"é"`, d.String())

	_, err = ti.BinlogColumnType("missing")
	require.ErrorContains(t, err, `column "missing" not found`)

	// The charset is spliced into SQL as an introducer, so a name that is not
	// one is refused rather than emitted.
	_, err = NewTableInfoFromMeta("mydb", "t2", []ColumnMeta{
		{Name: "c", MySQLType: "varchar(20)", Charset: "latin1 x", Collation: "latin1 x_bin"},
	}, nil)
	require.ErrorContains(t, err, "unexpected charset name")
}

// TestBinlogCharsetLiteralRoundTrip checks the introduced literal against a
// real server for every charset it supports: the literal must be the stored
// bytes, and must find the row by equality. The stored bytes are what a binlog
// row image carries for the column. Deprecated charsets (ucs2, macroman, ...)
// are included; naming them raises a deprecation warning, not an error.
func TestBinlogCharsetLiteralRoundTrip(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	rows, err := db.QueryContext(t.Context(), "SELECT CHARACTER_SET_NAME FROM information_schema.CHARACTER_SETS WHERE CHARACTER_SET_NAME <> 'binary' ORDER BY 1")
	require.NoError(t, err)
	var charsets []string
	for rows.Next() {
		var cs string
		require.NoError(t, rows.Scan(&cs))
		charsets = append(charsets, cs)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Contains(t, charsets, "latin1")

	testutils.RunSQL(t, "DROP TABLE IF EXISTS binlog_charset_literal")
	t.Cleanup(func() { testutils.RunSQL(t, "DROP TABLE IF EXISTS binlog_charset_literal") })
	for _, cs := range charsets {
		t.Run(cs, func(t *testing.T) {
			testutils.RunSQL(t, "DROP TABLE IF EXISTS binlog_charset_literal")
			testutils.RunSQL(t, fmt.Sprintf("CREATE TABLE binlog_charset_literal (id INT NOT NULL PRIMARY KEY, c VARCHAR(40) CHARACTER SET %s NOT NULL)", cs))
			// Characters the charset lacks are stored as '?', which is fine:
			// whatever bytes are stored are what the binlog carries.
			testutils.RunSQL(t, "INSERT IGNORE INTO binlog_charset_literal VALUES (1, 'Mé Ã© €中'), (2, ''), (3, 'M')")

			ti := NewTableInfo(db, "test", "binlog_charset_literal")
			require.NoError(t, ti.SetInfo(t.Context()))
			ct, err := ti.BinlogColumnType("c")
			require.NoError(t, err)
			// MySQL before 8.0.30 lists utf8mb3 as utf8.
			if cs == "utf8mb4" || cs == "utf8mb3" || cs == "utf8" {
				require.Empty(t, ti.BinlogCharset("c"))
			} else {
				require.Equal(t, cs, ti.BinlogCharset("c"))
			}

			for id := 1; id <= 3; id++ {
				var storedHex string
				require.NoError(t, db.QueryRowContext(t.Context(), "SELECT HEX(c) FROM binlog_charset_literal WHERE id = ?", id).Scan(&storedHex))
				stored, err := hex.DecodeString(storedHex)
				require.NoError(t, err)
				d, err := NewDatumFromValueWithType(stored, ct)
				require.NoError(t, err)

				var gotHex string
				var matches int
				require.NoError(t, db.QueryRowContext(t.Context(),
					fmt.Sprintf("SELECT HEX(%s), (SELECT COUNT(*) FROM binlog_charset_literal WHERE c = %s AND id = %d)", d.String(), d.String(), id)).Scan(&gotHex, &matches))
				assert.Equal(t, storedHex, gotHex, "row %d literal %s", id, d.String())
				assert.Equal(t, 1, matches, "row %d literal %s", id, d.String())
			}
		})
	}
}

// TestTableInfoRefusesUnsafeCharsetName: a column's charset is spliced into
// SQL as an introducer and as a CONVERT target, and its collation as a COLLATE
// clause, so a name that is not a plausible charset or collation name is
// refused when the table is built.
func TestTableInfoRefusesUnsafeCharsetName(t *testing.T) {
	for _, cs := range []string{"latin1 0x00) --", "latin1'", "lat-in1"} {
		_, err := NewTableInfoFromMeta("test", "t", []ColumnMeta{
			{Name: "id", MySQLType: "int"},
			{Name: "s", MySQLType: "varchar(10)", Charset: cs, Collation: "latin1_swedish_ci"},
		}, []string{"id"})
		require.ErrorContains(t, err, "unexpected charset name", cs)
	}
	for _, coll := range []string{"latin1_swedish_ci --", "latin1_bin'", "latin1-bin"} {
		_, err := NewTableInfoFromMeta("test", "t", []ColumnMeta{
			{Name: "id", MySQLType: "int"},
			{Name: "s", MySQLType: "varchar(10)", Charset: "latin1", Collation: coll},
		}, []string{"id"})
		require.ErrorContains(t, err, "unexpected collation name", coll)
	}
	ti, err := NewTableInfoFromMeta("test", "t", []ColumnMeta{
		{Name: "id", MySQLType: "int"},
		{Name: "s", MySQLType: "varchar(10)", Charset: "LATIN1", Collation: "latin1_swedish_ci"},
	}, []string{"id"})
	require.NoError(t, err)
	require.Equal(t, "latin1", ti.BinlogCharset("s"))
}

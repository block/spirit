package applier

import (
	"database/sql"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// TestApplierBinlogCharsets applies binlog row images of latin1, gbk and utf16
// columns with UpsertRows and DeleteKeys, into a target with the same charsets
// and into one converted to utf8mb4. A row image carries each column's own
// bytes. They used to be emitted as a quoted string whenever they were valid
// UTF-8, which MySQL read as utf8mb4: latin1 C3 A9 ('Ã©') was stored as 'é',
// utf16 00 4D ('M') became two characters, and a DELETE of the latin1 key
// C3 A9 removed the row whose key is E9 ('é'). Bytes that are not valid UTF-8
// were emitted as a binary literal, which a utf8mb4 target rejects.
func TestApplierBinlogCharsets(t *testing.T) {
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS applier_binlog_charsets")
	testutils.RunSQL(t, "CREATE DATABASE applier_binlog_charsets")
	base, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	cfg := base.Clone()
	cfg.DBName = "applier_binlog_charsets"
	db, err := sql.Open("block-mysql", cfg.FormatDSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	for _, stmt := range []string{
		`CREATE TABLE src (
			id VARCHAR(10) CHARACTER SET latin1 NOT NULL PRIMARY KEY,
			l1 TEXT CHARACTER SET latin1 NOT NULL,
			gb VARCHAR(20) CHARACTER SET gbk NOT NULL,
			u16 VARCHAR(20) CHARACTER SET utf16 NOT NULL
		)`,
		"CREATE TABLE dst_same LIKE src",
		`CREATE TABLE dst_utf8mb4 (
			id VARCHAR(10) CHARACTER SET utf8mb4 NOT NULL PRIMARY KEY,
			l1 TEXT CHARACTER SET utf8mb4 NOT NULL,
			gb VARCHAR(20) CHARACTER SET utf8mb4 NOT NULL,
			u16 VARCHAR(20) CHARACTER SET utf8mb4 NOT NULL
		)`,
	} {
		_, err := db.ExecContext(t.Context(), stmt)
		require.NoError(t, err)
	}
	info := func(name string) *table.TableInfo {
		ti := table.NewTableInfo(db, cfg.DBName, name)
		require.NoError(t, ti.SetInfo(t.Context()))
		return ti
	}
	src := info("src")

	applier, err := New([]Target{{DB: db, Config: cfg, KeyRange: "0"}}, NewApplierDefaultConfig())
	require.NoError(t, err)

	// Row images as the binlog decodes them: VARCHAR/CHAR as string, TEXT as
	// []byte, each in its column's charset.
	images := []LogicalRow{
		{RowImage: []any{"\xc3\xa9", []byte("\xc3\xa9 caf\xe9"), "\xd0\xb0", "\x00M\x00\xe9"}},
		{RowImage: []any{"\xe9", []byte(""), "", "\x00M"}},
		{RowImage: []any{"a", []byte("a"), "a", "\x00a"}},
	}
	for _, tc := range []struct {
		target string
		want   []string // id,l1,gb,u16 in hex, ordered by id
	}{
		{"dst_same", []string{
			"61,61,61,0061",
			"C3A9,C3A920636166E9,D0B0,004D00E9",
			"E9,,,004D",
		}},
		{"dst_utf8mb4", []string{
			"61,61,61,61",
			"C383C2A9,C383C2A920636166C3A9,E982AA,4DC3A9",
			"C3A9,,,4D",
		}},
	} {
		t.Run(tc.target, func(t *testing.T) {
			dst := info(tc.target)
			_, err := applier.UpsertRows(t.Context(), table.NewColumnMapping(src, dst, nil), images, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, hexRows(t, db, tc.target))

			// Delete the key whose bytes, read as utf8mb4, are the other key.
			_, err = applier.DeleteKeys(t.Context(), src, dst, [][]any{{"\xc3\xa9"}}, nil)
			require.NoError(t, err)
			require.Equal(t, []string{tc.want[0], tc.want[2]}, hexRows(t, db, tc.target))
			_, err = applier.DeleteKeys(t.Context(), src, dst, [][]any{{"\xe9"}}, nil)
			require.NoError(t, err)
			require.Equal(t, []string{tc.want[0]}, hexRows(t, db, tc.target))
		})
	}
}

func hexRows(t *testing.T, db *sql.DB, tbl string) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "SELECT CONCAT_WS(',', HEX(id), HEX(l1), HEX(gb), HEX(u16)) FROM "+tbl+" ORDER BY HEX(id)")
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	var got []string
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		got = append(got, s)
	}
	require.NoError(t, rows.Err())
	return got
}

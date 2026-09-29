package migration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// runWithDMLAfterChecksum drives the runner through the phases of Run (setup,
// copy, post-copy with the checksum, cutover) and calls afterChecksum between a
// passing checksum and the cutover. A write there reaches the new table only
// through the binlog applier, and nothing verifies it before the cutover: that
// is the window of any write that commits after the checksum's snapshot, which
// on a large table can be hours long.
//
// It skips attemptMySQLDDL, so even an ALTER MySQL could apply as INSTANT is
// copied.
func runWithDMLAfterChecksum(t *testing.T, m *Runner, afterChecksum func()) error {
	t.Helper()
	ctx := t.Context()
	m.status.Begin()
	m.dbConfig = dbconn.NewDBConfig()
	var err error
	m.db, err = dbconn.New(testutils.DSN(), m.dbConfig)
	require.NoError(t, err)
	m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.changes[0].stmt.Table)
	require.NoError(t, m.changes[0].table.SetInfo(ctx))
	require.NoError(t, m.setup(ctx))

	m.status.Set(status.CopyRows)
	chunkCopier, ok := m.copier.(copier.ChunkCopier)
	require.True(t, ok)
	for {
		chunk, err := m.copyChunker.Next()
		if errors.Is(err, table.ErrTableIsRead) {
			break
		}
		require.NoError(t, err)
		require.NoError(t, chunkCopier.CopyChunk(ctx, chunk))
	}
	require.NoError(t, m.replClient.BlockWait(ctx))
	require.NoError(t, m.replClient.SetWatermarkOptimization(ctx, false))
	if err := m.postCopyPhase(ctx); err != nil {
		return err
	}
	afterChecksum()
	require.NoError(t, m.replClient.BlockWait(ctx))

	return m.status.Do(status.CutOver, func() error {
		cutover, err := NewCutOver(m.db, []*cutoverConfig{{
			table:        m.changes[0].table,
			newTable:     m.changes[0].newTable,
			oldTableName: m.changes[0].oldTableName(),
		}}, m.replClient, m.dbConfig, m.logger)
		if err != nil {
			return err
		}
		if err := m.changes[0].dropOldTable(ctx); err != nil {
			return err
		}
		return cutover.Run(ctx)
	})
}

// charsetColumns is a table with a string column in each charset whose bytes
// are not UTF-8 or are not read as the same text when they happen to be valid
// UTF-8. It includes a latin1 ENUM with a non-ASCII element, which the binlog
// carries as an ordinal and is decoded separately, and a utf8mb4 column.
const charsetColumns = `
	id INT NOT NULL PRIMARY KEY,
	l1 VARCHAR(40) CHARACTER SET latin1 NULL,
	l1_text TEXT CHARACTER SET latin1 NULL,
	l1_char CHAR(10) CHARACTER SET latin1 NULL,
	gb VARCHAR(40) CHARACTER SET gbk NULL,
	u16 VARCHAR(40) CHARACTER SET utf16 NULL,
	u16_char CHAR(4) CHARACTER SET utf16 NULL,
	u16le VARCHAR(40) CHARACTER SET utf16le NULL,
	ucs VARCHAR(40) CHARACTER SET ucs2 NULL,
	u32 VARCHAR(40) CHARACTER SET utf32 NULL,
	e ENUM('café', 'thé') CHARACTER SET latin1 NULL,
	u8 VARCHAR(40) CHARACTER SET utf8mb4 NULL`

// charsetColumnNames are the columns of charsetColumns after id.
var charsetColumnNames = []string{"l1", "l1_text", "l1_char", "gb", "u16", "u16_char", "u16le", "ucs", "u32", "e", "u8"}

// charsetValues is a VALUES tuple (after id) for charsetColumns. The latin1
// values include 'Ã©', whose latin1 bytes C3 A9 are valid UTF-8 for 'é', and
// gbk D0B0 is valid UTF-8 for a character gbk stores as A7D1. Every utf16,
// ucs2 and utf32 value is valid UTF-8 byte for byte ('M' is 00 4D).
const charsetValues = `'Ã© café', 'naïve Ã©', 'Ã©', X'D0B0', 'Mé', 'M', 'M', 'M', 'M€', 'café', 'café'`

// charsetHex returns every row of tbl as its charsetColumnNames in hex, keyed
// by id, so two tables can be compared byte for byte.
func charsetHex(t *testing.T, tt *testutils.TestTable, tbl string) map[int]string {
	t.Helper()
	exprs := make([]string, len(charsetColumnNames))
	for i, col := range charsetColumnNames {
		exprs[i] = fmt.Sprintf("IFNULL(HEX(%s), 'NULL')", col)
	}
	rows, err := tt.DB.QueryContext(t.Context(), fmt.Sprintf("SELECT id, CONCAT_WS(',', %s) FROM %s", strings.Join(exprs, ", "), tbl))
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	got := map[int]string{}
	for rows.Next() {
		var id int
		var h string
		require.NoError(t, rows.Scan(&id, &h))
		got[id] = h
	}
	require.NoError(t, rows.Err())
	return got
}

// TestBinlogCharsetValuesAfterChecksum writes non-ASCII values into latin1,
// gbk, utf16, utf16le, ucs2 and utf32 columns after the checksum has passed,
// so they reach the new table only through the binlog applier. The binlog row
// image carries each column's own bytes. They used to be emitted as a quoted
// string whenever they were valid UTF-8, which MySQL read as utf8mb4 and
// converted: latin1 'Ã©' was stored as 'é', and utf16 'M' became two
// characters. The migration succeeded with the rows corrupted.
//
// The same statements are applied to a reference table, and every column must
// match it byte for byte after the cutover.
func TestBinlogCharsetValuesAfterChecksum(t *testing.T) {
	tt := testutils.NewTestTable(t, "charset_binlog", "CREATE TABLE charset_binlog ("+charsetColumns+")")
	testutils.NewTestTable(t, "charset_binlog_ref", "CREATE TABLE charset_binlog_ref ("+charsetColumns+")")
	before := []string{
		"INSERT INTO %s VALUES (1, " + charsetValues + ")",
		"INSERT INTO %s (id, l1, u16) VALUES (2, 'a', 'b')",
	}
	after := []string{
		"INSERT INTO %s VALUES (3, " + charsetValues + ")",
		// Every column of row 2 changes, including to the empty string.
		"UPDATE %s SET l1 = 'Ã©', l1_text = '', l1_char = 'é', gb = '中文', u16 = '', u16_char = 'é', u16le = 'é', ucs = 'é', u32 = '', e = 'thé', u8 = 'Ã©' WHERE id = 2",
		"INSERT INTO %s (id, l1, gb, u16) VALUES (4, '', '', '')",
	}
	for _, stmt := range before {
		testutils.RunSQL(t, fmt.Sprintf(stmt, "charset_binlog"))
		testutils.RunSQL(t, fmt.Sprintf(stmt, "charset_binlog_ref"))
	}

	m := NewTestRunner(t, "charset_binlog", "ADD COLUMN c INT")
	defer func() { _ = m.Close() }()
	err := runWithDMLAfterChecksum(t, m, func() {
		for _, stmt := range after {
			testutils.RunSQL(t, fmt.Sprintf(stmt, "charset_binlog"))
		}
	})
	require.NoError(t, err)
	for _, stmt := range after {
		testutils.RunSQL(t, fmt.Sprintf(stmt, "charset_binlog_ref"))
	}

	want := charsetHex(t, tt, "charset_binlog_ref")
	require.Len(t, want, 4)
	require.Equal(t, want, charsetHex(t, tt, "charset_binlog"))
}

// TestBinlogCharsetPrimaryKeyDelete deletes and updates rows by a latin1 or
// gbk primary key after the checksum. Each table holds two keys where the
// bytes of one, read as utf8mb4, are the other: latin1 C3A9 ('Ã©') and E9
// ('é'), gbk D0B0 and A7D1. The replayed DELETE of the first used to remove
// the second and leave the first in place.
func TestBinlogCharsetPrimaryKeyDelete(t *testing.T) {
	for _, tc := range []struct {
		charset      string
		lookalike    string // valid UTF-8 whose utf8mb4 reading is other
		other        string
		wantAfterAll []string
	}{
		{"latin1", "C3A9", "E9", []string{"61:30", "E9:21"}},
		{"gbk", "D0B0", "A7D1", []string{"61:30", "A7D1:21"}},
	} {
		t.Run(tc.charset, func(t *testing.T) {
			tbl := "charset_pk_" + tc.charset
			tt := testutils.NewTestTable(t, tbl, fmt.Sprintf(`CREATE TABLE %s (
				id VARCHAR(10) CHARACTER SET %s NOT NULL PRIMARY KEY,
				v INT NOT NULL
			)`, tbl, tc.charset))
			testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s VALUES (X'%s', 10), (X'%s', 20), ('a', 30)", tbl, tc.lookalike, tc.other))

			m := NewTestRunner(t, tbl, "ADD COLUMN c INT")
			defer func() { _ = m.Close() }()
			err := runWithDMLAfterChecksum(t, m, func() {
				testutils.RunSQL(t, fmt.Sprintf("DELETE FROM %s WHERE id = X'%s'", tbl, tc.lookalike))
				testutils.RunSQL(t, fmt.Sprintf("UPDATE %s SET v = 21 WHERE id = X'%s'", tbl, tc.other))
			})
			require.NoError(t, err)

			var got []string
			rows, err := tt.DB.QueryContext(t.Context(), "SELECT CONCAT(HEX(id), ':', v) FROM "+tbl+" ORDER BY HEX(id)")
			require.NoError(t, err)
			for rows.Next() {
				var s string
				require.NoError(t, rows.Scan(&s))
				got = append(got, s)
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			require.Equal(t, tc.wantAfterAll, got)
		})
	}
}

// TestLatin1ToUtf8mb4WithConcurrentDML converts a latin1 table to utf8mb4
// while non-ASCII rows are written through the binlog applier. latin1 E9 ('é')
// is not valid UTF-8 and used to be emitted as a binary literal, which a
// utf8mb4 column rejects (warning 1366), failing the migration. latin1 C3A9
// ('Ã©') is valid UTF-8 and used to be stored as 'é' instead of 'Ã©'.
//
// The DML runs during the copy (verified by the checksum) and after the
// checksum (verified by nothing).
func TestLatin1ToUtf8mb4WithConcurrentDML(t *testing.T) {
	// latin1 bytes written to the source, and their utf8mb4 conversion.
	values := []struct{ latin1, utf8mb4 string }{
		{"E9", "C3A9"},
		{"C3A9", "C383C2A9"},
		{"63616645", "63616645"},
	}
	wantConverted := func(t *testing.T, tt *testutils.TestTable, tbl string, ids []int64) {
		t.Helper()
		for i, id := range ids {
			var got string
			require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT HEX(b) FROM "+tbl+" WHERE id = ?", id).Scan(&got))
			require.Equal(t, values[i%len(values)].utf8mb4, got, "row %d, latin1 %s", id, values[i%len(values)].latin1)
		}
		var charset string
		require.NoError(t, tt.DB.QueryRowContext(t.Context(),
			"SELECT CHARACTER_SET_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = 'b'", tbl).Scan(&charset))
		require.Equal(t, "utf8mb4", charset)
	}

	t.Run("during copy", func(t *testing.T) {
		tbl := "charset_l1_to_u8_copy"
		tt := testutils.NewTestTable(t, tbl, "CREATE TABLE "+tbl+" (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, b VARCHAR(40) NOT NULL) CHARSET=latin1")
		tt.SeedRows(t, "INSERT INTO "+tbl+" (b) SELECT 'a'", 5000)

		m := NewTestRunner(t, tbl, "CONVERT TO CHARACTER SET utf8mb4", WithThreads(1), WithTestThrottler())
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		dmlDone := make(chan struct{})
		var ids []int64
		go func() {
			defer close(dmlDone)
			if !waitForCopyRows(t, ctx, m) {
				return
			}
			// Update seeded rows (some already copied) and insert new ones.
			for i, v := range values {
				if _, err := tt.DB.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET b = X'%s' WHERE id = ?", tbl, v.latin1), i+1); err != nil {
					return
				}
				ids = append(ids, int64(i+1))
			}
			for _, v := range values {
				res, err := tt.DB.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (b) VALUES (X'%s')", tbl, v.latin1))
				if err != nil {
					return
				}
				id, err := res.LastInsertId()
				if err != nil {
					return
				}
				ids = append(ids, id)
			}
		}()
		migrationErr := m.Run(ctx)
		cancel()
		<-dmlDone
		require.NoError(t, m.Close())
		require.NoError(t, migrationErr)
		require.Len(t, ids, 2*len(values), "the DML must run during the migration")
		wantConverted(t, tt, tbl, ids)
	})

	t.Run("after checksum", func(t *testing.T) {
		tbl := "charset_l1_to_u8_after"
		tt := testutils.NewTestTable(t, tbl, "CREATE TABLE "+tbl+" (id INT NOT NULL PRIMARY KEY, b VARCHAR(40) NOT NULL) CHARSET=latin1")
		ids := make([]int64, 0, 2*len(values))
		for i := range values {
			testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s VALUES (%d, 'a')", tbl, i+1))
		}
		m := NewTestRunner(t, tbl, "CONVERT TO CHARACTER SET utf8mb4")
		defer func() { _ = m.Close() }()
		err := runWithDMLAfterChecksum(t, m, func() {
			for i, v := range values {
				testutils.RunSQL(t, fmt.Sprintf("UPDATE %s SET b = X'%s' WHERE id = %d", tbl, v.latin1, i+1))
				ids = append(ids, int64(i+1))
			}
			for i, v := range values {
				id := int64(len(values) + i + 1)
				testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s VALUES (%d, X'%s')", tbl, id, v.latin1))
				ids = append(ids, id)
			}
		})
		require.NoError(t, err)
		wantConverted(t, tt, tbl, ids)
	})
}

// TestCopyNonUTF8Charsets copies non-ASCII values in every charset of
// charsetColumns, with no concurrent DML. The copier reads rows over the
// utf8mb4 connection, which converts them to utf8mb4, so quoted literals are
// correct there and must stay quoted: emitting those utf8mb4 bytes with the
// column's charset introducer would corrupt them.
func TestCopyNonUTF8Charsets(t *testing.T) {
	tt := testutils.NewTestTable(t, "charset_copy", "CREATE TABLE charset_copy ("+charsetColumns+")")
	testutils.NewTestTable(t, "charset_copy_ref", "CREATE TABLE charset_copy_ref ("+charsetColumns+")")
	for _, tbl := range []string{"charset_copy", "charset_copy_ref"} {
		testutils.RunSQL(t, "INSERT INTO "+tbl+" VALUES (1, "+charsetValues+")")
		testutils.RunSQL(t, "INSERT INTO "+tbl+" (id, l1, gb, u16, u32) VALUES (2, '', '', '', '')")
		testutils.RunSQL(t, "INSERT INTO "+tbl+" (id) VALUES (3)")
	}
	m := NewTestRunner(t, "charset_copy", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())

	want := charsetHex(t, tt, "charset_copy_ref")
	require.Len(t, want, 3)
	require.Equal(t, want, charsetHex(t, tt, "charset_copy"))
}

package applier

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/contract"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// binaryLiteralTarget creates dbName with one table and returns an applier
// that writes to it over dbconn connections (spirit's session settings and
// packet limit, which the binary literal depends on), plus the column mapping
// for that table onto itself.
func binaryLiteralTarget(t *testing.T, dbName, createTable string) (*MySQLApplier, *table.ColumnMapping, *table.TableInfo) {
	t.Helper()
	testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dbName)
	testutils.RunSQL(t, "CREATE DATABASE "+dbName)
	t.Cleanup(func() { testutils.RunSQL(t, "DROP DATABASE IF EXISTS "+dbName) })

	dsn := testutils.DSNForDatabase(dbName)
	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)
	db, err := dbconn.New(dsn, dbconn.NewDBConfig())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })

	_, err = db.ExecContext(t.Context(), createTable)
	require.NoError(t, err)
	tbl := table.NewTableInfo(db, dbName, "t")
	require.NoError(t, tbl.SetInfo(t.Context()))

	a, err := New([]Target{{DB: db, Config: cfg, KeyRange: "0"}}, NewApplierDefaultConfig())
	require.NoError(t, err)
	return a, table.NewColumnMapping(tbl, tbl, nil), tbl
}

// applyAndWait writes rows through the copy path (Apply -> INSERT IGNORE).
func applyAndWait(t *testing.T, a *MySQLApplier, tbl *table.TableInfo, mapping *table.ColumnMapping, rows [][]any) {
	t.Helper()
	require.NoError(t, a.Start(t.Context()))
	defer func() { require.NoError(t, a.Stop()) }()
	chunk := &table.Chunk{Table: tbl, NewTable: tbl, ColumnMapping: mapping}
	errCh := make(chan error, 1)
	require.NoError(t, a.Apply(t.Context(), chunk, rows, func(_ int64, err error) { errCh <- err }))
	require.NoError(t, a.Wait(t.Context()))
	require.NoError(t, <-errCh)
}

// TestApplierBinaryLiteralRoundTrip writes binary values chosen to break an
// escaped literal — every byte value, quotes and backslashes in every
// arrangement, and multibyte UTF-8 lead bytes directly before a quote or a
// backslash — through both write paths, and checks the stored bytes.
func TestApplierBinaryLiteralRoundTrip(t *testing.T) {
	all := make([]byte, 256)
	for i := range all {
		all[i] = byte(i)
	}
	values := [][]byte{
		all,
		{},
		{'\''},
		{'\\'},
		[]byte(`\'`),
		[]byte(`'\`),
		[]byte(`\\`),
		[]byte(`''`),
		[]byte(`\0\n\r\Z\%\_`), // escape sequences as literal text, not as their bytes
		[]byte("ends with a backslash \\"),
		{0x00, 0x00, 0x00},
		{0xE0, '\''}, // 3-byte UTF-8 lead byte before a quote
		{0xF0, 0x9F, 0x98, '\\'},
		{0xC3, '\\', '\''},
		{0xFF, 0xFE, '\'', 0x00, '\\'},
		[]byte("valid UTF-8 é 中 😀 with a quote ' and a backslash \\"),
	}
	// vb holds each value as is, which renders as a _binary 0x... hex literal.
	// b wraps it in table.EscapedBinaryMinBytes of padding, so the same bytes
	// also go through the escaped _binary'...' form, at both ends of a literal.
	pad := bytes.Repeat([]byte{'x'}, table.EscapedBinaryMinBytes)
	large := make([][]byte, len(values))
	for i, v := range values {
		large[i] = slices.Concat(v, pad, v)
	}

	for _, path := range []string{"apply", "upsert"} {
		t.Run(path, func(t *testing.T) {
			a, mapping, tbl := binaryLiteralTarget(t, "applier_binlit_"+path,
				"CREATE TABLE t (id INT NOT NULL PRIMARY KEY, b LONGBLOB, vb VARBINARY(512))")
			rows := make([][]any, len(values))
			for i, v := range values {
				rows[i] = []any{int64(i), large[i], v}
			}
			if path == "apply" {
				applyAndWait(t, a, tbl, mapping, rows)
			} else {
				logical := make([]contract.LogicalRow, len(rows))
				for i, r := range rows {
					logical[i] = contract.LogicalRow{RowImage: r}
				}
				_, err := a.UpsertRows(t.Context(), mapping, logical, nil)
				require.NoError(t, err)
			}

			res, err := tbl.DB().QueryContext(t.Context(), "SELECT id, SHA2(b, 256), HEX(vb) FROM t ORDER BY id")
			require.NoError(t, err)
			defer utils.CloseAndLog(res)
			n := 0
			for res.Next() {
				var id int
				var b, vb string
				require.NoError(t, res.Scan(&id, &b, &vb))
				sum := sha256.Sum256(large[id])
				require.Equalf(t, hex.EncodeToString(sum[:]), b, "LONGBLOB value %d", id)
				want := hex.EncodeToString(values[id])
				require.Equalf(t, want, strings.ToLower(vb), "VARBINARY value %d", id)
				n++
			}
			require.NoError(t, res.Err())
			require.Equal(t, len(values), n)
		})
	}
}

// TestApplierBinaryIntoNumericColumn checks that a binary value written into
// a numeric column converts the way ALTER TABLE ... MODIFY does: as the number
// its text spells, not as a hex literal's number (0x3132 is 12594).
func TestApplierBinaryIntoNumericColumn(t *testing.T) {
	a, mapping, _ := binaryLiteralTarget(t, "applier_binlit_numeric",
		"CREATE TABLE t (id INT NOT NULL PRIMARY KEY, n BIGINT)")
	// The source column is VARBINARY: the value is classified by the source
	// type, as in a VARBINARY -> BIGINT migration.
	db := mapping.TargetTable().DB()
	_, err := db.ExecContext(t.Context(), "CREATE TABLE src (id INT NOT NULL PRIMARY KEY, n VARBINARY(20))")
	require.NoError(t, err)
	src := table.NewTableInfo(db, "applier_binlit_numeric", "src")
	require.NoError(t, src.SetInfo(t.Context()))
	mapping = table.NewColumnMapping(src, mapping.TargetTable(), nil)
	_, err = a.UpsertRows(t.Context(), mapping, []contract.LogicalRow{{RowImage: []any{int64(1), []byte("12")}}}, nil)
	require.NoError(t, err)
	var n int64
	require.NoError(t, mapping.TargetTable().DB().QueryRowContext(t.Context(), "SELECT n FROM t WHERE id = 1").Scan(&n))
	require.Equal(t, int64(12), n)
}

// TestApplierLargeBinaryValue writes one value larger than half of the
// server's max_allowed_packet through both write paths. As a 0x hex literal
// it rendered to a statement over the packet limit, and the copy failed on
// every retry for a row the application had written without trouble.
func TestApplierLargeBinaryValue(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	var maxPacket int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@max_allowed_packet").Scan(&maxPacket))
	utils.CloseAndLog(db)
	if maxPacket > 256<<20 {
		t.Skipf("max_allowed_packet is %d; the value this test needs would be too large to be cheap", maxPacket)
	}
	// Over half the limit (so hex cannot fit) with room for the escaping of
	// random bytes (~2/256) and the rest of the statement.
	size := maxPacket / 10 * 6
	value := make([]byte, size)
	rng := rand.NewChaCha8([32]byte{1})
	_, _ = rng.Read(value)
	sum := sha256.Sum256(value)
	want := hex.EncodeToString(sum[:])

	for _, path := range []string{"apply", "upsert"} {
		t.Run(path, func(t *testing.T) {
			a, mapping, tbl := binaryLiteralTarget(t, "applier_binlit_large_"+path,
				"CREATE TABLE t (id INT NOT NULL PRIMARY KEY, b LONGBLOB)")
			row := []any{int64(1), value}
			if path == "apply" {
				applyAndWait(t, a, tbl, mapping, [][]any{row})
			} else {
				_, err := a.UpsertRows(t.Context(), mapping, []contract.LogicalRow{{RowImage: row}}, nil)
				require.NoError(t, err)
			}
			var length int
			var got string
			require.NoError(t, tbl.DB().QueryRowContext(t.Context(), "SELECT LENGTH(b), SHA2(b, 256) FROM t WHERE id = 1").Scan(&length, &got))
			require.Equal(t, size, length, "stored length, packet limit "+strconv.Itoa(maxPacket))
			require.Equal(t, want, got)
		})
	}
}

// TestApplierDeleteBinaryKeysWithQuotingBytes sends binary primary keys that
// hold quotes, backslashes, NULs and UTF-8 lead bytes through DeleteKeys. A
// key rendered wrongly would match no row, a silent no-op delete that only
// the checksum would catch later.
func TestApplierDeleteBinaryKeysWithQuotingBytes(t *testing.T) {
	a, mapping, tbl := binaryLiteralTarget(t, "applier_binlit_delkeys",
		"CREATE TABLE t (k VARBINARY(64) NOT NULL PRIMARY KEY, v INT NOT NULL)")
	keys := [][]byte{
		{'\''}, {'\\'}, []byte(`\'`), []byte(`'\`), {0x00, 0x01}, {0xFF, 0xFE},
		{0xE0, '\''}, {0xC3, '\\', '\''}, []byte("plain"), {0x7f, 0x80, 0x81},
	}
	rows := make([][]any, len(keys))
	for i, k := range keys {
		rows[i] = []any{k, int64(i)}
	}
	applyAndWait(t, a, tbl, mapping, rows)

	// A bystander whose bytes differ from the quote key only by a prefix.
	_, err := tbl.DB().ExecContext(t.Context(), "INSERT INTO t VALUES (X'AA27', 99)")
	require.NoError(t, err)

	del := make([][]any, len(keys))
	for i, k := range keys {
		del[i] = []any{k}
	}
	n, err := a.DeleteKeys(t.Context(), tbl, tbl, del, nil)
	require.NoError(t, err)
	require.Equal(t, int64(len(keys)), n)

	var left int
	require.NoError(t, tbl.DB().QueryRowContext(t.Context(), "SELECT COUNT(*) FROM t").Scan(&left))
	require.Equal(t, 1, left, "only the bystander remains")
}

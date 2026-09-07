package checksum

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"

	mysql "github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// recopierHarness wires up everything MySQLRecopier needs against real
// MySQL: a source table in the DSN database, an identical target table in
// a unique throwaway database, TableInfos for both sides, and a started
// SingleTargetApplier pointed at the target. This mirrors the production
// wiring in datasync.Runner.runContinuousChecksum.
type recopierHarness struct {
	srcDB        *sql.DB
	dstDB        *sql.DB
	srcTable     *table.TableInfo
	dstTable     *table.TableInfo
	targetDBName string
	tableName    string
	recopier     *MySQLRecopier
}

// newRecopierHarness creates source + target copies of tableName and a
// recopier between them. Source rows are ids 1..20 with id 8 skipped (the
// gap lets tests plant a "row that exists only on the target" inside a
// chunk range).
func newRecopierHarness(t *testing.T, tableName string) *recopierHarness {
	t.Helper()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	targetDBName, _ := testutils.CreateUniqueTestDatabase(t)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS "+tableName)
	testutils.RunSQL(t, fmt.Sprintf(
		"CREATE TABLE %s (id INT NOT NULL, b INT NOT NULL, c VARCHAR(32) NOT NULL, PRIMARY KEY (id))", tableName))
	t.Cleanup(func() {
		testutils.RunSQL(t, "DROP TABLE IF EXISTS "+tableName)
	})
	var values []string
	for i := 1; i <= 20; i++ {
		if i == 8 {
			continue // deliberate gap, see doc comment
		}
		values = append(values, fmt.Sprintf("(%d, %d, 'row-%d')", i, i*10, i))
	}
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s VALUES %s", tableName, strings.Join(values, ",")))

	// Target is an exact copy in the throwaway database.
	testutils.RunSQL(t, fmt.Sprintf("CREATE TABLE %s.%s LIKE %s", targetDBName, tableName, tableName))
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s.%s SELECT * FROM %s", targetDBName, tableName, tableName))

	srcDB, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(srcDB) })

	dstCfg := cfg.Clone()
	dstCfg.DBName = targetDBName
	dstDB, err := dbconn.New(dstCfg.FormatDSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(dstDB) })

	srcTable := table.NewTableInfo(srcDB, cfg.DBName, tableName)
	require.NoError(t, srcTable.SetInfo(t.Context()))
	dstTable := table.NewTableInfo(dstDB, targetDBName, tableName)
	require.NoError(t, dstTable.SetInfo(t.Context()))

	app, err := applier.NewSingleTargetApplier(applier.Target{
		DB:       dstDB,
		Config:   dstCfg,
		KeyRange: "0",
	}, applier.NewApplierDefaultConfig())
	require.NoError(t, err)
	require.NoError(t, app.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, app.Stop()) })

	recopier, err := NewMySQLRecopier(srcDB, dstDB, app, dbconn.NewDBConfig(), nil)
	require.NoError(t, err)

	return &recopierHarness{
		srcDB:        srcDB,
		dstDB:        dstDB,
		srcTable:     srcTable,
		dstTable:     dstTable,
		targetDBName: targetDBName,
		tableName:    tableName,
		recopier:     recopier,
	}
}

// chunk builds a [lo, hi) chunk over the harness tables, the same shape
// the continuous-checksum chunker hands to the Recopier.
func (h *recopierHarness) chunk(t *testing.T, lo, hi int) *table.Chunk {
	t.Helper()
	loDatum, err := table.NewDatumFromValue(lo, "int")
	require.NoError(t, err)
	hiDatum, err := table.NewDatumFromValue(hi, "int")
	require.NoError(t, err)
	return &table.Chunk{
		Key:           []string{"id"},
		ChunkSize:     uint64(hi - lo), //nolint:gosec // test values are small and positive
		LowerBound:    &table.Boundary{Value: []table.Datum{loDatum}, Inclusive: true},
		UpperBound:    &table.Boundary{Value: []table.Datum{hiDatum}, Inclusive: false},
		Table:         h.srcTable,
		NewTable:      h.dstTable,
		ColumnMapping: table.NewColumnMapping(h.srcTable, h.dstTable, nil),
	}
}

// rows returns every row of the given table as "id#b#c" strings, ordered
// by id, for whole-table equality assertions.
func (h *recopierHarness) rows(t *testing.T, quotedTableName string) []string {
	t.Helper()
	rows, err := h.srcDB.QueryContext(t.Context(),
		"SELECT CONCAT_WS('#', id, b, c) FROM "+quotedTableName+" ORDER BY id")
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	var result []string
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		result = append(result, s)
	}
	require.NoError(t, rows.Err())
	return result
}

func (h *recopierHarness) requireTargetMatchesSource(t *testing.T) {
	t.Helper()
	require.Equal(t,
		h.rows(t, h.srcTable.QuotedTableName),
		h.rows(t, h.dstTable.QuotedTableName),
		"target table must match source table after recopy")
}

func TestMySQLRecopierRepairsDivergedChunk(t *testing.T) {
	h := newRecopierHarness(t, "recopier_repair_t1")

	// Diverge the target inside chunk [1, 11): two modified rows, one
	// deleted row, and one extra row (id 8 — the gap in the source).
	testutils.RunSQLInDatabase(t, h.targetDBName,
		fmt.Sprintf("UPDATE %s SET b = 999 WHERE id IN (2, 5)", h.tableName))
	testutils.RunSQLInDatabase(t, h.targetDBName,
		fmt.Sprintf("DELETE FROM %s WHERE id = 7", h.tableName))
	testutils.RunSQLInDatabase(t, h.targetDBName,
		fmt.Sprintf("INSERT INTO %s VALUES (8, 888, 'phantom')", h.tableName))

	require.NoError(t, h.recopier.Recopy(t.Context(), h.chunk(t, 1, 11)))
	h.requireTargetMatchesSource(t)
}

// TestMySQLRecopierEmptySourceRange covers the DELETE-only path: the
// source has no rows in the chunk range, so a recopy must remove the
// target's stray rows and return without invoking the applier.
func TestMySQLRecopierEmptySourceRange(t *testing.T) {
	h := newRecopierHarness(t, "recopier_empty_t1")

	// Source ids stop at 20; plant a target-only row at id 55.
	testutils.RunSQLInDatabase(t, h.targetDBName,
		fmt.Sprintf("INSERT INTO %s VALUES (55, 5550, 'stray')", h.tableName))

	require.NoError(t, h.recopier.Recopy(t.Context(), h.chunk(t, 50, 60)))
	h.requireTargetMatchesSource(t)
}

// TestMySQLRecopierConcurrent locks in the Recopier interface contract
// from continuous.go: "Recopy must be safe to call concurrently from
// multiple worker goroutines". Two goroutines recopy adjacent diverged
// chunks; the internal mutex serializes them and both must succeed.
// Run with -race.
func TestMySQLRecopierConcurrent(t *testing.T) {
	h := newRecopierHarness(t, "recopier_concurrent_t1")

	// Diverge both halves of the table.
	testutils.RunSQLInDatabase(t, h.targetDBName,
		fmt.Sprintf("UPDATE %s SET b = 111 WHERE id IN (3, 9)", h.tableName))
	testutils.RunSQLInDatabase(t, h.targetDBName,
		fmt.Sprintf("UPDATE %s SET b = 222 WHERE id IN (12, 19)", h.tableName))
	testutils.RunSQLInDatabase(t, h.targetDBName,
		fmt.Sprintf("DELETE FROM %s WHERE id IN (4, 15)", h.tableName))

	chunks := []*table.Chunk{h.chunk(t, 1, 11), h.chunk(t, 11, 21)}
	errs := make([]error, len(chunks))
	var wg sync.WaitGroup
	for i, c := range chunks {
		wg.Go(func() {
			errs[i] = h.recopier.Recopy(t.Context(), c)
		})
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "concurrent Recopy of chunk %d failed", i)
	}
	h.requireTargetMatchesSource(t)
}

// TestMySQLRecopierTargetTableMissing covers the error path: the target
// table is dropped before Recopy, so the DELETE step fails and the error
// must propagate (not hang, not panic).
func TestMySQLRecopierTargetTableMissing(t *testing.T) {
	h := newRecopierHarness(t, "recopier_missing_t1")

	testutils.RunSQLInDatabase(t, h.targetDBName, "DROP TABLE "+h.tableName)

	err := h.recopier.Recopy(t.Context(), h.chunk(t, 1, 11))
	require.Error(t, err)
	require.ErrorContains(t, err, "delete target chunk range")
}

// TestMySQLRecopierApplierWriteFails covers the applier-error branch: the
// DELETE succeeds but the rewrite fails (target schema diverged so the
// INSERT references a column that no longer exists). The applier reports
// the failure through the Apply callback and Recopy must surface it.
func TestMySQLRecopierApplierWriteFails(t *testing.T) {
	h := newRecopierHarness(t, "recopier_applyerr_t1")

	// Mutate the target schema *after* TableInfo was captured, so the
	// DELETE (by id) still works but the applier's INSERT of column c fails.
	testutils.RunSQLInDatabase(t, h.targetDBName,
		fmt.Sprintf("ALTER TABLE %s DROP COLUMN c", h.tableName))

	err := h.recopier.Recopy(t.Context(), h.chunk(t, 1, 11))
	require.Error(t, err)
	require.ErrorContains(t, err, "applier write")
}

func TestNewMySQLRecopierValidation(t *testing.T) {
	srcDB, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(srcDB)

	app, err := applier.NewSingleTargetApplier(applier.Target{DB: srcDB}, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	_, err = NewMySQLRecopier(nil, srcDB, app, nil, nil)
	require.ErrorContains(t, err, "sourceDB must be non-nil")
	_, err = NewMySQLRecopier(srcDB, nil, app, nil, nil)
	require.ErrorContains(t, err, "targetDB must be non-nil")
	_, err = NewMySQLRecopier(srcDB, srcDB, nil, nil, nil)
	require.ErrorContains(t, err, "applier must be non-nil")

	// nil dbConfig and logger are allowed; defaults are applied.
	r, err := NewMySQLRecopier(srcDB, srcDB, app, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, r.dbConfig)
	require.NotNil(t, r.logger)
}

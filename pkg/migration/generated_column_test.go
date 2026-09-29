package migration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// genColTable is the table every generated-column test starts from: s is a
// STORED generated column, v a VIRTUAL one, and r a regular column.
const genColTable = `CREATE TABLE %s (
	id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	a INT NOT NULL,
	s INT AS (a * 2) STORED,
	v INT AS (a * 3) VIRTUAL,
	r INT NULL,
	pad VARCHAR(100) NOT NULL DEFAULT ''
)`

// genColDML writes to the base columns only, so every write changes the
// generated values on the source: it inserts a row, moves `a` on an existing
// row, and deletes one.
func genColDML(ctx context.Context, db *sql.DB, tbl string, i int) error {
	stmts := []string{
		fmt.Sprintf("INSERT INTO %s (a, r, pad) VALUES (%d, -1, 'dml')", tbl, 100000+i),
		fmt.Sprintf("UPDATE %s SET a = a + 7 WHERE id = %d", tbl, 10+i*5),
		fmt.Sprintf("DELETE FROM %s WHERE id = %d", tbl, 12+i*5),
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// TestGeneratedColumnModify covers ALTERs that change whether a column is
// generated, or its expression. MySQL's own ALTER keeps the stored values of
// a STORED generated column that becomes a regular column; Spirit used to
// leave that column out of the copy, the binlog replay and the checksum,
// because it only mapped columns that are not generated on the source, so
// every value became NULL at cutover (and a NOT NULL target failed the copy).
// The same class of bug is https://github.com/github/gh-ost/issues/808.
//
// Each case writes to the base columns concurrently with the copy and again
// while the migration waits on the sentinel, so the values also arrive
// through the binlog replay. The source's generated value for each write is
// a pure function of `a`, and `a` is carried over unchanged, so after cutover
// every row must satisfy the expected expression.
func TestGeneratedColumnModify(t *testing.T) {
	tests := []struct {
		name  string
		alter string
		// column that is checked, and whether it is generated after cutover
		column    string
		generated bool
		// expected value of column, as an SQL expression over the new table
		expected string
	}{
		// Generated on the source, regular on the target: values must be copied.
		{name: "stored to regular", alter: "MODIFY s INT", column: "s", expected: "a * 2"},
		{name: "stored to regular not null", alter: "MODIFY s INT NOT NULL", column: "s", expected: "a * 2"},
		{name: "stored to regular wider type", alter: "MODIFY s BIGINT NOT NULL", column: "s", expected: "a * 2"},
		{name: "stored to regular renamed", alter: "CHANGE s s2 INT", column: "s2", expected: "a * 2"},
		// Generated on the target: the target computes the value, so Spirit must
		// not write it.
		{name: "regular to stored", alter: "MODIFY r INT AS (a * 4) STORED", column: "r", generated: true, expected: "a * 4"},
		{name: "stored expression change", alter: "MODIFY s INT AS (a * 6) STORED", column: "s", generated: true, expected: "a * 6"},
		{name: "virtual expression change", alter: "MODIFY v INT AS (a * 5) VIRTUAL", column: "v", generated: true, expected: "a * 5"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// A unique database per case: the sentinel table is per schema.
			dbName, db := testutils.CreateUniqueTestDatabase(t)
			const tbl = "gencol"
			testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(genColTable, tbl))
			testutils.RunSQLInDatabase(t, dbName, "INSERT INTO gencol (a, r) SELECT 1, 1 FROM dual")
			for range 12 { // 4096 rows
				testutils.RunSQLInDatabase(t, dbName, "INSERT INTO gencol (a, r) SELECT a + id, r FROM gencol")
			}

			m := NewTestRunner(t, tbl, tc.alter,
				WithDBName(dbName),
				WithThreads(1),
				WithTestThrottler(),
				WithDeferCutOver(),
				WithRespectSentinel(),
			)
			running := startTestRun(t, m.Run, m.Close)

			// Concurrent DML during the copy: replayed from the binlog.
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			dmlDone := make(chan int, 1)
			go func() {
				n := 0
				defer func() { dmlDone <- n }()
				if !waitForCopyRows(t, ctx, m) {
					return
				}
				for ; n < 200 && m.status.Get() < status.WaitingOnSentinelTable; n++ {
					if err := genColDML(ctx, db, tbl, n); err != nil {
						t.Errorf("concurrent DML: %v", err)
						return
					}
				}
			}()
			waitForStatus(t, m, status.WaitingOnSentinelTable, running)
			n := <-dmlDone
			require.Positive(t, n, "no DML ran during the copy")
			t.Logf("%d DML rounds ran during the copy", n)

			// The initial checksum compared the copied and replayed rows and
			// needed no repairs. (Without the fix it did not compare the column
			// at all, so this alone would not catch it; the final check does.)
			require.Zero(t, m.checker.DifferencesFound())

			// More DML while waiting on the sentinel: these rows only reach
			// the new table through the binlog replay.
			for i := n; i < n+50; i++ {
				require.NoError(t, genColDML(t.Context(), db, tbl, i))
			}
			testutils.RunSQLInDatabase(t, dbName, "DROP TABLE _spirit_sentinel")
			require.NoError(t, running.wait(t))
			require.Zero(t, m.checker.DifferencesFound(), "the checksum had to repair rows")

			var genExpr string
			require.NoError(t, db.QueryRowContext(t.Context(),
				"SELECT GENERATION_EXPRESSION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?",
				dbName, tbl, tc.column).Scan(&genExpr))
			require.Equal(t, tc.generated, genExpr != "", "column %s generation expression: %q", tc.column, genExpr)

			var total, bad, dmlRows int
			require.NoError(t, db.QueryRowContext(t.Context(), fmt.Sprintf(
				"SELECT COUNT(*), COALESCE(SUM(NOT (`%s` <=> %s)), 0), COALESCE(SUM(pad = 'dml'), 0) FROM %s",
				tc.column, tc.expected, tbl)).Scan(&total, &bad, &dmlRows))
			require.Equal(t, n+50, dmlRows, "rows inserted during the migration")
			require.Zero(t, bad, "%d of %d rows have %s != %s", bad, total, tc.column, tc.expected)
		})
	}
}

// TestGeneratedColumnVirtualToRegular: MySQL refuses to turn a VIRTUAL
// generated column into a regular one (ER_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN,
// 'Changing the STORED status'), and so does Spirit, because it applies the
// same ALTER to the new table. The source table must be left untouched.
func TestGeneratedColumnVirtualToRegular(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "gencol_v2r", fmt.Sprintf(genColTable, "gencol_v2r"))
	tt.SeedRows(t, "INSERT INTO gencol_v2r (a, r) SELECT 3, 1", 100)

	m := NewTestRunner(t, "gencol_v2r", "MODIFY v INT")
	err := m.Run(t.Context())
	require.NoError(t, m.Close())
	require.ErrorContains(t, err, "Changing the STORED status")

	var genExpr string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT GENERATION_EXPRESSION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'gencol_v2r' AND COLUMN_NAME = 'v'").Scan(&genExpr))
	require.NotEmpty(t, genExpr)
	var bad int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM gencol_v2r WHERE NOT (v <=> 9)").Scan(&bad))
	require.Zero(t, bad)
}

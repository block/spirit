package migration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// floatTestValues are FLOAT values whose 6-significant-digit text form (the
// form the text protocol returns) is a different FLOAT: 0.123456789 is read
// back as 0.123457 and 16777217 (stored as 16777216) as 16777200. FLT_MAX
// has a shortest float32 form, 3.4028235e+38, that is above FLT_MAX as a
// DOUBLE. The smallest subnormal and -0 are edge values of the format.
var floatTestValues = []string{"0.123456789", "16777217", "1234.5678", "3.402823466E+38", "-1.401298464E-45", "-0", "0.1"}

// exactValues returns the text of expr for every row, keyed by id. For a
// FLOAT column, "f + 0E0" is its exact value.
func exactValues(t *testing.T, db *sql.DB, table, expr string) map[int]string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), fmt.Sprintf("SELECT id, CONCAT(%s) FROM %s", expr, table))
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	got := map[int]string{}
	for rows.Next() {
		var id int
		var v sql.NullString
		require.NoError(t, rows.Scan(&id, &v))
		got[id] = v.String
	}
	require.NoError(t, rows.Err())
	return got
}

// TestFloatValuesSurviveCopy copies FLOAT columns into FLOAT, DOUBLE, DECIMAL
// and VARCHAR targets. The copier reads rows with the text protocol, which
// renders a FLOAT with 6 significant digits, so a FLOAT read as-is loses
// precision. Each target must end up with what MySQL's own ALTER TABLE would
// write: the exact value for a numeric target, the 6-digit text for a string.
func TestFloatValuesSurviveCopy(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "float_copy", `CREATE TABLE float_copy (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		f FLOAT NULL,
		f_double FLOAT NULL,
		f_decimal FLOAT NULL,
		f_char FLOAT NULL
	)`)
	for _, v := range floatTestValues {
		// FLT_MAX and the subnormal do not fit DECIMAL(65,30) exactly.
		dec := v
		if strings.Contains(v, "E") {
			dec = "NULL"
		}
		testutils.RunSQL(t, fmt.Sprintf("INSERT INTO float_copy (f, f_double, f_decimal, f_char) VALUES (%s, %s, %s, %s)", v, v, dec, v))
	}
	testutils.RunSQL(t, "INSERT INTO float_copy (f) VALUES (NULL)")

	wantExact := exactValues(t, tt.DB, "float_copy", "f + 0E0")
	wantDecimal := exactValues(t, tt.DB, "float_copy", "CAST(f_decimal AS DECIMAL(65,30))")
	wantChar := map[int]string{}
	rows, err := tt.DB.QueryContext(t.Context(), "SELECT id, COALESCE(CAST(f_char AS CHAR), '') FROM float_copy")
	require.NoError(t, err)
	for rows.Next() {
		var id int
		var v string
		require.NoError(t, rows.Scan(&id, &v))
		wantChar[id] = v
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	m := NewTestRunner(t, "float_copy",
		"MODIFY f_double DOUBLE NULL, MODIFY f_decimal DECIMAL(65,30) NULL, MODIFY f_char VARCHAR(64) NULL",
		WithThreads(1))
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())

	require.Equal(t, wantExact, exactValues(t, tt.DB, "float_copy", "f + 0E0"), "FLOAT -> FLOAT")
	require.Equal(t, wantExact, exactValues(t, tt.DB, "float_copy", "f_double + 0E0"), "FLOAT -> DOUBLE")
	require.Equal(t, wantDecimal, exactValues(t, tt.DB, "float_copy", "f_decimal"), "FLOAT -> DECIMAL")
	gotChar := map[int]string{}
	rows, err = tt.DB.QueryContext(t.Context(), "SELECT id, COALESCE(f_char, '') FROM float_copy")
	require.NoError(t, err)
	for rows.Next() {
		var id int
		var v string
		require.NoError(t, rows.Scan(&id, &v))
		gotChar[id] = v
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Equal(t, wantChar, gotChar, "FLOAT -> VARCHAR")
}

// TestFloatBinlogDML writes FLOAT values while the table is copied, so they
// reach the new table through the binlog applier, and then must pass the
// checksum and match the source exactly. The binlog delivers a FLOAT as a Go
// float32, whose shortest text form is not the FLOAT's value as a DOUBLE:
// FLT_MAX would overflow a FLOAT, and a DOUBLE target would get a rounded
// value.
//
// A string target is not yet written correctly by the applier: it formats the
// FLOAT itself rather than as MySQL does (0.12345679104328156, not 0.123457).
// The checksum detects those rows and the repair recopies them from the
// source, which is what this case pins.
func TestFloatBinlogDML(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, alter string }{
		{"float", "ENGINE=InnoDB"},
		{"double", "MODIFY f DOUBLE NULL"},
		{"varchar", "MODIFY f VARCHAR(64) NULL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tbl := "float_binlog_" + tc.name
			tt := testutils.NewTestTable(t, tbl, fmt.Sprintf(`CREATE TABLE %s (
				id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
				v INT NOT NULL,
				f FLOAT NULL
			)`, tbl))
			tt.SeedRows(t, fmt.Sprintf("INSERT INTO %s (v, f) SELECT 0, 1.5", tbl), 5000)
			markers := make([]int64, len(floatTestValues))
			for i := range floatTestValues {
				res, err := tt.DB.ExecContext(t.Context(), fmt.Sprintf("INSERT INTO %s (v, f) VALUES (-1, 2.5)", tbl))
				require.NoError(t, err)
				markers[i], err = res.LastInsertId()
				require.NoError(t, err)
			}

			m := NewTestRunner(t, tbl, tc.alter, WithThreads(1), WithTestThrottler())
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			dmlDone := make(chan struct{})
			var updated bool
			go func() {
				defer close(dmlDone)
				if !waitForCopyRows(t, ctx, m) {
					return
				}
				for i, id := range markers {
					if _, err := tt.DB.ExecContext(ctx,
						fmt.Sprintf("UPDATE %s SET v = v + 1, f = %s WHERE id = ?", tbl, floatTestValues[i]), id); err != nil {
						return
					}
				}
				updated = true
			}()
			migrationErr := m.Run(ctx)
			cancel()
			<-dmlDone
			require.NoError(t, m.Close())
			require.NoError(t, migrationErr)
			require.True(t, updated, "the updates must run during the migration")

			// Each target must hold what MySQL's own conversion of the FLOAT
			// gives: its exact value, or for a string its 6-digit text. The
			// expected values are read back from a FLOAT column, because
			// CAST(... AS FLOAT) does not round to FLOAT on every 8.0 version.
			expTbl := tbl + "_expected"
			testutils.NewTestTable(t, expTbl, "CREATE TABLE "+expTbl+" (id INT NOT NULL PRIMARY KEY, f FLOAT NULL)")
			for i, v := range floatTestValues {
				testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s VALUES (%d, %s)", expTbl, i, v))
			}
			wantExpr, gotExpr := "f + 0E0", "f + 0E0"
			if tc.name == "varchar" {
				wantExpr, gotExpr = "CAST(f AS CHAR)", "f"
			}
			wantByIdx := exactValues(t, tt.DB, expTbl, wantExpr)
			want := make([]string, len(floatTestValues))
			for i := range floatTestValues {
				want[i] = wantByIdx[i]
			}
			got := exactValues(t, tt.DB, tbl, gotExpr)
			for i, id := range markers {
				require.Equal(t, want[i], got[int(id)], "value %s", floatTestValues[i])
			}
		})
	}
}

// TestYear0000SurvivesMigration copies YEAR 0000 values and writes more of
// them through the binlog applier. MySQL stores the number 0 in a YEAR column
// as 0000 but the string '0' as 2000, and the copier and the applier receive
// YEAR 0000 as the number 0. Two rows are copied so that a corruption of both
// is not hidden by the checksum's XOR of row hashes.
func TestYear0000SurvivesMigration(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "year_zero", `CREATE TABLE year_zero (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		v INT NOT NULL,
		y YEAR NULL
	)`)
	testutils.RunSQL(t, "INSERT INTO year_zero (v, y) VALUES (0, 0), (0, 0), (0, 2000), (0, 1901), (0, 2155), (0, NULL)")
	tt.SeedRows(t, "INSERT INTO year_zero (v, y) SELECT 0, 1999", 5000)
	res, err := tt.DB.ExecContext(t.Context(), "INSERT INTO year_zero (v, y) VALUES (-1, 2001)")
	require.NoError(t, err)
	marker, err := res.LastInsertId()
	require.NoError(t, err)

	m := NewTestRunner(t, "year_zero", "ENGINE=InnoDB", WithThreads(1), WithTestThrottler())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	dmlDone := make(chan struct{})
	var inserted bool
	go func() {
		defer close(dmlDone)
		if !waitForCopyRows(t, ctx, m) {
			return
		}
		if _, err := tt.DB.ExecContext(ctx, "UPDATE year_zero SET v = v + 1, y = 0 WHERE id = ?", marker); err != nil {
			return
		}
		if _, err := tt.DB.ExecContext(ctx, "INSERT INTO year_zero (v, y) VALUES (1, 0)"); err != nil {
			return
		}
		inserted = true
	}()
	migrationErr := m.Run(ctx)
	cancel()
	<-dmlDone
	require.NoError(t, m.Close())
	require.NoError(t, migrationErr)
	require.True(t, inserted, "the DML must run during the migration")

	var got []string
	rows, err := tt.DB.QueryContext(t.Context(), "SELECT COALESCE(CAST(y AS CHAR), 'NULL') FROM year_zero WHERE v <> 0 OR y <> 1999 OR y IS NULL ORDER BY id")
	require.NoError(t, err)
	for rows.Next() {
		var s string
		require.NoError(t, rows.Scan(&s))
		got = append(got, s)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Equal(t, []string{"0000", "0000", "2000", "1901", "2155", "NULL", "0000", "0000"}, got)
}

// TestFloatPrimaryKeyRefused refuses a table with a FLOAT in its primary key,
// as gh-ost does, even for an ALTER MySQL could apply as INSTANT, and refuses
// changing a primary key column to a FLOAT. Replayed DELETEs cannot locate a
// FLOAT key by its text form, so a row deleted during the migration would
// survive the cutover.
func TestFloatPrimaryKeyRefused(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "float_pk", `CREATE TABLE float_pk (
		id INT NOT NULL,
		f FLOAT NOT NULL,
		PRIMARY KEY (id, f)
	)`)
	testutils.RunSQL(t, "INSERT INTO float_pk VALUES (1, 0.1), (2, 0.2)")
	// ADD COLUMN is INSTANT on every supported server: the refusal has to
	// come from the statement-scope checks the runner runs before it
	// attempts native DDL.
	m := NewTestRunner(t, "float_pk", "ADD COLUMN c INT")
	err := m.Run(t.Context())
	require.NoError(t, m.Close())
	require.ErrorContains(t, err, `primary key column "f" of table "float_pk" is a FLOAT, which is not supported`)
	require.False(t, m.usedInstantDDL)
	var n int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'float_pk' AND COLUMN_NAME = 'c'").Scan(&n))
	require.Zero(t, n, "the refused ALTER must not change the table")

	tt = testutils.NewTestTable(t, "double_pk", `CREATE TABLE double_pk (
		id INT NOT NULL,
		d DOUBLE NOT NULL,
		PRIMARY KEY (id, d)
	)`)
	testutils.RunSQL(t, "INSERT INTO double_pk VALUES (1, 0.1), (2, 0.2)")
	m = NewTestRunner(t, "double_pk", "MODIFY d FLOAT NOT NULL")
	err = m.Run(t.Context())
	require.NoError(t, m.Close())
	require.ErrorContains(t, err, `changing primary key column "d" of table "double_pk" to a FLOAT is not supported`)
	var tp string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'double_pk' AND COLUMN_NAME = 'd'").Scan(&tp))
	require.Equal(t, "double", tp, "the refused ALTER must not change the table")
}

// TestFloatPrimaryKeyRefusedAfterKeyChange refuses an ALTER that replaces the
// primary key with one that includes a FLOAT column, which no MODIFY or
// CHANGE of a key column spells out. The primarykey check refuses the DROP
// PRIMARY KEY before native DDL is attempted; primarykeyfloat would refuse
// the new table at post-setup if that ever stopped being the case.
func TestFloatPrimaryKeyRefusedAfterKeyChange(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "float_pk_swap", `CREATE TABLE float_pk_swap (
		id INT NOT NULL,
		f FLOAT NOT NULL,
		PRIMARY KEY (id)
	)`)
	testutils.RunSQL(t, "INSERT INTO float_pk_swap VALUES (1, 0.1), (2, 0.2)")
	m := NewTestRunner(t, "float_pk_swap", "DROP PRIMARY KEY, ADD PRIMARY KEY (f)")
	err := m.Run(t.Context())
	require.NoError(t, m.Close())
	require.ErrorContains(t, err, "dropping primary key is not supported")
	var key string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION) FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'float_pk_swap' AND CONSTRAINT_NAME = 'PRIMARY'").Scan(&key))
	require.Equal(t, "id", key, "the refused ALTER must not change the table")
}

// TestNarrowToFloat changes DOUBLE, VARCHAR, DECIMAL and BIGINT columns to
// FLOAT. MySQL's ALTER rounds each value to the nearest FLOAT, so the checksum
// must compare the source at FLOAT precision rather than report the rounding
// as a difference. The values include ones that need rounding, a tie that
// rounds to even (16777217), a subnormal and a value next to a power of two.
func TestNarrowToFloat(t *testing.T) {
	t.Parallel()
	values := []string{"0.1", "1.5", "0.123456789", "16777217", "-8388608.5", "1.1754942106924411e-38", "0.9999999701976776", "0"}
	for _, tc := range []struct{ name, srcType, quote string }{
		{"double", "DOUBLE", ""},
		{"varchar", "VARCHAR(40)", "'"},
		{"decimal", "DECIMAL(65,30)", ""},
		{"bigint", "BIGINT", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tbl := "narrow_to_float_" + tc.name
			tt := testutils.NewTestTable(t, tbl, fmt.Sprintf("CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, f %s NULL)", tbl, tc.srcType))
			for _, v := range values {
				if (tc.name == "bigint" && strings.ContainsAny(v, ".e")) || (tc.name == "decimal" && strings.Contains(v, "e")) {
					continue // does not fit the source type
				}
				testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (f) VALUES (%s%s%s)", tbl, tc.quote, v, tc.quote))
			}
			testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (f) VALUES (NULL)", tbl))

			// The expected values are what MySQL stores in a FLOAT column.
			expTbl := tbl + "_expected"
			testutils.NewTestTable(t, expTbl, "CREATE TABLE "+expTbl+" (id INT NOT NULL PRIMARY KEY, f FLOAT NULL)")
			testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s SELECT id, f FROM %s", expTbl, tbl))
			want := exactValues(t, tt.DB, expTbl, "f + 0E0")

			m := NewTestRunner(t, tbl, "MODIFY f FLOAT NULL", WithThreads(1))
			err := m.Run(t.Context())
			require.NoError(t, m.Close())
			require.NoError(t, err)
			require.Equal(t, want, exactValues(t, tt.DB, tbl, "f + 0E0"))
		})
	}
}

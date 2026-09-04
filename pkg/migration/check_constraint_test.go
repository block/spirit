package migration

import (
	"database/sql"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// Tests for issue #351: Add more tests for CHECK constraints.
// CHECK constraints have a schema-level namespace, so creating two tables with
// the same named CHECK constraint would fail. Spirit uses CREATE TABLE .. LIKE
// which auto-renames constraints, avoiding this issue.

// TestCheckConstraintNamed tests migration of a table with named CHECK constraints.
func TestCheckConstraintNamed(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_named", `CREATE TABLE chk_named (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		c1 INT DEFAULT NULL,
		c2 INT DEFAULT NULL,
		c3 INT DEFAULT NULL,
		CONSTRAINT chk_named_c1pos CHECK (c1 > 0),
		CONSTRAINT chk_named_c2pos CHECK (c2 > 0),
		CONSTRAINT chk_named_c3rng CHECK (c3 BETWEEN 1 AND 100)
	)`)
	testutils.RunSQL(t, `INSERT INTO chk_named (c1, c2, c3) VALUES (1, 1, 50), (10, 20, 99), (5, 5, 1)`)

	m := NewTestRunner(t, "chk_named", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())

	var count int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM chk_named").Scan(&count))
	require.Equal(t, 3, count)

	// Verify CHECK constraints are still enforced after migration.
	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_named (c1, c2, c3) VALUES (-1, 1, 50)")
	require.Error(t, err, "CHECK constraint on c1 should reject negative values")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_named (c1, c2, c3) VALUES (1, -1, 50)")
	require.Error(t, err, "CHECK constraint on c2 should reject negative values")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_named (c1, c2, c3) VALUES (1, 1, 200)")
	require.Error(t, err, "CHECK constraint on c3 should reject out-of-range values")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_named (c1, c2, c3) VALUES (1, 1, 50)")
	require.NoError(t, err, "valid row should be accepted")
}

// TestCheckConstraintUnnamed tests migration of a table with unnamed (auto-generated) CHECK constraints.
func TestCheckConstraintUnnamed(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_unnamed", `CREATE TABLE chk_unnamed (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		c1 INT CHECK (c1 > 10),
		c2 INT CHECK (c2 > 0),
		CHECK (c1 <> c2)
	)`)
	testutils.RunSQL(t, `INSERT INTO chk_unnamed (c1, c2) VALUES (11, 1), (20, 2), (100, 3)`)

	m := NewTestRunner(t, "chk_unnamed", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())

	var count int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM chk_unnamed").Scan(&count))
	require.Equal(t, 3, count)

	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_unnamed (c1, c2) VALUES (5, 1)")
	require.Error(t, err, "CHECK constraint should reject c1 <= 10")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_unnamed (c1, c2) VALUES (11, -1)")
	require.Error(t, err, "CHECK constraint should reject c2 <= 0")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_unnamed (c1, c2) VALUES (11, 11)")
	require.Error(t, err, "CHECK constraint should reject c1 == c2")
}

// TestCheckConstraintMixed tests migration of a table with both named and unnamed CHECK constraints.
func TestCheckConstraintMixed(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_mixed", `CREATE TABLE chk_mixed (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		CHECK (c1 <> c2),
		c1 INT CHECK (c1 > 10),
		c2 INT CONSTRAINT chk_mixed_c2pos CHECK (c2 > 0),
		c3 INT CHECK (c3 < 100),
		CONSTRAINT chk_mixed_c1nz CHECK (c1 <> 0),
		CHECK (c1 > c3)
	)`)
	testutils.RunSQL(t, `INSERT INTO chk_mixed (c1, c2, c3) VALUES (50, 1, 10), (20, 5, 15), (80, 3, 70)`)

	m := NewTestRunner(t, "chk_mixed", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())

	var count int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM chk_mixed").Scan(&count))
	require.Equal(t, 3, count)

	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (5, 1, 1)")
	require.Error(t, err, "CHECK constraint should reject c1 <= 10")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (50, -1, 10)")
	require.Error(t, err, "CHECK constraint should reject c2 <= 0")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (50, 1, 200)")
	require.Error(t, err, "CHECK constraint should reject c3 >= 100")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (50, 50, 10)")
	require.Error(t, err, "CHECK constraint should reject c1 == c2")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_mixed (c1, c2, c3) VALUES (50, 1, 10)")
	require.NoError(t, err, "valid row should be accepted")
}

// TestCheckConstraintWithDML tests migration with concurrent DML on a table with CHECK constraints.
func TestCheckConstraintWithDML(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_dml", `CREATE TABLE chk_dml (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		val INT NOT NULL,
		CONSTRAINT chk_dml_valpos CHECK (val > 0),
		CONSTRAINT chk_dml_valrng CHECK (val < 10000)
	)`)
	tt.SeedRows(t, "INSERT INTO chk_dml (val) SELECT 1", 6000)

	m := NewTestRunner(t, "chk_dml", "ENGINE=InnoDB",
		WithThreads(1),
		WithTestThrottler())

	var wg sync.WaitGroup
	wg.Go(func() {
		if !waitForCopyRows(t, t.Context(), m) {
			return
		}
		for i := range 100 {
			// Insert valid rows (val must be > 0 and < 10000).
			_, _ = tt.DB.ExecContext(t.Context(), fmt.Sprintf(`INSERT INTO chk_dml (val) VALUES (%d)`, i+1))
			_, _ = tt.DB.ExecContext(t.Context(), fmt.Sprintf(`UPDATE chk_dml SET val = %d WHERE id = %d`, (i%9999)+1, i+1))
			_, _ = tt.DB.ExecContext(t.Context(), fmt.Sprintf(`DELETE FROM chk_dml WHERE id = %d`, i+50))
		}
	})

	require.NoError(t, m.Run(t.Context()))
	wg.Wait()
	require.NoError(t, m.Close())

	// Verify CHECK constraints are still enforced after migration with DML.
	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_dml (val) VALUES (0)")
	require.Error(t, err, "CHECK constraint should reject val <= 0")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_dml (val) VALUES (10000)")
	require.Error(t, err, "CHECK constraint should reject val >= 10000")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_dml (val) VALUES (5000)")
	require.NoError(t, err, "valid row should be accepted")
}

// TestCheckConstraintAddColumn tests adding a column to a table that has CHECK constraints.
func TestCheckConstraintAddColumn(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_addcol", `CREATE TABLE chk_addcol (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		val INT NOT NULL,
		CONSTRAINT chk_addcol_valchk CHECK (val > 0)
	)`)
	testutils.RunSQL(t, `INSERT INTO chk_addcol (val) VALUES (1), (2), (3)`)

	m := NewTestRunner(t, "chk_addcol", "ADD COLUMN extra VARCHAR(100) DEFAULT 'hello', ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())

	var count int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM chk_addcol WHERE extra = 'hello'").Scan(&count))
	require.Equal(t, 3, count)

	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_addcol (val) VALUES (0)")
	require.Error(t, err, "CHECK constraint should still reject val <= 0 after adding column")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_addcol (val) VALUES (1)")
	require.NoError(t, err, "valid row should be accepted")
}

// TestCheckConstraintReplaceSameName covers the common "widen an enum-style
// CHECK" idiom: drop a named CHECK constraint and immediately re-add it under
// the same name. MySQL accepts this directly, but the copy algorithm cannot
// replay it verbatim on the _new table: CREATE TABLE .. LIKE renames the
// constraint, so DROP CHECK by the original name is "not found" (error 3821),
// and the original name is still owned by the source table, so re-adding it
// would be a "duplicate check constraint name" (error 3822).
func TestCheckConstraintReplaceSameName(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_replace", `CREATE TABLE chk_replace (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		state VARCHAR(32) NOT NULL,
		CONSTRAINT chk_replace_state CHECK (state IN ('processing', 'succeeded', 'failed'))
	)`)
	testutils.RunSQL(t, `INSERT INTO chk_replace (state) VALUES ('processing'), ('succeeded'), ('failed')`)

	m := NewTestRunner(t, "chk_replace",
		`DROP CHECK chk_replace_state,
		 ADD CONSTRAINT chk_replace_state CHECK (state IN ('processing', 'succeeded', 'failed', 'warning'))`)
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())

	var count int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM chk_replace").Scan(&count))
	require.Equal(t, 3, count)

	// The widened constraint is in force: the new value is accepted...
	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_replace (state) VALUES ('warning')")
	require.NoError(t, err, "the re-added CHECK constraint should accept the new value")

	// ...and a value outside the new list is still rejected.
	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_replace (state) VALUES ('bogus')")
	require.Error(t, err, "the re-added CHECK constraint should reject unlisted values")

	// The name the user asked for is not what the constraint ends up with: the
	// old table still owned it while the new table was being built, so MySQL
	// generated a <table>_chk_<n> name instead. This is the same thing that
	// happens to every CHECK constraint a copy migration copies (issue #418).
	constraints := tableCheckConstraints(t, tt.DB, "chk_replace")
	require.Len(t, constraints, 1)
	name := slices.Sorted(maps.Keys(constraints))[0]
	require.NotEqual(t, "chk_replace_state", name)
	require.True(t, strings.HasPrefix(name, "chk_replace_chk_"),
		"expected a server-generated name, got %q", name)
	require.True(t, constraints[name], "the re-added constraint should be enforced")
}

// TestCheckConstraintNotEnforcedOnCopyPath switches a named CHECK constraint to
// NOT ENFORCED in an ALTER that also forces a rebuild, so the ALTER CHECK clause
// has to be retargeted at the new table's name for that constraint.
func TestCheckConstraintNotEnforcedOnCopyPath(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_enforce", `CREATE TABLE chk_enforce (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		val INT NOT NULL,
		CONSTRAINT chk_enforce_valpos CHECK (val > 0)
	)`)
	testutils.RunSQL(t, `INSERT INTO chk_enforce (val) VALUES (1), (2)`)

	m := NewTestRunner(t, "chk_enforce", "ALTER CHECK chk_enforce_valpos NOT ENFORCED, ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())

	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_enforce (val) VALUES (-1)")
	require.NoError(t, err, "a NOT ENFORCED CHECK constraint should not reject anything")
}

// TestCheckConstraintDropOnCopyPath drops a named CHECK constraint in an ALTER
// that also forces a table rebuild, so it takes the copy path rather than
// INSTANT DDL. The DROP CHECK has to be retargeted at the name CREATE TABLE ..
// LIKE gave the constraint on the _new table.
func TestCheckConstraintDropOnCopyPath(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_dropcopy", `CREATE TABLE chk_dropcopy (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		c1 INT NOT NULL,
		c2 INT NOT NULL,
		CONSTRAINT chk_dropcopy_c1pos CHECK (c1 > 0),
		CONSTRAINT chk_dropcopy_c2pos CHECK (c2 > 0)
	)`)
	testutils.RunSQL(t, `INSERT INTO chk_dropcopy (c1, c2) VALUES (1, 1), (2, 2)`)

	// ENGINE=InnoDB forces a rebuild, so this cannot be INSTANT.
	m := NewTestRunner(t, "chk_dropcopy", "DROP CHECK chk_dropcopy_c1pos, ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())

	// c1 is now unconstrained, c2 is still constrained.
	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_dropcopy (c1, c2) VALUES (-1, 1)")
	require.NoError(t, err, "the dropped CHECK constraint should no longer be enforced")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_dropcopy (c1, c2) VALUES (1, -1)")
	require.Error(t, err, "the remaining CHECK constraint should still be enforced")
}

// TestCheckConstraintAddNewCheckConstraint tests adding a new CHECK constraint
// to a table that already has CHECK constraints.
func TestCheckConstraintAddNewCheckConstraint(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_addchk", `CREATE TABLE chk_addchk (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		c1 INT NOT NULL,
		c2 INT NOT NULL,
		CONSTRAINT chk_addchk_c1pos CHECK (c1 > 0)
	)`)
	testutils.RunSQL(t, `INSERT INTO chk_addchk (c1, c2) VALUES (1, 50), (10, 50), (5, 50)`)

	m := NewTestRunner(t, "chk_addchk", "ADD CONSTRAINT chk_addchk_c2rng CHECK (c2 BETWEEN 1 AND 100)")
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())

	// Violate original constraint c1 > 0
	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_addchk (c1, c2) VALUES (0, 50)")
	require.Error(t, err, "original CHECK constraint should still reject c1 <= 0")

	// Violate new constraint c2 BETWEEN 1 AND 100
	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_addchk (c1, c2) VALUES (1, 200)")
	require.Error(t, err, "new CHECK constraint should reject c2 > 100")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_addchk (c1, c2) VALUES (1, 0)")
	require.Error(t, err, "new CHECK constraint should reject c2 < 1")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_addchk (c1, c2) VALUES (1, 50)")
	require.NoError(t, err, "valid row should be accepted")
}

// A CHECK constraint can be declared NOT ENFORCED, which keeps the rule in the
// schema without applying it to rows (MySQL 8.0.16+). That state changes which
// algorithm MySQL will use for a change, so it changes which path Spirit takes:
//
//	DROP CHECK / DROP CONSTRAINT            INSTANT
//	ALTER CHECK ... NOT ENFORCED            INSTANT
//	ADD CONSTRAINT ... CHECK NOT ENFORCED   INSTANT
//	ALTER CHECK ... ENFORCED                ALGORITHM=COPY only
//	ADD CONSTRAINT ... CHECK                ALGORITHM=COPY only
//
// (Verified on MySQL 8.0.28, 8.0.45 and 9.7.) The two COPY-only ones are the
// ones that have to look at the rows already in the table, and they are the
// ones that reach Spirit's copy algorithm - where the constraint names have to
// be rewritten for the _new table.

// TestCheckConstraintNotEnforcedPreserved migrates a table whose CHECK
// constraint is NOT ENFORCED, and whose rows violate it. Both facts have to
// survive the copy: if the _new table enforced the constraint, copying the rows
// would fail.
func TestCheckConstraintNotEnforcedPreserved(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_ne_keep", `CREATE TABLE chk_ne_keep (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		val INT NOT NULL,
		CONSTRAINT chk_ne_keep_valpos CHECK (val > 0) NOT ENFORCED
	)`)
	testutils.RunSQL(t, `INSERT INTO chk_ne_keep (val) VALUES (-5), (1), (-1)`)

	m := NewTestRunner(t, "chk_ne_keep", "ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())

	var negatives int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM chk_ne_keep WHERE val < 0").Scan(&negatives))
	require.Equal(t, 2, negatives, "rows that violate the unenforced constraint should be copied as-is")

	constraints := tableCheckConstraints(t, tt.DB, "chk_ne_keep")
	require.Len(t, constraints, 1)
	require.False(t, slices.Contains(slices.Collect(maps.Values(constraints)), true),
		"the constraint should still be NOT ENFORCED: %v", constraints)

	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_ne_keep (val) VALUES (-2)")
	require.NoError(t, err, "an unenforced constraint should still reject nothing")
}

// TestCheckConstraintSwitchToEnforced turns enforcement on. MySQL supports only
// ALGORITHM=COPY for that - it has to validate the rows already in the table -
// so this reaches Spirit's copy algorithm on its own, without a rebuild-forcing
// clause to push it there.
func TestCheckConstraintSwitchToEnforced(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_enable", `CREATE TABLE chk_enable (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		val INT NOT NULL,
		CONSTRAINT chk_enable_valpos CHECK (val > 0) NOT ENFORCED
	)`)
	testutils.RunSQL(t, `INSERT INTO chk_enable (val) VALUES (1), (2), (3)`)

	m := NewTestRunner(t, "chk_enable", "ALTER CHECK chk_enable_valpos ENFORCED")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.False(t, m.usedInplaceDDL)
	require.NoError(t, m.Close())

	constraints := tableCheckConstraints(t, tt.DB, "chk_enable")
	require.Len(t, constraints, 1)
	require.False(t, slices.Contains(slices.Collect(maps.Values(constraints)), false),
		"the constraint should now be ENFORCED: %v", constraints)

	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_enable (val) VALUES (-1)")
	require.Error(t, err, "the now-enforced constraint should reject a violating row")

	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_enable (val) VALUES (4)")
	require.NoError(t, err, "valid row should be accepted")
}

// TestCheckConstraintSwitchToEnforcedViolatingRows turns enforcement on for a
// constraint the existing rows violate. MySQL rejects that outright (error
// 3819); Spirit has to fail too rather than leave the violating rows behind.
func TestCheckConstraintSwitchToEnforcedViolatingRows(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_enable_bad", `CREATE TABLE chk_enable_bad (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		val INT NOT NULL,
		CONSTRAINT chk_enable_bad_valpos CHECK (val > 0) NOT ENFORCED
	)`)
	testutils.RunSQL(t, `INSERT INTO chk_enable_bad (val) VALUES (1), (-5), (3)`)

	m := NewTestRunner(t, "chk_enable_bad", "ALTER CHECK chk_enable_bad_valpos ENFORCED")
	require.Error(t, m.Run(t.Context()), "copying a row that violates the constraint must fail")
	require.NoError(t, m.Close())

	// The table is untouched: the violating row is still there, and the
	// constraint is still not enforced.
	var count int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM chk_enable_bad WHERE val = -5").Scan(&count))
	require.Equal(t, 1, count)

	constraints := tableCheckConstraints(t, tt.DB, "chk_enable_bad")
	require.Equal(t, map[string]bool{"chk_enable_bad_valpos": false}, constraints)
}

// TestCheckConstraintAddNotEnforced adds a NOT ENFORCED CHECK constraint. There
// are no rows to validate, so MySQL does it as INSTANT and Spirit never builds a
// _new table - which is also why the constraint keeps the name it was given,
// unlike the enforced case.
func TestCheckConstraintAddNotEnforced(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_addne", `CREATE TABLE chk_addne (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		val INT NOT NULL
	)`)
	testutils.RunSQL(t, `INSERT INTO chk_addne (val) VALUES (-5), (1)`)

	m := NewTestRunner(t, "chk_addne", "ADD CONSTRAINT chk_addne_valpos CHECK (val > 0) NOT ENFORCED")
	require.NoError(t, m.Run(t.Context()))
	require.True(t, m.usedInstantDDL)
	require.NoError(t, m.Close())

	require.Equal(t, map[string]bool{"chk_addne_valpos": false},
		tableCheckConstraints(t, tt.DB, "chk_addne"))

	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_addne (val) VALUES (-2)")
	require.NoError(t, err, "an unenforced constraint should reject nothing")
}

// TestCheckConstraintReplaceNotEnforcedKeepsName is the drop-and-re-add idiom
// with the replacement declared NOT ENFORCED. Both clauses are INSTANT, so this
// never reaches the copy algorithm - and so, unlike
// TestCheckConstraintReplaceSameName, the constraint keeps the user's name.
func TestCheckConstraintReplaceNotEnforcedKeepsName(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_replace_ne", `CREATE TABLE chk_replace_ne (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		state VARCHAR(32) NOT NULL,
		CONSTRAINT chk_replace_ne_state CHECK (state IN ('processing', 'succeeded', 'failed'))
	)`)
	testutils.RunSQL(t, `INSERT INTO chk_replace_ne (state) VALUES ('processing')`)

	m := NewTestRunner(t, "chk_replace_ne",
		`DROP CHECK chk_replace_ne_state,
		 ADD CONSTRAINT chk_replace_ne_state CHECK (state IN ('processing', 'succeeded', 'failed', 'warning')) NOT ENFORCED`)
	require.NoError(t, m.Run(t.Context()))
	require.True(t, m.usedInstantDDL)
	require.NoError(t, m.Close())

	require.Equal(t, map[string]bool{"chk_replace_ne_state": false},
		tableCheckConstraints(t, tt.DB, "chk_replace_ne"))

	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_replace_ne (state) VALUES ('bogus')")
	require.NoError(t, err, "an unenforced constraint should reject nothing")
}

// TestCheckConstraintRewrittenAlterErrorNamesStatement covers the error message
// for a rewritten ALTER that fails for some unrelated reason. It has to say what
// was actually run, because the statement is no longer the user's and MySQL can
// report a constraint name they have never seen.
func TestCheckConstraintRewrittenAlterErrorNamesStatement(t *testing.T) {
	t.Parallel()
	testutils.NewTestTable(t, "chk_errmsg", `CREATE TABLE chk_errmsg (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		val INT NOT NULL,
		CONSTRAINT chk_errmsg_valpos CHECK (val > 0)
	)`)
	testutils.RunSQL(t, `INSERT INTO chk_errmsg (val) VALUES (1)`)

	// The DROP CHECK is rewritten for the new table; the ADD COLUMN then fails
	// because the column already exists.
	m := NewTestRunner(t, "chk_errmsg", "DROP CHECK chk_errmsg_valpos, ADD COLUMN id INT")
	err := m.Run(t.Context())
	require.Error(t, err)
	require.Contains(t, err.Error(), "applied to the new table as")
	require.Contains(t, err.Error(), "_chk_errmsg_new_chk_1")
	require.NoError(t, m.Close())
}

// TestCheckConstraintDropWithTenPlusConstraints drops a named CHECK constraint
// from a table that has more than nine of them. That is where the two tables'
// constraint listings stop corresponding position by position: SHOW CREATE TABLE
// sorts CHECK constraints by name, and the tenth copy CREATE TABLE .. LIKE makes
// is named _<table>_new_chk_10, which sorts between _chk_1 and _chk_2. Pairing by
// listing position retargets the DROP CHECK at some other constraint.
func TestCheckConstraintDropWithTenPlusConstraints(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_many", `CREATE TABLE chk_many (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		c01 INT NOT NULL DEFAULT 1,
		c02 INT NOT NULL DEFAULT 1,
		c03 INT NOT NULL DEFAULT 1,
		c04 INT NOT NULL DEFAULT 1,
		c05 INT NOT NULL DEFAULT 1,
		c06 INT NOT NULL DEFAULT 1,
		c07 INT NOT NULL DEFAULT 1,
		c08 INT NOT NULL DEFAULT 1,
		c09 INT NOT NULL DEFAULT 1,
		c10 INT NOT NULL DEFAULT 1,
		c11 INT NOT NULL DEFAULT 1,
		CONSTRAINT chk_many_c01 CHECK (c01 > 0),
		CONSTRAINT chk_many_c02 CHECK (c02 > 0),
		CONSTRAINT chk_many_c03 CHECK (c03 > 0),
		CONSTRAINT chk_many_c04 CHECK (c04 > 0),
		CONSTRAINT chk_many_c05 CHECK (c05 > 0),
		CONSTRAINT chk_many_c06 CHECK (c06 > 0),
		CONSTRAINT chk_many_c07 CHECK (c07 > 0),
		CONSTRAINT chk_many_c08 CHECK (c08 > 0),
		CONSTRAINT chk_many_c09 CHECK (c09 > 0),
		CONSTRAINT chk_many_c10 CHECK (c10 > 0),
		CONSTRAINT chk_many_c11 CHECK (c11 > 0)
	)`)
	testutils.RunSQL(t, `INSERT INTO chk_many (id) VALUES (1), (2)`)

	// ENGINE=InnoDB forces a rebuild, so this cannot be INSTANT.
	m := NewTestRunner(t, "chk_many", "DROP CHECK chk_many_c05, ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())

	require.Len(t, tableCheckConstraints(t, tt.DB, "chk_many"), 10)

	// Only c05 lost its constraint. The columns whose generated names sort out
	// of order are the ones a positional pairing drops instead.
	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_many (c05) VALUES (-1)")
	require.NoError(t, err, "the dropped CHECK constraint should no longer be enforced")

	for _, column := range []string{"c01", "c02", "c03", "c04", "c06", "c07", "c08", "c09", "c10", "c11"} {
		_, err = tt.DB.ExecContext(t.Context(),
			fmt.Sprintf("INSERT INTO chk_many (%s) VALUES (-1)", column))
		require.Error(t, err, "the CHECK constraint on %s should still be enforced", column)
	}
}

// TestCheckConstraintDropAmongIdenticalExpressions is the same mispairing as
// TestCheckConstraintDropWithTenPlusConstraints, on a table where the
// expressions cannot tell the constraints apart. Comparing expressions catches a
// mispairing when they differ; here only enforcement differs, so getting the
// pairing wrong silently drops a constraint the user did not name and leaves the
// one they dropped in force.
func TestCheckConstraintDropAmongIdenticalExpressions(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_dup", `CREATE TABLE chk_dup (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		val INT NOT NULL,
		CONSTRAINT chk_dup_01 CHECK (val > 0) NOT ENFORCED,
		CONSTRAINT chk_dup_02 CHECK (val > 0) NOT ENFORCED,
		CONSTRAINT chk_dup_03 CHECK (val > 0) NOT ENFORCED,
		CONSTRAINT chk_dup_04 CHECK (val > 0) NOT ENFORCED,
		CONSTRAINT chk_dup_05 CHECK (val > 0),
		CONSTRAINT chk_dup_06 CHECK (val > 0) NOT ENFORCED,
		CONSTRAINT chk_dup_07 CHECK (val > 0) NOT ENFORCED,
		CONSTRAINT chk_dup_08 CHECK (val > 0) NOT ENFORCED,
		CONSTRAINT chk_dup_09 CHECK (val > 0) NOT ENFORCED,
		CONSTRAINT chk_dup_10 CHECK (val > 0) NOT ENFORCED,
		CONSTRAINT chk_dup_11 CHECK (val > 0) NOT ENFORCED
	)`)
	testutils.RunSQL(t, `INSERT INTO chk_dup (val) VALUES (1), (2)`)

	m := NewTestRunner(t, "chk_dup", "DROP CHECK chk_dup_05, ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.False(t, m.usedInstantDDL)
	require.NoError(t, m.Close())

	// The only enforced constraint was the one that was dropped.
	constraints := tableCheckConstraints(t, tt.DB, "chk_dup")
	require.Len(t, constraints, 10)
	require.False(t, slices.Contains(slices.Collect(maps.Values(constraints)), true),
		"no constraint should be enforced after dropping the only enforced one: %v", constraints)

	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_dup (val) VALUES (-1)")
	require.NoError(t, err, "the dropped CHECK constraint should no longer be enforced")
}

// TestDropConstraintAmbiguousWithUniqueKey covers a DROP CONSTRAINT whose name
// is held by both a CHECK constraint and a unique key. MySQL refuses to guess
// which one is meant (error 3939), and Spirit must refuse too: it applies the
// ALTER to the _new table, where CREATE TABLE .. LIKE has renamed the check
// constraint, so the name is no longer ambiguous there and the copy would drop
// the check constraint, keep the unique key, and report success.
func TestDropConstraintAmbiguousWithUniqueKey(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_ambig", `CREATE TABLE chk_ambig (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(32) NOT NULL,
		val INT NOT NULL,
		CONSTRAINT dup UNIQUE (name),
		CONSTRAINT dup CHECK (val > 0)
	)`)
	testutils.RunSQL(t, `INSERT INTO chk_ambig (name, val) VALUES ('a', 1), ('b', 2)`)

	// MySQL rejects this statement outright, so run it to confirm the premise
	// rather than assuming it.
	_, err := tt.DB.ExecContext(t.Context(), "ALTER TABLE chk_ambig DROP CONSTRAINT dup")
	require.ErrorContains(t, err, "multiple constraints with the name")

	m := NewTestRunner(t, "chk_ambig", "DROP CONSTRAINT dup")
	err = m.Run(t.Context())
	require.ErrorContains(t, err, "ambiguous")
	require.NoError(t, m.Close())

	// Neither constraint was dropped.
	require.Equal(t, map[string]bool{"dup": true}, tableCheckConstraints(t, tt.DB, "chk_ambig"))
	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_ambig (name, val) VALUES ('c', -1)")
	require.Error(t, err, "the CHECK constraint should still be enforced")
	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_ambig (name, val) VALUES ('a', 1)")
	require.Error(t, err, "the unique key should still be enforced")

	// DROP CHECK says which one is meant, so it is not ambiguous and goes ahead.
	m = NewTestRunner(t, "chk_ambig", "DROP CHECK dup, ENGINE=InnoDB")
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())
	require.Empty(t, tableCheckConstraints(t, tt.DB, "chk_ambig"))
	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_ambig (name, val) VALUES ('c', -1)")
	require.NoError(t, err, "the CHECK constraint should have been dropped")
}

// TestDropConstraintAmbiguousWithPrimaryKey is the same clash against the
// primary key, which MySQL matches under the symbol PRIMARY even though
// SHOW CREATE TABLE prints no name for it.
func TestDropConstraintAmbiguousWithPrimaryKey(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_ambig_pk", `CREATE TABLE chk_ambig_pk (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		val INT NOT NULL,
		CONSTRAINT `+"`PRIMARY`"+` CHECK (val > 0)
	)`)
	testutils.RunSQL(t, "INSERT INTO chk_ambig_pk (val) VALUES (1), (2)")

	m := NewTestRunner(t, "chk_ambig_pk", "DROP CONSTRAINT `PRIMARY`")
	require.ErrorContains(t, m.Run(t.Context()), "ambiguous")
	require.NoError(t, m.Close())

	require.Equal(t, map[string]bool{"PRIMARY": true}, tableCheckConstraints(t, tt.DB, "chk_ambig_pk"))
}

// TestDropConstraintResolvesToUniqueKey pins the case the ambiguity check must
// not fire on: the name is held by a unique key only, so it is unambiguous even
// though the table has check constraints for the rewriting to consider.
func TestDropConstraintResolvesToUniqueKey(t *testing.T) {
	t.Parallel()
	tt := testutils.NewTestTable(t, "chk_uq_only", `CREATE TABLE chk_uq_only (
		id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
		name VARCHAR(32) NOT NULL,
		val INT NOT NULL,
		CONSTRAINT uq_chk_uq_only_name UNIQUE (name),
		CONSTRAINT chk_uq_only_valpos CHECK (val > 0)
	)`)
	testutils.RunSQL(t, `INSERT INTO chk_uq_only (name, val) VALUES ('a', 1), ('b', 2)`)

	m := NewTestRunner(t, "chk_uq_only", "DROP CONSTRAINT uq_chk_uq_only_name")
	require.NoError(t, m.Run(t.Context()))
	require.NoError(t, m.Close())

	// The unique key is gone and the check constraint - renamed by the copy, as
	// every copied check constraint is - is still enforced.
	_, err := tt.DB.ExecContext(t.Context(), "INSERT INTO chk_uq_only (name, val) VALUES ('a', 1)")
	require.NoError(t, err, "the unique key should have been dropped")
	require.Len(t, tableCheckConstraints(t, tt.DB, "chk_uq_only"), 1)
	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO chk_uq_only (name, val) VALUES ('c', -1)")
	require.Error(t, err, "the CHECK constraint should still be enforced")
}

// tableCheckConstraints returns the table's CHECK constraints as a map of name to
// whether the constraint is enforced.
func tableCheckConstraints(t *testing.T, db *sql.DB, tableName string) map[string]bool {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT CONSTRAINT_NAME, ENFORCED
		FROM information_schema.TABLE_CONSTRAINTS
		WHERE CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = ? AND CONSTRAINT_TYPE = 'CHECK'`,
		tableName)
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck // test cleanup
	constraints := make(map[string]bool)
	for rows.Next() {
		var name, enforced string
		require.NoError(t, rows.Scan(&name, &enforced))
		constraints[name] = enforced == "YES"
	}
	require.NoError(t, rows.Err())
	return constraints
}

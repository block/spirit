package check

import (
	"context"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ordersTable is the current definition of the table the statement-scope tests
// alter, in the form SHOW CREATE TABLE reports it.
const ordersTable = "CREATE TABLE `orders` (\n" +
	"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
	"  `status` enum('new','shipped','done') NOT NULL DEFAULT 'new',\n" +
	"  `perms` set('read','write','execute') DEFAULT NULL,\n" +
	"  `name` varchar(100) DEFAULT NULL,\n" +
	"  PRIMARY KEY (`id`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// TestStatementScopeMembership pins the checks ScopeStatement carries. A check
// joining the scope has to clear the scope's contract first: no earlier stage
// may bypass it, so its failure is a refusal a caller can report as certain,
// and it must tolerate every resource except the statement being unset. Adding
// the name here is the same change that decides it does, and a caller that
// reports these refusals to its own users pins this set too — the message text
// is only as safe to repeat as the check that produced it.
func TestStatementScopeMembership(t *testing.T) {
	assert.Equal(t, []string{
		"addforeignkey",
		"enumReorder",
		"enumSetRemoval",
		"illegalClause",
		"primarykey",
		"primarykeyexists",
		"setReorder",
	}, ChecksInScope(ScopeStatement))
}

// TestStatementScopeChecks runs the statement-scoped checks the way an
// external classifier does: only Resources.Statement is set — no database
// connection and no table metadata. Statements Spirit deterministically
// refuses must fail with the same message preflight would report, and
// ordinary schema changes must pass.
func TestStatementScopeChecks(t *testing.T) {
	tests := []struct {
		name    string
		stmt    string
		wantErr string
	}{
		{
			name:    "drop primary key is refused",
			stmt:    "ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY (anothercol)",
			wantErr: "dropping primary key is not supported",
		},
		{
			name:    "add foreign key is refused",
			stmt:    "ALTER TABLE t1 ADD CONSTRAINT fk FOREIGN KEY (user_id) REFERENCES users (id)",
			wantErr: "adding foreign key constraints is not supported",
		},
		{
			name:    "explicit algorithm clause is refused",
			stmt:    "ALTER TABLE t1 ADD COLUMN b INT, ALGORITHM=INPLACE",
			wantErr: "ALGORITHM=",
		},
		{
			name:    "explicit lock clause is refused",
			stmt:    "ALTER TABLE t1 ADD COLUMN b INT, LOCK=NONE",
			wantErr: "LOCK=",
		},
		{
			name: "add column passes",
			stmt: "ALTER TABLE t1 ADD COLUMN b INT",
		},
		{
			name: "add index passes",
			stmt: "ALTER TABLE t1 ADD INDEX idx_b (b)",
		},
		{
			name: "column rename passes without table metadata",
			stmt: "ALTER TABLE t1 RENAME COLUMN c1 TO c2",
		},
		{
			name: "enum reorder passes without table metadata",
			stmt: "ALTER TABLE t1 MODIFY COLUMN status ENUM('shipped','new')",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := Resources{Statement: statement.MustNew(tt.stmt)[0]}
			err := RunChecks(t.Context(), r, discardLogger(), ScopeStatement)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// TestStatementScopeExcludesNativeDDLChecks covers the preflight checks
// deliberately kept out of ScopeStatement. Spirit attempts MySQL's native DDL
// before running preflight checks, and the native DDL can complete these
// metadata-only statements, so a classifier must not report them as refusals.
// Each is still refused at preflight, once Spirit knows the native DDL did not
// take the statement.
func TestStatementScopeExcludesNativeDDLChecks(t *testing.T) {
	tests := []struct {
		name    string
		stmt    string
		check   func(context.Context, Resources, *slog.Logger) error
		wantErr string
	}{
		{
			name:    "drop and re-add of the same column",
			stmt:    "ALTER TABLE t1 DROP COLUMN b, ADD COLUMN b INT",
			check:   dropAddCheck,
			wantErr: "mentioned 2 times",
		},
		{
			name:    "table rename",
			stmt:    "ALTER TABLE t1 RENAME TO t2",
			check:   renameCheck,
			wantErr: "table renames are not supported",
		},
		{
			name:    "rename overlapping an added column",
			stmt:    "ALTER TABLE t1 RENAME COLUMN c1 TO n1, ADD COLUMN c1 INT",
			check:   renameCheck,
			wantErr: "could cause data corruption",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := Resources{Statement: statement.MustNew(tt.stmt)[0]}
			require.NoError(t, RunChecks(t.Context(), r, discardLogger(), ScopeStatement),
				"statement-scope checks must stay silent on statements MySQL's native DDL can complete")
			err := tt.check(t.Context(), r, discardLogger())
			require.Error(t, err, "the preflight check must still refuse the statement")
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// TestStatementRefusalWithTableMetadata classifies statements with the table's
// current definition supplied, which widens coverage to the checks that compare
// a redeclared column against its existing type. MySQL cannot complete any of
// the refused shapes as native DDL — each needs a table rebuild — so they are
// safe for a classifier to report ahead of an apply.
func TestStatementRefusalWithTableMetadata(t *testing.T) {
	tests := []struct {
		name       string
		stmt       string
		wantReason string
	}{
		{
			name:       "enum reorder is refused",
			stmt:       "ALTER TABLE orders MODIFY COLUMN status ENUM('shipped','new','done') NOT NULL",
			wantReason: `unsafe ENUM value reorder on column "status"`,
		},
		{
			name:       "enum middle insertion is refused",
			stmt:       "ALTER TABLE orders MODIFY COLUMN status ENUM('new','pending','shipped','done') NOT NULL",
			wantReason: `unsafe ENUM value reorder on column "status"`,
		},
		{
			name:       "set reorder is refused",
			stmt:       "ALTER TABLE orders MODIFY COLUMN perms SET('write','read','execute')",
			wantReason: `unsafe SET value reorder on column "perms"`,
		},
		{
			name:       "enum to numeric conversion is refused",
			stmt:       "ALTER TABLE orders MODIFY COLUMN status INT NOT NULL",
			wantReason: `unsafe ENUM to int(11) type conversion on column "status"`,
		},
		{
			name:       "set to enum conversion is refused",
			stmt:       "ALTER TABLE orders MODIFY COLUMN perms ENUM('read','write')",
			wantReason: `unsafe SET to ENUM type conversion on column "perms"`,
		},
		{
			name: "appending enum values passes",
			stmt: "ALTER TABLE orders MODIFY COLUMN status ENUM('new','shipped','done','lost') NOT NULL",
		},
		{
			name: "dropping enum values passes",
			stmt: "ALTER TABLE orders MODIFY COLUMN status ENUM('new','done') NOT NULL",
		},
		{
			name: "appending set values passes",
			stmt: "ALTER TABLE orders MODIFY COLUMN perms SET('read','write','execute','admin')",
		},
		{
			name: "enum to varchar conversion passes",
			stmt: "ALTER TABLE orders MODIFY COLUMN status VARCHAR(20) NOT NULL",
		},
		{
			name: "widening an unrelated column passes",
			stmt: "ALTER TABLE orders MODIFY COLUMN name VARCHAR(255)",
		},
		{
			name: "adding a column passes",
			stmt: "ALTER TABLE orders ADD COLUMN shipped_at DATETIME",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason, refused, err := StatementRefusal(t.Context(), tt.stmt, ordersTable, discardLogger())
			require.NoError(t, err)
			if tt.wantReason == "" {
				assert.False(t, refused)
				assert.Empty(t, reason)
				return
			}
			require.True(t, refused)
			assert.Contains(t, reason, tt.wantReason)
		})
	}
}

// TestStatementRefusalNonAlterStatements classifies the statement types Spirit
// runs as native DDL rather than through the copy process. They are never
// refused, so a classifier reports them as applying normally.
func TestStatementRefusalNonAlterStatements(t *testing.T) {
	for _, stmt := range []string{
		"CREATE TABLE t9 (id INT NOT NULL PRIMARY KEY)",
		"DROP TABLE t9",
	} {
		t.Run(stmt, func(t *testing.T) {
			reason, refused, err := StatementRefusal(t.Context(), stmt, ordersTable, discardLogger())
			require.NoError(t, err)
			assert.False(t, refused)
			assert.Empty(t, reason)
		})
	}
}

// TestStatementRefusalWithoutTableMetadata omits the table's current definition.
// The ENUM/SET checks cannot run without the existing column types, so they are
// skipped rather than guessed at, while the statement-only refusals still apply.
func TestStatementRefusalWithoutTableMetadata(t *testing.T) {
	reason, refused, err := StatementRefusal(t.Context(),
		"ALTER TABLE orders MODIFY COLUMN status ENUM('shipped','new','done') NOT NULL", "", discardLogger())
	require.NoError(t, err)
	assert.False(t, refused)
	assert.Empty(t, reason)

	reason, refused, err = StatementRefusal(t.Context(),
		"ALTER TABLE orders DROP PRIMARY KEY", "", discardLogger())
	require.NoError(t, err)
	assert.True(t, refused)
	assert.Equal(t, "dropping primary key is not supported", reason)
}

// TestStatementRefusalNilLogger classifies statements without a logger. A caller
// running the checks outside a migration has no logger to hand, and every path —
// a refusal, a pass, and the skip taken when no table metadata is supplied —
// must still return a verdict.
func TestStatementRefusalNilLogger(t *testing.T) {
	reason, refused, err := StatementRefusal(t.Context(),
		"ALTER TABLE orders MODIFY COLUMN status ENUM('shipped','new','done') NOT NULL", ordersTable, nil)
	require.NoError(t, err)
	assert.True(t, refused)
	assert.Contains(t, reason, `unsafe ENUM value reorder on column "status"`)

	reason, refused, err = StatementRefusal(t.Context(),
		"ALTER TABLE orders MODIFY COLUMN status ENUM('shipped','new','done') NOT NULL", "", nil)
	require.NoError(t, err)
	assert.False(t, refused, "the ENUM check must skip without table metadata")
	assert.Empty(t, reason)

	reason, refused, err = StatementRefusal(t.Context(),
		"ALTER TABLE orders ADD COLUMN shipped_at DATETIME", ordersTable, nil)
	require.NoError(t, err)
	assert.False(t, refused)
	assert.Empty(t, reason)
}

// TestStatementRefusalErrors covers input that cannot be classified at all. Each
// case must surface as an error rather than a refusal: reporting "Spirit refuses
// this statement" for input Spirit never judged would block an apply for the
// wrong reason.
func TestStatementRefusalErrors(t *testing.T) {
	tests := []struct {
		name               string
		stmt               string
		currentCreateTable string
		wantErr            string
	}{
		{
			name:    "unparseable statement",
			stmt:    "ALTER TABLE orders FLUX CAPACITOR",
			wantErr: "parse statement to check for refusal",
		},
		{
			name:    "more than one statement",
			stmt:    "ALTER TABLE a ADD COLUMN x INT; ALTER TABLE b ADD COLUMN y INT",
			wantErr: "exactly one statement, got 2",
		},
		{
			name:               "unparseable current definition",
			stmt:               "ALTER TABLE orders ADD COLUMN x INT",
			currentCreateTable: "CREATE TABLE orders (",
			wantErr:            `parse current definition of table "orders"`,
		},
		{
			name:               "current definition is for another table",
			stmt:               "ALTER TABLE orders MODIFY COLUMN status ENUM('shipped','new')",
			currentCreateTable: "CREATE TABLE `customers` (`id` INT NOT NULL PRIMARY KEY)",
			wantErr:            `current definition is for table "customers" but the statement alters table "orders"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason, refused, err := StatementRefusal(t.Context(), tt.stmt, tt.currentCreateTable, discardLogger())
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.False(t, refused)
			assert.Empty(t, reason)
		})
	}
}

// TestStatementRefusalUncoveredColumn classifies a statement that redeclares a
// column the supplied definition does not carry — a stale definition, or one
// read after the column was dropped. The ENUM/SET checks cannot compare against
// a current type they do not have, and that says nothing about whether Spirit
// accepts the statement, so it surfaces as an error rather than a refusal a
// caller would act on.
func TestStatementRefusalUncoveredColumn(t *testing.T) {
	tests := []struct {
		name    string
		stmt    string
		wantErr string
	}{
		{
			name:    "enum column absent from the definition",
			stmt:    "ALTER TABLE orders MODIFY COLUMN ghost ENUM('a','b')",
			wantErr: `unable to validate ENUM change for column "ghost"`,
		},
		{
			name:    "set column absent from the definition",
			stmt:    "ALTER TABLE orders MODIFY COLUMN ghost SET('a','b')",
			wantErr: `unable to validate SET reorder for column "ghost"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reason, refused, err := StatementRefusal(t.Context(), tt.stmt, ordersTable, discardLogger())
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.False(t, refused, "an unclassifiable statement must not be reported as refused")
			assert.Empty(t, reason)
		})
	}
}

// TestEnumSetChecksRequireTableMetadataOutsideStatementScope covers the ENUM/SET
// checks running without table metadata in a scope other than ScopeStatement.
// Only a statement-scope caller is allowed to omit it; a migration loads the
// table before it runs any check, so metadata missing there means these guards
// against a data-corrupting change would not run at all, and the check fails
// rather than passing quietly.
func TestEnumSetChecksRequireTableMetadataOutsideStatementScope(t *testing.T) {
	const stmt = "ALTER TABLE orders MODIFY COLUMN status ENUM('shipped','new','done') NOT NULL"
	checks := map[string]func(context.Context, Resources, *slog.Logger) error{
		"enumReorder":    enumReorderCheck,
		"setReorder":     setReorderCheck,
		"enumSetRemoval": enumSetRemovalCheck,
	}
	for name, check := range checks {
		t.Run(name, func(t *testing.T) {
			r := Resources{Statement: statement.MustNew(stmt)[0], scope: ScopePreflight}
			err := check(t.Context(), r, discardLogger())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "check "+name+" cannot run")

			r.scope = ScopeStatement
			require.NoError(t, check(t.Context(), r, discardLogger()),
				"a statement-scope caller may omit the table metadata")
		})
	}
}

// TestStatementScopeChecksDeterministicError verifies that a statement failing
// more than one statement-scoped check reports the same error every run:
// RunChecks iterates checks in name order, so classifiers surfacing the error
// to users see a stable message.
func TestStatementScopeChecksDeterministicError(t *testing.T) {
	const stmt = "ALTER TABLE t1 DROP PRIMARY KEY, ADD CONSTRAINT fk FOREIGN KEY (user_id) REFERENCES users (id)"
	first := RunChecks(t.Context(), Resources{Statement: statement.MustNew(stmt)[0]}, discardLogger(), ScopeStatement)
	require.Error(t, first)
	for range 20 {
		err := RunChecks(t.Context(), Resources{Statement: statement.MustNew(stmt)[0]}, discardLogger(), ScopeStatement)
		require.Error(t, err)
		assert.Equal(t, first.Error(), err.Error())
	}
}

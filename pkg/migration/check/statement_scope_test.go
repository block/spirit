package check

import (
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
			name:    "drop and re-add of the same column is refused",
			stmt:    "ALTER TABLE t1 DROP COLUMN b, ADD COLUMN b INT",
			wantErr: "mentioned 2 times",
		},
		{
			name:    "table rename is refused",
			stmt:    "ALTER TABLE t1 RENAME TO t2",
			wantErr: "table renames are not supported",
		},
		{
			name:    "rename overlapping an added column is refused",
			stmt:    "ALTER TABLE t1 RENAME COLUMN c1 TO n1, ADD COLUMN c1 INT",
			wantErr: "could cause data corruption",
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := Resources{Statement: statement.MustNew(tt.stmt)[0]}
			err := RunChecks(t.Context(), r, slog.Default(), ScopeStatement)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// TestStatementScopeChecksDeterministicError verifies that a statement failing
// more than one statement-scoped check reports the same error every run:
// RunChecks iterates checks in name order, so classifiers surfacing the error
// to users see a stable message.
func TestStatementScopeChecksDeterministicError(t *testing.T) {
	const stmt = "ALTER TABLE t1 DROP PRIMARY KEY, ADD CONSTRAINT fk FOREIGN KEY (user_id) REFERENCES users (id)"
	first := RunChecks(t.Context(), Resources{Statement: statement.MustNew(stmt)[0]}, slog.Default(), ScopeStatement)
	require.Error(t, first)
	for range 20 {
		err := RunChecks(t.Context(), Resources{Statement: statement.MustNew(stmt)[0]}, slog.Default(), ScopeStatement)
		require.Error(t, err)
		assert.Equal(t, first.Error(), err.Error())
	}
}

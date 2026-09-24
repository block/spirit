package dbconn

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestForceKillAfterValidation(t *testing.T) {
	for _, tc := range []struct {
		delay time.Duration
		valid bool
	}{
		{0, true}, {500 * time.Millisecond, true}, {5 * time.Second, true},
		{-time.Second, false}, {10 * time.Second, false}, {11 * time.Second, false},
	} {
		config := NewDBConfig()
		config.LockWaitTimeout = 10
		config.ForceKillAfter = tc.delay
		if tc.valid {
			require.NoError(t, config.ValidateForceKillAfter())
		} else {
			require.Error(t, config.ValidateForceKillAfter())
		}
	}
	config := NewDBConfig()
	config.LockWaitTimeout = 10
	require.Equal(t, 9*time.Second, config.forceKillDelay())
	config.ForceKillAfter = 500 * time.Millisecond
	require.Equal(t, 500*time.Millisecond, config.forceKillDelay())
	config.LockWaitTimeout = 30
	require.Equal(t, 500*time.Millisecond, config.forceKillDelay(), "explicit delay is independent of the lock timeout")
}

// Both consumers must honor the explicit delay rather than waiting 90% of 10s.
func TestExplicitForceKillAfter(t *testing.T) {
	for _, operation := range []string{"ddl", "table-lock"} {
		t.Run(operation, func(t *testing.T) {
			tt := testutils.NewTestTable(t, "explicit_kill_after", "CREATE TABLE explicit_kill_after (id INT PRIMARY KEY)")
			config := NewDBConfig()
			config.LockWaitTimeout = 10
			config.ForceKillAfter = 500 * time.Millisecond
			db, err := New(testutils.DSN(), config)
			require.NoError(t, err)
			defer utils.CloseAndLog(db)
			blocker, err := tt.DB.BeginTx(t.Context(), nil)
			require.NoError(t, err)
			defer func() { _ = blocker.Rollback() }()
			_, err = blocker.ExecContext(t.Context(), "SELECT * FROM explicit_kill_after")
			require.NoError(t, err)
			var schema string
			require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT DATABASE()").Scan(&schema))
			tables := []*table.TableInfo{{SchemaName: schema, TableName: "explicit_kill_after", QuotedTableName: "`explicit_kill_after`"}}
			// The old 9s delay cannot acquire the lock before this deadline.
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			start := time.Now()
			if operation == "ddl" {
				require.NoError(t, ForceExec(ctx, db, tables, config, slog.Default(), "ALTER TABLE explicit_kill_after ADD COLUMN c INT, ALGORITHM=INSTANT"))
			} else {
				lock, err := NewTableLock(ctx, db, tables, config, slog.Default())
				require.NoError(t, err)
				require.NoError(t, lock.Close(ctx))
			}
			require.GreaterOrEqual(t, time.Since(start), config.ForceKillAfter, "must preserve the grace period")
			_, err = blocker.ExecContext(ctx, "SELECT 1")
			require.Error(t, err)
		})
	}
}

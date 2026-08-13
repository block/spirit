package check

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/block/spirit/pkg/statement"
)

func init() {
	registerCheck("resume_state", resumeStateCheck, ScopeResume)
}

// resumeStateCheck validates that the state is compatible with resuming from a checkpoint.
// This check runs at ScopeResume and verifies:
// 1. Checkpoint table exists on the first target
// 2. Target tables exist with matching schema to source
// 3. All source tables have corresponding target tables
//
// This check is the complement to target_state check:
// - target_state validates we can start a NEW copy (empty or matching empty tables)
// - resume_state validates we can RESUME from checkpoint (tables exist with data)
func resumeStateCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	if len(r.SourceTables) == 0 {
		return errors.New("no source tables available for resume validation")
	}
	if len(r.Targets) == 0 {
		return errors.New("no targets configured")
	}
	// Check 1: Verify checkpoint table exists on targets[0] (by convention).
	// The runner sorts targets so targets[0] is stable across runs.
	tgt0 := r.Targets[0]
	if tgt0.DB == nil || tgt0.Config == nil {
		return errors.New("target[0] database connection or config is not initialized")
	}
	// Must match pkg/move's checkpointTableName. It is duplicated here because
	// the check package cannot import move (move imports check).
	checkpointTableName := "_spirit_move_checkpoint"
	var checkpointExists int
	err := tgt0.DB.QueryRowContext(ctx,
		"SELECT 1 FROM information_schema.TABLES WHERE table_schema = ? AND table_name = ?",
		tgt0.Config.DBName, checkpointTableName).Scan(&checkpointExists)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("checkpoint table '%s.%s' does not exist; cannot resume", tgt0.Config.DBName, checkpointTableName)
	}
	if err != nil {
		return fmt.Errorf("failed to check for checkpoint table: %w", err)
	}

	logger.Info("checkpoint table exists, validating target tables for resume",
		"checkpoint_table", fmt.Sprintf("%s.%s", tgt0.Config.DBName, checkpointTableName))

	// Check 2: Verify all source tables have corresponding target tables with matching schema
	for _, sourceTable := range r.SourceTables {
		for i, target := range r.Targets {
			// Check if table exists on target
			var tableExists int
			err := target.DB.QueryRowContext(ctx,
				"SELECT 1 FROM information_schema.TABLES WHERE table_schema = ? AND table_name = ?",
				target.Config.DBName, sourceTable.TableName).Scan(&tableExists)
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("table '%s' does not exist on target %d (%s); cannot resume. Target state is incomplete",
					sourceTable.TableName, i, target.Config.DBName)
			}
			if err != nil {
				return fmt.Errorf("failed to check for table '%s' on target %d: %w", sourceTable.TableName, i, err)
			}

			// Verify schema matches by comparing the canonicalized CREATE TABLE
			// of source and target, not just the ordered column names. A
			// names-only comparison let a target with the same columns but a
			// different type/charset/collation pass resume validation; on a
			// resume whose checksum watermark already covers the affected chunk,
			// that mismatch would never be re-verified. statement.SchemaDiff
			// compares types, charset, collation, indexes and constraints while
			// ignoring AUTO_INCREMENT counters and other instance-specific noise.
			sourceCreate, err := showCreateTable(ctx, sourceTable.DB(), sourceTable.SchemaName, sourceTable.TableName)
			if err != nil {
				return fmt.Errorf("failed to read source schema for table '%s': %w", sourceTable.TableName, err)
			}
			targetCreate, err := showCreateTable(ctx, target.DB, target.Config.DBName, sourceTable.TableName)
			if err != nil {
				return fmt.Errorf("failed to read target %d schema for table '%s': %w", i, sourceTable.TableName, err)
			}
			diff, err := statement.SchemaDiff(sourceTable.TableName, sourceCreate, targetCreate)
			if err != nil {
				return fmt.Errorf("failed to compare schema for table '%s' on target %d: %w", sourceTable.TableName, i, err)
			}
			if diff != "" {
				return fmt.Errorf("table '%s' schema mismatch between source and target %d (%s); cannot resume safely. Schema may have changed since checkpoint was created. Reconcile the target to the source with: %s",
					sourceTable.TableName, i, target.Config.DBName, diff)
			}

			logger.Debug("validated target table for resume",
				"table", sourceTable.TableName,
				"target", i,
				"database", target.Config.DBName)
		}
	}
	return nil
}

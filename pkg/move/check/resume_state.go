package check

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/block/spirit/pkg/table"
)

func init() {
	registerCheck("resume_state", resumeStateCheck, ScopeResume)
}

// resumeStateCheck validates that the state is compatible with resuming from a checkpoint.
// This check runs at ScopeResume and verifies:
// 1. Checkpoint table exists on source
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

	if len(r.Sources) == 0 {
		return errors.New("no sources configured for resume validation")
	}

	// Check 1: Verify checkpoint table exists on sources[0] (by convention).
	src0 := r.Sources[0]
	checkpointTableName := "_spirit_checkpoint"
	var checkpointExists int
	err := src0.DB.QueryRowContext(ctx,
		"SELECT 1 FROM information_schema.TABLES WHERE table_schema = ? AND table_name = ?",
		src0.Config.DBName, checkpointTableName).Scan(&checkpointExists)
	if err == sql.ErrNoRows {
		return fmt.Errorf("checkpoint table '%s.%s' does not exist; cannot resume", src0.Config.DBName, checkpointTableName)
	}
	if err != nil {
		return fmt.Errorf("failed to check for checkpoint table: %w", err)
	}

	logger.Info("checkpoint table exists, validating target tables for resume",
		"checkpoint_table", fmt.Sprintf("%s.%s", src0.Config.DBName, checkpointTableName))

	// Check 2: Verify all source tables have corresponding target tables with matching schema
	for _, sourceTable := range r.SourceTables {
		for i, target := range r.Targets {
			// Check if table exists on target
			var tableExists int
			err := target.DB.QueryRowContext(ctx,
				"SELECT 1 FROM information_schema.TABLES WHERE table_schema = ? AND table_name = ?",
				target.Config.DBName, sourceTable.TableName).Scan(&tableExists)
			if err == sql.ErrNoRows {
				return fmt.Errorf("table '%s' does not exist on target %d (%s); cannot resume. Target state is incomplete",
					sourceTable.TableName, i, target.Config.DBName)
			}
			if err != nil {
				return fmt.Errorf("failed to check for table '%s' on target %d: %w", sourceTable.TableName, i, err)
			}

			// Verify schema matches
			targetTable := table.NewTableInfo(target.DB, target.Config.DBName, sourceTable.TableName)
			if err := targetTable.SetInfo(ctx); err != nil {
				return fmt.Errorf("failed to get table info for target %d table '%s': %w", i, sourceTable.TableName, err)
			}

			if !slices.Equal(sourceTable.Columns, targetTable.Columns) {
				return fmt.Errorf("table '%s' schema mismatch between source and target %d (%s); cannot resume safely. Schema may have changed since checkpoint was created",
					sourceTable.TableName, i, target.Config.DBName)
			}

			logger.Debug("validated target table for resume",
				"table", sourceTable.TableName,
				"target", i,
				"database", target.Config.DBName,
				"columns", len(targetTable.Columns))
		}
	}
	return nil
}

package migration

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

type tableChange struct {
	stmt     *statement.AbstractStatement
	table    *table.TableInfo
	newTable *table.TableInfo

	// chunker is the MappedChunker specific to this change.
	// It is set during initChunkers() and used by setupCopierCheckerAndReplClient()
	// to pass to AddSubscription, which requires a single-table MappedChunker
	// (not the multi-chunker wrapper stored on the Runner).
	chunker table.MappedChunker

	// Store a pointer back to the migration runner
	// (for compatibility, we want to eventually remove this)
	runner *Runner
}

func (c *tableChange) createNewTable(ctx context.Context) error {
	newName := utils.NewTableName(c.table.TableName)
	// drop the newName if we've decided to call this func.
	if err := dbconn.Exec(ctx, c.runner.db, "DROP TABLE IF EXISTS %n", newName); err != nil {
		return err
	}
	if err := dbconn.Exec(ctx, c.runner.db, "CREATE TABLE %n LIKE %n",
		newName, c.table.TableName); err != nil {
		return err
	}
	c.newTable = table.NewTableInfo(c.runner.db, c.stmt.Schema, newName)
	if err := c.newTable.SetInfo(ctx); err != nil {
		return err
	}
	return nil
}

// alterNewTable applies the ALTER to the new table.
// It has been pre-checked it is not a rename, or modifying the PRIMARY KEY.
// We first attempt to do this using ALGORITHM=COPY so we don't burn
// an INSTANT version. But surprisingly this is not supported for all DDLs (issue #277)
func (c *tableChange) alterNewTable(ctx context.Context) error {
	// The user's ALTER clause is raw SQL, not a sqlescape format string: it may
	// legitimately contain % characters (e.g. COMMENT '100%new'). Escape the
	// trusted prefix only, append the clause verbatim, and execute via ExecRaw.
	alterPrefix := sqlescape.MustEscapeSQL("ALTER TABLE %n ", c.newTable.TableName)
	if err := dbconn.ExecRaw(ctx, c.runner.db, alterPrefix+c.stmt.TrimAlter()+", ALGORITHM=COPY"); err != nil {
		// Retry without the ALGORITHM=COPY. If there is a second error, then the DDL itself
		// is not supported. It could be a syntax error, in which case we return the second error,
		// which will probably be easier to read because it is unaltered.
		if err := dbconn.ExecRaw(ctx, c.runner.db, alterPrefix+c.stmt.Alter); err != nil {
			return err
		}
	}
	// Call GetInfo on the table again, since the columns
	// might have changed and this will affect the row copiers intersect func.
	if err := c.newTable.SetInfo(ctx); err != nil {
		return err
	}

	// Preserve AUTO_INCREMENT value from the original table AFTER the ALTER.
	// CREATE TABLE LIKE doesn't copy AUTO_INCREMENT, and ALTER with ALGORITHM=COPY
	// can reset it. For empty tables, INSERT SELECT won't trigger MySQL's automatic
	// adjustment, so we explicitly set it to prevent new inserts from restarting at 1.
	return c.preserveAutoIncrement(ctx)
}

func (c *tableChange) preserveAutoIncrement(ctx context.Context) error {
	// Get AUTO_INCREMENT from the original table.
	var originalAutoInc sql.NullInt64
	err := c.runner.db.QueryRowContext(ctx,
		"SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
		c.table.TableName).Scan(&originalAutoInc)
	if err != nil {
		return fmt.Errorf("failed to get AUTO_INCREMENT value from original table: %w", err)
	}

	// If the original table doesn't have a meaningful AUTO_INCREMENT, nothing to preserve.
	if !originalAutoInc.Valid || originalAutoInc.Int64 <= 1 {
		return nil
	}

	// Get AUTO_INCREMENT from the new table to detect if it was explicitly set by the ALTER.
	var newTableAutoInc sql.NullInt64
	err = c.runner.db.QueryRowContext(ctx,
		"SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
		c.newTable.TableName).Scan(&newTableAutoInc)
	if err != nil {
		return fmt.Errorf("failed to get AUTO_INCREMENT value from new table: %w", err)
	}

	// Only override AUTO_INCREMENT on the new table if it doesn't appear to have been explicitly set.
	// If the new table's AUTO_INCREMENT is different from the original, the user explicitly changed it.
	if newTableAutoInc.Valid && newTableAutoInc.Int64 > 1 && newTableAutoInc.Int64 != originalAutoInc.Int64 {
		// Respect the explicitly configured AUTO_INCREMENT on the new table.
		return nil
	}

	if err := dbconn.Exec(ctx, c.runner.db, "ALTER TABLE %n AUTO_INCREMENT = %?",
		c.newTable.TableName, originalAutoInc.Int64); err != nil {
		return fmt.Errorf("failed to set AUTO_INCREMENT on new table: %w", err)
	}
	c.runner.logger.Info("preserved AUTO_INCREMENT value",
		"table", c.table.TableName,
		"auto_increment", originalAutoInc.Int64)
	return nil
}

func (c *tableChange) dropOldTable(ctx context.Context) error {
	return dbconn.Exec(ctx, c.runner.db, "DROP TABLE IF EXISTS %n", c.oldTableName())
}

func (c *tableChange) oldTableName() string {
	if !c.runner.migration.SkipDropAfterCutover {
		return utils.OldTableName(c.table.TableName)
	}
	timestamp := c.runner.startTime.UTC().Format(utils.NameFormatTimestamp)
	return utils.OldTableNameWithTimestamp(c.table.TableName, timestamp)
}

func (c *tableChange) attemptInstantDDL(ctx context.Context) error {
	// The user's ALTER clause is raw SQL, not a sqlescape format string: escape
	// the trusted prefix only, then append the clause verbatim.
	stmt := sqlescape.MustEscapeSQL("ALTER TABLE %n ALGORITHM=INSTANT, ", c.table.TableName) + c.stmt.Alter
	if !c.runner.migration.SkipForceKill {
		return dbconn.ForceExecRaw(
			ctx,
			c.runner.db,
			[]*table.TableInfo{c.table},
			c.runner.dbConfig,
			c.runner.logger,
			stmt,
		)
	}
	return dbconn.ExecRaw(ctx, c.runner.db, stmt)
}

func (c *tableChange) attemptInplaceDDL(ctx context.Context) error {
	// As in attemptInstantDDL, the user's ALTER clause must not be
	// format-interpreted.
	stmt := sqlescape.MustEscapeSQL("ALTER TABLE %n ALGORITHM=INPLACE, LOCK=NONE, ", c.table.TableName) + c.stmt.Alter
	if !c.runner.migration.SkipForceKill {
		return dbconn.ForceExecRaw(
			ctx,
			c.runner.db,
			[]*table.TableInfo{c.table},
			c.runner.dbConfig,
			c.runner.logger,
			stmt,
		)
	}
	return dbconn.ExecRaw(ctx, c.runner.db, stmt)
}

func (c *tableChange) cleanup(ctx context.Context) error {
	if c.newTable != nil {
		if err := dbconn.Exec(ctx, c.runner.db, "DROP TABLE IF EXISTS %n", c.newTable.TableName); err != nil {
			return err
		}
	}
	return nil
}

// attemptMySQLDDL "attempts" to use DDL directly on MySQL with an assertion
// such as ALGORITHM=INSTANT. If MySQL is able to use the INSTANT algorithm,
// it will perform the operation without error. If it can't, it will return
// an error. It is important to let MySQL decide if it can handle the DDL
// operation, because keeping track of which operations are "INSTANT"
// is incredibly difficult. It will depend on MySQL minor version,
// and could possibly be specific to the table.
func (c *tableChange) attemptMySQLDDL(ctx context.Context) error {
	err := c.attemptInstantDDL(ctx)
	if err == nil {
		c.runner.usedInstantDDL = true // success
		return nil
	}

	// Many "inplace" operations (such as adding an index)
	// are only online-safe to do in Aurora GLOBAL
	// because replicas do not use the binlog. Some, however,
	// only modify the table metadata and are safe.
	//
	// Spirit automatically detects safe operations that can use
	// the INPLACE algorithm without blocking read replicas.
	err = c.stmt.AlgorithmInplaceConsideredSafe()
	if err == nil {
		err = c.attemptInplaceDDL(ctx)
		if err == nil {
			c.runner.usedInplaceDDL = true // success
			return nil
		}
	}
	c.runner.logger.Info("unable to use INPLACE", "error", err)

	// Failure is expected, since MySQL DDL only applies in limited scenarios
	// Return the error, which will be ignored by the caller.
	// Proceed with regular copy algorithm.
	return err
}

func (c *tableChange) Close() error {
	if c.table != nil {
		return c.table.Close()
	}
	return nil
}

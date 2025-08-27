package migration

import (
	"context"
	"fmt"

	"github.com/block/spirit/pkg/check"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
)

type change struct {
	stmt     *statement.AbstractStatement
	table    *table.TableInfo
	newTable *table.TableInfo

	// Store a pointer back to the migration runner
	// (for compatibility, we want to eventually remove this)
	runner *Runner
}

func (c *change) createNewTable(ctx context.Context) error {
	newName := fmt.Sprintf(check.NameFormatNew, c.table.TableName)
	// drop both if we've decided to call this func.
	if err := dbconn.Exec(ctx, c.runner.db, "DROP TABLE IF EXISTS %n.%n", c.table.SchemaName, newName); err != nil {
		return err
	}
	if err := dbconn.Exec(ctx, c.runner.db, "CREATE TABLE %n.%n LIKE %n.%n",
		c.table.SchemaName, newName, c.table.SchemaName, c.table.TableName); err != nil {
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
func (c *change) alterNewTable(ctx context.Context) error {
	if err := dbconn.Exec(ctx, c.runner.db, "ALTER TABLE %n.%n "+c.stmt.TrimAlter()+", ALGORITHM=COPY",
		c.newTable.SchemaName, c.newTable.TableName); err != nil {
		// Retry without the ALGORITHM=COPY. If there is a second error, then the DDL itself
		// is not supported. It could be a syntax error, in which case we return the second error,
		// which will probably be easier to read because it is unaltered.
		if err := dbconn.Exec(ctx, c.runner.db, "ALTER TABLE %n.%n "+c.stmt.Alter, c.newTable.SchemaName, c.newTable.TableName); err != nil {
			return err
		}
	}
	// Call GetInfo on the table again, since the columns
	// might have changed and this will affect the row copiers intersect func.
	return c.newTable.SetInfo(ctx)
}

func (c *change) dropOldTable(ctx context.Context) error {
	return dbconn.Exec(ctx, c.runner.db, "DROP TABLE IF EXISTS %n.%n", c.table.SchemaName, c.oldTableName())
}

func (c *change) oldTableName() string {
	// By default we just set the old table name to _<table>_old
	// but if they've enabled SkipDropAfterCutover, we add a timestamp
	if !c.runner.migration.SkipDropAfterCutover {
		return fmt.Sprintf(check.NameFormatOld, c.table.TableName)
	}
	return fmt.Sprintf(check.NameFormatOldTimeStamp, c.table.TableName, c.runner.startTime.UTC().Format(check.NameFormatTimestamp))
}

func (c *change) attemptInstantDDL(ctx context.Context) error {
	return dbconn.Exec(ctx, c.runner.db, "ALTER TABLE %n.%n ALGORITHM=INSTANT, "+c.stmt.Alter, c.table.SchemaName, c.table.TableName)
}

func (c *change) attemptInplaceDDL(ctx context.Context) error {
	return dbconn.Exec(ctx, c.runner.db, "ALTER TABLE %n.%n ALGORITHM=INPLACE, LOCK=NONE, "+c.stmt.Alter, c.table.SchemaName, c.table.TableName)
}

func (c *change) cleanup(ctx context.Context) error {
	if c.newTable != nil {
		if err := dbconn.Exec(ctx, c.runner.db, "DROP TABLE IF EXISTS %n.%n", c.newTable.SchemaName, c.newTable.TableName); err != nil {
			return err
		}
	}
	// TODO: only cleanup this migration, not the checkpoint table.
	if c.runner.checkpointTable != nil {
		if err := c.runner.dropCheckpoint(ctx); err != nil {
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
func (c *change) attemptMySQLDDL(ctx context.Context) error {
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
	// If the operator has specified that they want to attempt
	// an inplace add index, we will attempt inplace regardless
	// of the statement.
	err = c.stmt.AlgorithmInplaceConsideredSafe()
	if c.runner.migration.ForceInplace || err == nil {
		err = c.attemptInplaceDDL(ctx)
		if err == nil {
			c.runner.usedInplaceDDL = true // success
			return nil
		}
	}
	c.runner.logger.Infof("unable to use INPLACE: %v", err)

	// Failure is expected, since MySQL DDL only applies in limited scenarios
	// Return the error, which will be ignored by the caller.
	// Proceed with regular copy algorithm.
	return err
}

func (c *change) Close() error {
	if c.table != nil {
		return c.table.Close()
	}
	return nil
}

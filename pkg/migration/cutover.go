package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/siddontang/loggers"

	"github.com/cashapp/spirit/pkg/dbconn"
	"github.com/cashapp/spirit/pkg/repl"
	"github.com/cashapp/spirit/pkg/table"
	"github.com/cashapp/spirit/pkg/utils"
)

type CutOver struct {
	db        *sql.DB
	table     *table.TableInfo
	newTable  *table.TableInfo
	feed      *repl.Client
	dbConfig  *dbconn.DBConfig
	logger    loggers.Advanced
	timestamp string
}

// NewCutOver contains the logic to perform the final cut over. It requires the original table,
// new table, and a replication feed which is used to ensure consistency before the cut over.
func NewCutOver(db *sql.DB, table, newTable *table.TableInfo, feed *repl.Client, dbConfig *dbconn.DBConfig, logger loggers.Advanced, startTime time.Time) (*CutOver, error) {
	if feed == nil {
		return nil, errors.New("feed must be non-nil")
	}
	if table == nil || newTable == nil {
		return nil, errors.New("table and newTable must be non-nil")
	}
	timestamp := utils.ConvertToTimestampString(startTime)

	return &CutOver{
		db:        db,
		table:     table,
		newTable:  newTable,
		feed:      feed,
		dbConfig:  dbConfig,
		logger:    logger,
		timestamp: timestamp,
	}, nil
}

func (c *CutOver) Run(ctx context.Context) error {
	var err error
	if c.dbConfig.MaxOpenConnections < 5 {
		// The gh-ost cutover algorithm requires a minimum of 3 connections:
		// - The LOCK TABLES connection
		// - The RENAME TABLE connection
		// - The Flush() threads
		// Because we want to safely flush quickly, we set the limit to 5.
		c.db.SetMaxOpenConns(5)
	}
	for i := 0; i < c.dbConfig.MaxRetries; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Try and catch up before we attempt the cutover.
		// since we will need to catch up again with the lock held
		// and we want to minimize that.
		if err := c.feed.Flush(ctx); err != nil {
			return err
		}
		// We use maxCutoverRetries as our retrycount, but nested
		// within c.algorithmX() it may also have a retry for the specific statement
		c.logger.Warnf("Attempting final cut over operation (attempt %d/%d)", i+1, c.dbConfig.MaxRetries)
		err = c.algorithmRenameUnderLock(ctx)
		if err != nil {
			c.logger.Warnf("cutover failed. err: %s", err.Error())
			continue
		}
		c.logger.Warn("final cut over operation complete")
		return nil
	}
	c.logger.Error("cutover failed, and retries exhausted")
	return err
}

// algorithmRenameUnderLock is the preferred cutover algorithm.
// As of MySQL 8.0.13, you can rename tables locked with a LOCK TABLES statement
// https://dev.mysql.com/worklog/task/?id=9826
func (c *CutOver) algorithmRenameUnderLock(ctx context.Context) error {
	// Lock the source table in a trx
	// so the connection is not used by others
	serverLock, err := dbconn.NewTableLock(ctx, c.db, c.table, true, c.dbConfig, c.logger)
	if err != nil {
		return err
	}
	defer serverLock.Close()
	if err := c.feed.FlushUnderLock(ctx, serverLock); err != nil {
		return err
	}
	if !c.feed.AllChangesFlushed() {
		return errors.New("not all changes flushed, final flush might be broken")
	}
	oldName := fmt.Sprintf("_%s_old_%s", c.table.TableName, c.timestamp)
	oldQuotedName := fmt.Sprintf("`%s`.`%s`", c.table.SchemaName, oldName)
	renameStatement := fmt.Sprintf("RENAME TABLE %s TO %s, %s TO %s",
		c.table.QuotedName, oldQuotedName,
		c.newTable.QuotedName, c.table.QuotedName,
	)
	return serverLock.ExecUnderLock(ctx, []string{renameStatement})
}

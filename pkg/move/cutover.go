package move

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/table"
)

type CutOver struct {
	db          *sql.DB
	feed        *repl.Client
	tables      []*table.TableInfo
	cutoverFunc func(ctx context.Context) error
	dbConfig    *dbconn.DBConfig
	logger      *slog.Logger
}

// NewCutOver contains the logic to perform the final cut over. It can cutover multiple tables
// at once based on config. A replication feed which is used to ensure consistency before the cut over.
func NewCutOver(db *sql.DB, tables []*table.TableInfo, cutoverFunc func(ctx context.Context) error, feed *repl.Client, dbConfig *dbconn.DBConfig, logger *slog.Logger) (*CutOver, error) {
	if feed == nil {
		return nil, errors.New("feed must be non-nil")
	}
	// validate the cutoverConfig
	for _, tbl := range tables {
		if tbl == nil {
			return nil, errors.New("table must be non-nil")
		}
	}
	return &CutOver{
		db:          db,
		tables:      tables,
		cutoverFunc: cutoverFunc,
		feed:        feed,
		dbConfig:    dbConfig,
		logger:      logger,
	}, nil
}

func (c *CutOver) Run(ctx context.Context) error {
	var err error
	for i := range c.dbConfig.MaxRetries {
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
		c.logger.Warn("Attempting final cut over operation",
			"attempt", i+1,
			"max-retries", c.dbConfig.MaxRetries)
		err = c.algorithmCutover(ctx)
		if err != nil {
			c.logger.Warn("cutover failed", "error", err.Error())
			continue
		}
		c.logger.Warn("final cut over operation complete")
		return nil
	}
	c.logger.Error("cutover failed, and retries exhausted")
	return err
}

func (c *CutOver) algorithmCutover(ctx context.Context) error {
	tableLock, err := dbconn.NewTableLock(ctx, c.db, c.tables, c.dbConfig, c.logger)
	if err != nil {
		return err
	}
	defer tableLock.Close()

	// We don't use FlushUnderLock: that's for within the same server.
	// We use a regular flush. No new changes will arrive because of the table lock.
	if err := c.feed.Flush(ctx); err != nil {
		return err
	}

	if !c.feed.AllChangesFlushed() {
		return errors.New("not all changes flushed, final flush might be broken")
	}

	// If we have all changes flushed under a lock, we can now run the cutover func.
	if c.cutoverFunc != nil {
		c.logger.Info("Running cutover function")
		if err := c.cutoverFunc(ctx); err != nil {
			return err
		}
		c.logger.Info("Cutover function complete")
	}

	// If the cutover func succeeded, we can do a rename to prevent
	// the original tables from being used. We are now effectively serving
	// from the new location.
	renameFragments := []string{}
	for _, tbl := range c.tables {
		oldQuotedName := fmt.Sprintf("`%s`.`%s_old`", tbl.SchemaName, tbl.TableName)
		renameFragments = append(renameFragments,
			fmt.Sprintf("%s TO %s", tbl.QuotedName, oldQuotedName),
		)
	}
	renameStatement := "RENAME TABLE " + strings.Join(renameFragments, ", ")
	return tableLock.ExecUnderLock(ctx, renameStatement)
}

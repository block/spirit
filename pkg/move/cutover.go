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
	"github.com/block/spirit/pkg/utils"
)

// CutOverSource holds per-source state needed for the cutover.
type CutOverSource struct {
	DB         *sql.DB
	ReplClient *repl.Client
	Tables     []*table.TableInfo
}

type CutOver struct {
	sources     []CutOverSource
	cutoverFunc func(ctx context.Context) error
	dbConfig    *dbconn.DBConfig
	logger      *slog.Logger
}

// NewCutOver creates a new CutOver that handles multiple sources.
func NewCutOver(sources []CutOverSource, cutoverFunc func(ctx context.Context) error, dbConfig *dbconn.DBConfig, logger *slog.Logger) (*CutOver, error) {
	if len(sources) == 0 {
		return nil, errors.New("at least one source must be provided")
	}
	for i, src := range sources {
		if src.ReplClient == nil {
			return nil, fmt.Errorf("source %d: repl client must be non-nil", i)
		}
		for _, tbl := range src.Tables {
			if tbl == nil {
				return nil, fmt.Errorf("source %d: table must be non-nil", i)
			}
		}
	}
	return &CutOver{
		sources:     sources,
		cutoverFunc: cutoverFunc,
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
		// Flush all sources before attempting the cutover.
		for _, src := range c.sources {
			if err := src.ReplClient.Flush(ctx); err != nil {
				return err
			}
		}
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
	// Lock tables on ALL sources.
	var sourceLocks []*dbconn.TableLock
	for i, src := range c.sources {
		lock, err := dbconn.NewTableLock(ctx, src.DB, src.Tables, c.dbConfig, c.logger)
		if err != nil {
			// Close any locks we already acquired.
			for _, l := range sourceLocks {
				utils.CloseAndLogWithContext(ctx, l)
			}
			return fmt.Errorf("failed to lock tables on source %d: %w", i, err)
		}
		sourceLocks = append(sourceLocks, lock)
	}
	defer func() {
		for _, l := range sourceLocks {
			utils.CloseAndLogWithContext(ctx, l)
		}
	}()

	// Flush ALL repl clients. No new changes will arrive because all sources are locked.
	for i, src := range c.sources {
		if err := src.ReplClient.Flush(ctx); err != nil {
			return fmt.Errorf("failed to flush repl client for source %d: %w", i, err)
		}
	}

	// Check ALL changes flushed.
	for i, src := range c.sources {
		if !src.ReplClient.AllChangesFlushed() {
			return fmt.Errorf("%w on source %d, final flush might be broken", repl.ErrChangesNotFlushed, i)
		}
	}

	// Run the cutover function (Vitess coordination).
	if c.cutoverFunc != nil {
		c.logger.Info("Running cutover function")
		if err := c.cutoverFunc(ctx); err != nil {
			return err
		}
		c.logger.Info("Cutover function complete")
	}

	// Rename tables on each source, with rollback on partial failure.
	var completedRenames []int
	for i, src := range c.sources {
		renameFragments := make([]string, 0, len(src.Tables))
		for _, tbl := range src.Tables {
			oldQuotedName := fmt.Sprintf("`%s_old`", tbl.TableName)
			renameFragments = append(renameFragments,
				fmt.Sprintf("%s TO %s", tbl.QuotedTableName, oldQuotedName),
			)
		}
		renameStatement := "RENAME TABLE " + strings.Join(renameFragments, ", ")
		if err := sourceLocks[i].ExecUnderLock(ctx, renameStatement); err != nil {
			// Rollback completed renames.
			for _, j := range completedRenames {
				undoFragments := make([]string, 0, len(c.sources[j].Tables))
				for _, tbl := range c.sources[j].Tables {
					oldQuotedName := fmt.Sprintf("`%s_old`", tbl.TableName)
					undoFragments = append(undoFragments,
						fmt.Sprintf("%s TO %s", oldQuotedName, tbl.QuotedTableName),
					)
				}
				undoStatement := "RENAME TABLE " + strings.Join(undoFragments, ", ")
				_ = sourceLocks[j].ExecUnderLock(ctx, undoStatement)
			}
			return fmt.Errorf("rename failed on source %d, rolled back %d completed renames: %w",
				i, len(completedRenames), err)
		}
		completedRenames = append(completedRenames, i)
	}
	return nil
}

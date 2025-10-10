package move

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/block/spirit/pkg/checksum"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/metrics"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/throttler"
	"github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
)

type Runner struct {
	move         *Move
	source       *sql.DB
	sourceConfig *mysql.Config
	target       *sql.DB
	targetConfig *mysql.Config

	sourceTables []*table.TableInfo

	replClient      *repl.Client
	copyChunker     table.Chunker
	checksumChunker table.Chunker
	copier          copier.Copier

	logger   *logrus.Logger
	dbConfig *dbconn.DBConfig
}

func NewRunner(m *Move) (*Runner, error) {
	r := &Runner{
		move: m,
	}
	return r, nil
}

func (r *Runner) Close() error {
	return nil
}

// getSourceTables connects to r.source and fetches the list of tables
// to populate r.sourceTables.
func (r *Runner) getSourceTables(ctx context.Context) error {
	rows, err := r.source.Query("SHOW TABLES")
	if err != nil {
		return err
	}
	defer rows.Close()

	var tableName string
	for rows.Next() {
		if err := rows.Scan(&tableName); err != nil {
			return err
		}
		tableInfo := table.NewTableInfo(r.source, r.sourceConfig.DBName, tableName)
		if err := tableInfo.SetInfo(ctx); err != nil {
			return err
		}
		r.sourceTables = append(r.sourceTables, tableInfo)
	}
	return rows.Err()
}

// checkTargetEmpty checks that the target database is empty.
// If any tables exist it returns an error and the move fails.
func (r *Runner) checkTargetEmpty() error {
	rows, err := r.target.Query("SHOW TABLES")
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		return errors.New("target database is not empty")
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

// createTargetTables fetches the CREATE TABLE statement for each table
// in r.sourceTables and runs it on r.target. For now, we require that
// the target is identical to the source. In future, we may create
// the target with secondary indexes disabled, and re-add them after.
func (r *Runner) createTargetTables() error {
	for _, t := range r.sourceTables {
		var createStmt string
		row := r.source.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", t.TableName))
		var tbl string
		if err := row.Scan(&tbl, &createStmt); err != nil {
			return err
		}
		// Execute the create statement on the target.
		if _, err := r.target.Exec(createStmt); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) Run(ctx context.Context) error {
	var err error
	r.dbConfig = dbconn.NewDBConfig()
	r.logger = logrus.New()
	r.logger.Warn("the move command is experimental and not yet safe for production use.")
	r.source, err = dbconn.New(r.move.SourceDSN, r.dbConfig)
	if err != nil {
		return err
	}
	defer r.source.Close()
	r.target, err = dbconn.New(r.move.TargetDSN, r.dbConfig)
	if err != nil {
		return err
	}
	defer r.target.Close()

	r.sourceConfig, err = mysql.ParseDSN(r.move.SourceDSN)
	if err != nil {
		return err
	}
	r.targetConfig, err = mysql.ParseDSN(r.move.TargetDSN)
	if err != nil {
		return err
	}

	// Parse the r.move.SourceDSN and fetch the table list.
	if err = r.getSourceTables(ctx); err != nil {
		return err
	}

	// Check target is empty.
	if err := r.checkTargetEmpty(); err != nil {
		return err
	}

	// For each table, fetch the CREATE TABLE statement from the source and run it on the target.
	if err := r.createTargetTables(); err != nil {
		return err
	}

	r.replClient = repl.NewClient(r.source, r.sourceConfig.Addr, r.sourceConfig.User, r.sourceConfig.Passwd, &repl.ClientConfig{
		Logger:                     r.logger,
		Concurrency:                r.move.Threads,
		TargetBatchTime:            r.move.TargetChunkTime,
		ServerID:                   repl.NewServerID(),
		UseExperimentalBufferedMap: true,
		WriteDB:                    r.target,
	})

	copyChunkers := make([]table.Chunker, 0, len(r.sourceTables))
	checksumChunkers := make([]table.Chunker, 0, len(r.sourceTables))

	// For each table create a chunker and add a subscription to the replication client.
	for _, src := range r.sourceTables {
		dest := table.NewTableInfo(r.target, r.targetConfig.DBName, src.TableName)
		if err := dest.SetInfo(ctx); err != nil {
			return err
		}
		copyChunker, err := table.NewChunker(src, dest, r.move.TargetChunkTime, r.logger)
		if err != nil {
			return err
		}
		if err := r.replClient.AddSubscription(src, dest, copyChunker.KeyAboveHighWatermark); err != nil {
			return err
		}
		checksumChunker, err := table.NewChunker(src, dest, r.move.TargetChunkTime, r.logger)
		if err != nil {
			return err
		}
		copyChunkers = append(copyChunkers, copyChunker)
		checksumChunkers = append(checksumChunkers, checksumChunker)
	}

	// Then create a multi chunker of all chunkers.
	r.copyChunker = table.NewMultiChunker(copyChunkers...)
	r.checksumChunker = table.NewMultiChunker(checksumChunkers...)

	// Then open the multi chunker.
	if err := r.copyChunker.Open(); err != nil {
		return err
	}
	defer r.copyChunker.Close()

	// Create a copier that reads from the multi chunker and writes to the target.
	r.copier, err = copier.NewCopier(r.source, r.copyChunker, &copier.CopierConfig{
		Concurrency:                   r.move.Threads,
		TargetChunkTime:               r.move.TargetChunkTime,
		Logger:                        r.logger,
		Throttler:                     &throttler.Noop{},
		MetricsSink:                   &metrics.NoopSink{},
		DBConfig:                      r.dbConfig,
		UseExperimentalBufferedCopier: true,
		WriteDB:                       r.target,
	})
	if err != nil {
		return err
	}

	// Start the replication client.
	if err := r.replClient.Run(ctx); err != nil {
		return err
	}

	// Run the copier.
	if err := r.copier.Run(ctx); err != nil {
		return err
	}

	// When the copier has finished, catch up the replication client
	// This is in a non-blocking way first.
	if err := r.replClient.Flush(ctx); err != nil {
		return err
	}

	r.logger.Infof("All tables copied successfully.")

	// Perform a checksum operation + ANALYZE TABLEs
	// To make sure they are all in a ready state.
	if err := r.prepareForCutover(ctx); err != nil {
		return err
	}

	// In here, what we ideally do is a pluggable "cutover" step.
	// The steps to update metadata are going to vary by deployment.
	// Most likely we need to:
	// - Lock tables on the source.
	// - Final catch up of the replication client.
	// - Update external metadata system.
	// - Rename the tables on the source to be unreachable.
	// - UNLOCK TABLES on the source.
	return nil
}

func (r *Runner) prepareForCutover(ctx context.Context) error {
	// Disable the periodic flush and flush all pending events.
	// We want it disabled for ANALYZE TABLE and acquiring a table lock
	// *but* it will be started again briefly inside of the checksum
	// runner to ensure that the lag does not grow too long.
	r.replClient.StopPeriodicFlush()
	if err := r.replClient.Flush(ctx); err != nil {
		return err
	}

	// Run ANALYZE TABLE to update the statistics on the new table.
	// This is required so on cutover plans don't go sideways, which
	// is at elevated risk because the batch loading can cause statistics
	// to be out of date.
	r.logger.Infof("Running ANALYZE TABLE")
	for _, tbl := range r.sourceTables {
		if err := dbconn.Exec(ctx, r.target, "ANALYZE TABLE %n.%n", tbl.SchemaName, tbl.TableName); err != nil {
			return err
		}
	}

	if err := r.checksumChunker.Open(); err != nil {
		return err
	}
	defer r.checksumChunker.Close()

	// Perform a checksum operation
	checker, err := checksum.NewChecker(r.source, r.checksumChunker, r.replClient, &checksum.CheckerConfig{
		Concurrency:     r.move.Threads,
		TargetChunkTime: r.move.TargetChunkTime,
		DBConfig:        r.dbConfig,
		Logger:          r.logger,
		WriteDB:         r.target,
	})
	if err != nil {
		return err
	}
	return checker.Run(ctx)
}

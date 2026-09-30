package check

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/block/mysql"
	parsermysql "github.com/block/spirit/pkg/parser/mysql"
)

func init() {
	registerCheck("configuration", configurationCheck, ScopePreflight)
}

// configurationCheck verifies the MySQL configuration on all source databases.
func configurationCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	if len(r.Sources) == 0 {
		return errors.New("no sources configured")
	}
	for i, src := range r.Sources {
		if src.DB == nil {
			return fmt.Errorf("source %d: database connection is not initialized", i)
		}
		var binlogFormat, binlogRowImage, logBin, logSlaveUpdates, binlogRowValueOptions, performanceSchema, binlogOrderCommits string
		err := src.DB.QueryRowContext(ctx,
			`SELECT @@global.binlog_format,
			@@global.binlog_row_image,
			@@global.log_bin,
			@@global.log_slave_updates,
			@@global.binlog_row_value_options,
			@@global.performance_schema,
			@@global.binlog_order_commits`).Scan(
			&binlogFormat,
			&binlogRowImage,
			&logBin,
			&logSlaveUpdates,
			&binlogRowValueOptions,
			&performanceSchema,
			&binlogOrderCommits,
		)
		if err != nil {
			return fmt.Errorf("source %d: %w", i, err)
		}
		if binlogFormat != "ROW" {
			return fmt.Errorf("source %d: binlog_format must be ROW", i)
		}
		if binlogRowImage != "FULL" {
			return fmt.Errorf("source %d: binlog_row_image must be FULL for move operations", i)
		}
		if binlogRowValueOptions != "" {
			return fmt.Errorf("source %d: binlog_row_value_options must be empty for move operations", i)
		}
		// binlog_transaction_compression=ON delivers every transaction wrapped
		// in a compressed Transaction_payload event. The change client can
		// decode these — it has to, because any session can enable the setting
		// for its own transactions regardless of the global value — but as a
		// server-wide default it is not a supported configuration. Queried
		// separately from the batch above because the variable only exists on
		// MySQL 8.0.20+; on older servers compression cannot be enabled at
		// all, so unknown-variable passes the check.
		var binlogTransactionCompression string
		err = src.DB.QueryRowContext(ctx, `SELECT @@global.binlog_transaction_compression`).Scan(&binlogTransactionCompression)
		if err != nil {
			if myErr, ok := errors.AsType[*mysql.MySQLError](err); !ok || myErr.Number != parsermysql.ErrUnknownSystemVariable {
				return fmt.Errorf("source %d: %w", i, err)
			}
		} else if binlogTransactionCompression != "0" {
			return fmt.Errorf("source %d: binlog_transaction_compression must be OFF for move operations", i)
		}
		var partialRevokes string
		err = src.DB.QueryRowContext(ctx, `SELECT @@global.partial_revokes`).Scan(&partialRevokes)
		if err := partialRevokesError(i, partialRevokes, err); err != nil {
			return err
		}
		if logBin != "1" {
			return fmt.Errorf("source %d: log_bin must be enabled", i)
		}
		if logSlaveUpdates != "1" {
			return fmt.Errorf("source %d: log_slave_updates must be enabled", i)
		}
		if performanceSchema != "1" {
			return fmt.Errorf("source %d: performance_schema must be enabled for move operations", i)
		}
		if binlogOrderCommits != "1" {
			// binlog_order_commits=ON is the MySQL default. Setting it OFF
			// allows commit reordering that breaks Spirit's binlog→applier
			// visibility assumption (see issue #818).
			return fmt.Errorf("source %d: binlog_order_commits must be ON (this is the MySQL default; setting it OFF allows commit reordering that breaks Spirit's binlog→applier visibility assumption)", i)
		}
		// There is no GTID preflight here: each source's change source is
		// selected automatically (change.NewAutoClient probes gtid_mode /
		// enforce_gtid_consistency per source), so a source without GTIDs
		// simply gets the binlog file+position client rather than an error.
	}
	return nil
}

// partialRevokesError decides the partial_revokes part of the configuration
// check for source i from the value of @@global.partial_revokes and the error
// reading it. partial_revokes=ON lets SHOW GRANTS print a REVOKE line that
// removes a global grant for one schema, and makes MySQL read the database
// name in a grant literally rather than as a pattern. The privileges check
// reads SHOW GRANTS as additive GRANT lines, so it would pass for a user that
// cannot act on the schema. The variable only exists on MySQL 8.0.16+; an
// older server cannot have partial revokes, so unknown-variable passes.
func partialRevokesError(i int, value string, err error) error {
	if err != nil {
		if myErr, ok := errors.AsType[*mysql.MySQLError](err); ok && myErr.Number == parsermysql.ErrUnknownSystemVariable {
			return nil
		}
		return fmt.Errorf("source %d: %w", i, err)
	}
	if value != "0" {
		return fmt.Errorf("source %d: partial_revokes must be OFF for move operations", i)
	}
	return nil
}

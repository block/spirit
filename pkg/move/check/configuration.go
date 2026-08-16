package check

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/go-sql-driver/mysql"
)

// erUnknownSystemVariable is MySQL error 1193 (ER_UNKNOWN_SYSTEM_VARIABLE),
// returned when selecting a system variable the server predates.
const erUnknownSystemVariable = 1193

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
			if myErr, ok := errors.AsType[*mysql.MySQLError](err); !ok || myErr.Number != erUnknownSystemVariable {
				return fmt.Errorf("source %d: %w", i, err)
			}
		} else if binlogTransactionCompression != "0" {
			return fmt.Errorf("source %d: binlog_transaction_compression must be OFF for move operations", i)
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

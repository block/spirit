package check

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

func init() {
	registerCheck("configuration", configurationCheck, ScopePreflight)
}

// configurationCheck verifies the MySQL configuration on all source databases.
func configurationCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	for i, src := range r.Sources {
		var binlogFormat, binlogRowImage, logBin, logSlaveUpdates, binlogRowValueOptions, performanceSchema string
		err := src.DB.QueryRowContext(ctx,
			`SELECT @@global.binlog_format,
			@@global.binlog_row_image,
			@@global.log_bin,
			@@global.log_slave_updates,
			@@global.binlog_row_value_options,
			@@global.performance_schema`).Scan(
			&binlogFormat,
			&binlogRowImage,
			&logBin,
			&logSlaveUpdates,
			&binlogRowValueOptions,
			&performanceSchema,
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
		if logBin != "1" {
			return fmt.Errorf("source %d: log_bin must be enabled", i)
		}
		if logSlaveUpdates != "1" {
			return fmt.Errorf("source %d: log_slave_updates must be enabled", i)
		}
		if performanceSchema != "1" {
			return fmt.Errorf("source %d: performance_schema must be enabled for move operations", i)
		}
	}
	// Preserve the original single-source error format for backward compatibility.
	if len(r.Sources) == 0 {
		return errors.New("no sources configured")
	}
	return nil
}

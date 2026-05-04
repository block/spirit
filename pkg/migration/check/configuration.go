package check

import (
	"context"
	"errors"
	"log/slog"
)

func init() {
	registerCheck("configuration", configurationCheck, ScopePreflight)
}

// check the configuration of the database. There are some hard nos,
// and some suggestions around configuration for performance.
func configurationCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	var binlogFormat, innodbAutoincLockMode, binlogRowImage, logBin, logSlaveUpdates, binlogRowValueOptions, performanceSchema, binlogOrderCommits string
	err := r.DB.QueryRowContext(ctx,
		`SELECT @@global.binlog_format,
		@@global.innodb_autoinc_lock_mode,
		@@global.binlog_row_image,
		@@global.log_bin,
		@@global.log_slave_updates,
		@@global.binlog_row_value_options,
		@@global.performance_schema,
		@@global.binlog_order_commits`).Scan(
		&binlogFormat,
		&innodbAutoincLockMode,
		&binlogRowImage,
		&logBin,
		&logSlaveUpdates,
		&binlogRowValueOptions,
		&performanceSchema,
		&binlogOrderCommits,
	)
	if err != nil {
		return err
	}
	if binlogFormat != "ROW" {
		return errors.New("binlog_format must be ROW")
	}
	if innodbAutoincLockMode != "2" {
		// This is strongly encouraged because otherwise running parallel threads is pointless.
		// i.e. on a test with 2 threads running INSERT INTO new SELECT * FROM old WHERE <range>
		// the inserts will run in serial when there is an autoinc column on new and innodbAutoincLockMode != "2"
		// This is the auto-inc lock. It won't show up in SHOW PROCESSLIST that they are serial.
		logger.Warn("innodb_autoinc_lock_mode != 2. This will cause the migration to run slower than expected because concurrent inserts to the new table will be serialized.", "innodb_autoinc_lock_mode", innodbAutoincLockMode)
	}
	// Spirit's replication applier reads full row images directly from the
	// binlog instead of `REPLACE INTO _new ... SELECT FROM original ...`,
	// which sidesteps the MySQL binlog/visibility race that caused silent
	// row loss under load (issue #746). That requires the source server to
	// publish full images.
	if binlogRowImage != "FULL" {
		return errors.New("binlog_row_image must be FULL: spirit no longer supports minimal")
	}
	if binlogRowValueOptions != "" {
		return errors.New("binlog_row_value_options must be empty: spirit does not support non-empty values")
	}

	if logBin != "1" {
		// This is a hard requirement because we need to be able to read the binlog.
		return errors.New("log_bin must be enabled")
	}
	if logSlaveUpdates != "1" {
		// This is a hard requirement unless we enhance this to confirm
		//  its not receiving any updates via the replication stream.
		return errors.New("log_slave_updates must be enabled")
	}
	if performanceSchema != "1" {
		// Force-kill (the default) uses performance_schema to find and kill
		// locking transactions via metadata_locks and threads tables. We could technically
		// support performance_schema = 0 for other cases, but to simplify testing and future
		// use, we just require it.
		return errors.New("performance_schema must be enabled")
	}
	if binlogOrderCommits != "1" {
		// binlog_order_commits=ON is the MySQL default. Setting it OFF allows
		// MySQL to commit transactions to InnoDB in a different order than they
		// appear in the binlog — specifically, a transaction's row events can
		// be written to the binlog before that transaction's commit becomes
		// visible to fresh SELECTs on other connections. Spirit's binlog
		// applier path (deltaMap/bufferedMap.Flush running REPLACE INTO _new
		// SELECT FROM original WHERE pk IN (...)) assumes the opposite: when
		// the streamer delivers a row event, that row is already visible. With
		// binlog_order_commits=OFF that assumption breaks and rows can be
		// silently lost during cutover. See issue #818.
		return errors.New("binlog_order_commits must be ON (this is the MySQL default; setting it OFF allows commit reordering that breaks Spirit's binlog→applier visibility assumption)")
	}
	return nil
}

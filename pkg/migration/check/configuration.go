package check

import (
	"context"
	"errors"
	"log/slog"

	"github.com/go-sql-driver/mysql"
)

// erUnknownSystemVariable is MySQL error 1193 (ER_UNKNOWN_SYSTEM_VARIABLE),
// returned when selecting a system variable the server predates.
const erUnknownSystemVariable = 1193

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
	// binlog_transaction_compression=ON delivers every transaction wrapped in
	// a compressed Transaction_payload event. The change client can decode
	// these — it has to, because any session can enable the setting for its
	// own transactions regardless of the global value — but as a server-wide
	// default it is not a supported configuration, so reject it up front with
	// a clear message. Queried separately from the batch above because the
	// variable only exists on MySQL 8.0.20+; on older servers compression
	// cannot be enabled at all, so unknown-variable passes the check.
	var binlogTransactionCompression string
	err = r.DB.QueryRowContext(ctx, `SELECT @@global.binlog_transaction_compression`).Scan(&binlogTransactionCompression)
	if err != nil {
		if myErr, ok := errors.AsType[*mysql.MySQLError](err); !ok || myErr.Number != erUnknownSystemVariable {
			return err
		}
	} else if binlogTransactionCompression != "0" {
		return errors.New("binlog_transaction_compression must be OFF: spirit does not support compressed transactions in the binary log")
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
	// There is no GTID preflight here: the change source is selected
	// automatically (change.NewAutoClient probes gtid_mode /
	// enforce_gtid_consistency itself), so a server without GTIDs simply
	// gets the binlog file+position client rather than an error.
	return nil
}

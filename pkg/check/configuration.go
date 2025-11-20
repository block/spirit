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
	var binlogFormat, innodbAutoincLockMode, binlogRowImage, logBin, logSlaveUpdates, binlogRowValueOptions string
	err := r.DB.QueryRowContext(ctx,
		`SELECT @@global.binlog_format,
		@@global.innodb_autoinc_lock_mode,
		@@global.binlog_row_image,
		@@global.log_bin,
		@@global.log_slave_updates,
		@@global.binlog_row_value_options`).Scan(
		&binlogFormat,
		&innodbAutoincLockMode,
		&binlogRowImage,
		&logBin,
		&logSlaveUpdates,
		&binlogRowValueOptions,
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
	if binlogRowImage != "FULL" && binlogRowImage != "MINIMAL" {
		// This might not be required, but these are the only options that have been tested so far.
		// To keep the testing scope reduced for now, it is required.
		return errors.New("binlog_row_image must be FULL or MINIMAL")
	}
	if r.ExperimentalBufferedCopy {
		if binlogRowImage != "FULL" {
			return errors.New("binlog_row_image must be FULL when using experimental buffered copy because it relies on reading all columns from the binlog")
		}
		if binlogRowValueOptions != "" {
			return errors.New("binlog_row_value_options must be empty when using experimental buffered copy because it relies on reading all columns from the binlog")
		}
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
	return nil
}

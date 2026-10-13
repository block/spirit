package check

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"

	"github.com/block/mysql"
	parsermysql "github.com/block/spirit/pkg/parser/mysql"
)

// ConfigurationCheckName is the configuration check's name, for a runner
// that excludes it (see RunChecks).
const ConfigurationCheckName = "configuration"

func init() {
	// A sync runs this check only when it reads the source with the built-in
	// binlog client: the settings are the binary log's, and an injected
	// change.Source (e.g. a Vitess VStream) does not read it.
	register(check{name: ConfigurationCheckName, run: configurationCheck, phases: PhasePreflight, runtimes: Migration | Move | Sync})
}

// configurationCheck checks the configuration of every source. There are some
// hard nos, and some suggestions around configuration for performance.
func configurationCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	if len(r.Sources) == 0 {
		return errors.New("no sources configured")
	}
	for i, src := range r.Sources {
		if src.DB == nil {
			return r.sourceError(i, errors.New("database connection is not initialized"))
		}
		if err := sourceConfiguration(ctx, r.Runtime, src, logger); err != nil {
			return r.sourceError(i, err)
		}
	}
	return nil
}

// sourceConfiguration checks the configuration of one source.
func sourceConfiguration(ctx context.Context, runtime Runtime, src Endpoint, logger *slog.Logger) error {
	settings, err := readSourceSettings(ctx, src.DB)
	if err != nil {
		return err
	}
	return settings.check(runtime, logger)
}

// sourceSettings are the server settings the configuration check decides on.
type sourceSettings struct {
	binlogFormat, innodbAutoincLockMode, binlogRowImage, logBin, logSlaveUpdates string
	binlogRowValueOptions, performanceSchema, binlogOrderCommits                 string
	binlogTransactionCompression                                                 string // "" when the server does not have the variable
	partialRevokesErr                                                            error  // the result of partialRevokesError
}

// readSourceSettings reads the settings of the server db is connected to.
func readSourceSettings(ctx context.Context, db *sql.DB) (sourceSettings, error) {
	var s sourceSettings
	err := db.QueryRowContext(ctx,
		`SELECT @@global.binlog_format,
		@@global.innodb_autoinc_lock_mode,
		@@global.binlog_row_image,
		@@global.log_bin,
		@@global.log_slave_updates,
		@@global.binlog_row_value_options,
		@@global.performance_schema,
		@@global.binlog_order_commits`).Scan(
		&s.binlogFormat,
		&s.innodbAutoincLockMode,
		&s.binlogRowImage,
		&s.logBin,
		&s.logSlaveUpdates,
		&s.binlogRowValueOptions,
		&s.performanceSchema,
		&s.binlogOrderCommits,
	)
	if err != nil {
		return s, err
	}
	// Queried separately from the batch above because the variable only
	// exists on MySQL 8.0.20+; on older servers compression cannot be enabled
	// at all, so unknown-variable passes the check.
	err = db.QueryRowContext(ctx, `SELECT @@global.binlog_transaction_compression`).Scan(&s.binlogTransactionCompression)
	if err != nil {
		if myErr, ok := errors.AsType[*mysql.MySQLError](err); !ok || myErr.Number != parsermysql.ErrUnknownSystemVariable {
			return s, err
		}
		s.binlogTransactionCompression = ""
	}
	var partialRevokes string
	err = db.QueryRowContext(ctx, `SELECT @@global.partial_revokes`).Scan(&partialRevokes)
	s.partialRevokesErr = partialRevokesError(partialRevokes, err)
	return s, nil
}

// check decides the configuration check for runtime from the settings. There
// are some hard nos, and some suggestions around configuration for
// performance.
func (s sourceSettings) check(runtime Runtime, logger *slog.Logger) error {
	if s.binlogFormat != "ROW" {
		return errors.New("binlog_format must be ROW")
	}
	if s.innodbAutoincLockMode != "2" && runtime == Migration {
		// This is strongly encouraged because otherwise running parallel threads is pointless.
		// i.e. on a test with 2 threads running INSERT INTO new SELECT * FROM old WHERE <range>
		// the inserts will run in serial when there is an autoinc column on new and innodbAutoincLockMode != "2"
		// This is the auto-inc lock. It won't show up in SHOW PROCESSLIST that they are serial.
		// Only a migration writes to the source server; a move's or sync's
		// inserts land on the target, where this source setting has no effect.
		logger.Warn("innodb_autoinc_lock_mode != 2. This will cause the migration to run slower than expected because concurrent inserts to the new table will be serialized.", "innodb_autoinc_lock_mode", s.innodbAutoincLockMode)
	}
	// Spirit's replication applier reads full row images directly from the
	// binlog instead of `REPLACE INTO _new ... SELECT FROM original ...`,
	// which sidesteps the MySQL binlog/visibility race that caused silent
	// row loss under load (issue #746). That requires the source server to
	// publish full images.
	if s.binlogRowImage != "FULL" {
		return errors.New("binlog_row_image must be FULL: spirit no longer supports minimal")
	}
	if s.binlogRowValueOptions != "" {
		return errors.New("binlog_row_value_options must be empty: spirit does not support non-empty values")
	}
	// binlog_transaction_compression=ON delivers every transaction wrapped in
	// a compressed Transaction_payload event. The change client can decode
	// these — it has to, because any session can enable the setting for its
	// own transactions regardless of the global value — but as a server-wide
	// default it is not a supported configuration, so reject it up front with
	// a clear message.
	if s.binlogTransactionCompression != "" && s.binlogTransactionCompression != "0" {
		return errors.New("binlog_transaction_compression must be OFF: spirit does not support compressed transactions in the binary log")
	}
	// partial_revokes only matters to the privileges check, which a sync
	// does not run.
	if runtime != Sync && s.partialRevokesErr != nil {
		return s.partialRevokesErr
	}
	if s.logBin != "1" {
		// This is a hard requirement because we need to be able to read the binlog.
		return errors.New("log_bin must be enabled")
	}
	if s.logSlaveUpdates != "1" {
		// This is a hard requirement unless we enhance this to confirm
		//  its not receiving any updates via the replication stream.
		return errors.New("log_slave_updates must be enabled")
	}
	if s.performanceSchema != "1" && runtime != Sync {
		// A sync takes no table locks, so it never force-kills.
		// Force-kill (the default) uses performance_schema to find and kill
		// locking transactions via metadata_locks and threads tables. We could technically
		// support performance_schema = 0 for other cases, but to simplify testing and future
		// use, we just require it.
		return errors.New("performance_schema must be enabled")
	}
	if s.binlogOrderCommits != "1" {
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
	// enforce_gtid_consistency per source itself), so a server without GTIDs
	// simply gets the binlog file+position client rather than an error.
	return nil
}

// partialRevokesError decides the partial_revokes part of the configuration
// check from the value of @@global.partial_revokes and the error reading it.
// partial_revokes=ON lets SHOW GRANTS print a REVOKE line that removes a
// global grant for one schema, and makes MySQL read the database name in a
// grant literally rather than as a pattern. The privileges check reads SHOW
// GRANTS as additive GRANT lines, so it would pass for a user that cannot act
// on the schema. The variable only exists on MySQL 8.0.16+; an older server
// cannot have partial revokes, so unknown-variable passes.
func partialRevokesError(value string, err error) error {
	if err != nil {
		if myErr, ok := errors.AsType[*mysql.MySQLError](err); ok && myErr.Number == parsermysql.ErrUnknownSystemVariable {
			return nil
		}
		return err
	}
	if value != "0" {
		return errors.New("partial_revokes must be OFF: spirit does not support partial revokes")
	}
	return nil
}

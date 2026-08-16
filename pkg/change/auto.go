package change

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/block/spirit/pkg/applier"
	"github.com/go-mysql-org/go-mysql/mysql"
)

// This file selects between the two built-in change.Source implementations
// (binlog file+position and GTID). There is no flag: fresh runs probe the
// server and use GTIDs when they are enabled, and resumed runs stay in the
// coordinate scheme their checkpoint was written in. It lives here rather
// than pkg/utils because it constructs Sources (and pkg/change already
// imports pkg/utils).

// GTIDEnabled reports whether the server has GTIDs enabled — gtid_mode=ON
// and enforce_gtid_consistency=ON — i.e. whether it can serve the GTID-based
// change source (COM_BINLOG_DUMP_GTID, and @@GLOBAL.gtid_executed /
// gtid_purged for resume validation). The permissive modes (ON_PERMISSIVE /
// OFF_PERMISSIVE) report false: in those modes the server may still write
// anonymous transactions, which have no GTID to resume from.
func GTIDEnabled(ctx context.Context, db *sql.DB) (bool, error) {
	var gtidMode, enforceGTIDConsistency string
	if err := db.QueryRowContext(ctx,
		`SELECT @@global.gtid_mode, @@global.enforce_gtid_consistency`).Scan(
		&gtidMode, &enforceGTIDConsistency,
	); err != nil {
		return false, fmt.Errorf("could not read gtid_mode/enforce_gtid_consistency: %w", err)
	}
	return gtidMode == "ON" && enforceGTIDConsistency == "ON", nil
}

// IsGTIDPosition reports whether pos — an opaque position previously returned
// by Source.Position() — is a GTID-set coordinate rather than a binlog
// file:offset coordinate. The two encodings cannot collide: a GTID set is
// "uuid:interval[,uuid:interval]..." and a binlog coordinate is
// "<file>:<offset>" where the file ("binlog.000001") never parses as a UUID.
// The empty string (no position observed yet) is not a GTID position.
func IsGTIDPosition(pos string) bool {
	if pos == "" {
		return false
	}
	_, err := mysql.ParseMysqlGTIDSet(normalizeGTIDString(pos))
	return err == nil
}

// useGTIDForResume decides the client type for a resumed run: the
// checkpointed position's own encoding is authoritative, so a run always
// finishes in the coordinate scheme it started with. A GTID-set position
// additionally requires the server to still have GTIDs enabled — resuming it
// through the binlog client is impossible (the coordinate doesn't translate),
// so a server that lost GTIDs (e.g. a failover to a differently-configured
// replica) is a hard error rather than a silent fresh start: the caller's
// checkpoint is still valid and the operator may prefer to fix the server.
func useGTIDForResume(resumePosition string, gtidEnabled bool) (bool, error) {
	if !IsGTIDPosition(resumePosition) {
		return false, nil
	}
	if !gtidEnabled {
		return false, fmt.Errorf("checkpoint position %q is a GTID set but the server no longer has GTIDs enabled; resuming this run requires gtid_mode=ON and enforce_gtid_consistency=ON", resumePosition)
	}
	return true, nil
}

// NewAutoClient constructs the built-in change.Source for a server, selecting
// between the GTID and binlog file:offset implementations:
//
//   - Fresh runs (resumePosition == ""): the server is probed and the GTID
//     client is used when GTIDs are enabled (GTIDEnabled), the binlog client
//     otherwise.
//   - Resumed runs (resumePosition != ""): the position's own encoding
//     decides, so the run stays in the coordinate scheme it started with. A
//     GTID-set position requires the GTID client (and errors if the server no
//     longer has GTIDs enabled); a file:offset position uses the binlog
//     client even when the server could serve GTIDs — e.g. a checkpoint
//     written by an older spirit.
//
// The remaining arguments mirror NewBinlogClient / NewGTIDClient, which this
// delegates to. The chosen implementation is logged on config.Logger.
func NewAutoClient(ctx context.Context, db *sql.DB, host string, username, password string, appl applier.Applier, config *ClientConfig, resumePosition string) (Source, error) {
	gtidEnabled, err := GTIDEnabled(ctx, db)
	if err != nil {
		return nil, err
	}
	useGTID := gtidEnabled
	reason := "server has GTIDs enabled"
	if !gtidEnabled {
		reason = "server does not have GTIDs enabled"
	}
	if resumePosition != "" {
		if useGTID, err = useGTIDForResume(resumePosition, gtidEnabled); err != nil {
			return nil, err
		}
		reason = "resuming from a binlog file:offset position"
		if useGTID {
			reason = "resuming from a GTID position"
		}
	}
	if config.Logger != nil {
		if useGTID {
			config.Logger.Info("using GTID-based change source", "reason", reason, "host", host)
		} else {
			config.Logger.Info("using binlog file+position change source", "reason", reason, "host", host)
		}
	}
	if useGTID {
		return NewGTIDClient(db, host, username, password, appl, config), nil
	}
	return NewBinlogClient(db, host, username, password, appl, config), nil
}

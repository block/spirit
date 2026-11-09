package move

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"

	"github.com/block/spirit/pkg/dbconn"
)

// revertMarkerName is the table an operator creates to ask an in-flight reverse
// window to roll back. Like the pre-cutover sentinel (pkg/sentinel), it is a
// coordination signal expressed as a table's existence — the difference is
// polarity: the sentinel is created by the move and dropped by the operator to
// proceed, whereas the revert marker is created by the operator to trigger a
// rollback.
//
// The reverse-window driver polls for it on targets[0] (where the checkpoint
// also lives). A stale marker (from a prior reverse-window move that did not
// complete) makes a new move refuse at pre-flight/pre-cutover; the marker is
// removed by the terminal action (complete-forward or reverse cutover). Keeping
// the trigger in its own table decouples the operator-owned signal from the
// move-owned checkpoint row.
const revertMarkerName = "_spirit_move_revert"

// revertMarkerExists reports whether the revert marker is present in db's
// currently-selected schema (DATABASE()).
func revertMarkerExists(ctx context.Context, db *sql.DB) (bool, error) {
	var one int
	err := db.QueryRowContext(ctx,
		"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
		revertMarkerName).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// dropRevertMarker removes the revert marker (idempotent). A cancelled drop
// is ended on the server before it returns, so it cannot remove a marker an
// operator creates afterwards.
func dropRevertMarker(ctx context.Context, db *sql.DB, logger *slog.Logger) error {
	return dbconn.ExecKillOnCancel(ctx, db, logger, "DROP TABLE IF EXISTS %n", revertMarkerName)
}

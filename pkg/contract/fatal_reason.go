package contract

import "fmt"

// FatalReason tells the caller's CancelFunc why the change client hit a fatal
// condition, so the caller can decide which of its state (if any) must be
// invalidated before cancelling.
type FatalReason int

const (
	// FatalReasonSchemaChange means DDL was detected on a watched table.
	// Persisted resume state (checkpoints) describes the table's old
	// definition, so a caller that keeps such state must invalidate it:
	// resuming against the changed table could corrupt data.
	FatalReasonSchemaChange FatalReason = iota
	// FatalReasonStreamError means the change stream itself failed fatally
	// (streamer recreation attempts exhausted, or a row event could not be
	// processed). The watched tables are not known to have changed, so
	// persisted resume state remains valid and a retry can resume from it.
	FatalReasonStreamError
	// FatalReasonUnsupportedXA means an XA group was found in the stream.
	// Replaying the checkpoint would hit the same group, so finite runs
	// must discard their checkpoint and start fresh after XA activity stops.
	FatalReasonUnsupportedXA
	// FatalReasonLogPosWrapped means a binlog file grew past 4GiB and the
	// 4-byte LogPos wrapped, so file+offset coordinates no longer identify
	// a unique point in the stream. Replaying the checkpoint would stream
	// forward into the same oversized transaction and wrap again, so finite
	// runs must discard their checkpoint; a fresh run started after the
	// file rotates gets usable coordinates again.
	FatalReasonLogPosWrapped
	// FatalReasonFlushError means the periodic flush failed to apply buffered
	// changes to the target, after the applier's own retries. The failed
	// changes stay buffered and the resume coordinate stops advancing, so
	// carrying on only spends binlog retention until the checkpoint can no
	// longer be resumed. The watched tables are not known to have changed,
	// so persisted resume state remains valid.
	FatalReasonFlushError
)

// PreservesCheckpoint reports whether a finite run (a migration or a move)
// can still resume from its checkpoint after aborting for this reason. Only a
// failed stream or a failed flush leaves the watched tables unchanged and the
// checkpoint usable. Every other reason — including any future one — reports
// false: invalidating costs a restart, while wrongly resuming could corrupt
// data.
func (f FatalReason) PreservesCheckpoint() bool {
	return f == FatalReasonStreamError || f == FatalReasonFlushError
}

// String implements fmt.Stringer for logging.
func (f FatalReason) String() string {
	switch f {
	case FatalReasonSchemaChange:
		return "schema-change"
	case FatalReasonStreamError:
		return "stream-error"
	case FatalReasonUnsupportedXA:
		return "unsupported-xa"
	case FatalReasonLogPosWrapped:
		return "logpos-wrapped"
	case FatalReasonFlushError:
		return "flush-error"
	default:
		return fmt.Sprintf("unknown-fatal-reason(%d)", int(f))
	}
}

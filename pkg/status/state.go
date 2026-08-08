package status

import (
	"errors"
	"sync/atomic"
)

//nolint:recvcheck // String() uses value receiver (called on State values), Get/Set use pointer receivers (atomic ops)
type State int32

var (
	ErrMismatchedAlter         = errors.New("alter statement in checkpoint table does not match the alter statement specified here")
	ErrBinlogNotFound          = errors.New("checkpoint binlog file not found on server")
	ErrCheckpointTooOld        = errors.New("checkpoint is too old to safely resume")
	ErrCheckpointCollision     = errors.New("checkpoint belongs to a different table (truncation collision)")
	ErrCouldNotWriteCheckpoint = errors.New("could not write checkpoint")
	ErrWatermarkNotReady       = errors.New("watermark not ready")
)

const (
	Initial State = iota
	CopyRows
	ApplyChangeset // first mass apply
	RestoreSecondaryIndexes
	AnalyzeTable
	Checksum
	PostChecksum // second mass apply
	// WaitingOnSentinelTable comes after the initial checksum so that
	// `state >= Checksum` is true while the sentinel-wait blocks the cutover.
	// During this state Spirit also runs the "continuous checksum" loop
	// described in docs/migrate.md.
	WaitingOnSentinelTable
	CutOver
	// ReverseWindow is the post-cutover reverse window, entered only when
	// --reverse-window > 0: traffic is on the target and spirit keeps the source
	// current in change-only mode while watching for a revert request. It sorts
	// after CutOver (so `state >= Checksum` stays true) and lets orchestration
	// surface that a revert is still possible.
	ReverseWindow
	Close
	ErrCleanup
)

func (s State) String() string {
	switch s {
	case Initial:
		return "initial"
	case CopyRows:
		return "copyRows"
	case WaitingOnSentinelTable:
		return "waitingOnSentinelTable"
	case ApplyChangeset:
		return "applyChangeset"
	case RestoreSecondaryIndexes:
		return "restoreSecondaryIndexes"
	case AnalyzeTable:
		return "analyzeTable"
	case Checksum:
		return "checksum"
	case PostChecksum:
		return "postChecksum"
	case CutOver:
		return "cutOver"
	case ReverseWindow:
		return "reverseWindow"
	case Close:
		return "close"
	case ErrCleanup:
		return "errCleanup"
	}
	return "unknown"
}

// get is now private, use tracker.Get instead
func (s *State) get() State {
	return State(atomic.LoadInt32((*int32)(s)))
}

// set is now private, use tracker.Set / tracker.Do instead
func (s *State) set(newState State) {
	atomic.StoreInt32((*int32)(s), int32(newState))
}

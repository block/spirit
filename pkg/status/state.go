package status

import (
	"errors"
	"sync/atomic"
)

//nolint:recvcheck // String() uses value receiver (called on State values), Get/Set use pointer receivers (atomic ops)
type State int32

var (
	ErrMismatchedAlter         = errors.New("alter statement in checkpoint table does not match the alter statement specified here")
	ErrCouldNotWriteCheckpoint = errors.New("could not write checkpoint")
	ErrWatermarkNotReady       = errors.New("watermark not ready")
)

const (
	Initial State = iota
	CopyRows
	WaitingOnSentinelTable
	ApplyChangeset // first mass apply
	AnalyzeTable
	Checksum
	PostChecksum // second mass apply
	CutOver
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
	case AnalyzeTable:
		return "analyzeTable"
	case Checksum:
		return "checksum"
	case PostChecksum:
		return "postChecksum"
	case CutOver:
		return "cutOver"
	case Close:
		return "close"
	case ErrCleanup:
		return "errCleanup"
	}
	return "unknown"
}

func (s *State) Get() State {
	return State(atomic.LoadInt32((*int32)(s)))
}

func (s *State) Set(newState State) {
	atomic.StoreInt32((*int32)(s), int32(newState))
}

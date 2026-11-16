package contract

import "errors"

// ErrPositionNotFound is returned by StartFromPosition when the
// underlying source can no longer resume from the requested opaque
// position — most commonly because the binlog file has been purged on
// a MySQL source. Wrapped with %w so callers can errors.Is against it.
var ErrPositionNotFound = errors.New("change.Source: cannot resume from position; it is no longer available on the source")

// ErrRowRewritten is returned by a parking source's VerifyRowAtNextChange. It
// means the watched row was written again before the
// verification could read the target, so the image the caller was handed is no
// longer what the target holds. The caller should retry.
var ErrRowRewritten = errors.New("change: watched row was rewritten during verification")

// ErrFlushIncomplete is returned by a parking source's VerifyRowAtNextChange.
// It means the parked flush could not empty the buffer, so the
// watched row's change may not have reached the target. The caller should
// retry; it must not read the target and call the difference a divergence.
var ErrFlushIncomplete = errors.New("change: parked flush did not drain the buffer")

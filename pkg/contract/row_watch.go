package contract

import "context"

// RowWatch names the rows a verification is waiting for. Match is called with
// the row's primary key values as the source decoded them, in the source
// table's key order; returning true fires the watch.
type RowWatch struct {
	Schema string
	Table  string
	Match  func(key []any) bool
}

// RowVerifier is handed the event the feed parked at: the row's key, its
// after-image (nil for a delete), and whether it was a delete. It runs with the
// reader parked and the change flushed, so the target holds that image and
// nothing beyond it. Returning an error fails the verification; the reader is
// unparked either way.
type RowVerifier func(ctx context.Context, key []any, image []any, deleted bool) error

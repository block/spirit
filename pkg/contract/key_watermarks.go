package contract

// KeyWatermarks is what a change stream asks a table copier about a row key
// while both run at the same time. The copier reads the source table in key
// order and writes each row to the target without overwriting a row that is
// already there. The change stream buffers row changes, and uses these
// answers to decide which changes to discard, which to write now, and which
// to defer.
//
// key0 is the value of the first primary-key column.
//
// Every method must return FALSE when it cannot tell. TRUE from
// KeyAboveHighWatermark discards a change; TRUE from KeyBelowLowWatermark or
// KeyNotYetDispatched lets the change stream write it immediately.
type KeyWatermarks interface {
	// KeyAboveHighWatermark reports whether the copier has not yet read
	// key0 and will read it later, so the copier's read will carry the
	// change to the target and the change stream may discard it.
	//
	// It must return FALSE for a key that an earlier run copied, when this
	// run resumed from that run's progress. The row is already on the
	// target, and the copier leaves an existing row as it is, so its later
	// read would not carry the change: discarding it would lose the change.
	//
	// A change stream must not discard on this answer for an
	// implementation that is not also a BufferedKeyNoter.
	KeyAboveHighWatermark(key0 any) bool

	// KeyBelowLowWatermark reports whether the copier has read key0 and
	// committed it to the target, so a change for it can be written now.
	KeyBelowLowWatermark(key0 any) bool

	// KeyNotYetDispatched reports whether the copier has definitely not yet
	// started a read that covers key0. When it returns TRUE no copier read
	// for key0 is in flight, so a buffered change for it can be written
	// now. It is the flush-time counterpart of KeyBelowLowWatermark
	// ("already copied and committed"); between the two sits the in-flight
	// band, which is the only region a flush must defer.
	//
	// Writing such a change puts the row on the target ahead of the copier,
	// and the copier then leaves that row as it is. That is correct only
	// because every later change for the key also reaches the target: see
	// BufferedKeyNoter.
	//
	// Unlike KeyAboveHighWatermark this is not a discard decision, so it
	// ignores keys copied by an earlier run that this run resumed from:
	// those have no read in flight in this run.
	KeyNotYetDispatched(key0 any) bool
}

// BufferedKeyNoter is an optional capability of a KeyWatermarks
// implementation. A change stream type-asserts for it once, when it starts
// tracking a table.
//
// A change stream must never discard a change as above the high watermark
// for an implementation without it: every change is buffered and written,
// which is always correct, but writes changes the copier would otherwise
// have carried.
type BufferedKeyNoter interface {
	// NoteBufferedKey records that the change stream admitted a change for
	// key0 into its buffer instead of discarding it. The change stream calls
	// it for every admitted change, whether or not it is discarding changes
	// at that moment.
	//
	// If no copier read covers key0 yet, the buffered change can reach the
	// target before the copier does (see KeyNotYetDispatched), and the
	// copier cannot overwrite it afterwards. A later change for the same key
	// must therefore never be discarded as above the high watermark: the
	// target would keep the earlier image or, for a delete, a row the source
	// no longer has. After this call KeyAboveHighWatermark returns FALSE for
	// key0 and every key below it, the same way it does for keys an earlier
	// run copied.
	NoteBufferedKey(key0 any)
}

package status

import (
	"sync/atomic"
	"time"
)

// LastCheckpoint records what the last successful checkpoint write saved, so a
// runner can report it as the ckpt row of its periodic status block
// ("ckpt  12s ago  binlog.000123:4567" — see Row) instead of the checkpoint
// dumper logging a line of its own every time it runs. See
// github.com/block/spirit/issues/329.
//
// The age and the position belong together because together they answer the
// only question worth asking about a checkpoint mid-run: if this process died
// now, where would the next one resume, and is that point still available on
// the source? A position the source has since purged is not resumable, and the
// age is what tells you how fast the run is drifting toward that.
//
// The zero value is ready to use and reports "never" / "none". Safe for
// concurrent use: the recorder and the status goroutine are always different
// goroutines. The timestamp and the position are stored separately, so a reader
// can in principle pair an age from one write with the position from the
// previous one — harmless at status-line cadence, where both are approximate by
// construction.
//
// Contains sync/atomic values, so it must not be copied after first use.
type LastCheckpoint struct {
	nanos    atomic.Int64
	position atomic.Pointer[string]
}

// Record marks a checkpoint as having just been persisted at position, which is
// the opaque resume coordinate the change feed reported: a binlog file:offset,
// a GTID set, or whatever an alternative source encodes.
func (c *LastCheckpoint) Record(position string) {
	c.position.Store(&position)
	// Stored last so a reader that sees a fresh age never pairs it with the
	// position from an older checkpoint.
	c.nanos.Store(time.Now().UnixNano())
}

// At returns when the last checkpoint was written, or the zero time if none has
// been.
func (c *LastCheckpoint) At() time.Time {
	nanos := c.nanos.Load()
	if nanos == 0 {
		return time.Time{}
	}
	return time.Unix(0, nanos)
}

// Age renders how long ago the last checkpoint was written, rounded to the
// second, or "never" if none has been.
func (c *LastCheckpoint) Age() string {
	at := c.At()
	if at.IsZero() {
		return "never"
	}
	return time.Since(at).Round(time.Second).String()
}

// Row renders the checkpoint row of a status block: how long ago the last
// checkpoint was written and the position it saved, or "never" before the
// first one.
func (c *LastCheckpoint) Row() string {
	if c.At().IsZero() {
		return "never"
	}
	return c.Age() + " ago  " + c.Position()
}

// Position returns the resume coordinate the last checkpoint saved, or "none"
// if no checkpoint has been written yet (or the source reported no position).
// Never returns the empty string: the status block needs something to render.
func (c *LastCheckpoint) Position() string {
	p := c.position.Load()
	if p == nil || *p == "" {
		return "none"
	}
	return *p
}

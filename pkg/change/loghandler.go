package change

import (
	"context"
	"log/slog"
)

// syncerQuietMessages are the go-mysql BinlogSyncer log messages spirit
// demotes from INFO to DEBUG.
//
// "rotate to next binlog" is the noisy one: it fires on every binlog rotation
// on the source, which on a busy server (or when BlockWait has to issue
// `FLUSH BINARY LOGS` to unstick a stalled position) is often enough to
// dominate the log. The information it carried is not lost — the feed counts
// rotations and reports them on the binlog row of the runner's periodic status
// block as "rotations=<n> (<m> forced)" (see FeedStats).
//
// Everything else go-mysql logs — reconnects, errors, "begin to re-sync" —
// is left alone, because those do indicate something worth seeing. The map is
// a map so adding another message later is a one-line change.
var syncerQuietMessages = map[string]struct{}{
	"rotate to next binlog": {},
}

// demoteHandler is a slog.Handler middleware that rewrites the level of
// records whose message appears in demote. It is used to wrap the logger
// handed to go-mysql, which logs at INFO on paths spirit considers routine.
//
// Demoting rather than dropping keeps the lines available under debug
// logging, which matters when diagnosing a feed that is reconnecting or
// rotating more than expected.
type demoteHandler struct {
	inner  slog.Handler
	demote map[string]struct{}
	level  slog.Level
}

// newDemotingLogger returns a logger that writes through l's handler but
// emits the messages in demote at debug level instead. A nil l falls back to
// slog.Default() so callers can pass an unset logger safely.
func newDemotingLogger(l *slog.Logger, demote map[string]struct{}) *slog.Logger {
	if l == nil {
		l = slog.Default()
	}
	return slog.New(&demoteHandler{
		inner:  l.Handler(),
		demote: demote,
		level:  slog.LevelDebug,
	})
}

// Enabled reports on the *inner* handler, but must stay permissive for any
// level we might demote from: slog checks Enabled before building the record,
// so answering "no" for INFO here would be correct only if we never demoted.
// Since a demoted record is emitted at debug level, the accurate answer for a
// demotable level is whether debug is enabled — but we cannot see the message
// yet. Returning the union keeps behaviour correct: a record that survives to
// Handle is level-checked again there.
func (h *demoteHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level) || h.inner.Enabled(ctx, h.level)
}

func (h *demoteHandler) Handle(ctx context.Context, r slog.Record) error {
	if _, ok := h.demote[r.Message]; ok {
		r.Level = h.level
	}
	if !h.inner.Enabled(ctx, r.Level) {
		return nil
	}
	return h.inner.Handle(ctx, r)
}

// WithAttrs and WithGroup must rewrap: returning the inner handler directly
// (the trap of embedding slog.Handler) would silently drop the demotion for
// any logger derived with attributes.
func (h *demoteHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &demoteHandler{inner: h.inner.WithAttrs(attrs), demote: h.demote, level: h.level}
}

func (h *demoteHandler) WithGroup(name string) slog.Handler {
	return &demoteHandler{inner: h.inner.WithGroup(name), demote: h.demote, level: h.level}
}

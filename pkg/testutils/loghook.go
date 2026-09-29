package testutils

import (
	"context"
	"log/slog"
	"sync"
)

// OnLogHandler wraps a slog.Handler and calls fn, once, the first time a
// record with message msg is handled, before passing the record on. Tests use
// it to act at a precise point in code that has no other hook, such as the
// log line a runner writes as Run starts.
type OnLogHandler struct {
	slog.Handler
	state *onLogState
}

type onLogState struct {
	msg  string
	once sync.Once
	fn   func()
}

// NewOnLogHandler returns an OnLogHandler that wraps h.
func NewOnLogHandler(h slog.Handler, msg string, fn func()) *OnLogHandler {
	return &OnLogHandler{Handler: h, state: &onLogState{msg: msg, fn: fn}}
}

// Handle calls fn the first time msg is logged, then passes rec on. fn runs
// on the logging goroutine and may itself log.
func (h *OnLogHandler) Handle(ctx context.Context, rec slog.Record) error {
	if rec.Message == h.state.msg {
		h.state.once.Do(h.state.fn)
	}
	return h.Handler.Handle(ctx, rec)
}

// WithAttrs keeps the hook on loggers derived with With.
func (h *OnLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &OnLogHandler{Handler: h.Handler.WithAttrs(attrs), state: h.state}
}

// WithGroup keeps the hook on loggers derived with WithGroup.
func (h *OnLogHandler) WithGroup(name string) slog.Handler {
	return &OnLogHandler{Handler: h.Handler.WithGroup(name), state: h.state}
}

package change

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestLogger(buf *bytes.Buffer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: level}))
}

// The message go-mysql logs on every binlog rotation is dropped from an
// INFO-level log, and everything else it logs still gets through.
func TestDemotingLoggerSuppressesAtInfo(t *testing.T) {
	var buf bytes.Buffer
	logger := newDemotingLogger(newTestLogger(&buf, slog.LevelInfo), syncerQuietMessages)

	logger.Info("rotate to next binlog", "file", "binlog.000042", "position", 4)
	require.Empty(t, buf.String())

	logger.Info("begin to re-sync", "file", "binlog.000042")
	require.Contains(t, buf.String(), "begin to re-sync")
	logger.Error("invalid stream header", "header", 7)
	require.Contains(t, buf.String(), "invalid stream header")
}

// Demoted, not dropped: the line is still there for anyone debugging a feed
// that is rotating more than expected, along with its attributes.
func TestDemotingLoggerKeepsMessageAtDebug(t *testing.T) {
	var buf bytes.Buffer
	logger := newDemotingLogger(newTestLogger(&buf, slog.LevelDebug), syncerQuietMessages)

	logger.Info("rotate to next binlog", "file", "binlog.000042")
	out := buf.String()
	require.Contains(t, out, "level=DEBUG")
	require.Contains(t, out, "rotate to next binlog")
	require.Contains(t, out, "file=binlog.000042")
}

// WithAttrs/WithGroup must rewrap rather than hand back the inner handler,
// which would silently lose the demotion for any derived logger.
func TestDemotingLoggerSurvivesWithAttrsAndGroup(t *testing.T) {
	var buf bytes.Buffer
	base := newDemotingLogger(newTestLogger(&buf, slog.LevelInfo), syncerQuietMessages)

	base.With("component", "syncer").Info("rotate to next binlog")
	require.Empty(t, buf.String())

	base.WithGroup("syncer").Info("rotate to next binlog")
	require.Empty(t, buf.String())

	base.With("component", "syncer").Info("begin to re-sync")
	require.Contains(t, buf.String(), "component=syncer")
}

// A nil logger is the documented fallback so callers need not guard.
func TestDemotingLoggerNilFallback(t *testing.T) {
	require.NotPanics(t, func() {
		newDemotingLogger(nil, syncerQuietMessages).Info("rotate to next binlog")
	})
}

// The syncer's logger is the wrapped one, for both change.Source
// implementations — otherwise one of them keeps flooding the log.
func TestSyncerConfigUsesDemotingLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := newTestLogger(&buf, slog.LevelInfo)

	for name, syncerLogger := range map[string]*slog.Logger{
		"binlog": (&binlogClient{logger: logger}).buildSyncerConfig("127.0.0.1", 3306).Logger,
		"gtid":   (&gtidClient{logger: logger}).buildSyncerConfig("127.0.0.1", 3306).Logger,
	} {
		buf.Reset()
		syncerLogger.Info("rotate to next binlog")
		require.Empty(t, buf.String(), "%s syncer must not log rotations at info", name)
		syncerLogger.Info("begin to re-sync")
		require.Contains(t, buf.String(), "begin to re-sync",
			"%s syncer must still log everything else", name)
	}
}

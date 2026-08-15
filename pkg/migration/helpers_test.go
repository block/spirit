package migration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// disableDynamicChunking turns off the chunker's adaptive resizing so the
// caller sees a stable ChunkSize regardless of per-chunk timing under CI
// load. Tests that assert exact chunk boundaries should call this after
// setup; production callers should leave dynamic chunking on.
func disableDynamicChunking(t *testing.T, c table.Chunker) {
	t.Helper()
	setter, ok := c.(interface{ SetDynamicChunking(bool) })
	require.True(t, ok, "copyChunker does not expose SetDynamicChunking")
	setter.SetDynamicChunking(false)
}

// mkIniFile creates a temporary INI config file with the given content and returns its path.
// The file is automatically cleaned up when the test finishes (via t.TempDir()).
func mkIniFile(t *testing.T, content string) string {
	t.Helper()
	tmpFile, err := os.CreateTemp(t.TempDir(), "test_creds_*.cnf")
	require.NoError(t, err)
	_, err = tmpFile.WriteString(content)
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())
	return tmpFile.Name()
}

// stringerFunc adapts a func() string to fmt.Stringer so that message
// arguments to require.Eventually can be evaluated lazily, at the moment the
// failure message is formatted, rather than eagerly when Eventually is called.
type stringerFunc func() string

func (f stringerFunc) String() string { return f() }

// waitForStatus polls until the runner reaches the target status or times out.
// The timeout is generous because the runner must finish the copy and
// checksum phases before it reaches the later states (e.g.
// WaitingOnSentinelTable); under CI load those phases can starve and a
// tighter budget produces spurious timeouts (see issue #946). Each checksum
// attempt alone can legitimately spend up to change.DefaultTimeout (30s) on
// binlog catch-up plus 30s acquiring the table lock, and the runner retries
// the checksum up to 3 times, so the budget must cover at least two full
// attempts.
func waitForStatus(t *testing.T, m *Runner, target status.State) {
	t.Helper()
	// The status is read lazily via a Stringer: fmt args are evaluated when
	// the failure message is formatted (at timeout), so the message reports
	// the status the runner was actually stuck in, not the status when the
	// wait began.
	lastStatus := stringerFunc(func() string { return m.status.Get().String() })
	require.Eventually(t, func() bool {
		return m.status.Get() >= target
	}, 3*time.Minute, 10*time.Millisecond,
		"timeout waiting for status >= %s, last status: %s", target, lastStatus)
}

// waitForCopyRows blocks until the runner reaches the CopyRows state (returning
// true) or ctx is done (returning false). It is for the load-generating
// goroutines in concurrent-DML tests: they begin writing once the copy phase is
// live, but must unblock promptly if the migration ends before then — several
// of these tests deliberately drive the migration to an early failure, so the
// copy phase may never begin. It avoids testify so it is safe to call off the
// test goroutine (require would call runtime.Goexit — testifylint go-require);
// callers should return when it reports false.
func waitForCopyRows(ctx context.Context, m *Runner) bool {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if m.status.Get() >= status.CopyRows {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

// RunnerOption is a functional option for configuring a test Runner.
type RunnerOption func(*Migration)

// WithThreads sets the number of concurrent threads.
func WithThreads(n int) RunnerOption {
	return func(m *Migration) {
		m.Threads = n
	}
}

// WithWriteThreads sets the number of concurrent apply (write) threads.
func WithWriteThreads(n int) RunnerOption {
	return func(m *Migration) {
		m.WriteThreads = n
	}
}

// WithAutoscaling enables the experimental thread autoscaler. Note that it only
// engages against an Aurora target, and when it does it overrides both Threads
// and WriteThreads (see setupCopierCheckerAndReplClient).
func WithAutoscaling() RunnerOption {
	return func(m *Migration) {
		m.EnableExperimentalAutoscaling = true
	}
}

// WithStatement sets the SQL statement for the migration.
func WithStatement(s string) RunnerOption {
	return func(m *Migration) {
		m.Statement = s
	}
}

// WithTargetChunkTime sets the target chunk time.
func WithTargetChunkTime(d time.Duration) RunnerOption {
	return func(m *Migration) {
		m.TargetChunkTime = d
	}
}

// WithBuffered enables (b=true) or disables (b=false) the buffered copier.
// Buffered copy is the default, so this sets the inverse Unbuffered field;
// WithBuffered(false) opts a test back into the legacy unbuffered copier.
func WithBuffered(b bool) RunnerOption {
	return func(m *Migration) {
		m.Unbuffered = !b
	}
}

// WithGTID enables the experimental GTID-based change source.
func WithGTID(b bool) RunnerOption {
	return func(m *Migration) {
		m.EnableExperimentalGTID = b
	}
}

// WithTestThrottler enables the test throttler (slows the copier
// so the repl client has time to observe events).
func WithTestThrottler() RunnerOption {
	return func(m *Migration) {
		m.useTestThrottler = true
	}
}

// WithDeferCutOver enables deferred cutover mode.
func WithDeferCutOver() RunnerOption {
	return func(m *Migration) {
		m.DeferCutOver = true
	}
}

// WithDBName overrides the database name (for tests using CreateUniqueTestDatabase).
func WithDBName(name string) RunnerOption {
	return func(m *Migration) {
		m.Database = name
	}
}

// WithRespectSentinel enables sentinel table detection.
func WithRespectSentinel() RunnerOption {
	return func(m *Migration) {
		m.RespectSentinel = true
	}
}

// WithHost overrides the host address.
func WithHost(host string) RunnerOption {
	return func(m *Migration) {
		m.Host = host
	}
}

// WithReplicaDSN sets the replica DSN for lag monitoring.
func WithReplicaDSN(dsn string) RunnerOption {
	return func(m *Migration) {
		m.ReplicaDSN = dsn
	}
}

// WithReplicaMaxLag sets the maximum replica lag tolerance.
func WithReplicaMaxLag(d time.Duration) RunnerOption {
	return func(m *Migration) {
		m.ReplicaMaxLag = d
	}
}

// WithSkipDropAfterCutover keeps the old table after cutover.
func WithSkipDropAfterCutover() RunnerOption {
	return func(m *Migration) {
		m.SkipDropAfterCutover = true
	}
}

// newTestMigration creates a Migration with sensible defaults for integration tests.
// It parses the test DSN and fills in Host/Username/Password/Database.
// Callers must set Statement before calling Run().
func newTestMigration(t *testing.T, opts ...RunnerOption) *Migration {
	t.Helper()

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	migration := &Migration{
		Host:         cfg.Addr,
		Username:     cfg.User,
		Password:     &cfg.Passwd,
		Database:     cfg.DBName,
		Threads:      2,
		WriteThreads: 2,
	}
	for _, opt := range opts {
		opt(migration)
	}
	return migration
}

// NewTestRunner creates a Runner with sensible defaults, composing the table
// and alter arguments into a full ALTER TABLE statement so tests exercise the
// same --statement path as production callers.
//
// Defaults: Threads=2, TargetChunkTime=500ms (the production default).
//
// Example:
//
//	m := NewTestRunner(t, "mytable", "ENGINE=InnoDB")
//	require.NoError(t, m.Run(t.Context()))
//	require.NoError(t, m.Close())
//
//	m := NewTestRunner(t, "mytable", "ADD INDEX idx_a (a)",
//	    WithThreads(1),
//	    WithTargetChunkTime(100*time.Millisecond),
//	    WithBuffered(true),
//	)
func NewTestRunner(t *testing.T, table, alter string, opts ...RunnerOption) *Runner {
	t.Helper()

	migration := newTestMigration(t, opts...)
	migration.Statement = fmt.Sprintf("ALTER TABLE %s %s", sqlescape.EscapeIdentifier(table), alter)

	runner, err := NewRunner(migration)
	require.NoError(t, err)
	return runner
}

// NewTestRunnerFromStatement creates a Runner for a Statement-based migration
// with sensible defaults. Use this for tests that need the raw statement form
// (CREATE INDEX, CREATE TABLE, etc.) rather than the composed ALTER TABLE of
// NewTestRunner.
//
// Example:
//
//	m := NewTestRunnerFromStatement(t, "ALTER TABLE mytable ADD COLUMN c INT")
//	require.NoError(t, m.Run(t.Context()))
//	require.NoError(t, m.Close())
func NewTestRunnerFromStatement(t *testing.T, statement string, opts ...RunnerOption) *Runner {
	t.Helper()

	migration := newTestMigration(t, opts...)
	migration.Statement = statement

	runner, err := NewRunner(migration)
	require.NoError(t, err)
	return runner
}

// NewTestMigration creates a Migration struct with sensible defaults for tests
// that need to call migration.Run() directly (testing the Migration API rather
// than the Runner API). Use RunnerOption functions to configure it.
//
// Example:
//
//	m := NewTestMigration(t, WithStatement("ALTER TABLE mytable ENGINE=InnoDB"))
//	require.NoError(t, m.Run())
func NewTestMigration(t *testing.T, opts ...RunnerOption) *Migration {
	t.Helper()
	return newTestMigration(t, opts...)
}

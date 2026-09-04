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

// waitForStatus polls until the runner reaches the target status or times out.
// The timeout is generous because the runner must finish the copy and
// checksum phases before it reaches the later states (e.g.
// WaitingOnSentinelTable); under CI load those phases can starve and a
// tighter budget produces spurious timeouts (see issue #946). Each checksum
// attempt alone can legitimately spend up to change.DefaultTimeout (30s) on
// binlog catch-up plus 30s acquiring the table lock, and the runner retries
// the checksum up to 3 times, so the budget must cover at least two full
// attempts.
func waitForStatus(t *testing.T, m *Runner, target status.State, run *testRun) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	require.NotNil(t, run)
	require.NoError(t, awaitTestStatus(ctx, m, target, run))
}

func awaitTestStatus(ctx context.Context, m *Runner, target status.State, run *testRun) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var done <-chan struct{}
	if run != nil {
		done = run.done
	}
	for {
		// Run may fail without ever publishing a later state. Its completion is
		// authoritative, and closing done publishes its error to this goroutine.
		select {
		case <-done:
			return fmt.Errorf("runner exited before waiting for %s completed: %w", target, run.result())
		default:
		}
		current := m.status.Get()
		if current >= status.Close {
			return fmt.Errorf("runner entered terminal state %s while waiting for %s", current, target)
		}
		if current >= target {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for %s (last state %s): %w", target, m.status.Get(), ctx.Err())
		case <-done:
		case <-ticker.C:
		}
	}
}

// testRun owns a runner until Run has exited and Close has finished. Cleanup is
// installed before launch, so FailNow anywhere in the test cannot strand a
// result send or race table cleanup against the runner.
type testRun struct {
	cancel context.CancelFunc
	done   chan struct{}
	err    error // published by closing done
}

func startTestRun(t *testing.T, run func(context.Context) error, closeRunner func() error) *testRun {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	running := &testRun{done: make(chan struct{}), cancel: cancel}
	t.Cleanup(func() {
		cancel()
		select {
		case <-running.done:
			// Close only after Run has stopped touching the runner's fields.
			if err := closeRunner(); err != nil {
				t.Errorf("closing test runner: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("runner did not stop during cleanup")
		}
	})
	go func() {
		running.err = run(ctx)
		close(running.done)
	}()
	return running
}

func (r *testRun) result() error {
	if r.err != nil {
		return r.err
	}
	return fmt.Errorf("runner completed")
}

func (r *testRun) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-r.done:
		return r.err
	case <-time.After(3 * time.Minute):
		t.Fatal("runner did not complete")
		return nil
	}
}

// waitForCopyRows blocks until the runner reaches the CopyRows state (returning
// true) or ctx is done (returning false). It is for the load-generating
// goroutines in concurrent-DML tests: they begin writing once the copy phase is
// live, but must unblock promptly if the migration ends before then — several
// of these tests deliberately drive the migration to an early failure, so the
// copy phase may never begin. It avoids testify so it is safe to call off the
// test goroutine (require would call runtime.Goexit — testifylint go-require);
// callers should return when it reports false.
func waitForCopyRows(t *testing.T, ctx context.Context, m *Runner) bool {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		current := m.status.Get()
		if err := ctx.Err(); err != nil {
			t.Logf("load generator skipped: context ended before observing CopyRows (state %s): %v", current, err)
			return false
		}
		if current >= status.Close {
			t.Logf("load generator skipped: runner reached terminal state %s before CopyRows was observed", current)
			return false
		}
		if current >= status.CopyRows {
			return true
		}
		select {
		case <-ctx.Done():
			t.Logf("load generator skipped: context ended before observing CopyRows (state %s): %v", m.status.Get(), ctx.Err())
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

// WithMaxConnections sets the size of the main connection pool. It is the pool
// size verbatim, not a ceiling on a computed one — see the MaxOpenConnections
// assignment in (*Runner).Run.
func WithMaxConnections(n int) RunnerOption {
	return func(m *Migration) {
		m.MaxConnections = n
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
// Defaults: Threads=2, WriteThreads=2. Copy chunk sizing uses the production
// byte budget (table.DefaultTargetChunkBytes) and the checksum's time budget is
// the constant table.ChunkerDefaultTarget; neither is settable per-test here.
//
// Example:
//
//	m := NewTestRunner(t, "mytable", "ENGINE=InnoDB")
//	require.NoError(t, m.Run(t.Context()))
//	require.NoError(t, m.Close())
//
//	m := NewTestRunner(t, "mytable", "ADD INDEX idx_a (a)",
//	    WithThreads(1),
//	    WithTestThrottler(),
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

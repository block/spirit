package checksum

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"testing"
	"time"

	mysql "github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/change"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// This file pins the behavioural differences between the legacy snapshot
// checker (SingleChecker) and the default lockless checker when both are
// built the way pkg/migration builds them. Anything asserted here is a
// difference an operator would see if they turned --legacy-checksum on, so
// each case is either parity (both implementations behave the same) or a
// documented gap.

// parityFixture is a source/target table pair on one server with a live change
// feed and an open chunker — the same shape the migration runner hands to
// checksum.NewChecker.
type parityFixture struct {
	db      *sql.DB
	feed    change.Source
	chunker table.Chunker
	source  *table.TableInfo
	target  *table.TableInfo
}

// newParityFixture creates `source` and `_source_new` with the supplied DDL
// suffix (everything after the column list) and seeds both from the same rows.
func newParityFixture(t *testing.T, name, columns string) *parityFixture {
	t.Helper()
	targetName := utils.NewTableName(name)
	testutils.RunSQL(t, fmt.Sprintf("DROP TABLE IF EXISTS %s, %s", name, targetName))
	t.Cleanup(func() {
		testutils.RunSQL(t, fmt.Sprintf("DROP TABLE IF EXISTS %s, %s", name, targetName))
	})
	testutils.RunSQL(t, fmt.Sprintf("CREATE TABLE %s (%s)", name, columns))
	testutils.RunSQL(t, fmt.Sprintf("CREATE TABLE %s (%s)", targetName, columns))

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })

	return &parityFixture{db: db}
}

// start captures table info, opens the chunker and starts the feed. Call it
// after seeding both tables (and after any divergence the test injects), the
// way the runner does after the copy phase.
func (f *parityFixture) start(t *testing.T, name string) {
	t.Helper()
	f.source = table.NewTableInfo(f.db, "test", name)
	require.NoError(t, f.source.SetInfo(t.Context()))
	f.target = table.NewTableInfo(f.db, "test", utils.NewTableName(name))
	require.NoError(t, f.target.SetInfo(t.Context()))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	appl := applier.NewSingleTargetForTest(t, f.db)
	feed := change.NewBinlogClient(f.db, cfg.Addr, cfg.User, cfg.Passwd, change.NewClientDefaultConfig())
	t.Cleanup(feed.Close)
	f.feed = feed

	chunker, err := table.NewChunker(f.source, table.ChunkerConfig{NewTable: f.target})
	require.NoError(t, err)
	f.chunker = chunker
	require.NoError(t, feed.AddSubscription(change.BufferedSubscriptionFactory(f.source, f.target, chunker, appl)))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, f.chunker.Open())
}

// checker builds the checker the migration runner would build: an Applier is
// always supplied (pkg/migration passes it unconditionally), and `lockless` selects the algorithm exactly
// as !Migration.LegacyChecksum does.
func (f *parityFixture) checker(t *testing.T, lockless bool, opts ...func(*CheckerConfig)) Checker {
	t.Helper()
	config := NewCheckerDefaultConfig()
	config.Concurrency = 2
	config.Applier = applier.NewSingleTargetForTest(t, f.db)
	if lockless {
		config.Lockless = true
		// Shorter than DefaultLocklessRetryDelay so a confirmed divergence
		// surfaces within the test budget.
		config.RetryDelay = 100 * time.Millisecond
	}
	for _, opt := range opts {
		opt(config)
	}
	checker, err := NewChecker([]*sql.DB{f.db}, f.chunker, []change.Source{f.feed}, config)
	require.NoError(t, err)
	return checker
}

func (f *parityFixture) rowsOnTarget(t *testing.T, name string) int {
	t.Helper()
	var n int
	require.NoError(t, f.db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM "+utils.NewTableName(name)).Scan(&n))
	return n
}

// TestParityCleanTables: identical tables verify successfully under both
// algorithms. Baseline parity.
func TestParityCleanTables(t *testing.T) {
	for _, lockless := range []bool{false, true} {
		t.Run(fmt.Sprintf("lockless=%v", lockless), func(t *testing.T) {
			name := fmt.Sprintf("parity_clean_%v", lockless)
			f := newParityFixture(t, name, "a INT NOT NULL PRIMARY KEY, b INT")
			testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s VALUES (1,1),(2,2),(3,3)", name))
			testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s VALUES (1,1),(2,2),(3,3)", utils.NewTableName(name)))
			f.start(t, name)

			checker := f.checker(t, lockless)
			require.NoError(t, checker.Run(t.Context()))
			require.Zero(t, checker.DifferencesFound())
		})
	}
}

// TestParityTargetDivergence: a _new table that has diverged is REPAIRED and
// the migration is allowed to proceed, under both algorithms. This used to be
// the headline gap — the lockless checker aborted the migration instead — so it
// is asserted here in the same shape for both.
func TestParityTargetDivergence(t *testing.T) {
	for _, lockless := range []bool{false, true} {
		t.Run(fmt.Sprintf("lockless=%v", lockless), func(t *testing.T) {
			name := fmt.Sprintf("parity_diverge_%v", lockless)
			f := newParityFixture(t, name, "a INT NOT NULL PRIMARY KEY, b INT")
			testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s VALUES (1,1),(2,2),(3,3)", name))
			testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s VALUES (1,1),(2,2)", utils.NewTableName(name)))
			f.start(t, name)

			checker := f.checker(t, lockless)
			require.NoError(t, checker.Run(t.Context()), "the missing row is repaired and the run passes")
			// DifferencesFound is not asserted: the snapshot checker clears it
			// per attempt (the re-verify attempt ends at zero) while the
			// lockless one accumulates it across passes. The repair shows up in
			// the data either way, which is what an operator cares about.
			require.Equal(t, 3, f.rowsOnTarget(t, name), "the missing row was recopied")
		})
	}
}

// RunContinuous never repairs: the same divergence Run would recopy is
// reported as ErrPermanentDivergence and left in place, under both algorithms.
// The applier is supplied, as the runners supply it, so the two modes differ in
// the method called alone.
func TestParityContinuousReportsDivergence(t *testing.T) {
	prev := LocklessMinPassInterval
	LocklessMinPassInterval = 0 // the snapshot checker waits one interval before its first pass
	t.Cleanup(func() { LocklessMinPassInterval = prev })
	for _, lockless := range []bool{false, true} {
		t.Run(fmt.Sprintf("lockless=%v", lockless), func(t *testing.T) {
			name := fmt.Sprintf("parity_norepair_%v", lockless)
			f := newParityFixture(t, name, "a INT NOT NULL PRIMARY KEY, b INT")
			testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s VALUES (1,1),(2,2),(3,3)", name))
			testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s VALUES (1,1),(2,2)", utils.NewTableName(name)))
			f.start(t, name)

			checker := f.checker(t, lockless)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			err := checker.RunContinuous(ctx)
			require.ErrorIs(t, err, ErrPermanentDivergence)
			// A divergence verdict is about the data, so a fresh attempt
			// reaches the same conclusion. Assert it was not retried:
			// ErrAttemptsExhausted wraps the last attempt's error with %w, so
			// an ErrorIs on the sentinel alone would still pass after three
			// attempts had been spent on it.
			require.NotErrorIs(t, err, ErrAttemptsExhausted, "a divergence verdict is not retried")
			require.Equal(t, 2, f.rowsOnTarget(t, name), "no repair was attempted")
		})
	}
}

// cancelOnInspect cancels a context when the checker starts logging the row
// differences of a confirmed divergence — the moment a sentinel drop would have
// to land to race the verdict.
type cancelOnInspect struct {
	slog.Handler
	cancel context.CancelFunc
}

func (h cancelOnInspect) Handle(ctx context.Context, r slog.Record) error {
	if r.Message == "inspecting differences for chunk" {
		h.cancel()
	}
	return h.Handler.Handle(ctx, r)
}

// A divergence RunContinuous has confirmed is reported even when the run is
// cancelled straight after: cancellation is a clean stop only for a target the
// run had no verdict on. Otherwise a sentinel drop racing the report would cut
// over a table known to be wrong.
func TestParityContinuousDivergenceSurvivesCancel(t *testing.T) {
	prev := LocklessMinPassInterval
	LocklessMinPassInterval = 0
	t.Cleanup(func() { LocklessMinPassInterval = prev })
	for _, lockless := range []bool{false, true} {
		t.Run(fmt.Sprintf("lockless=%v", lockless), func(t *testing.T) {
			name := fmt.Sprintf("parity_divcancel_%v", lockless)
			f := newParityFixture(t, name, "a INT NOT NULL PRIMARY KEY, b INT")
			testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s VALUES (1,1),(2,2),(3,3)", name))
			testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s VALUES (1,1),(2,2)", utils.NewTableName(name)))
			f.start(t, name)

			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			checker := f.checker(t, lockless, func(c *CheckerConfig) {
				c.Logger = slog.New(cancelOnInspect{Handler: slog.NewTextHandler(testWriter{}, nil), cancel: cancel})
			})
			require.ErrorIs(t, checker.RunContinuous(ctx), ErrPermanentDivergence)
			require.ErrorIs(t, ctx.Err(), context.Canceled, "the run was cancelled after the verdict")
			if lockless, ok := checker.(*LocklessChecker); ok {
				require.Equal(t, uint64(1), lockless.Stats().PermanentFailures, "counted even if the worker dropped its result")
			}
			require.Equal(t, 2, f.rowsOnTarget(t, name), "no repair was attempted")
		})
	}
}

// TestParityLossyAlter: adding a UNIQUE index over non-unique data must fail
// under both algorithms with ErrDifferencesExhausted, the signal a caller uses
// to decide not to retry. Repairs are on, so the lockless gate keeps repairing
// and re-reading the chunk until MaxPasses is spent; because every pass
// repaired, its ErrVerificationUnresolved also wraps ErrDifferencesExhausted.
func TestParityLossyAlter(t *testing.T) {
	for _, lockless := range []bool{false, true} {
		t.Run(fmt.Sprintf("lockless=%v", lockless), func(t *testing.T) {
			name := fmt.Sprintf("parity_lossy_%v", lockless)
			targetName := utils.NewTableName(name)
			testutils.RunSQL(t, fmt.Sprintf("DROP TABLE IF EXISTS %s, %s", name, targetName))
			t.Cleanup(func() {
				testutils.RunSQL(t, fmt.Sprintf("DROP TABLE IF EXISTS %s, %s", name, targetName))
			})
			testutils.RunSQL(t, fmt.Sprintf("CREATE TABLE %s (id INT NOT NULL PRIMARY KEY, b VARCHAR(16) NOT NULL)", name))
			testutils.RunSQL(t, fmt.Sprintf("CREATE TABLE %s (id INT NOT NULL PRIMARY KEY, b VARCHAR(16) NOT NULL, UNIQUE(b))", targetName))
			testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s VALUES (1,'a'),(2,'b'),(3,'a')", name))
			testutils.RunSQL(t, fmt.Sprintf("INSERT IGNORE INTO %s SELECT * FROM %s", targetName, name))

			db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
			require.NoError(t, err)
			t.Cleanup(func() { utils.CloseAndLog(db) })
			f := &parityFixture{db: db}
			f.start(t, name)

			err = f.checker(t, lockless).Run(t.Context())
			require.Error(t, err)
			if lockless {
				// Repairs are on, so the chunk is rewritten and re-read rather
				// than condemned; INSERT IGNORE cannot close the gap, so the
				// pass budget is what ends it.
				require.ErrorIs(t, err, ErrVerificationUnresolved)
			}
			require.ErrorIs(t, err, ErrDifferencesExhausted)
		})
	}
}

// TestParityResumeWatermark: after a clean pass BOTH checkers publish resume
// evidence a restarted migration can skip verified rows with. The lockless
// checker used to publish none, which meant every resumed migration
// re-verified the whole table from the beginning.
func TestParityResumeWatermark(t *testing.T) {
	for _, lockless := range []bool{false, true} {
		t.Run(fmt.Sprintf("lockless=%v", lockless), func(t *testing.T) {
			name := fmt.Sprintf("parity_resume_%v", lockless)
			f := newParityFixture(t, name, "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, b INT NOT NULL")
			testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (b) SELECT 1 FROM dual", name))
			for range 10 {
				testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (b) SELECT 1 FROM %s", name, name))
			}
			testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", utils.NewTableName(name), name))
			f.start(t, name)

			checker := f.checker(t, lockless)
			require.NoError(t, checker.Run(t.Context()))
			wm, err := checker.ResumeWatermark()
			require.NoError(t, err)
			require.NotEmpty(t, wm, "a clean pass is resumable evidence")
		})
	}
}

// A repaired chunk is not resume evidence. The repair happens after the read
// that condemned the chunk, so nothing has compared source and target since;
// publishing it would let a resume skip a range no one has verified.
//
// The run is capped at the repairing pass so the assertion lands on that pass
// rather than on the clean re-verification that normally follows it.
func TestLocklessRepairIsNotResumeEvidence(t *testing.T) {
	name := "parity_repair_wm"
	f := newParityFixture(t, name, "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, b INT NOT NULL")
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (b) SELECT 1 FROM dual", name))
	for range 10 {
		testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (b) SELECT 1 FROM %s", name, name))
	}
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", utils.NewTableName(name), name))
	testutils.RunSQL(t, fmt.Sprintf("UPDATE %s SET b = 99 WHERE id = 1", utils.NewTableName(name)))
	f.start(t, name)

	checker := f.checker(t, true, func(c *CheckerConfig) { c.MaxPasses = 1 })
	require.ErrorIs(t, checker.Run(t.Context()), ErrVerificationUnresolved,
		"the repairing pass is not clean, and the budget stops the re-verification")

	wm, err := checker.ResumeWatermark()
	require.NoError(t, err)
	require.Empty(t, wm, "the first chunk was repaired, so nothing below it is verified evidence")

	var b int
	require.NoError(t, f.db.QueryRowContext(t.Context(),
		"SELECT b FROM "+utils.NewTableName(name)+" WHERE id = 1").Scan(&b))
	require.Equal(t, 1, b, "the diverged row was repaired from the source")
}

// After a run that repaired and then re-verified cleanly, the watermark is
// evidence from the clean pass: every chunk below it was compared equal after
// the repair landed.
func TestLocklessRepairWatermarkIsCleanPassEvidence(t *testing.T) {
	name := "parity_repair_wm2"
	f := newParityFixture(t, name, "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, b INT NOT NULL")
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (b) SELECT 1 FROM dual", name))
	for range 10 {
		testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (b) SELECT 1 FROM %s", name, name))
	}
	testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", utils.NewTableName(name), name))
	testutils.RunSQL(t, fmt.Sprintf("UPDATE %s SET b = 99 WHERE id = 1", utils.NewTableName(name)))
	f.start(t, name)

	checker := f.checker(t, true)
	require.NoError(t, checker.Run(t.Context()))

	wm, err := checker.ResumeWatermark()
	require.NoError(t, err)
	require.NotEmpty(t, wm, "the re-verification pass compared every chunk equal")
}

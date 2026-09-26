package checksum

import (
	"database/sql"
	"fmt"
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

// This file pins the behavioural differences between the default snapshot
// checker (SingleChecker) and the experimental lockless checker when both are
// built the way pkg/migration builds them. Anything asserted here is a
// difference an operator would see if they turned the experimental flag on, so
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
	feed := change.NewBinlogClient(f.db, cfg.Addr, cfg.User, cfg.Passwd,
		applier.NewSingleTargetForTest(t, f.db), change.NewClientDefaultConfig())
	t.Cleanup(feed.Close)
	f.feed = feed

	chunker, err := table.NewChunker(f.source, table.ChunkerConfig{NewTable: f.target})
	require.NoError(t, err)
	f.chunker = chunker
	require.NoError(t, feed.AddSubscription(f.source, f.target, chunker))
	require.NoError(t, feed.Start(t.Context()))
	require.NoError(t, f.chunker.Open())
}

// checker builds the checker the migration runner would build: FixDifferences
// and a RepairApplier are always supplied (pkg/migration passes both
// unconditionally), and `lockless` selects the experimental algorithm exactly
// as Migration.EnableExperimentalLocklessChecksum does.
func (f *parityFixture) checker(t *testing.T, lockless bool, opts ...func(*CheckerConfig)) Checker {
	t.Helper()
	config := NewCheckerDefaultConfig()
	config.Concurrency = 2
	config.FixDifferences = true
	config.RepairApplier = applier.NewSingleTargetForTest(t, f.db)
	if lockless {
		config.Lockless = &LocklessCheckerConfig{
			SplitHotChunks:    true,
			SnapshotHotChunks: true,
			// Repair policy is deliberately not set: the factory derives it
			// from FixDifferences, which is the whole point of these tests.
			//
			// Production uses DefaultLocklessRetryDelay (1 minute). Shortened
			// here so a confirmed divergence surfaces within the test budget.
			RetryDelay: 100 * time.Millisecond,
		}
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
// the headline gap — the lockless checker ignored FixDifferences and aborted
// the migration instead — so it is asserted here in the same shape for both.
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

// A caller that did NOT ask for repairs still gets a hard error rather than a
// silent pass — this is the `spirit sync`-shaped configuration, and it is the
// only thing FixDifferences=false should change.
func TestParityDivergenceWithoutRepair(t *testing.T) {
	for _, lockless := range []bool{false, true} {
		t.Run(fmt.Sprintf("lockless=%v", lockless), func(t *testing.T) {
			name := fmt.Sprintf("parity_norepair_%v", lockless)
			f := newParityFixture(t, name, "a INT NOT NULL PRIMARY KEY, b INT")
			testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s VALUES (1,1),(2,2),(3,3)", name))
			testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s VALUES (1,1),(2,2)", utils.NewTableName(name)))
			f.start(t, name)

			config := NewCheckerDefaultConfig()
			config.Concurrency = 2
			config.FixDifferences = false
			// Still required: the snapshot checker builds its repair path
			// unconditionally and only consults FixDifferences at the point of
			// use. Supplied for both so the two differ in policy alone.
			config.RepairApplier = applier.NewSingleTargetForTest(t, f.db)
			if lockless {
				config.Lockless = &LocklessCheckerConfig{RetryDelay: 100 * time.Millisecond}
			}
			checker, err := NewChecker([]*sql.DB{f.db}, f.chunker, []change.Source{f.feed}, config)
			require.NoError(t, err)
			require.Error(t, checker.Run(t.Context()))
			require.Equal(t, 2, f.rowsOnTarget(t, name), "no repair was attempted")
		})
	}
}

// TestParityLossyAlter: adding a UNIQUE index over non-unique data must fail
// under both algorithms. The sentinel differs (ErrDifferencesExhausted vs
// ErrPermanentDivergence) but pkg/migration wraps either with the same
// "likely a UNIQUE index on non-unique data" guidance, so the operator-visible
// outcome is at parity.
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

			require.Error(t, f.checker(t, lockless).Run(t.Context()))
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

	checker := f.checker(t, true, func(c *CheckerConfig) { c.Lockless.MaxPasses = 1 })
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

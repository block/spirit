package applier

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// This file characterizes which InnoDB index records two concurrent
// `REPLACE INTO` statements actually contend on, for a table shaped like the
// one that produced the production deadlock in block/spirit#1168: a bigint
// auto-increment primary key, two UNIQUE secondary keys (one single-column,
// one composite), and two non-unique secondary keys.
//
// It exists because the flush path's concurrency controller currently treats
// contention as an opaque signal — it halves width until the deadlocks stop —
// and a proposed replacement partitions each drain into disjoint ranges of a
// UNIQUE secondary index so sibling batches cannot touch the same record or
// gap. That proposal rests on two claims about InnoDB that are documented but
// load-bearing enough to be worth pinning down in a test:
//
//  1. A *non-unique* secondary index is not a conflict surface between
//     PK-disjoint rows. Its record key is (indexed columns, PK), so two rows
//     with different PKs always occupy distinct records even when the indexed
//     value is identical.
//
//  2. A *unique* secondary index is a conflict surface, via next-key locks
//     taken during duplicate detection. Those cover the gap below the record,
//     so *adjacent* values conflict — which is why value-level disjointness is
//     not enough and the partitioning has to be by range.
//
// The seed deliberately scrambles the three orderings against each other with
// coprime strides, so a pair of rows can be adjacent in `unq_group` while being
// far apart in both `unq_token` and the primary key. Without that, every
// "adjacent in the composite key" pair would also be adjacent in the PK and the
// test could not say which index the contention came from.
const (
	lockProbeRows = 1000
	// Coprime with lockProbeRows, and with each other, so id order, token order
	// and group order are three mutually independent permutations.
	tokenStride = 391
	groupStride = 397
)

// The column names are deliberately structural — `key_a`/`key_b`/`key_c` are
// the composite UNIQUE key's parts in order, `payload` is a column no index
// covers — rather than describing anything. Only the *shape* is load-bearing,
// and every part of it earns its place: the bigint auto-increment PK (so PK
// order is a third independent ordering), a single-column UNIQUE key and a
// three-column one (so the composite case is covered, where only the leading
// column varies), a varchar leading column (so adjacency is collation order,
// which is where the gap locks land), and two non-unique keys on columns the
// seed holds constant (the strongest possible case for a non-unique index being
// a conflict surface — see the `row` comment). Naming a column after a domain
// concept would add nothing a test can assert and would invite the reader to
// reason about the workload instead of the index.
const lockProbeDDL = "CREATE TABLE lock_probe (\n" +
	"  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
	"  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
	"  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
	"  `version` bigint NOT NULL DEFAULT '0',\n" +
	"  `token` varchar(255) NOT NULL,\n" +
	"  `key_a` varchar(255) NOT NULL,\n" +
	"  `key_b` varchar(255) NOT NULL,\n" +
	"  `key_c` varchar(50) NOT NULL,\n" +
	"  `payload` bigint NOT NULL DEFAULT '0',\n" +
	"  PRIMARY KEY (`id`),\n" +
	"  UNIQUE KEY `unq_token` (`token`),\n" +
	"  UNIQUE KEY `unq_group` (`key_a`,`key_b`,`key_c`),\n" +
	"  KEY `idx_created_at` (`created_at`),\n" +
	"  KEY `idx_updated_at` (`updated_at`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC"

// lockProbeTable holds the id<->position mappings for each index, so a scenario
// can ask for "the row at position p of unq_group" without recomputing strides.
type lockProbeTable struct {
	db         *sql.DB
	idByToken  map[int]int // token position -> id
	idByGroup  map[int]int // unq_group position -> id
	tokenPosOf map[int]int // id -> token position
	groupPosOf map[int]int // id -> unq_group position
}

func tokenPos(id int) int { return (id * tokenStride) % lockProbeRows }
func groupPos(id int) int { return (id * groupStride) % lockProbeRows }

// row renders a full REPLACE value tuple for a row, keeping every column's
// value a pure function of the id so the statement is idempotent and so two
// scenarios touching the same id write identical bytes. `created_at` is
// constant across all rows on purpose: that makes idx_created_at a single
// enormous run of equal values, which is the strongest possible case for a
// non-unique index being a conflict surface. If it isn't one there, it isn't
// one anywhere.
func (lp *lockProbeTable) row(id int) string {
	return fmt.Sprintf(
		"(%d, '2020-01-01 00:00:00', '2020-01-01 00:00:00', %d, 'tok-%04d', 'a-%04d', 'b1', 'c1', %d)",
		id, id, tokenPos(id), groupPos(id), id*10,
	)
}

func (lp *lockProbeTable) replace(ids ...int) string {
	tuples := make([]string, 0, len(ids))
	for _, id := range ids {
		tuples = append(tuples, lp.row(id))
	}
	return "REPLACE INTO lock_probe (`id`, `created_at`, `updated_at`, `version`, " +
		"`token`, `key_a`, `key_b`, `key_c`, `payload`) VALUES " +
		strings.Join(tuples, ", ")
}

func newLockProbeTable(t *testing.T) *lockProbeTable {
	t.Helper()
	testutils.RunSQL(t, "DROP TABLE IF EXISTS lock_probe")
	testutils.RunSQL(t, lockProbeDDL)

	lp := &lockProbeTable{
		idByToken:  make(map[int]int, lockProbeRows),
		idByGroup:  make(map[int]int, lockProbeRows),
		tokenPosOf: make(map[int]int, lockProbeRows),
		groupPosOf: make(map[int]int, lockProbeRows),
	}
	for id := 1; id <= lockProbeRows; id++ {
		tp, gp := tokenPos(id), groupPos(id)
		lp.idByToken[tp], lp.idByGroup[gp] = id, id
		lp.tokenPosOf[id], lp.groupPosOf[id] = tp, gp
	}

	// Seed every row so that each REPLACE below finds an existing row and
	// therefore takes the duplicate-detection path. This is the state the
	// migration is in once the copy has finished, and it is the state in which
	// the production deadlocks appeared — before the copy completes most
	// REPLACEs insert without finding a duplicate at all.
	var b strings.Builder
	ids := make([]int, 0, lockProbeRows)
	for id := 1; id <= lockProbeRows; id++ {
		ids = append(ids, id)
	}
	b.WriteString(lp.replace(ids...))
	testutils.RunSQL(t, b.String())

	// Through dbconn rather than sql.Open, so the probe runs on the same
	// connection settings the flush path does — above all
	// transaction_isolation=READ-COMMITTED, which is what makes the clustered
	// index a record lock with no gap. Under the server default of REPEATABLE
	// READ the claims below are about a different lock set than production's.
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	lp.db = db
	return lp
}

// blocks holds `first` open in one transaction, then runs `second` in another
// with a 1-second lock wait, and reports whether `second` was blocked by
// `first`. A lock-contention error (1205/1213) means yes; success means no.
func (lp *lockProbeTable) blocks(t *testing.T, first, second string) bool {
	t.Helper()
	ctx := context.Background()

	c1, err := lp.db.Conn(ctx)
	require.NoError(t, err)
	defer utils.CloseAndLog(c1)
	c2, err := lp.db.Conn(ctx)
	require.NoError(t, err)
	defer utils.CloseAndLog(c2)

	// Keep the wait short so a blocked probe reports quickly, and keep it on
	// the *second* connection only: the first must never be the one that times
	// out, or the result would say nothing about who blocked whom.
	_, err = c2.ExecContext(ctx, "SET SESSION innodb_lock_wait_timeout = 1")
	require.NoError(t, err)

	_, err = c1.ExecContext(ctx, "BEGIN")
	require.NoError(t, err)
	defer func() { _, _ = c1.ExecContext(ctx, "ROLLBACK") }()
	_, err = c1.ExecContext(ctx, first)
	require.NoError(t, err, "the holding transaction must succeed")

	_, err = c2.ExecContext(ctx, "BEGIN")
	require.NoError(t, err)
	defer func() { _, _ = c2.ExecContext(ctx, "ROLLBACK") }()

	_, err = c2.ExecContext(ctx, second)
	if err == nil {
		return false
	}
	require.True(t, dbconn.IsLockContentionError(err),
		"expected either success or a lock-contention error, got: %v", err)
	return true
}

// TestReplaceContendsOnlyOnUniqueIndexes pins down which index records two
// concurrent REPLACEs fight over. Each scenario picks two rows that are far
// apart in every index except the one under test.
func TestReplaceContendsOnlyOnUniqueIndexes(t *testing.T) {
	lp := newLockProbeTable(t)

	// A pair that is distant in all three orderings, used as the baseline and
	// as the "far apart" arm of the unique-index scenarios.
	farA := lp.idByGroup[100]
	farB := lp.idByGroup[600]

	for _, tc := range []struct {
		name   string
		a, b   int
		expect bool
		why    string
	}{
		{
			name:   "distant in every index",
			a:      farA,
			b:      farB,
			expect: false,
			why: "the control: if this blocks, the probe is measuring something " +
				"other than index adjacency and every other result is suspect",
		},
		{
			name:   "identical non-unique index value",
			a:      farA,
			b:      farB,
			expect: false,
			why: "every seeded row shares one created_at, so these two rows sit in " +
				"the same run of idx_created_at while being distant in both UNIQUE " +
				"keys and the PK. Non-unique records are keyed (value, PK), so " +
				"PK-disjoint rows never share one",
		},
		{
			name:   "adjacent in unq_group",
			a:      lp.idByGroup[300],
			b:      lp.idByGroup[301],
			expect: true,
			why: "consecutive positions in the composite UNIQUE key, and the stride " +
				"puts them far apart in unq_token and the PK. This is the shape of " +
				"the production cycle: rows whose leading key-column values sort " +
				"next to each other, which is what a correlated leading column " +
				"produces once random map iteration scatters siblings across " +
				"batches",
		},
		{
			name:   "adjacent in unq_token",
			a:      lp.idByToken[300],
			b:      lp.idByToken[301],
			expect: true,
			why: "same mechanism on the single-column UNIQUE key, isolated by the " +
				"stride from unq_group and PK adjacency",
		},
		{
			name:   "adjacent in the primary key",
			a:      500,
			b:      501,
			expect: false,
			why: "the asymmetry the design depends on. On the clustered index a " +
				"REPLACE's conflict is with the row bearing that exact PK, so it " +
				"takes a record lock and — under READ COMMITTED — no gap. PK " +
				"neighbours therefore lock strictly distinct records and never " +
				"contend. On a UNIQUE *secondary* index the duplicate check takes " +
				"a next-key lock instead, gap included, which is why the two cases " +
				"above block and this one does not. Consequence: PK separation is " +
				"not needed at all, and the complete conflict surface is the set " +
				"of UNIQUE secondary indexes",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Guard the premise of each scenario rather than trusting the strides.
			if tc.name == "adjacent in unq_group" {
				require.Greater(t, abs(lp.tokenPosOf[tc.a]-lp.tokenPosOf[tc.b]), 10, "must be distant in unq_token")
				require.Greater(t, abs(tc.a-tc.b), 10, "must be distant in the PK")
			}
			if tc.name == "adjacent in unq_token" {
				require.Greater(t, abs(lp.groupPosOf[tc.a]-lp.groupPosOf[tc.b]), 10, "must be distant in unq_group")
				require.Greater(t, abs(tc.a-tc.b), 10, "must be distant in the PK")
			}
			if tc.name == "adjacent in the primary key" {
				require.Greater(t, abs(lp.groupPosOf[tc.a]-lp.groupPosOf[tc.b]), 10, "must be distant in unq_group")
				require.Greater(t, abs(lp.tokenPosOf[tc.a]-lp.tokenPosOf[tc.b]), 10, "must be distant in unq_token")
			}
			got := lp.blocks(t, lp.replace(tc.a), lp.replace(tc.b))
			require.Equal(t, tc.expect, got, tc.why)
		})
	}
}

// TestUniqueKeyRangePartitioningRemovesContention is the A/B that decides
// whether the proposed design is worth building. Both arms apply the same 20
// rows split into two 10-row batches; they differ only in *how* the rows are
// assigned to batches.
//
//   - interleaved: batches alternate positions in unq_group, which is what the
//     current randomized map iteration produces.
//   - partitioned: each batch takes a contiguous, well-separated range of
//     unq_group, which is what partitioning by that key would produce.
//
// If the interleaved arm contends and the partitioned arm does not, range
// partitioning by a UNIQUE secondary key is a real fix rather than a plausible
// one, and the concurrency controller can stop being the primary defence.
func TestUniqueKeyRangePartitioningRemovesContention(t *testing.T) {
	lp := newLockProbeTable(t)

	var interleavedA, interleavedB []int
	for i := range 10 {
		interleavedA = append(interleavedA, lp.idByGroup[300+2*i])
		interleavedB = append(interleavedB, lp.idByGroup[301+2*i])
	}

	var partitionedA, partitionedB []int
	for i := range 10 {
		partitionedA = append(partitionedA, lp.idByGroup[300+i])
		partitionedB = append(partitionedB, lp.idByGroup[600+i])
	}

	interleaved := lp.blocks(t, lp.replace(interleavedA...), lp.replace(interleavedB...))
	partitioned := lp.blocks(t, lp.replace(partitionedA...), lp.replace(partitionedB...))

	t.Logf("interleaved by unq_group: blocks=%v", interleaved)
	t.Logf("partitioned by unq_group: blocks=%v", partitioned)

	require.True(t, interleaved,
		"if interleaving batches across unq_group does NOT contend, then unique-key "+
			"adjacency is not the production mechanism and the partitioning design "+
			"is aimed at the wrong target")
	require.False(t, partitioned,
		"two batches holding disjoint, separated ranges of unq_group must not "+
			"contend — this is the property the whole design rests on")
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

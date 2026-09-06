package change

import (
	"fmt"
	"strings"
	"testing"

	mysql2 "github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// partitionProbeDDL is the shape that motivated this whole mechanism: a bigint
// auto-increment PK, one UNIQUE secondary key whose leading column is
// *correlated* (siblings share it, so their records are physically adjacent),
// and one whose value is opaque and unique per row.
func partitionProbeDDL(name string) string {
	return fmt.Sprintf(`CREATE TABLE %s (
		id bigint NOT NULL AUTO_INCREMENT,
		token varchar(255) NOT NULL,
		key_a varchar(255) NOT NULL,
		key_b varchar(255) NOT NULL,
		key_c varchar(50) NOT NULL,
		payload bigint NOT NULL DEFAULT 0,
		PRIMARY KEY (id),
		UNIQUE KEY unq_token (token),
		UNIQUE KEY unq_composite (key_a, key_b, key_c),
		KEY idx_payload (payload)
	)`, name)
}

// TestFlushPartitioningAppliesEveryRow is the correctness half. Partitioning
// reorders and regroups a drain's rows, so the thing that must be pinned first
// is that reordering changes nothing about the outcome: every buffered change
// still lands, exactly once, with its final value.
//
// This runs the real subscription against a real server rather than a fake
// applier, because the reordering happens in the same code path that builds the
// statements and the two are only worth testing together.
func TestFlushPartitioningAppliesEveryRow(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS partprobe_t1, partprobe_t2")
	testutils.RunSQL(t, partitionProbeDDL("partprobe_t1"))
	testutils.RunSQL(t, partitionProbeDDL("partprobe_t2"))

	// 400 rows in sibling groups of 8 sharing key_a. Note what has to be true
	// for this to be a legal table *and* a clustered one: the composite key's
	// leading column repeats while a trailing component varies, which is exactly
	// the production shape — a correlated leading column with a discriminator
	// after it. A UNIQUE key cannot repeat in full, so a repeating *leading*
	// column is the only form clustering can take.
	const rows, siblings = 400, 8
	tuples := make([]string, 0, rows)
	for i := range rows {
		tuples = append(tuples, fmt.Sprintf("(%d, 'tok-%05d', 'grp-%04d', 'b', 'c%d', %d)",
			i+1, i, i/siblings, i%siblings, i*10))
	}
	testutils.RunSQL(t, "INSERT INTO partprobe_t1 (id, token, key_a, key_b, key_c, payload) VALUES "+
		strings.Join(tuples, ","))

	t1 := table.NewTableInfo(db, "test", "partprobe_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "partprobe_t2")
	require.NoError(t, t2.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	client := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd,
		applier.NewSingleTargetForTest(t, db), NewClientDefaultConfig()).(*binlogClient)
	chunker, err := table.NewChunker(t1, table.ChunkerConfig{NewTable: t2})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(t1, t2, chunker))
	require.NoError(t, client.Start(t.Context()))
	t.Cleanup(client.Close)

	// Update every row, then update a subset again so dedup collapses those to
	// one image — the flush must land the *last* value for each key.
	testutils.RunSQL(t, "UPDATE partprobe_t1 SET payload = payload + 1")
	testutils.RunSQL(t, "UPDATE partprobe_t1 SET payload = payload + 1 WHERE id % 3 = 0")
	// And delete a few, which exercise the no-row-image tail of the sort.
	testutils.RunSQL(t, "DELETE FROM partprobe_t1 WHERE id % 97 = 0")

	require.NoError(t, client.BlockWait(t.Context()))
	require.NoError(t, client.Flush(t.Context()))

	sub := client.subs.Snapshot()[0].(*bufferedMap)
	require.NotEmpty(t, sub.partitioner.candidates,
		"the probe table has two UNIQUE secondary indexes; both should have resolved")

	// The target now matches the source, row for row and value for value. A
	// misordered sort, a dropped batch, or a row batched twice all show up here.
	var diff int
	require.NoError(t, db.QueryRowContext(t.Context(), `
		SELECT COUNT(*) FROM (
			SELECT id FROM (
				SELECT id, token, key_a, key_b, key_c, payload FROM partprobe_t1
				UNION ALL
				SELECT id, token, key_a, key_b, key_c, payload FROM partprobe_t2
			) u
			GROUP BY id, token, key_a, key_b, key_c, payload
			HAVING COUNT(*) <> 2
		) mismatched`).Scan(&diff))
	require.Zero(t, diff, "every buffered change must land with its final value")

	var srcCount, dstCount int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM partprobe_t1").Scan(&srcCount))
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM partprobe_t2").Scan(&dstCount))
	require.Equal(t, srcCount, dstCount)
	require.Less(t, srcCount, rows, "the deletes must actually have been applied")
}

// TestFlushPartitioningChoosesTheCorrelatedIndex pins the self-configuration
// against a real table's index metadata, rather than against hand-built
// partitionIndex values. The chooser has to pick the *correlated* key: sorting
// by an opaque one buys almost nothing (its adjacency probability is about
// n^2/N per drain), while sorting by a key whose leading column repeats brings
// guaranteed-adjacent sibling records into a single statement.
func TestFlushPartitioningChoosesTheCorrelatedIndex(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS partchoose_t1, partchoose_t2")
	testutils.RunSQL(t, partitionProbeDDL("partchoose_t1"))
	testutils.RunSQL(t, partitionProbeDDL("partchoose_t2"))

	t1 := table.NewTableInfo(db, "test", "partchoose_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "partchoose_t2")
	require.NoError(t, t2.SetInfo(t.Context()))

	candidates, err := resolvePartitionIndexes(t.Context(), t1, t2)
	require.NoError(t, err)
	require.Len(t, candidates, 2, "unq_token and unq_composite")

	// key_a is the leading column of unq_composite; ordinals index the source
	// row image, so this also pins the column-name-to-ordinal mapping.
	byName := map[string]partitionIndex{}
	for _, c := range candidates {
		byName[c.name] = c
	}
	require.Equal(t, []int{1}, byName["unq_token"].ordinals, "token is column 1")
	require.Equal(t, []int{2, 3, 4}, byName["unq_composite"].ordinals, "key_a, key_b, key_c")

	// Rows whose composite key has a leading column repeating every 8, and a
	// token that is unique per row as a UNIQUE single-column key must be.
	newRows := func(clustered bool) []drainRow {
		var rows []drainRow
		for i := range int64(160) {
			leading := fmt.Sprintf("grp-%05d", i)
			if clustered {
				leading = fmt.Sprintf("grp-%04d", i/8)
			}
			rows = append(rows, drainRow{
				key: fmt.Sprintf("k%d", i),
				change: bufferedChange{
					originalKey: []any{i},
					logicalRow: applier.LogicalRow{RowImage: []any{
						i,
						fmt.Sprintf("tok-%05d", i),
						leading,
						"b", fmt.Sprintf("c%d", i%8), i * 10,
					}},
				},
			})
		}
		return rows
	}

	chosen := choosePartitionIndex(candidates, newRows(true))
	require.NotNil(t, chosen)
	require.Equal(t, "unq_composite", chosen.name,
		"the correlated key must win: its leading column repeats, so its records are adjacent")

	// With no clustering anywhere — every leading value distinct in both keys —
	// there is nothing to discriminate on and any candidate is as good as the
	// other. The contract in that case is only that partitioning still happens:
	// sorting is what brings a chance-adjacent pair into one batch, and it costs
	// one sort whichever key it is.
	chosen = choosePartitionIndex(candidates, newRows(false))
	require.NotNil(t, chosen, "an unclustered drain is still worth sorting")
}

// TestFlushPartitioningInactiveWithoutUniqueIndex pins the degradation path. A
// table with no unique secondary index has no conflict surface for two
// PK-disjoint batches to fight over — the clustered index takes record locks
// with no gap — so the sort would be pure cost and must not happen.
func TestFlushPartitioningInactiveWithoutUniqueIndex(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS partnone_t1, partnone_t2")
	for _, name := range []string{"partnone_t1", "partnone_t2"} {
		testutils.RunSQL(t, fmt.Sprintf(`CREATE TABLE %s (
			id bigint NOT NULL AUTO_INCREMENT,
			name varchar(255) NOT NULL,
			PRIMARY KEY (id),
			KEY idx_name (name)
		)`, name))
	}

	t1 := table.NewTableInfo(db, "test", "partnone_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "partnone_t2")
	require.NoError(t, t2.SetInfo(t.Context()))

	candidates, err := resolvePartitionIndexes(t.Context(), t1, t2)
	require.NoError(t, err)
	require.Empty(t, candidates, "a non-unique index is not a conflict surface between PK-disjoint rows")
	require.Nil(t, choosePartitionIndex(candidates, []drainRow{
		{key: "k", change: bufferedChange{logicalRow: applier.LogicalRow{RowImage: []any{int64(1), "a"}}}},
	}))
}

// TestFlushPartitioningSkipsIndexesOnAddedColumns covers the ALTER that adds a
// unique index on a column the source does not have. The row images come from
// the source's binlog, so there is no value to sort by — and a partially mapped
// index would be worse than none, since the drain would sort by a prefix while
// believing it had the whole key.
func TestFlushPartitioningSkipsIndexesOnAddedColumns(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS partadded_t1, partadded_t2")
	testutils.RunSQL(t, `CREATE TABLE partadded_t1 (
		id bigint NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id),
		UNIQUE KEY unq_name (name)
	)`)
	// The new table has unq_name plus a unique index on a column the source
	// lacks entirely.
	testutils.RunSQL(t, `CREATE TABLE partadded_t2 (
		id bigint NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		added varchar(255) NOT NULL DEFAULT '',
		PRIMARY KEY (id),
		UNIQUE KEY unq_name (name),
		UNIQUE KEY unq_added (added)
	)`)

	t1 := table.NewTableInfo(db, "test", "partadded_t1")
	require.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "partadded_t2")
	require.NoError(t, t2.SetInfo(t.Context()))

	candidates, err := resolvePartitionIndexes(t.Context(), t1, t2)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, "unq_name", candidates[0].name,
		"unq_added covers a column the source row image does not contain")
}

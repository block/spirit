package migration

import (
	"testing"

	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// TestProgressCopyReconcilesWithTablesOnAutoIncrementKey exercises Progress.Copy
// on the chunker Spirit selects for a single auto_increment key. That chunker
// paces itself on keyspace distance against the auto_increment max, which is
// what the copier's own progress reports, and a table whose ids are sparse
// makes that measure differ from the row count by orders of magnitude. Copy
// must follow the row-count path Tables uses, so a caller reading both in one
// snapshot sees one story, and the reading must survive leaving the copy phase.
func TestProgressCopyReconcilesWithTablesOnAutoIncrementKey(t *testing.T) {
	testutils.NewTestTable(t, "copyprog", `CREATE TABLE copyprog (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		pad INT NOT NULL DEFAULT 0)`)
	// 500 contiguous ids, then one row far out so the auto_increment max
	// dwarfs the row count.
	testutils.RunSQL(t, `INSERT INTO copyprog (id)
		WITH RECURSIVE seq (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 500)
		SELECT n FROM seq`)
	testutils.RunSQL(t, `INSERT INTO copyprog (id) VALUES (1000000)`)

	m := NewTestRunner(t, "copyprog", "ENGINE=InnoDB")
	defer utils.CloseAndLog(m)
	m.status.Begin()
	m.dbConfig = dbconn.NewDBConfig()
	var err error
	m.db, err = dbconn.New(testutils.DSN(), m.dbConfig)
	require.NoError(t, err)
	defer utils.CloseAndLog(m.db)
	m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.changes[0].stmt.Table)
	require.NoError(t, m.changes[0].table.SetInfo(t.Context()))
	require.NoError(t, m.setup(t.Context()))
	disableDynamicChunking(t, m.copyChunker)
	m.status.Set(status.CopyRows)

	// The first chunk is the open lower bound below the minimum id and copies
	// nothing; the second covers ids 1..1000 and so every contiguous row.
	ccopier, ok := m.copier.(copier.ChunkCopier)
	require.True(t, ok)
	for range 2 {
		chunk, nextErr := m.copyChunker.Next()
		require.NoError(t, nextErr)
		require.NoError(t, ccopier.CopyChunk(t.Context(), chunk))
	}

	p := m.Progress()
	require.Len(t, p.Tables, 1)
	require.EqualValues(t, 500, p.Copy.RowsCopied)
	require.Equal(t, p.Tables[0].RowsCopied, p.Copy.RowsCopied)
	require.Equal(t, p.Tables[0].RowsTotal, p.Copy.RowsTotal)
	require.Less(t, p.Copy.RowsTotal, uint64(1000000), "the total is the row estimate, not the auto_increment max")
	require.Equal(t, p.Copy.String()+" copyRows ETA TBD", p.Summary)

	// The copier's own measure is the one Copy must not be: two chunks of
	// keyspace against the auto_increment max.
	require.Equal(t, status.CopyProgress{RowsCopied: 2000, RowsTotal: 1000000}, m.copier.CopyProgress())

	m.status.Set(status.WaitingOnSentinelTable)
	require.Equal(t, p.Copy, m.Progress().Copy)
}

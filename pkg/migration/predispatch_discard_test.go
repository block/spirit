package migration

import (
	"errors"
	"fmt"
	"testing"

	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// TestE2EPreDispatchChangeThenAboveHighWatermark steps a migration through a
// change that lands before the copier's first dispatch, followed by a second
// change to the same key once the copier has started.
//
// The first change reaches the new table ahead of the copier (setup enables
// the watermark optimization before any chunk is dispatched, and a key the
// copier has not reached is flushed immediately). Before the fix the second
// change was then discarded as above the high watermark, and the copier's
// INSERT IGNORE skipped the key because it was already there. The new table
// kept the first change's image (or, after a DELETE, a row the source no
// longer has) and only the pre-cutover checksum repaired it. This test checks
// the new table before the checksum runs.
func TestE2EPreDispatchChangeThenAboveHighWatermark(t *testing.T) {
	for _, secondIsDelete := range []bool{false, true} {
		name := "update"
		if secondIsDelete {
			name = "delete"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tbl := "predisp_mig_upd"
			if secondIsDelete {
				tbl = "predisp_mig_del"
			}
			testutils.NewTestTable(t, tbl, fmt.Sprintf(`CREATE TABLE %s (
				id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
				b INT NOT NULL DEFAULT 0)`, tbl))
			testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (b) VALUES (1)", tbl))
			for range 14 {
				testutils.RunSQL(t, fmt.Sprintf("INSERT INTO %s (b) SELECT 1 FROM %s", tbl, tbl))
			}
			testutils.RunSQL(t, "ANALYZE TABLE "+tbl)
			const key = 16384 // the top of the key space

			m := NewTestRunner(t, tbl, "ENGINE=InnoDB")
			defer utils.CloseAndLog(m)
			m.dbConfig = dbconn.NewDBConfig()
			m.status.Begin()
			var err error
			m.db, err = dbconn.New(testutils.DSN(), m.dbConfig)
			require.NoError(t, err)
			defer utils.CloseAndLog(m.db)
			m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.changes[0].stmt.Table)
			require.NoError(t, m.changes[0].table.SetInfo(t.Context()))
			// setup enables the watermark optimization before any chunk is
			// dispatched, exactly as Run does.
			require.NoError(t, m.setup(t.Context()))
			m.status.Set(status.CopyRows)

			// (1) The first change, before the copier dispatches anything.
			// The flush writes it to the new table ahead of the copier.
			testutils.RunSQL(t, fmt.Sprintf("UPDATE %s SET b = 99 WHERE id = %d", tbl, key))
			require.NoError(t, m.replClient.BlockWait(t.Context()))
			require.NoError(t, m.replClient.Flush(t.Context()))
			newTbl := m.changes[0].newTable.QuotedTableName
			var b int
			require.NoError(t, m.db.QueryRowContext(t.Context(),
				fmt.Sprintf("SELECT b FROM %s WHERE id = %d", newTbl, key)).Scan(&b))
			require.Equal(t, 99, b)

			ccopier, ok := m.copier.(copier.ChunkCopier)
			require.True(t, ok)

			// (2) The copier copies its first chunk, far below key.
			chunk, err := m.copyChunker.Next()
			require.NoError(t, err)
			require.NoError(t, ccopier.CopyChunk(t.Context(), chunk))

			// (3) The second change to the same key, now above the high
			// watermark.
			if secondIsDelete {
				testutils.RunSQL(t, fmt.Sprintf("DELETE FROM %s WHERE id = %d", tbl, key))
			} else {
				testutils.RunSQL(t, fmt.Sprintf("UPDATE %s SET b = 100 WHERE id = %d", tbl, key))
			}
			require.NoError(t, m.replClient.BlockWait(t.Context()))

			// (4) The copier finishes; every change is flushed.
			for {
				chunk, err := m.copyChunker.Next()
				if errors.Is(err, table.ErrTableIsRead) {
					break
				}
				require.NoError(t, err)
				require.NoError(t, ccopier.CopyChunk(t.Context(), chunk))
			}
			require.NoError(t, m.replClient.Flush(t.Context()))

			// The new table must already match, before any checksum runs.
			var n int
			require.NoError(t, m.db.QueryRowContext(t.Context(),
				fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id = %d", newTbl, key)).Scan(&n))
			if secondIsDelete {
				require.Equal(t, 0, n, "the row deleted on the source is still in the new table")
			} else {
				require.Equal(t, 1, n)
				require.NoError(t, m.db.QueryRowContext(t.Context(),
					fmt.Sprintf("SELECT b FROM %s WHERE id = %d", newTbl, key)).Scan(&b))
				require.Equal(t, 100, b, "the new table kept the first change's image")
			}

			m.status.Set(status.ApplyChangeset)
			m.status.Set(status.Checksum)
			require.NoError(t, m.checksum(t.Context()))
		})
	}
}

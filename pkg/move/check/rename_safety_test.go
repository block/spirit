package check

import (
	"log/slog"
	"strings"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestRenameSafetyCheckRegistered(t *testing.T) {
	// The check must run before a fresh copy (post-setup) AND before a
	// resume from checkpoint, since the runner only runs one of the two.
	lock.Lock()
	defer lock.Unlock()
	require.Contains(t, checks, "rename_safety")
	require.Equal(t, ScopePostSetup, checks["rename_safety"].scope)
	require.Contains(t, checks, "rename_safety_resume")
	require.Equal(t, ScopeResume, checks["rename_safety_resume"].scope)
}

func TestCutoverOldName(t *testing.T) {
	require.Equal(t, "t1_old", CutoverOldName("t1"))
}

func TestRenameSafetyCheck(t *testing.T) {
	srcName, srcDB := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, srcName,
		"CREATE TABLE t1 (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100))")

	sourceTable := table.NewTableInfo(srcDB, srcName, "t1")
	require.NoError(t, sourceTable.SetInfo(t.Context()))

	resources := Resources{
		Sources: []SourceResource{{
			DB:     srcDB,
			Config: &mysql.Config{DBName: srcName},
		}},
		SourceTables: []*table.TableInfo{sourceTable},
	}

	t.Run("passes with no leftover _old table", func(t *testing.T) {
		require.NoError(t, renameSafetyCheck(t.Context(), resources, slog.Default()))
	})

	t.Run("fails when _old table already exists on source", func(t *testing.T) {
		testutils.RunSQLInDatabase(t, srcName, "CREATE TABLE t1_old (id INT NOT NULL PRIMARY KEY)")
		defer testutils.RunSQLInDatabase(t, srcName, "DROP TABLE t1_old")

		err := renameSafetyCheck(t.Context(), resources, slog.Default())
		require.Error(t, err)
		require.Contains(t, err.Error(), "'t1_old' already exists")
	})

	t.Run("fails when table name plus _old exceeds 64 characters", func(t *testing.T) {
		// 61 chars is a valid MySQL table name, but 61+4 ("_old") exceeds
		// the 64-character identifier limit. The length check is computed
		// from the name alone, so a synthetic TableInfo is sufficient.
		longName := strings.Repeat("a", utils.MaxTableNameLength-3)
		r := Resources{
			Sources:      resources.Sources,
			SourceTables: []*table.TableInfo{{TableName: longName}},
		}
		err := renameSafetyCheck(t.Context(), r, slog.Default())
		require.Error(t, err)
		require.Contains(t, err.Error(), "too long")
	})

	t.Run("passes when table name plus _old is exactly 64 characters", func(t *testing.T) {
		boundaryName := strings.Repeat("a", utils.MaxTableNameLength-4)
		r := Resources{
			Sources:      resources.Sources,
			SourceTables: []*table.TableInfo{{TableName: boundaryName}},
		}
		require.NoError(t, renameSafetyCheck(t.Context(), r, slog.Default()))
	})
}

package statement

import (
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestDiffRejectsPrimaryKeyOptionsMySQLIgnores(t *testing.T) {
	for _, tc := range []struct {
		name, source, target string
	}{
		{
			name:   "remove old compressed page size",
			source: `(id INT, PRIMARY KEY (id) KEY_BLOCK_SIZE=8) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4`,
			target: `(id INT, PRIMARY KEY (id)) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4`,
		},
		{
			name:   "add compressed page size",
			source: `(id INT, PRIMARY KEY (id)) ROW_FORMAT=COMPRESSED`,
			target: `(id INT, PRIMARY KEY (id) KEY_BLOCK_SIZE=4) ROW_FORMAT=COMPRESSED`,
		},
		{
			name:   "add secondary engine attribute",
			source: `(id INT, PRIMARY KEY (id))`,
			target: `(id INT, PRIMARY KEY (id) SECONDARY_ENGINE_ATTRIBUTE='{"x":1}')`,
		},
		{
			name:   "change attribute with unrelated column change",
			source: `(id INT, PRIMARY KEY (id) SECONDARY_ENGINE_ATTRIBUTE='{"x":1}')`,
			target: `(id INT, c INT, PRIMARY KEY (id) SECONDARY_ENGINE_ATTRIBUTE='{"x":2}')`,
		},
		{
			name:   "remove secondary engine attribute",
			source: `(id INT, PRIMARY KEY (id) SECONDARY_ENGINE_ATTRIBUTE='{"x":1}')`,
			target: `(id INT, PRIMARY KEY (id))`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, err := ParseCreateTable("CREATE TABLE t " + tc.source)
			require.NoError(t, err)
			target, err := ParseCreateTable("CREATE TABLE t " + tc.target)
			require.NoError(t, err)
			stmts, err := source.Diff(target, nil)
			require.ErrorContains(t, err, "changing PRIMARY KEY options without changing its columns is unsupported")
			require.Nil(t, stmts, "an unsupported change must not return a partial plan")
		})
	}
}

func TestDiffIntegrationAfterKeyBlockSizeChangeRejectsPrimaryKeyRebuild(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_pk_block_size",
		"CREATE TABLE diff_pk_block_size (id INT PRIMARY KEY, c INT, KEY k (c)) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8")
	target, err := ParseCreateTable("CREATE TABLE diff_pk_block_size (id INT PRIMARY KEY, c INT, KEY k (c)) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4")
	require.NoError(t, err)
	opts := NewDiffOptions()
	opts.IgnoreRowFormat = false
	source, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	stmts, err := source.Diff(target, opts)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "ALTER TABLE `diff_pk_block_size` KEY_BLOCK_SIZE=4", stmts[0].Statement)
	execStatements(t, tt.DB, stmts)

	live, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	stmts, err = live.Diff(target, opts)
	require.ErrorContains(t, err, "changing PRIMARY KEY options without changing its columns is unsupported")
	require.Nil(t, stmts, "the second diff must not emit the perpetual DROP PRIMARY KEY")
}

package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A TRUE/FALSE keyword default on an integer column is the same default MySQL
// stores as 1/0 and reports that way, so the two spellings must reach Diff
// already folded together. The declared side below is what an author writes;
// the live side is the SHOW CREATE TABLE form of the same table.
func TestBooleanKeywordDefaultConverges(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		live     string
	}{
		{
			name:     "boolean DEFAULT FALSE",
			declared: "`a` boolean NOT NULL DEFAULT FALSE",
			live:     "`a` tinyint(1) NOT NULL DEFAULT '0'",
		},
		{
			name:     "boolean DEFAULT TRUE",
			declared: "`a` boolean NOT NULL DEFAULT TRUE",
			live:     "`a` tinyint(1) NOT NULL DEFAULT '1'",
		},
		{
			name:     "lowercase keyword folds the same way",
			declared: "`a` boolean NOT NULL DEFAULT false",
			live:     "`a` tinyint(1) NOT NULL DEFAULT '0'",
		},
		{
			name:     "tinyint(1) written out rather than as boolean",
			declared: "`a` tinyint(1) NOT NULL DEFAULT FALSE",
			live:     "`a` tinyint(1) NOT NULL DEFAULT '0'",
		},
		{
			name:     "a nullable column with a keyword default",
			declared: "`a` boolean DEFAULT FALSE",
			live:     "`a` tinyint(1) DEFAULT '0'",
		},
		{
			name:     "every integer width, not only tinyint",
			declared: "`a` smallint NOT NULL DEFAULT TRUE, `b` mediumint NOT NULL DEFAULT FALSE, `c` int NOT NULL DEFAULT TRUE, `d` bigint NOT NULL DEFAULT FALSE",
			live:     "`a` smallint NOT NULL DEFAULT '1', `b` mediumint NOT NULL DEFAULT '0', `c` int NOT NULL DEFAULT '1', `d` bigint NOT NULL DEFAULT '0'",
		},
		{
			name:     "unsigned is folded too",
			declared: "`a` tinyint unsigned NOT NULL DEFAULT FALSE",
			live:     "`a` tinyint unsigned NOT NULL DEFAULT '0'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target, err := ParseCreateTable("CREATE TABLE `t` (" + tt.declared + ")")
			require.NoError(t, err)
			source, err := ParseCreateTable("CREATE TABLE `t` (" + tt.live + ")")
			require.NoError(t, err)

			stmts, err := source.Diff(target, nil)
			require.NoError(t, err)
			assert.Nil(t, stmts, "the declared and live forms are the same default")

			// Folding is symmetric: neither direction may produce a diff.
			stmts, err = target.Diff(source, nil)
			require.NoError(t, err)
			assert.Nil(t, stmts)
		})
	}
}

// The fold is only correct where MySQL stores the keyword as exactly 1/0.
// Everywhere else the keyword goes through a conversion of its own, so the
// default is left as written rather than folded to a value MySQL would not
// store — a wrong fold would report a converged schema that is still different.
func TestBooleanKeywordDefaultLeavesOtherTypesAlone(t *testing.T) {
	tests := []struct {
		name   string
		column string
		want   string
	}{
		{
			name:   "decimal applies its scale, so 1 is not what is stored",
			column: "`a` decimal(4,2) NOT NULL DEFAULT TRUE",
			want:   "TRUE",
		},
		{
			name:   "year reads the keyword as a year, storing 2001",
			column: "`a` year NOT NULL DEFAULT TRUE",
			want:   "TRUE",
		},
		{
			name:   "a string column stores the string form",
			column: "`a` varchar(8) NOT NULL DEFAULT FALSE",
			want:   "FALSE",
		},
		{
			name:   "a quoted 'FALSE' is a string value, not the keyword",
			column: "`a` varchar(8) NOT NULL DEFAULT 'FALSE'",
			want:   "FALSE",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ct, err := ParseCreateTable("CREATE TABLE `t` (" + tt.column + ")")
			require.NoError(t, err)
			require.Len(t, ct.Columns, 1)
			require.NotNil(t, ct.Columns[0].Default)
			assert.Equal(t, tt.want, *ct.Columns[0].Default)
		})
	}
}

// A default that is genuinely different must still diff. The fold changes how
// two spellings of one value compare, never whether two values do.
func TestBooleanKeywordDefaultStillDiffsRealChanges(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		live     string
	}{
		{
			name:     "FALSE against a stored 1",
			declared: "`a` boolean NOT NULL DEFAULT FALSE",
			live:     "`a` tinyint(1) NOT NULL DEFAULT '1'",
		},
		{
			name:     "TRUE against a stored 0",
			declared: "`a` boolean NOT NULL DEFAULT TRUE",
			live:     "`a` tinyint(1) NOT NULL DEFAULT '0'",
		},
		{
			name:     "a keyword default added to a column that had none",
			declared: "`a` boolean NOT NULL DEFAULT FALSE",
			live:     "`a` tinyint(1) NOT NULL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target, err := ParseCreateTable("CREATE TABLE `t` (" + tt.declared + ")")
			require.NoError(t, err)
			source, err := ParseCreateTable("CREATE TABLE `t` (" + tt.live + ")")
			require.NoError(t, err)

			stmts, err := source.Diff(target, nil)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			assert.Contains(t, stmts[0].Statement, "MODIFY COLUMN `a`")
		})
	}
}

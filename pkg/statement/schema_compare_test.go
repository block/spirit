package statement

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDiffCreateTables(t *testing.T) {
	tests := []struct {
		name     string
		table    string // defaults to "t1"
		want     string
		got      string
		opts     *DiffOptions
		wantDiff string
	}{
		{
			name: "identical",
			want: "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, b VARCHAR(255))",
			got:  "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, b VARCHAR(255))",
		},
		{
			// The two CREATEs come from different instances, so the table names
			// are normalized away and never compared — only the "table"
			// argument reaches the output.
			name: "different table names are not a difference",
			want: "CREATE TABLE src (id INT NOT NULL PRIMARY KEY)",
			got:  "CREATE TABLE dest (id INT NOT NULL PRIMARY KEY)",
		},
		{
			name:     "column missing from got",
			want:     "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, added VARCHAR(64))",
			got:      "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY)",
			wantDiff: "ALTER TABLE `t1` ADD COLUMN `added` varchar(64) NULL",
		},
		{
			name:     "column type differs",
			want:     "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, b VARCHAR(255))",
			got:      "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, b VARCHAR(64))",
			wantDiff: "ALTER TABLE `t1` MODIFY COLUMN `b` varchar(255) NULL",
		},
		{
			// nil opts means NewDiffOptions(), which ignores the AUTO_INCREMENT
			// counter.
			name: "nil opts ignores the auto_increment counter",
			want: "CREATE TABLE t1 (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY) AUTO_INCREMENT=500",
			got:  "CREATE TABLE t1 (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY) AUTO_INCREMENT=1",
		},
		{
			name: "opts can relax the column auto_increment attribute",
			want: "CREATE TABLE t1 (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY)",
			got:  "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY)",
			opts: func() *DiffOptions {
				o := NewDiffOptions()
				o.IgnoreColumnAutoIncrement = true
				return o
			}(),
		},
		{
			// The name used to build the runnable prefix is escaped, so a table
			// name containing a backtick stays valid SQL.
			name:     "table name is escaped in the prefix",
			table:    "we`ird",
			want:     "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, added INT)",
			got:      "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY)",
			wantDiff: "ALTER TABLE `we``ird` ADD COLUMN `added` int NULL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name := tt.table
			if name == "" {
				name = "t1"
			}
			diff, err := DiffCreateTables(name, tt.want, tt.got, tt.opts)
			require.NoError(t, err)
			require.Equal(t, tt.wantDiff, diff)
		})
	}
}

// TestDiffCreateTablesKeepsStatementBoundaries covers a reconciliation that
// Diff deliberately splits across more than one ALTER. Here it is an
// option-only index change: MySQL no-ops a DROP and ADD of the same index in a
// single statement, so merging the two into one comma-joined ALTER would emit
// SQL that silently fails to reconcile the schemas.
func TestDiffCreateTablesKeepsStatementBoundaries(t *testing.T) {
	want := "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, b VARCHAR(255), KEY idx_b (b) KEY_BLOCK_SIZE=8)"
	got := "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, b VARCHAR(255), KEY idx_b (b) KEY_BLOCK_SIZE=4)"

	diff, err := DiffCreateTables("t1", want, got, nil)
	require.NoError(t, err)
	require.Equal(t,
		"ALTER TABLE `t1` DROP INDEX `idx_b`; ALTER TABLE `t1` ADD INDEX `idx_b` (`b`) KEY_BLOCK_SIZE=8",
		diff,
		"statements Diff kept separate must not be merged into one ALTER")
}

func TestDiffCreateTablesParseErrors(t *testing.T) {
	_, err := DiffCreateTables("t1", "NOT A CREATE TABLE", "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY)", nil)
	require.ErrorContains(t, err, "failed to parse reference CREATE TABLE")

	_, err = DiffCreateTables("t1", "CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY)", "NOT A CREATE TABLE", nil)
	require.ErrorContains(t, err, "failed to parse CREATE TABLE under validation")
}

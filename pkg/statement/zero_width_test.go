package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseZeroWidthColumn verifies that a zero width is recorded as a Length
// of 0 rather than dropped. MySQL stores varchar(0), char(0), binary(0) and
// varbinary(0) with their zero width; a nil Length would instead be emitted as
// a width-less type — invalid SQL for varchar and varbinary, and a width of 1
// for char and binary.
func TestParseZeroWidthColumn(t *testing.T) {
	tests := []struct {
		colDef  string
		wantLen *int // nil = no width recorded
	}{
		{"varchar(0)", new(0)},
		{"char(0)", new(0)},
		{"binary(0)", new(0)},
		{"varbinary(0)", new(0)},
		{"char", new(1)},   // parser supplies MySQL's default width of 1
		{"binary", new(1)}, // likewise
		{"year", nil},      // the parser's unspecified width (-1) is not a width
		{"float", nil},
		{"datetime(0)", nil}, // fractional-seconds precision, not a width
	}
	for _, tc := range tests {
		t.Run(tc.colDef, func(t *testing.T) {
			ct, err := ParseCreateTable("CREATE TABLE t (b " + tc.colDef + ")")
			require.NoError(t, err)
			require.Len(t, ct.Columns, 1)
			assert.Equal(t, tc.wantLen, ct.Columns[0].Length)
		})
	}
}

// TestDiffZeroWidthColumn verifies that Diff emits a zero-width type with its
// width, and that a change between a zero width and a non-zero width is
// reported in both directions.
func TestDiffZeroWidthColumn(t *testing.T) {
	tests := []struct {
		name     string
		live     string
		declared string
		want     string // "" = no diff
	}{
		{
			name:     "varchar(0) default change",
			live:     "b varchar(0)",
			declared: "b varchar(0) DEFAULT ''",
			want:     "ALTER TABLE `t` MODIFY COLUMN `b` varchar(0) NULL DEFAULT ''",
		},
		{
			name:     "varbinary(0) default change",
			live:     "b varbinary(0)",
			declared: "b varbinary(0) DEFAULT ''",
			want:     "ALTER TABLE `t` MODIFY COLUMN `b` varbinary(0) NULL DEFAULT ''",
		},
		{
			name:     "char(0) default change",
			live:     "b char(0)",
			declared: "b char(0) DEFAULT ''",
			want:     "ALTER TABLE `t` MODIFY COLUMN `b` char(0) NULL DEFAULT ''",
		},
		{
			name:     "binary(0) default change",
			live:     "b binary(0)",
			declared: "b binary(0) DEFAULT ''",
			want:     "ALTER TABLE `t` MODIFY COLUMN `b` binary(0) NULL DEFAULT ''",
		},
		{"varchar(0) to varchar(1)", "b varchar(0)", "b varchar(1)", "ALTER TABLE `t` MODIFY COLUMN `b` varchar(1) NULL"},
		{"varchar(1) to varchar(0)", "b varchar(1)", "b varchar(0)", "ALTER TABLE `t` MODIFY COLUMN `b` varchar(0) NULL"},
		{"char(0) to char", "b char(0)", "b char", "ALTER TABLE `t` MODIFY COLUMN `b` char(1) NULL"},
		{"char to char(0)", "b char", "b char(0)", "ALTER TABLE `t` MODIFY COLUMN `b` char(0) NULL"},
		{"binary(1) to binary(0)", "b binary(1)", "b binary(0)", "ALTER TABLE `t` MODIFY COLUMN `b` binary(0) NULL"},
		{"varbinary(1) to varbinary(0)", "b varbinary(1)", "b varbinary(0)", "ALTER TABLE `t` MODIFY COLUMN `b` varbinary(0) NULL"},
		{"varchar(0) unchanged", "b varchar(0)", "b varchar(0)", ""},
		{"char(0) unchanged", "b char(0)", "b char(0)", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			live, err := ParseCreateTable("CREATE TABLE t (" + tc.live + ")")
			require.NoError(t, err)
			declared, err := ParseCreateTable("CREATE TABLE t (" + tc.declared + ")")
			require.NoError(t, err)
			stmts, err := live.Diff(declared, nil)
			require.NoError(t, err)
			if tc.want == "" {
				assert.Nil(t, stmts)
				return
			}
			require.Len(t, stmts, 1)
			assert.Equal(t, tc.want, stmts[0].Statement)
		})
	}
}

package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The literal form of a default is read off the AST, so forms the restored text
// cannot tell apart must still arrive distinguished. TRUE, 1 and '1' all look
// numeric once restored, and b'1' looks like a quoted string; the kind is what
// separates them, and every later decision — whether to quote on emission,
// whether a keyword can be folded — reads it rather than the characters.
//
// The kind is recorded before the normalization rules run, and the keyword fold
// rewrites it, so the cases below are written on types the fold leaves alone:
// json for the string forms and datetime for the keyword, where no rule
// rewrites what the parser recorded.
func TestClassifyDefaultLiteral(t *testing.T) {
	tests := []struct {
		name   string
		column string
		want   DefaultKind
	}{
		{
			name:   "the bare TRUE keyword",
			column: "`a` datetime DEFAULT TRUE",
			want:   DefaultKindKeywordBool,
		},
		{
			name:   "the bare FALSE keyword",
			column: "`a` datetime DEFAULT FALSE",
			want:   DefaultKindKeywordBool,
		},
		{
			name:   "an integer, which is not the keyword even though TRUE aliases it",
			column: "`a` datetime DEFAULT 1",
			want:   DefaultKindNumber,
		},
		{
			name:   "a negative integer",
			column: "`a` datetime DEFAULT -1",
			want:   DefaultKindNumber,
		},
		{
			name:   "a decimal",
			column: "`a` datetime DEFAULT 1.5",
			want:   DefaultKindNumber,
		},
		{
			name:   "a quoted string",
			column: "`a` json DEFAULT ('{}')",
			want:   DefaultKindString,
		},
		{
			name:   "a quoted string that spells the keyword",
			column: "`a` json DEFAULT ('TRUE')",
			want:   DefaultKindString,
		},
		{
			name:   "a quoted string that spells a number",
			column: "`a` json DEFAULT ('1')",
			want:   DefaultKindString,
		},
		{
			name:   "a bit literal",
			column: "`a` datetime DEFAULT b'101'",
			want:   DefaultKindBitLiteral,
		},
		{
			name:   "NULL, which is not a literal form this models",
			column: "`a` datetime DEFAULT NULL",
			want:   DefaultKindUnknown,
		},
		{
			name:   "a function default",
			column: "`a` datetime DEFAULT CURRENT_TIMESTAMP",
			want:   DefaultKindUnknown,
		},
		{
			name:   "a hex literal, which MySQL converts rather than reports back",
			column: "`a` datetime DEFAULT 0x1A",
			want:   DefaultKindUnknown,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ct, err := ParseCreateTable("CREATE TABLE `t` (" + tt.column + ")")
			require.NoError(t, err)
			require.Len(t, ct.Columns, 1)
			assert.Equal(t, tt.want, ct.Columns[0].DefaultKind)
		})
	}
}

// A default whose form is not modelled keeps the emission it had before kinds
// existed: the text heuristic decides, so a function default stays bare and an
// unrecognized value stays quoted.
func TestUnknownDefaultKindKeepsHeuristicEmission(t *testing.T) {
	tests := []struct {
		name   string
		column string
		want   string
	}{
		{
			name:   "a function default is emitted bare",
			column: "`a` datetime DEFAULT CURRENT_TIMESTAMP",
			want:   "DEFAULT current_timestamp",
		},
		{
			name:   "NULL is emitted bare",
			column: "`a` datetime DEFAULT NULL",
			want:   "DEFAULT NULL",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ct, err := ParseCreateTable("CREATE TABLE `t` (" + tt.column + ")")
			require.NoError(t, err)
			require.Len(t, ct.Columns, 1)
			require.Equal(t, DefaultKindUnknown, ct.Columns[0].DefaultKind)
			assert.Contains(t, formatColumnDefinition(&ct.Columns[0]), tt.want)
		})
	}
}

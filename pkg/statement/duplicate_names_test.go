package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A table that repeats a column, index, or constraint name cannot be created
// by MySQL, so Diff rejects it on either side instead of planning against an
// element with two definitions.
func TestDiffRejectsRepeatedNames(t *testing.T) {
	const valid = "CREATE TABLE t (id int NOT NULL, x int, PRIMARY KEY (id))"
	tests := []struct {
		name     string
		repeated string
		wantErr  string
	}{
		{
			name:     "column",
			repeated: "CREATE TABLE t (id int NOT NULL, x int, x int, PRIMARY KEY (id))",
			wantErr:  `column "x" is declared more than once`,
		},
		{
			name:     "column in another case",
			repeated: "CREATE TABLE t (id int NOT NULL, x int, X int, PRIMARY KEY (id))",
			wantErr:  `column "X" is declared more than once`,
		},
		{
			name:     "index",
			repeated: "CREATE TABLE t (id int NOT NULL, x int, PRIMARY KEY (id), KEY k (x), KEY K (id))",
			wantErr:  `index "K" is declared more than once`,
		},
		{
			name:     "constraint",
			repeated: "CREATE TABLE t (id int NOT NULL, x int, PRIMARY KEY (id), CONSTRAINT c CHECK (x > 0), CONSTRAINT c CHECK (x < 9))",
			wantErr:  `constraint "c" is declared more than once`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validTable, err := ParseCreateTable(valid)
			require.NoError(t, err)
			repeatedTable, err := ParseCreateTable(tt.repeated)
			require.NoError(t, err, "the parser does not check name uniqueness; Diff does")

			stmts, err := validTable.Diff(repeatedTable, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), `invalid target table "t": `+tt.wantErr)
			assert.Nil(t, stmts)

			stmts, err = repeatedTable.Diff(validTable, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), `invalid source table "t": `+tt.wantErr)
			assert.Nil(t, stmts)
		})
	}
}

// Indexes and constraints MySQL names itself are not repeats: the index
// normalizer names unnamed indexes uniquely, and unnamed CHECK constraints
// stay unnamed until MySQL numbers them.
func TestDiffAcceptsUnnamedIndexesAndConstraints(t *testing.T) {
	source, err := ParseCreateTable("CREATE TABLE t (id int NOT NULL, x int, PRIMARY KEY (id))")
	require.NoError(t, err)
	target, err := ParseCreateTable("CREATE TABLE t (id int NOT NULL, x int UNIQUE, PRIMARY KEY (id), KEY (x), KEY (x, id), CHECK (x > 0), CHECK (x < 9))")
	require.NoError(t, err)

	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Contains(t, stmts[0].Statement, "ADD UNIQUE INDEX `x` (`x`)")
	assert.Contains(t, stmts[0].Statement, "ADD INDEX `x_2` (`x`)")
	assert.Contains(t, stmts[0].Statement, "ADD INDEX `x_3` (`x`, `id`)")
}

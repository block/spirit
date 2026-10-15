package statement

import (
	"fmt"
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
			wantErr:  `CHECK constraint "c" is declared more than once`,
		},
		{
			name:     "foreign key in another case",
			repeated: "CREATE TABLE t (id int NOT NULL, x int, PRIMARY KEY (id), CONSTRAINT fk FOREIGN KEY (x) REFERENCES parent (id), CONSTRAINT FK FOREIGN KEY (id) REFERENCES parent (id))",
			wantErr:  `FOREIGN KEY constraint "FK" is declared more than once`,
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

// A CHECK and a foreign key may share a name, since each kind of constraint
// is its own namespace in MySQL. Diff accepts the table and pairs each
// constraint with the one of its own kind and name: flipping the CHECK to NOT
// ENFORCED alters it in place and leaves the foreign key alone.
func TestDiffPairsConstraintsByKind(t *testing.T) {
	const table = "CREATE TABLE t (id int NOT NULL, p int, PRIMARY KEY (id), KEY c (p), CONSTRAINT c FOREIGN KEY (p) REFERENCES parent (id), CONSTRAINT c CHECK (p > 0)%s)"
	source, err := ParseCreateTable(fmt.Sprintf(table, ""))
	require.NoError(t, err)
	target, err := ParseCreateTable(fmt.Sprintf(table, " NOT ENFORCED"))
	require.NoError(t, err)

	stmts, err := source.Diff(source, nil)
	require.NoError(t, err)
	assert.Empty(t, stmts)

	stmts, err = source.Diff(target, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE `t` ALTER CHECK `c` NOT ENFORCED", stmts[0].Statement)
}

// MySQL compares index names case-insensitively when it names an unnamed
// index or an inline UNIQUE, so a name taken in another case is skipped:
// `KEY X (x), KEY (x)` stores the unnamed key as x_2, and so does an inline
// UNIQUE on x declared after `KEY X`. Diff accepts both on either side.
func TestDiffNamesIndexesAroundNamesInAnotherCase(t *testing.T) {
	tests := []struct {
		name  string
		plain string
		table string
		add   string
		drop  string
	}{
		{
			name:  "unnamed key",
			plain: "CREATE TABLE t (id int NOT NULL, x int, PRIMARY KEY (id))",
			table: "CREATE TABLE t (id int NOT NULL, x int, PRIMARY KEY (id), KEY X (x), KEY (x))",
			add:   "ALTER TABLE `t` ADD INDEX `X` (`x`), ADD INDEX `x_2` (`x`)",
			drop:  "ALTER TABLE `t` DROP INDEX `X`, DROP INDEX `x_2`",
		},
		{
			name:  "inline unique",
			plain: "CREATE TABLE t (id int NOT NULL, y int, x int, PRIMARY KEY (id))",
			table: "CREATE TABLE t (id int NOT NULL, y int, PRIMARY KEY (id), KEY X (y), x int UNIQUE)",
			add:   "ALTER TABLE `t` ADD INDEX `X` (`y`), ADD UNIQUE INDEX `x_2` (`x`)",
			drop:  "ALTER TABLE `t` DROP INDEX `X`, DROP INDEX `x_2`",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withIndexes, err := ParseCreateTable(tt.table)
			require.NoError(t, err)
			plainTable, err := ParseCreateTable(tt.plain)
			require.NoError(t, err)

			stmts, err := plainTable.Diff(withIndexes, nil)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			assert.Equal(t, tt.add, stmts[0].Statement)

			stmts, err = withIndexes.Diff(plainTable, nil)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			assert.Equal(t, tt.drop, stmts[0].Statement)
		})
	}
}

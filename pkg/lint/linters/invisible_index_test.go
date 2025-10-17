package linters

import (
	"testing"

	"github.com/block/spirit/pkg/lint"
	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvisibleIndexBeforeDropLinter_DropWithoutInvisible(t *testing.T) {
	sql := "ALTER TABLE users DROP INDEX idx_email"
	stmts, err := statement.New(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	linter := &InvisibleIndexBeforeDropLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	assert.Equal(t, "invisible_index_before_drop", violations[0].Linter.Name())
	assert.Equal(t, lint.SeverityWarning, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "should be made invisible before dropping")
	assert.Equal(t, "users", violations[0].Location.Table)
	assert.NotNil(t, violations[0].Location.Index)
	assert.Equal(t, "idx_email", *violations[0].Location.Index)
	assert.NotNil(t, violations[0].Suggestion)
	assert.Contains(t, *violations[0].Suggestion, "ALTER INDEX idx_email INVISIBLE")
}

func TestInvisibleIndexBeforeDropLinter_DropAfterInvisibleInSameAlter(t *testing.T) {
	sql := "ALTER TABLE users ALTER INDEX idx_email INVISIBLE, DROP INDEX idx_email"
	stmts, err := statement.New(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	linter := &InvisibleIndexBeforeDropLinter{}
	violations := linter.Lint(nil, stmts)

	// Making an index invisible in the same ALTER statement where you drop it is obviously not good enough
	assert.Len(t, violations, 1)
	assert.IsType(t, &InvisibleIndexBeforeDropLinter{}, violations[0].Linter)
	assert.Equal(t, "invisible_index_before_drop", violations[0].Linter.Name())
}

func TestInvisibleIndexBeforeDropLinter_DropAlreadyInvisibleIndex(t *testing.T) {
	// Create a table with an invisible index
	createSQL := `CREATE TABLE users (
		id INT PRIMARY KEY,
		email VARCHAR(255),
		INDEX idx_email (email) INVISIBLE
	)`
	ct, err := statement.ParseCreateTable(createSQL)
	require.NoError(t, err)

	// Drop the invisible index
	alterSQL := "ALTER TABLE users DROP INDEX idx_email"
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	linter := &InvisibleIndexBeforeDropLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, stmts)

	// Should not have violations since index is already invisible
	assert.Empty(t, violations)
}

func TestInvisibleIndexBeforeDropLinter_DropVisibleIndex(t *testing.T) {
	// Create a table with a visible index
	createSQL := `CREATE TABLE users (
		id INT PRIMARY KEY,
		email VARCHAR(255),
		INDEX idx_email (email)
	)`
	ct, err := statement.ParseCreateTable(createSQL)
	require.NoError(t, err)

	// Drop the visible index
	alterSQL := "ALTER TABLE users DROP INDEX idx_email"
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	linter := &InvisibleIndexBeforeDropLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, stmts)

	// Should have violation since index is visible
	require.Len(t, violations, 1)
	assert.Equal(t, "invisible_index_before_drop", violations[0].Linter.Name())
}

func TestInvisibleIndexBeforeDropLinter_MultipleDrops(t *testing.T) {
	sql := "ALTER TABLE users DROP INDEX idx_email, DROP INDEX idx_name"
	stmts, err := statement.New(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	linter := &InvisibleIndexBeforeDropLinter{}
	violations := linter.Lint(nil, stmts)

	// Should have violations for both indexes
	require.Len(t, violations, 2)

	indexNames := make(map[string]bool)

	for _, v := range violations {
		assert.Equal(t, "invisible_index_before_drop", v.Linter.Name())
		assert.Equal(t, lint.SeverityWarning, v.Severity)

		if v.Location.Index != nil {
			indexNames[*v.Location.Index] = true
		}
	}

	assert.True(t, indexNames["idx_email"])
	assert.True(t, indexNames["idx_name"])
}

func TestInvisibleIndexBeforeDropLinter_NonAlterStatement(t *testing.T) {
	sql := "CREATE TABLE users (id INT PRIMARY KEY)"
	stmts, err := statement.New(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	linter := &InvisibleIndexBeforeDropLinter{}
	violations := linter.Lint(nil, stmts)

	// Should not have violations for non-ALTER statements
	assert.Empty(t, violations)
}

func TestInvisibleIndexBeforeDropLinter_AlterWithoutDrop(t *testing.T) {
	sql := "ALTER TABLE users ADD COLUMN age INT"
	stmts, err := statement.New(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	linter := &InvisibleIndexBeforeDropLinter{}
	violations := linter.Lint(nil, stmts)

	// Should not have violations for ALTER without DROP INDEX
	assert.Empty(t, violations)
}

func TestInvisibleIndexBeforeDropLinter_Integration(t *testing.T) {
	// Reset registry and register linter
	lint.Reset()
	lint.Register(&InvisibleIndexBeforeDropLinter{})

	sql := "ALTER TABLE users DROP INDEX idx_email"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := lint.RunLinters(nil, stmts, lint.Config{})

	require.Len(t, violations, 1)
	assert.Equal(t, "invisible_index_before_drop", violations[0].Linter.Name())
}

func TestInvisibleIndexBeforeDropLinter_IntegrationDisabled(t *testing.T) {
	// Reset registry and register linter
	lint.Reset()
	lint.Register(&InvisibleIndexBeforeDropLinter{})

	sql := "ALTER TABLE users DROP INDEX idx_email"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	// Disable the linter
	violations := lint.RunLinters(nil, stmts, lint.Config{
		Enabled: map[string]bool{
			"invisible_index_before_drop": false,
		},
	})

	// Should not have violations when disabled
	assert.Empty(t, violations)
}

func TestInvisibleIndexBeforeDropLinter_Metadata(t *testing.T) {
	linter := &InvisibleIndexBeforeDropLinter{}

	assert.Equal(t, "invisible_index_before_drop", linter.Name())
	assert.Equal(t, "schema", linter.Category())
	assert.NotEmpty(t, linter.Description())
}

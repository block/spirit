package linters

import (
	"testing"

	"github.com/block/spirit/pkg/lint"
	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultipleAlterTableLinter_SingleAlter(t *testing.T) {
	sql := "ALTER TABLE users ADD COLUMN age INT"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &MultipleAlterTableLinter{}
	violations := linter.Lint(nil, stmts)

	// No violation for single ALTER
	assert.Empty(t, violations)
}

func TestMultipleAlterTableLinter_TwoAltersOnSameTable(t *testing.T) {
	sql := `ALTER TABLE users ADD COLUMN age INT;
			ALTER TABLE users ADD INDEX idx_age (age)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 2)

	linter := &MultipleAlterTableLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	assert.Equal(t, "multiple_alter_table", violations[0].Linter.Name())
	assert.Equal(t, lint.SeverityInfo, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "2 separate ALTER TABLE statements")
	assert.Equal(t, "users", violations[0].Location.Table)
	assert.NotNil(t, violations[0].Suggestion)
	assert.Contains(t, *violations[0].Suggestion, "Combine into")
	assert.Contains(t, *violations[0].Suggestion, "ADD COLUMN `age` INT")
	assert.Contains(t, *violations[0].Suggestion, "ADD INDEX `idx_age`(`age`)")
}

func TestMultipleAlterTableLinter_ThreeAltersOnSameTable(t *testing.T) {
	sql := `ALTER TABLE users ADD COLUMN age INT;
			ALTER TABLE users ADD COLUMN email VARCHAR(255);
			ALTER TABLE users ADD INDEX idx_email (email)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 3)

	linter := &MultipleAlterTableLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	assert.Equal(t, "multiple_alter_table", violations[0].Linter.Name())
	assert.Contains(t, violations[0].Message, "3 separate ALTER TABLE statements")

	// Check context
	assert.NotNil(t, violations[0].Context)
	assert.Equal(t, 3, violations[0].Context["alter_count"])
	indices, ok := violations[0].Context["statement_indices"].([]int)
	require.True(t, ok)
	assert.Len(t, indices, 3)
}

func TestMultipleAlterTableLinter_AltersOnDifferentTables(t *testing.T) {
	sql := `ALTER TABLE users ADD COLUMN age INT;
			ALTER TABLE orders ADD COLUMN total DECIMAL(10,2)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 2)

	linter := &MultipleAlterTableLinter{}
	violations := linter.Lint(nil, stmts)

	// No violation when altering different tables
	assert.Empty(t, violations)
}

func TestMultipleAlterTableLinter_MixedStatements(t *testing.T) {
	// Parse statements individually since statement.New() doesn't support mixed types
	var stmts []*statement.AbstractStatement

	sql1 := "CREATE TABLE products (id INT PRIMARY KEY)"
	s1, err := statement.New(sql1)
	require.NoError(t, err)

	stmts = append(stmts, s1...)

	sql2 := "ALTER TABLE users ADD COLUMN age INT"
	s2, err := statement.New(sql2)
	require.NoError(t, err)

	stmts = append(stmts, s2...)

	sql3 := "ALTER TABLE users ADD INDEX idx_age (age)"
	s3, err := statement.New(sql3)
	require.NoError(t, err)

	stmts = append(stmts, s3...)

	sql4 := "DROP TABLE old_table"
	s4, err := statement.New(sql4)
	require.NoError(t, err)

	stmts = append(stmts, s4...)

	require.Len(t, stmts, 4)

	linter := &MultipleAlterTableLinter{}
	violations := linter.Lint(nil, stmts)

	// Should only detect the two ALTER TABLE users statements
	require.Len(t, violations, 1)
	assert.Equal(t, "users", violations[0].Location.Table)
	assert.Contains(t, violations[0].Message, "2 separate ALTER TABLE statements")
}

func TestMultipleAlterTableLinter_MultipleTables(t *testing.T) {
	sql := `ALTER TABLE users ADD COLUMN age INT;
			ALTER TABLE users ADD INDEX idx_age (age);
			ALTER TABLE orders ADD COLUMN status VARCHAR(50);
			ALTER TABLE orders ADD INDEX idx_status (status)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 4)

	linter := &MultipleAlterTableLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect violations for both tables
	require.Len(t, violations, 2)

	tableNames := make(map[string]bool)
	for _, v := range violations {
		tableNames[v.Location.Table] = true
		assert.Equal(t, "multiple_alter_table", v.Linter.Name())
		assert.Contains(t, v.Message, "2 separate ALTER TABLE statements")
	}

	assert.True(t, tableNames["users"])
	assert.True(t, tableNames["orders"])
}

func TestMultipleAlterTableLinter_ComplexAlters(t *testing.T) {
	sql := `ALTER TABLE users ADD COLUMN age INT, ADD COLUMN email VARCHAR(255);
			ALTER TABLE users DROP COLUMN old_field`
	stmts, err := statement.New(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 2)

	linter := &MultipleAlterTableLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	assert.Equal(t, "users", violations[0].Location.Table)

	// Suggestion should include both ALTER operations (with backticks as generated by parser)
	assert.NotNil(t, violations[0].Suggestion)
	suggestion := *violations[0].Suggestion
	assert.Contains(t, suggestion, "ADD COLUMN `age` INT, ADD COLUMN `email` VARCHAR(255)")
	assert.Contains(t, suggestion, "DROP COLUMN `old_field`")
}

func TestMultipleAlterTableLinter_NonAlterStatements(t *testing.T) {
	// Parse statements individually since statement.New() doesn't support mixed types
	var stmts []*statement.AbstractStatement

	sql1 := "CREATE TABLE users (id INT PRIMARY KEY)"
	s1, err := statement.New(sql1)
	require.NoError(t, err)

	stmts = append(stmts, s1...)

	sql2 := "CREATE TABLE orders (id INT PRIMARY KEY)"
	s2, err := statement.New(sql2)
	require.NoError(t, err)

	stmts = append(stmts, s2...)

	linter := &MultipleAlterTableLinter{}
	violations := linter.Lint(nil, stmts)

	// No violations for non-ALTER statements
	assert.Empty(t, violations)
}

func TestMultipleAlterTableLinter_EmptyStatements(t *testing.T) {
	linter := &MultipleAlterTableLinter{}
	violations := linter.Lint(nil, nil)

	assert.Empty(t, violations)
}

func TestMultipleAlterTableLinter_Integration(t *testing.T) {
	lint.Reset()
	lint.Register(&MultipleAlterTableLinter{})

	sql := `ALTER TABLE users ADD COLUMN age INT;
			ALTER TABLE users ADD INDEX idx_age (age)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := lint.RunLinters(nil, stmts, lint.Config{})

	require.Len(t, violations, 1)
	assert.Equal(t, "multiple_alter_table", violations[0].Linter.Name())
}

func TestMultipleAlterTableLinter_IntegrationDisabled(t *testing.T) {
	lint.Reset()
	lint.Register(&MultipleAlterTableLinter{})

	sql := `ALTER TABLE users ADD COLUMN age INT;
			ALTER TABLE users ADD INDEX idx_age (age)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations := lint.RunLinters(nil, stmts, lint.Config{
		Enabled: map[string]bool{
			"multiple_alter_table": false,
		},
	})

	assert.Empty(t, violations)
}

func TestMultipleAlterTableLinter_Metadata(t *testing.T) {
	linter := &MultipleAlterTableLinter{}

	assert.Equal(t, "multiple_alter_table", linter.Name())
	assert.Equal(t, "schema", linter.Category())
	assert.NotEmpty(t, linter.Description())
}

func TestMultipleAlterTableLinter_ContextData(t *testing.T) {
	sql := `ALTER TABLE users ADD COLUMN age INT;
			ALTER TABLE users ADD COLUMN email VARCHAR(255);
			ALTER TABLE users ADD INDEX idx_email (email)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &MultipleAlterTableLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)

	// Verify context contains useful debugging info
	assert.NotNil(t, violations[0].Context)
	assert.Equal(t, 3, violations[0].Context["alter_count"])

	indices, ok := violations[0].Context["statement_indices"].([]int)
	require.True(t, ok)
	assert.Equal(t, []int{0, 1, 2}, indices)
}

func TestMultipleAlterTableLinter_SeverityIsInfo(t *testing.T) {
	sql := `ALTER TABLE users ADD COLUMN age INT;
			ALTER TABLE users ADD INDEX idx_age (age)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &MultipleAlterTableLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	// This is INFO level because it's an optimization suggestion, not an error
	assert.Equal(t, lint.SeverityInfo, violations[0].Severity)
}

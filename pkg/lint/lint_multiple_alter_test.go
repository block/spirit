package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/require"
)

func TestMultipleAlterTableLinter_SingleAlter(t *testing.T) {
	sql := "ALTER TABLE users ADD COLUMN age INT"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &MultipleAlterTableLinter{}
	violations := linter.Lint(nil, stmts)

	// No violation for single ALTER
	require.Empty(t, violations)
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
	require.Equal(t, "multiple_alter_table", violations[0].Linter.Name())
	require.Equal(t, SeverityInfo, violations[0].Severity)
	require.Equal(t, `Table "users" has 2 separate ALTER TABLE statements that could be combined into one for better performance`, violations[0].Message)
	require.Equal(t, "users", violations[0].Location.Table)
	require.NotNil(t, violations[0].Suggestion)
	require.Contains(t, *violations[0].Suggestion, "Combine into")
	require.Contains(t, *violations[0].Suggestion, "ADD COLUMN `age` INT")
	require.Contains(t, *violations[0].Suggestion, "ADD INDEX `idx_age`(`age`)")
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
	require.Equal(t, "multiple_alter_table", violations[0].Linter.Name())
	require.Contains(t, violations[0].Message, "3 separate ALTER TABLE statements")

	// Check context
	require.NotNil(t, violations[0].Context)
	require.Equal(t, 3, violations[0].Context["alter_count"])
	indices, ok := violations[0].Context["statement_indices"].([]int)
	require.True(t, ok)
	require.Len(t, indices, 3)
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
	require.Empty(t, violations)
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
	require.Equal(t, "users", violations[0].Location.Table)
	require.Contains(t, violations[0].Message, "2 separate ALTER TABLE statements")
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
		require.Equal(t, "multiple_alter_table", v.Linter.Name())
		require.Contains(t, v.Message, "2 separate ALTER TABLE statements")
	}

	require.True(t, tableNames["users"])
	require.True(t, tableNames["orders"])
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
	require.Equal(t, "users", violations[0].Location.Table)

	// Suggestion should include both ALTER operations (with backticks as generated by parser)
	require.NotNil(t, violations[0].Suggestion)
	suggestion := *violations[0].Suggestion
	require.Contains(t, suggestion, "ADD COLUMN `age` INT, ADD COLUMN `email` VARCHAR(255)")
	require.Contains(t, suggestion, "DROP COLUMN `old_field`")
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
	require.Empty(t, violations)
}

func TestMultipleAlterTableLinter_EmptyStatements(t *testing.T) {
	linter := &MultipleAlterTableLinter{}
	violations := linter.Lint(nil, nil)

	require.Empty(t, violations)
}

func TestMultipleAlterTableLinter_Integration(t *testing.T) {
	resetForTest(t)
	Register(&MultipleAlterTableLinter{})

	sql := `ALTER TABLE users ADD COLUMN age INT;
			ALTER TABLE users ADD INDEX idx_age (age)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations, err := RunLinters(nil, stmts, Config{})
	require.NoError(t, err)

	require.Len(t, violations, 1)
	require.Equal(t, "multiple_alter_table", violations[0].Linter.Name())
}

func TestMultipleAlterTableLinter_IntegrationDisabled(t *testing.T) {
	resetForTest(t)
	Register(&MultipleAlterTableLinter{})

	sql := `ALTER TABLE users ADD COLUMN age INT;
			ALTER TABLE users ADD INDEX idx_age (age)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations, err := RunLinters(nil, stmts, Config{
		Enabled: map[string]bool{
			"multiple_alter_table": false,
		},
	})
	require.NoError(t, err)

	require.Empty(t, violations)
}

func TestMultipleAlterTableLinter_Metadata(t *testing.T) {
	linter := &MultipleAlterTableLinter{}

	require.Equal(t, "multiple_alter_table", linter.Name())
	require.NotEmpty(t, linter.Description())
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
	require.NotNil(t, violations[0].Context)
	require.Equal(t, 3, violations[0].Context["alter_count"])

	indices, ok := violations[0].Context["statement_indices"].([]int)
	require.True(t, ok)
	require.Equal(t, []int{0, 1, 2}, indices)
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
	require.Equal(t, SeverityInfo, violations[0].Severity)
}

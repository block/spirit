package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/require"
)

func TestRenameColumnLinter_NoRename(t *testing.T) {
	sql := `ALTER TABLE users ADD COLUMN email VARCHAR(255)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &RenameColumnLinter{}
	violations := linter.Lint(nil, stmts)

	require.Empty(t, violations)
}

func TestRenameColumnLinter_RenameColumn(t *testing.T) {
	sql := `ALTER TABLE users RENAME COLUMN old_name TO new_name`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &RenameColumnLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	require.Equal(t, "rename_column", violations[0].Linter.Name())
	require.Equal(t, SeverityError, violations[0].Severity)
	require.Contains(t, violations[0].Message, "old_name")
	require.Contains(t, violations[0].Message, "new_name")
	require.Contains(t, violations[0].Message, "users")
	require.Equal(t, "users", violations[0].Location.Table)
	require.NotNil(t, violations[0].Location.Column)
	require.Equal(t, "old_name", *violations[0].Location.Column)
	require.NotNil(t, violations[0].Suggestion)
}

func TestRenameColumnLinter_ChangeColumnWithRename(t *testing.T) {
	sql := `ALTER TABLE users CHANGE COLUMN old_col new_col INT`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &RenameColumnLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	require.Equal(t, "rename_column", violations[0].Linter.Name())
	require.Equal(t, SeverityError, violations[0].Severity)
	require.Contains(t, violations[0].Message, "old_col")
	require.Contains(t, violations[0].Message, "new_col")
	require.Contains(t, violations[0].Message, "CHANGE COLUMN")
	require.Equal(t, "users", violations[0].Location.Table)
	require.NotNil(t, violations[0].Location.Column)
	require.Equal(t, "old_col", *violations[0].Location.Column)
}

func TestRenameColumnLinter_ChangeColumnSameName(t *testing.T) {
	// CHANGE COLUMN with same name is just a type change, not a rename
	sql := `ALTER TABLE users CHANGE COLUMN name name VARCHAR(512)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &RenameColumnLinter{}
	violations := linter.Lint(nil, stmts)

	require.Empty(t, violations)
}

func TestRenameColumnLinter_ModifyColumnNoRename(t *testing.T) {
	// MODIFY COLUMN only changes type, never renames
	sql := `ALTER TABLE users MODIFY COLUMN name VARCHAR(512)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &RenameColumnLinter{}
	violations := linter.Lint(nil, stmts)

	require.Empty(t, violations)
}

func TestRenameColumnLinter_MultipleRenames(t *testing.T) {
	sql := `ALTER TABLE users 
		RENAME COLUMN a TO b,
		RENAME COLUMN c TO d`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &RenameColumnLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 2)
	require.Equal(t, SeverityError, violations[0].Severity)
	require.Equal(t, SeverityError, violations[1].Severity)
}

func TestRenameColumnLinter_MixedOperations(t *testing.T) {
	// Only the rename should be flagged, not the add or modify
	sql := `ALTER TABLE users 
		ADD COLUMN email VARCHAR(255),
		RENAME COLUMN old_name TO new_name,
		MODIFY COLUMN age BIGINT`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &RenameColumnLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	require.Contains(t, violations[0].Message, "old_name")
	require.Contains(t, violations[0].Message, "new_name")
}

func TestRenameColumnLinter_NonAlterStatement(t *testing.T) {
	// CREATE TABLE should not trigger any violations
	sql := `CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255))`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &RenameColumnLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	require.Empty(t, violations)
}

func TestRenameColumnLinter_DropColumn(t *testing.T) {
	sql := `ALTER TABLE users DROP COLUMN old_col`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &RenameColumnLinter{}
	violations := linter.Lint(nil, stmts)

	require.Empty(t, violations)
}

func TestRenameColumnLinter_Name(t *testing.T) {
	linter := &RenameColumnLinter{}
	require.Equal(t, "rename_column", linter.Name())
}

func TestRenameColumnLinter_Description(t *testing.T) {
	linter := &RenameColumnLinter{}
	require.NotEmpty(t, linter.Description())
}

func TestRenameColumnLinter_String(t *testing.T) {
	linter := &RenameColumnLinter{}
	require.Contains(t, linter.String(), "rename_column")
}

// MySQL compares column identifiers case-insensitively, so a CHANGE COLUMN
// that restates a name in another case renames nothing: every query and ORM
// mapping that referenced the column still resolves to it. Such a clause is a
// redefinition, and reporting it would fail a plan over a no-op.
func TestRenameColumnLinter_ChangeColumnRestatingNameInAnotherCase(t *testing.T) {
	for _, sql := range []string{
		"ALTER TABLE users CHANGE COLUMN phone PHONE VARCHAR(40)",
		"ALTER TABLE users CHANGE COLUMN PHONE phone VARCHAR(40)",
		"ALTER TABLE users CHANGE COLUMN `phone` `PhOnE` VARCHAR(40) NOT NULL",
	} {
		t.Run(sql, func(t *testing.T) {
			stmts, err := statement.New(sql)
			require.NoError(t, err)

			require.Empty(t, (&RenameColumnLinter{}).Lint(nil, stmts))
		})
	}
}

// A CHANGE COLUMN whose new name differs by more than case is still a rename.
func TestRenameColumnLinter_ChangeColumnDifferingByMoreThanCase(t *testing.T) {
	stmts, err := statement.New("ALTER TABLE users CHANGE COLUMN phone PHONE_NUMBER VARCHAR(40)")
	require.NoError(t, err)

	violations := (&RenameColumnLinter{}).Lint(nil, stmts)
	require.Len(t, violations, 1)
	require.Equal(t, SeverityError, violations[0].Severity)
	require.Contains(t, violations[0].Message, "phone")
	require.Contains(t, violations[0].Message, "PHONE_NUMBER")
}

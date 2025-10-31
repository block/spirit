package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnsafeLinter_SafeCreateTable(t *testing.T) {
	sql := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// CREATE TABLE is safe - no violations
	assert.Empty(t, violations)
}

func TestUnsafeLinter_DropTable(t *testing.T) {
	sql := `DROP table users`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// DROP TABLE is unsafe
	require.Len(t, violations, 1)
	assert.Equal(t, "unsafe", violations[0].Linter.Name())
	assert.Equal(t, SeverityError, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "Unsafe operation")
	t.Log(violations[0].Message)
	assert.Equal(t, "users", violations[0].Location.Table)
}

func TestUnsafeLinter_DropTableIfExists(t *testing.T) {
	sql := `DROP TABLE IF EXISTS users`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// DROP TABLE IF EXISTS is still unsafe
	require.Len(t, violations, 1)
	assert.Equal(t, "unsafe", violations[0].Linter.Name())
	assert.Equal(t, SeverityError, violations[0].Severity)
}

func TestUnsafeLinter_AlterTableDropColumn(t *testing.T) {
	sql := `ALTER TABLE users DROP COLUMN email`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// DROP COLUMN is unsafe
	require.Len(t, violations, 1)
	assert.Equal(t, "unsafe", violations[0].Linter.Name())
	assert.Equal(t, SeverityError, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "Unsafe operation")
	t.Log(violations[0].Message)
	assert.Equal(t, "users", violations[0].Location.Table)
}

func TestUnsafeLinter_AlterTableDropPrimaryKey(t *testing.T) {
	sql := `ALTER TABLE users DROP PRIMARY KEY`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// DROP PRIMARY KEY is unsafe
	require.Len(t, violations, 1)
	assert.Equal(t, "unsafe", violations[0].Linter.Name())
	assert.Equal(t, SeverityError, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "Unsafe operation")
}

func TestUnsafeLinter_AlterTableDropPartition(t *testing.T) {
	sql := `ALTER TABLE users DROP PARTITION p0`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// DROP PARTITION is unsafe
	require.Len(t, violations, 1)
	assert.Equal(t, "unsafe", violations[0].Linter.Name())
	assert.Equal(t, SeverityError, violations[0].Severity)
}

func TestUnsafeLinter_AlterTableCoalescePartitions(t *testing.T) {
	sql := `ALTER TABLE users COALESCE PARTITION 2`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// COALESCE PARTITION is unsafe
	require.Len(t, violations, 1)
	assert.Equal(t, "unsafe", violations[0].Linter.Name())
	assert.Equal(t, SeverityError, violations[0].Severity)
}

func TestUnsafeLinter_AlterTableAddColumn(t *testing.T) {
	sql := `ALTER TABLE users ADD COLUMN email VARCHAR(255)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// ADD COLUMN is safe - no violations
	assert.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableAddIndex(t *testing.T) {
	sql := `ALTER TABLE users ADD INDEX idx_email (email)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// ADD INDEX is safe - no violations
	assert.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableDropIndex(t *testing.T) {
	sql := `ALTER TABLE users DROP INDEX idx_email`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// DROP INDEX is considered safe (no data loss)
	assert.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableDropForeignKey(t *testing.T) {
	sql := `ALTER TABLE orders DROP FOREIGN KEY fk_user_id`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// DROP FOREIGN KEY is considered safe (no data loss)
	assert.Empty(t, violations)
}

// DISABLED: This test is disabled because the MODIFY COLUMN warning functionality
// has been commented out in UnsafeLinter (lint_unsafe.go lines 101-117).
// The linter no longer warns about MODIFY COLUMN operations.
// To re-enable this test, uncomment the MODIFY COLUMN warning code in UnsafeLinter.
/*
func TestUnsafeLinter_AlterTableModifyColumn(t *testing.T) {
	sql := `ALTER TABLE users MODIFY COLUMN name VARCHAR(500)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// MODIFY COLUMN is considered safe (lossy changes detected at runtime)
	assert.Len(t, violations, 1)
	assert.Equal(t, "unsafe", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "Changing column type could lead to data loss by truncation")
	assert.Equal(t, "users", violations[0].Location.Table)
	assert.Equal(t, "name", *violations[0].Location.Column)
}
*/

// DISABLED: This test is disabled because the MODIFY/CHANGE COLUMN warning functionality
// has been commented out in UnsafeLinter (lint_unsafe.go lines 101-117).
// The linter no longer warns about CHANGE COLUMN operations.
// To re-enable this test, uncomment the MODIFY/CHANGE COLUMN warning code in UnsafeLinter.
/*
func TestUnsafeLinter_AlterTableChangeColumn(t *testing.T) {
	sql := `ALTER TABLE users CHANGE COLUMN name full_name VARCHAR(255)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// CHANGE COLUMN is considered safe (lossy changes detected at runtime)
	assert.Len(t, violations, 1)
	assert.Equal(t, "unsafe", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "Changing column type could lead to data loss by truncation")
	assert.Equal(t, "users", violations[0].Location.Table)
	assert.Equal(t, "name", *violations[0].Location.Column)
}
*/

func TestUnsafeLinter_AlterTableRenameColumn(t *testing.T) {
	sql := `ALTER TABLE users RENAME COLUMN name TO full_name`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// RENAME COLUMN is considered safe (no data loss)
	assert.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableRenameTable(t *testing.T) {
	sql := `ALTER TABLE users RENAME TO customers`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// RENAME TABLE (via ALTER) is considered safe (no data loss)
	assert.Empty(t, violations)
}

func TestUnsafeLinter_RenameTable(t *testing.T) {
	sql := `RENAME TABLE users TO customers`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// RENAME TABLE statement is considered safe (no data loss)
	assert.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableDropCheck(t *testing.T) {
	sql := `ALTER TABLE users DROP CHECK chk_age`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// DROP CHECK is considered safe (no data loss)
	assert.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableOption(t *testing.T) {
	sql := `ALTER TABLE users ENGINE=InnoDB`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// Changing table options is considered safe
	assert.Empty(t, violations)
}

func TestUnsafeLinter_MixedSafeAndUnsafeOperations(t *testing.T) {
	sql1 := `CREATE TABLE users (id INT PRIMARY KEY)`
	stmts1, err := statement.New(sql1)
	require.NoError(t, err)

	sql2 := `ALTER TABLE users ADD COLUMN name VARCHAR(255)`
	stmts2, err := statement.New(sql2)
	require.NoError(t, err)

	sql3 := `DROP TABLE old_users`
	stmts3, err := statement.New(sql3)
	require.NoError(t, err)

	sql4 := `ALTER TABLE products ADD INDEX idx_name (name)`
	stmts4, err := statement.New(sql4)
	require.NoError(t, err)

	// Combine statements
	stmts := append(stmts1, stmts2...)
	stmts = append(stmts, stmts3...)
	stmts = append(stmts, stmts4...)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// Should only detect the DROP TABLE
	require.Len(t, violations, 1)
	assert.Equal(t, "old_users", violations[0].Location.Table)
}

func TestUnsafeLinter_AlterTableMultipleSpecs(t *testing.T) {
	sql := `ALTER TABLE users 
		ADD COLUMN email VARCHAR(255),
		DROP COLUMN phone,
		ADD INDEX idx_email (email)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect the DROP COLUMN in the multi-spec ALTER
	require.Len(t, violations, 1)
	assert.Equal(t, "unsafe", violations[0].Linter.Name())
	assert.Equal(t, SeverityError, violations[0].Severity)
	assert.Equal(t, "users", violations[0].Location.Table)
}

func TestUnsafeLinter_AlterTableMultipleUnsafeSpecs(t *testing.T) {
	sql := `ALTER TABLE users 
		DROP COLUMN email,
		DROP COLUMN phone`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect both DROP COLUMN operations
	require.Len(t, violations, 2)

	for _, v := range violations {
		assert.Equal(t, "unsafe", v.Linter.Name())
		assert.Equal(t, SeverityError, v.Severity)
		assert.Equal(t, "users", v.Location.Table)
	}
}

func TestUnsafeLinter_EmptyInput(t *testing.T) {
	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, nil)

	// No statements - no violations
	assert.Empty(t, violations)
}

func TestUnsafeLinter_EmptyStatementList(t *testing.T) {
	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, []*statement.AbstractStatement{})

	// Empty list - no violations
	assert.Empty(t, violations)
}

func TestUnsafeLinter_CreateIndex(t *testing.T) {
	sql := `CREATE INDEX idx_email ON users(email)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// CREATE INDEX is safe - no violations
	assert.Empty(t, violations)
}

func TestUnsafeLinter_Integration(t *testing.T) {
	Reset()
	Register(&UnsafeLinter{})

	sql := `DROP TABLE users`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations, err := RunLinters(nil, stmts, Config{})
	require.NoError(t, err)

	// Should find at least one violation (from UnsafeLinter)
	var unsafeViolations []Violation
	for _, v := range violations {
		if v.Linter.Name() == "unsafe" {
			unsafeViolations = append(unsafeViolations, v)
		}
	}
	require.Len(t, unsafeViolations, 1)
	assert.Equal(t, "unsafe", unsafeViolations[0].Linter.Name())
}

func TestUnsafeLinter_IntegrationDisabled(t *testing.T) {
	Reset()
	Register(&UnsafeLinter{})

	sql := `DROP TABLE users`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations, err := RunLinters(nil, stmts, Config{
		Enabled: map[string]bool{
			"unsafe": false,
		},
	})
	require.NoError(t, err)

	// UnsafeLinter is disabled, so no violations from it
	for _, v := range violations {
		assert.NotEqual(t, "unsafe", v.Linter.Name())
	}
}

func TestUnsafeLinter_Metadata(t *testing.T) {
	linter := &UnsafeLinter{}

	assert.Equal(t, "unsafe", linter.Name())
	assert.NotEmpty(t, linter.Description())
	assert.Contains(t, linter.Description(), "unsafe")
}

func TestUnsafeLinter_String(t *testing.T) {
	linter := &UnsafeLinter{}
	str := linter.String()

	// String() should return a non-empty string
	assert.NotEmpty(t, str)
}

func TestUnsafeLinter_ViolationStructure(t *testing.T) {
	sql := `DROP TABLE users`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	v := violations[0]

	// Verify violation structure
	assert.NotNil(t, v.Linter)
	assert.Equal(t, "unsafe", v.Linter.Name())
	assert.Equal(t, SeverityError, v.Severity)
	assert.NotEmpty(t, v.Message)
	assert.NotNil(t, v.Location)
	assert.Equal(t, "users", v.Location.Table)
	assert.Nil(t, v.Location.Column)
	assert.Nil(t, v.Location.Index)
	assert.Nil(t, v.Location.Constraint)
}

func TestUnsafeLinter_ExistingTablesParameter(t *testing.T) {
	// The linter should ignore existingTables parameter and only check changes
	existingSQL := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	existingTable, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	// Unsafe operation
	sql := `DROP TABLE users`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, stmts)

	// Should only check the changes (stmts), not existingTables
	require.Len(t, violations, 1)
	assert.Equal(t, "users", violations[0].Location.Table)
}

func TestUnsafeLinter_AlterTableAddPrimaryKey(t *testing.T) {
	sql := `ALTER TABLE users ADD PRIMARY KEY (id)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// ADD PRIMARY KEY is safe - no violations
	assert.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableAddForeignKey(t *testing.T) {
	sql := `ALTER TABLE orders ADD CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES users(id)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// ADD FOREIGN KEY is safe - no violations
	assert.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableAddCheck(t *testing.T) {
	sql := `ALTER TABLE users ADD CONSTRAINT chk_age CHECK (age >= 18)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// ADD CHECK is safe - no violations
	assert.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableAddUnique(t *testing.T) {
	sql := `ALTER TABLE users ADD UNIQUE KEY uk_email (email)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// ADD UNIQUE is safe - no violations
	assert.Empty(t, violations)
}

func TestUnsafeLinter_DropMultipleTables(t *testing.T) {
	sql := `DROP TABLE users, orders`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// DROP TABLE with multiple tables should be detected
	// Note: The statement parser might handle this differently
	require.GreaterOrEqual(t, len(violations), 1)
	assert.Equal(t, "unsafe", violations[0].Linter.Name())
	assert.Equal(t, SeverityError, violations[0].Severity)
}

func TestUnsafeLinter_AlterTableCharset(t *testing.T) {
	sql := `ALTER TABLE users CONVERT TO CHARACTER SET utf8mb4`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// Changing charset is considered safe (handled at runtime)
	assert.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableCollate(t *testing.T) {
	sql := `ALTER TABLE users COLLATE utf8mb4_unicode_ci`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// Changing collation is considered safe (handled at runtime)
	assert.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableAutoIncrement(t *testing.T) {
	sql := `ALTER TABLE users AUTO_INCREMENT = 1000`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// Changing AUTO_INCREMENT is safe
	assert.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableComment(t *testing.T) {
	sql := `ALTER TABLE users COMMENT = 'User information table'`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// Changing table comment is safe
	assert.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableRowFormat(t *testing.T) {
	sql := `ALTER TABLE users ROW_FORMAT = DYNAMIC`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// Changing row format is safe
	assert.Empty(t, violations)
}

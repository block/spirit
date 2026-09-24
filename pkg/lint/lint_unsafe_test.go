package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
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
	require.Empty(t, violations)
}

func TestUnsafeLinter_DropTable(t *testing.T) {
	sql := `DROP table users`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// DROP TABLE is unsafe
	require.Len(t, violations, 1)
	require.Equal(t, "unsafe", violations[0].Linter.Name())
	require.Equal(t, SeverityError, violations[0].Severity)
	require.Contains(t, violations[0].Message, "Unsafe operation")
	t.Log(violations[0].Message)
	require.Equal(t, "users", violations[0].Location.Table)
}

func TestUnsafeLinter_DropTableIfExists(t *testing.T) {
	sql := `DROP TABLE IF EXISTS users`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// DROP TABLE IF EXISTS is still unsafe
	require.Len(t, violations, 1)
	require.Equal(t, "unsafe", violations[0].Linter.Name())
	require.Equal(t, SeverityError, violations[0].Severity)
}

func TestUnsafeLinter_AlterTableDropColumn(t *testing.T) {
	sql := `ALTER TABLE users DROP COLUMN email`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// DROP COLUMN is unsafe
	require.Len(t, violations, 1)
	require.Equal(t, "unsafe", violations[0].Linter.Name())
	require.Equal(t, SeverityError, violations[0].Severity)
	require.Contains(t, violations[0].Message, "Unsafe operation")
	require.Equal(t, "Unsafe operation detected: \"DROP COLUMN `email`\"", violations[0].Message)
	t.Log(violations[0].Message)
	require.Equal(t, "users", violations[0].Location.Table)
	require.NotNil(t, violations[0].Location.Column)
	require.Equal(t, "email", *violations[0].Location.Column)
}

func TestUnsafeLinter_AlterTableDropPrimaryKey(t *testing.T) {
	sql := `ALTER TABLE users DROP PRIMARY KEY`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// DROP PRIMARY KEY is unsafe
	require.Len(t, violations, 1)
	require.Equal(t, "unsafe", violations[0].Linter.Name())
	require.Equal(t, SeverityError, violations[0].Severity)
	require.Contains(t, violations[0].Message, "Unsafe operation")
}

func TestUnsafeLinter_AlterTableDropPartition(t *testing.T) {
	sql := `ALTER TABLE users DROP PARTITION p0`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// DROP PARTITION is unsafe
	require.Len(t, violations, 1)
	require.Equal(t, "unsafe", violations[0].Linter.Name())
	require.Equal(t, SeverityError, violations[0].Severity)
}

func TestUnsafeLinter_AlterTableCoalescePartitions(t *testing.T) {
	sql := `ALTER TABLE users COALESCE PARTITION 2`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// COALESCE PARTITION is unsafe
	require.Len(t, violations, 1)
	require.Equal(t, "unsafe", violations[0].Linter.Name())
	require.Equal(t, SeverityError, violations[0].Severity)
}

func TestUnsafeLinter_AlterTableTruncatePartition(t *testing.T) {
	sql := `ALTER TABLE users TRUNCATE PARTITION p0`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// TRUNCATE PARTITION deletes all rows in the partition, so it is unsafe
	require.Len(t, violations, 1)
	require.Equal(t, "unsafe", violations[0].Linter.Name())
	require.Equal(t, SeverityError, violations[0].Severity)
	require.Equal(t, "Unsafe operation detected: \"TRUNCATE PARTITION\"", violations[0].Message)
	require.Equal(t, "users", violations[0].Location.Table)
}

func TestUnsafeLinter_AlterTableTruncatePartitionAll(t *testing.T) {
	sql := `ALTER TABLE users TRUNCATE PARTITION ALL`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// TRUNCATE PARTITION ALL deletes all rows in every partition, so it is unsafe
	require.Len(t, violations, 1)
	require.Equal(t, "unsafe", violations[0].Linter.Name())
	require.Equal(t, SeverityError, violations[0].Severity)
	require.Equal(t, "Unsafe operation detected: \"TRUNCATE PARTITION\"", violations[0].Message)
	require.Equal(t, "users", violations[0].Location.Table)
}

func TestUnsafeLinter_AlterTableAddPartition(t *testing.T) {
	sql := `ALTER TABLE users ADD PARTITION (PARTITION p2026 VALUES LESS THAN (2027))`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// ADD PARTITION is safe - no violations
	require.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableAddColumn(t *testing.T) {
	sql := `ALTER TABLE users ADD COLUMN email VARCHAR(255)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// ADD COLUMN is safe - no violations
	require.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableAddIndex(t *testing.T) {
	sql := `ALTER TABLE users ADD INDEX idx_email (email)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// ADD INDEX is safe - no violations
	require.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableDropIndex(t *testing.T) {
	sql := `ALTER TABLE users DROP INDEX idx_email`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// DROP INDEX is considered safe (no data loss)
	require.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableDropForeignKey(t *testing.T) {
	sql := `ALTER TABLE orders DROP FOREIGN KEY fk_user_id`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// DROP FOREIGN KEY is considered safe (no data loss)
	require.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableRenameColumn(t *testing.T) {
	sql := `ALTER TABLE users RENAME COLUMN name TO full_name`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// RENAME COLUMN is considered safe (no data loss)
	require.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableRenameTable(t *testing.T) {
	sql := `ALTER TABLE users RENAME TO customers`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// RENAME TABLE (via ALTER) is considered safe (no data loss)
	require.Empty(t, violations)
}

func TestUnsafeLinter_RenameTable(t *testing.T) {
	sql := `RENAME TABLE users TO customers`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// RENAME TABLE statement is considered safe (no data loss)
	require.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableDropCheck(t *testing.T) {
	sql := `ALTER TABLE users DROP CHECK chk_age`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// DROP CHECK is considered safe (no data loss)
	require.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableDropConstraint(t *testing.T) {
	// DROP CONSTRAINT is its own spec type, not a synonym for DROP CHECK. It
	// can drop a CHECK, FOREIGN KEY or UNIQUE constraint, none of which lose
	// row data, so it belongs in the linter's safe-list.
	//
	// Note the switch in UnsafeLinter.Lint has no default, so an unlisted spec
	// type also produces no violation: what this pins is that DROP CONSTRAINT
	// is never classified as *unsafe*, not that it stays in the safe-list. The
	// same is true of TestUnsafeLinter_AlterTableDropCheck above.
	for _, sql := range []string{
		`ALTER TABLE users DROP CONSTRAINT chk_age`,
		`ALTER TABLE users DROP CONSTRAINT uq_name`,
		`ALTER TABLE users DROP CONSTRAINT fk_org`,
	} {
		stmts, err := statement.New(sql)
		require.NoError(t, err, sql)

		linter := &UnsafeLinter{}
		violations := linter.Lint(nil, stmts)

		require.Empty(t, violations, sql)
	}
}

func TestUnsafeLinter_AlterTableOption(t *testing.T) {
	sql := `ALTER TABLE users ENGINE=InnoDB`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// Changing table options is considered safe
	require.Empty(t, violations)
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
	stmts1 = append(stmts1, stmts2...)
	stmts1 = append(stmts1, stmts3...)
	stmts1 = append(stmts1, stmts4...)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts1)

	// Should only detect the DROP TABLE
	require.Len(t, violations, 1)
	require.Equal(t, "old_users", violations[0].Location.Table)
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
	require.Equal(t, "unsafe", violations[0].Linter.Name())
	require.Equal(t, SeverityError, violations[0].Severity)
	require.Equal(t, "users", violations[0].Location.Table)
	require.NotNil(t, violations[0].Location.Column)
	require.Equal(t, "phone", *violations[0].Location.Column)
	require.Equal(t, "Unsafe operation detected: \"DROP COLUMN `phone`\"", violations[0].Message)
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
		require.Equal(t, "unsafe", v.Linter.Name())
		require.Equal(t, SeverityError, v.Severity)
		require.Equal(t, "users", v.Location.Table)
	}

	columnMessages := map[string]string{}
	for _, v := range violations {
		require.NotNil(t, v.Location.Column)
		columnMessages[*v.Location.Column] = v.Message
	}
	require.Equal(t, map[string]string{
		"email": "Unsafe operation detected: \"DROP COLUMN `email`\"",
		"phone": "Unsafe operation detected: \"DROP COLUMN `phone`\"",
	}, columnMessages)
}

func TestUnsafeLinter_EmptyInput(t *testing.T) {
	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, nil)

	// No statements - no violations
	require.Empty(t, violations)
}

func TestUnsafeLinter_EmptyStatementList(t *testing.T) {
	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, []*statement.AbstractStatement{})

	// Empty list - no violations
	require.Empty(t, violations)
}

func TestUnsafeLinter_CreateIndex(t *testing.T) {
	sql := `CREATE INDEX idx_email ON users(email)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// CREATE INDEX is safe - no violations
	require.Empty(t, violations)
}

func TestUnsafeLinter_Integration(t *testing.T) {
	resetForTest(t)
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
	require.Equal(t, "unsafe", unsafeViolations[0].Linter.Name())
}

func TestUnsafeLinter_IntegrationDisabled(t *testing.T) {
	resetForTest(t)
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
		require.NotEqual(t, "unsafe", v.Linter.Name())
	}
}

func TestUnsafeLinter_Metadata(t *testing.T) {
	linter := &UnsafeLinter{}

	require.Equal(t, "unsafe", linter.Name())
	require.NotEmpty(t, linter.Description())
	require.Contains(t, linter.Description(), "unsafe")
}

func TestUnsafeLinter_String(t *testing.T) {
	linter := &UnsafeLinter{}
	str := linter.String()

	// String() should return a non-empty string
	require.NotEmpty(t, str)
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
	require.NotNil(t, v.Linter)
	require.Equal(t, "unsafe", v.Linter.Name())
	require.Equal(t, SeverityError, v.Severity)
	require.NotEmpty(t, v.Message)
	require.NotNil(t, v.Location)
	require.Equal(t, "users", v.Location.Table)
	require.Nil(t, v.Location.Column)
	require.Nil(t, v.Location.Index)
	require.Nil(t, v.Location.Constraint)
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
	require.Equal(t, "users", violations[0].Location.Table)
}

func TestUnsafeLinter_AlterTableAddPrimaryKey(t *testing.T) {
	sql := `ALTER TABLE users ADD PRIMARY KEY (id)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// ADD PRIMARY KEY is safe - no violations
	require.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableAddForeignKey(t *testing.T) {
	sql := `ALTER TABLE orders ADD CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES users(id)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// ADD FOREIGN KEY is safe - no violations
	require.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableAddCheck(t *testing.T) {
	sql := `ALTER TABLE users ADD CONSTRAINT chk_age CHECK (age >= 18)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// ADD CHECK is safe - no violations
	require.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableAddUnique(t *testing.T) {
	sql := `ALTER TABLE users ADD UNIQUE KEY uk_email (email)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// ADD UNIQUE is safe - no violations
	require.Empty(t, violations)
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
	require.Equal(t, "unsafe", violations[0].Linter.Name())
	require.Equal(t, SeverityError, violations[0].Severity)
}

func TestUnsafeLinter_AlterTableCharset(t *testing.T) {
	sql := `ALTER TABLE users CONVERT TO CHARACTER SET utf8mb4`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// Changing charset is considered safe (handled at runtime)
	require.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableCollate(t *testing.T) {
	sql := `ALTER TABLE users COLLATE utf8mb4_unicode_ci`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// Changing collation is considered safe (handled at runtime)
	require.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableAutoIncrement(t *testing.T) {
	sql := `ALTER TABLE users AUTO_INCREMENT = 1000`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// Changing AUTO_INCREMENT is safe
	require.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableComment(t *testing.T) {
	sql := `ALTER TABLE users COMMENT = 'User information table'`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// Changing table comment is safe
	require.Empty(t, violations)
}

func TestUnsafeLinter_AlterTableRowFormat(t *testing.T) {
	sql := `ALTER TABLE users ROW_FORMAT = DYNAMIC`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &UnsafeLinter{}
	violations := linter.Lint(nil, stmts)

	// Changing row format is safe
	require.Empty(t, violations)
}

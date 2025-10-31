package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHasFKLinter_NoForeignKey(t *testing.T) {
	sql := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255),
		email VARCHAR(255),
		INDEX idx_email (email)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// No foreign key - no violations
	assert.Empty(t, violations)
}

func TestHasFKLinter_SingleForeignKey(t *testing.T) {
	sql := `CREATE TABLE orders (
		id BIGINT UNSIGNED PRIMARY KEY,
		user_id BIGINT UNSIGNED,
		total DECIMAL(10,2),
		CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect the foreign key
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "orders")
	assert.Contains(t, violations[0].Message, "fk_orders_user")
	assert.Contains(t, violations[0].Message, "FOREIGN KEY")
	assert.Equal(t, "orders", violations[0].Location.Table)
	assert.NotNil(t, violations[0].Location.Constraint)
	assert.Equal(t, "fk_orders_user", *violations[0].Location.Constraint)
}

func TestHasFKLinter_MultipleForeignKeys(t *testing.T) {
	sql := `CREATE TABLE order_items (
		id BIGINT UNSIGNED PRIMARY KEY,
		order_id BIGINT UNSIGNED,
		product_id BIGINT UNSIGNED,
		quantity INT,
		CONSTRAINT fk_order_items_order FOREIGN KEY (order_id) REFERENCES orders(id),
		CONSTRAINT fk_order_items_product FOREIGN KEY (product_id) REFERENCES products(id)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect both foreign keys
	require.Len(t, violations, 2)

	// Check first violation
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Equal(t, "order_items", violations[0].Location.Table)
	assert.NotNil(t, violations[0].Location.Constraint)

	// Check second violation
	assert.Equal(t, "has_foreign_key", violations[1].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[1].Severity)
	assert.Equal(t, "order_items", violations[1].Location.Table)
	assert.NotNil(t, violations[1].Location.Constraint)

	// Verify both constraint names are present
	constraintNames := []string{*violations[0].Location.Constraint, *violations[1].Location.Constraint}
	assert.Contains(t, constraintNames, "fk_order_items_order")
	assert.Contains(t, constraintNames, "fk_order_items_product")
}

func TestHasFKLinter_ForeignKeyWithOnDelete(t *testing.T) {
	sql := `CREATE TABLE orders (
		id BIGINT UNSIGNED PRIMARY KEY,
		user_id BIGINT UNSIGNED,
		CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect foreign key with ON DELETE CASCADE
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
	assert.Contains(t, violations[0].Message, "fk_orders_user")
}

func TestHasFKLinter_ForeignKeyWithOnUpdate(t *testing.T) {
	sql := `CREATE TABLE orders (
		id BIGINT UNSIGNED PRIMARY KEY,
		user_id BIGINT UNSIGNED,
		CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id) ON UPDATE CASCADE
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect foreign key with ON UPDATE CASCADE
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
	assert.Contains(t, violations[0].Message, "fk_orders_user")
}

func TestHasFKLinter_ForeignKeyWithBothOnDeleteAndOnUpdate(t *testing.T) {
	sql := `CREATE TABLE orders (
		id BIGINT UNSIGNED PRIMARY KEY,
		user_id BIGINT UNSIGNED,
		CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE ON UPDATE CASCADE
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect foreign key with both ON DELETE and ON UPDATE
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
}

func TestHasFKLinter_UnnamedForeignKey(t *testing.T) {
	sql := `CREATE TABLE orders (
		id BIGINT UNSIGNED PRIMARY KEY,
		user_id BIGINT UNSIGNED,
		FOREIGN KEY (user_id) REFERENCES users(id)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect unnamed foreign key
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Equal(t, "orders", violations[0].Location.Table)
	assert.NotNil(t, violations[0].Location.Constraint)
	// Unnamed constraints typically get an empty string or auto-generated name
}

func TestHasFKLinter_CompositeForeignKey(t *testing.T) {
	sql := `CREATE TABLE order_details (
		order_id BIGINT UNSIGNED,
		line_number INT,
		product_id BIGINT UNSIGNED,
		PRIMARY KEY (order_id, line_number),
		CONSTRAINT fk_order_details FOREIGN KEY (order_id, line_number) REFERENCES orders(id, line_num)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect composite foreign key
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
	assert.Contains(t, violations[0].Message, "fk_order_details")
}

func TestHasFKLinter_MultipleTablesWithForeignKeys(t *testing.T) {
	sql1 := `CREATE TABLE orders (
		id BIGINT UNSIGNED PRIMARY KEY,
		user_id BIGINT UNSIGNED,
		CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id)
	)`
	stmts1, err := statement.New(sql1)
	require.NoError(t, err)

	sql2 := `CREATE TABLE order_items (
		id BIGINT UNSIGNED PRIMARY KEY,
		order_id BIGINT UNSIGNED,
		CONSTRAINT fk_items_order FOREIGN KEY (order_id) REFERENCES orders(id)
	)`
	stmts2, err := statement.New(sql2)
	require.NoError(t, err)

	// Combine statements
	stmts := append(stmts1, stmts2...)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect foreign keys in both tables
	require.Len(t, violations, 2)

	tables := []string{violations[0].Location.Table, violations[1].Location.Table}
	assert.Contains(t, tables, "orders")
	assert.Contains(t, tables, "order_items")
}

func TestHasFKLinter_MixedTablesWithAndWithoutForeignKeys(t *testing.T) {
	sql1 := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	stmts1, err := statement.New(sql1)
	require.NoError(t, err)

	sql2 := `CREATE TABLE orders (
		id BIGINT UNSIGNED PRIMARY KEY,
		user_id BIGINT UNSIGNED,
		CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id)
	)`
	stmts2, err := statement.New(sql2)
	require.NoError(t, err)

	sql3 := `CREATE TABLE products (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	stmts3, err := statement.New(sql3)
	require.NoError(t, err)

	// Combine statements
	stmts := append(stmts1, stmts2...)
	stmts = append(stmts, stmts3...)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should only detect foreign key in orders table
	require.Len(t, violations, 1)
	assert.Equal(t, "orders", violations[0].Location.Table)
	assert.Contains(t, violations[0].Message, "fk_orders_user")
}

func TestHasFKLinter_EmptyInput(t *testing.T) {
	linter := &HasFKLinter{}
	violations := linter.Lint(nil, nil)

	// No statements - no violations
	assert.Empty(t, violations)
}

func TestHasFKLinter_EmptyStatementList(t *testing.T) {
	linter := &HasFKLinter{}
	violations := linter.Lint(nil, []*statement.AbstractStatement{})

	// Empty list - no violations
	assert.Empty(t, violations)
}

func TestHasFKLinter_NonCreateTableStatements(t *testing.T) {
	sql := `ALTER TABLE users ADD COLUMN age INT`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// ALTER TABLE statements should be ignored
	assert.Empty(t, violations)
}

func TestHasFKLinter_TableWithOtherConstraints(t *testing.T) {
	sql := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		email VARCHAR(255),
		age INT,
		UNIQUE KEY uk_email (email),
		CHECK (age >= 18)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Only FOREIGN KEY constraints should be detected, not UNIQUE or CHECK
	assert.Empty(t, violations)
}

func TestHasFKLinter_TableWithMixedConstraints(t *testing.T) {
	sql := `CREATE TABLE orders (
		id BIGINT UNSIGNED PRIMARY KEY,
		user_id BIGINT UNSIGNED,
		status VARCHAR(50),
		total DECIMAL(10,2),
		UNIQUE KEY uk_order_id (id),
		CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id),
		CHECK (total >= 0)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should only detect the FOREIGN KEY constraint
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
	assert.Contains(t, violations[0].Message, "fk_orders_user")
}

func TestHasFKLinter_ForeignKeyWithSetNull(t *testing.T) {
	sql := `CREATE TABLE orders (
		id BIGINT UNSIGNED PRIMARY KEY,
		user_id BIGINT UNSIGNED,
		CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect foreign key with ON DELETE SET NULL
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
}

func TestHasFKLinter_ForeignKeyWithRestrict(t *testing.T) {
	sql := `CREATE TABLE orders (
		id BIGINT UNSIGNED PRIMARY KEY,
		user_id BIGINT UNSIGNED,
		CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect foreign key with ON DELETE RESTRICT
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
}

func TestHasFKLinter_ForeignKeyWithNoAction(t *testing.T) {
	sql := `CREATE TABLE orders (
		id BIGINT UNSIGNED PRIMARY KEY,
		user_id BIGINT UNSIGNED,
		CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE NO ACTION
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect foreign key with ON DELETE NO ACTION
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
}

func TestHasFKLinter_Integration(t *testing.T) {
	Reset()
	Register(&HasFKLinter{})

	sql := `CREATE TABLE orders (
		id BIGINT UNSIGNED PRIMARY KEY,
		user_id BIGINT UNSIGNED,
		CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations, err := RunLinters(nil, stmts, Config{})
	require.NoError(t, err)

	// Should find at least one violation (from HasFKLinter)
	var hasFKViolations []Violation
	for _, v := range violations {
		if v.Linter.Name() == "has_foreign_key" {
			hasFKViolations = append(hasFKViolations, v)
		}
	}
	require.Len(t, hasFKViolations, 1)
	assert.Equal(t, "has_foreign_key", hasFKViolations[0].Linter.Name())
}

func TestHasFKLinter_IntegrationDisabled(t *testing.T) {
	Reset()
	Register(&HasFKLinter{})

	sql := `CREATE TABLE orders (
		id BIGINT UNSIGNED PRIMARY KEY,
		user_id BIGINT UNSIGNED,
		CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations, err := RunLinters(nil, stmts, Config{
		Enabled: map[string]bool{
			"has_foreign_key": false,
		},
	})
	require.NoError(t, err)

	// HasFKLinter is disabled, so no violations from it
	for _, v := range violations {
		assert.NotEqual(t, "has_foreign_key", v.Linter.Name())
	}
}

func TestHasFKLinter_Metadata(t *testing.T) {
	linter := &HasFKLinter{}

	assert.Equal(t, "has_foreign_key", linter.Name())
	assert.NotEmpty(t, linter.Description())
	assert.Contains(t, linter.Description(), "FOREIGN KEY")
}

func TestHasFKLinter_String(t *testing.T) {
	linter := &HasFKLinter{}
	str := linter.String()

	// String() should return a non-empty string
	assert.NotEmpty(t, str)
}

func TestHasFKLinter_SelfReferencingForeignKey(t *testing.T) {
	sql := `CREATE TABLE employees (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255),
		manager_id BIGINT UNSIGNED,
		CONSTRAINT fk_employees_manager FOREIGN KEY (manager_id) REFERENCES employees(id)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect self-referencing foreign key
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
	assert.Contains(t, violations[0].Message, "fk_employees_manager")
}

func TestHasFKLinter_ForeignKeyReferencingDifferentDatabase(t *testing.T) {
	sql := `CREATE TABLE orders (
		id BIGINT UNSIGNED PRIMARY KEY,
		user_id BIGINT UNSIGNED,
		CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES other_db.users(id)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect foreign key even if it references another database
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
}

func TestHasFKLinter_ExistingTablesParameter(t *testing.T) {
	// The linter should ignore existingTables parameter and only check changes
	existingSQL := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	existingTable, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	// New table with foreign key
	sql := `CREATE TABLE orders (
		id BIGINT UNSIGNED PRIMARY KEY,
		user_id BIGINT UNSIGNED,
		CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, stmts)

	// Should only check the changes (stmts), not existingTables
	require.Len(t, violations, 1)
	assert.Equal(t, "orders", violations[0].Location.Table)
}

func TestHasFKLinter_TableWithEngineAndCharset(t *testing.T) {
	sql := `CREATE TABLE orders (
		id BIGINT UNSIGNED PRIMARY KEY,
		user_id BIGINT UNSIGNED,
		CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect foreign key regardless of table options
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
}

func TestHasFKLinter_TemporaryTable(t *testing.T) {
	sql := `CREATE TEMPORARY TABLE temp_orders (
		id BIGINT UNSIGNED PRIMARY KEY,
		user_id BIGINT UNSIGNED,
		CONSTRAINT fk_temp_orders_user FOREIGN KEY (user_id) REFERENCES users(id)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect foreign key even in temporary tables
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
}

func TestHasFKLinter_IfNotExists(t *testing.T) {
	sql := `CREATE TABLE IF NOT EXISTS orders (
		id BIGINT UNSIGNED PRIMARY KEY,
		user_id BIGINT UNSIGNED,
		CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect foreign key with IF NOT EXISTS
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
}

func TestHasFKLinter_ViolationStructure(t *testing.T) {
	sql := `CREATE TABLE orders (
		id BIGINT UNSIGNED PRIMARY KEY,
		user_id BIGINT UNSIGNED,
		CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	v := violations[0]

	// Verify violation structure
	assert.NotNil(t, v.Linter)
	assert.Equal(t, "has_foreign_key", v.Linter.Name())
	assert.Equal(t, SeverityWarning, v.Severity)
	assert.NotEmpty(t, v.Message)
	assert.NotNil(t, v.Location)
	assert.Equal(t, "orders", v.Location.Table)
	assert.NotNil(t, v.Location.Constraint)
	assert.Equal(t, "fk_orders_user", *v.Location.Constraint)
	assert.Nil(t, v.Location.Column)
	assert.Nil(t, v.Location.Index)
}

// Tests for ALTER TABLE handling

func TestHasFKLinter_AlterTableAddForeignKey(t *testing.T) {
	sql := `ALTER TABLE orders ADD CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES users(id)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect foreign key being added via ALTER TABLE
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "Adding foreign key constraint")
	assert.Contains(t, violations[0].Message, "fk_user_id")
	assert.Contains(t, violations[0].Message, "orders")
	assert.Equal(t, "orders", violations[0].Location.Table)
	assert.NotNil(t, violations[0].Location.Constraint)
	assert.Equal(t, "fk_user_id", *violations[0].Location.Constraint)
}

func TestHasFKLinter_AlterTableAddMultipleForeignKeys(t *testing.T) {
	sql := `ALTER TABLE order_items 
		ADD CONSTRAINT fk_order_id FOREIGN KEY (order_id) REFERENCES orders(id),
		ADD CONSTRAINT fk_product_id FOREIGN KEY (product_id) REFERENCES products(id)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect both foreign keys being added
	require.Len(t, violations, 2)

	for _, v := range violations {
		assert.Equal(t, "has_foreign_key", v.Linter.Name())
		assert.Equal(t, SeverityWarning, v.Severity)
		assert.Equal(t, "order_items", v.Location.Table)
		assert.NotNil(t, v.Location.Constraint)
		assert.Contains(t, v.Message, "Adding foreign key constraint")
	}

	// Verify both constraint names are present
	constraintNames := []string{*violations[0].Location.Constraint, *violations[1].Location.Constraint}
	assert.Contains(t, constraintNames, "fk_order_id")
	assert.Contains(t, constraintNames, "fk_product_id")
}

func TestHasFKLinter_AlterTableAddForeignKeyWithOnDelete(t *testing.T) {
	sql := `ALTER TABLE orders ADD CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect foreign key with ON DELETE CASCADE
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
	assert.Contains(t, violations[0].Message, "fk_user_id")
}

func TestHasFKLinter_AlterTableAddForeignKeyWithOnUpdate(t *testing.T) {
	sql := `ALTER TABLE orders ADD CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES users(id) ON UPDATE CASCADE`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect foreign key with ON UPDATE CASCADE
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
	assert.Contains(t, violations[0].Message, "fk_user_id")
}

func TestHasFKLinter_AlterTableAddForeignKeyWithBothOnDeleteAndOnUpdate(t *testing.T) {
	sql := `ALTER TABLE orders ADD CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE ON UPDATE CASCADE`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect foreign key with both ON DELETE and ON UPDATE
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
}

func TestHasFKLinter_AlterTableAddUnnamedForeignKey(t *testing.T) {
	sql := `ALTER TABLE orders ADD FOREIGN KEY (user_id) REFERENCES users(id)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect unnamed foreign key
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Equal(t, "orders", violations[0].Location.Table)
	assert.NotNil(t, violations[0].Location.Constraint)
}

func TestHasFKLinter_AlterTableAddCompositeForeignKey(t *testing.T) {
	sql := `ALTER TABLE order_details ADD CONSTRAINT fk_order_details FOREIGN KEY (order_id, line_number) REFERENCES orders(id, line_num)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect composite foreign key
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
	assert.Contains(t, violations[0].Message, "fk_order_details")
}

func TestHasFKLinter_AlterTableAddColumn(t *testing.T) {
	sql := `ALTER TABLE users ADD COLUMN age INT`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// ALTER TABLE ADD COLUMN without foreign key should not trigger violations
	assert.Empty(t, violations)
}

func TestHasFKLinter_AlterTableDropForeignKey(t *testing.T) {
	sql := `ALTER TABLE orders DROP FOREIGN KEY fk_user_id`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// DROP FOREIGN KEY should not trigger violations (we only care about adding them)
	assert.Empty(t, violations)
}

func TestHasFKLinter_AlterTableAddIndex(t *testing.T) {
	sql := `ALTER TABLE users ADD INDEX idx_email (email)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// ADD INDEX should not trigger violations
	assert.Empty(t, violations)
}

func TestHasFKLinter_AlterTableAddUniqueKey(t *testing.T) {
	sql := `ALTER TABLE users ADD UNIQUE KEY uk_email (email)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// ADD UNIQUE KEY should not trigger violations
	assert.Empty(t, violations)
}

func TestHasFKLinter_AlterTableAddCheck(t *testing.T) {
	sql := `ALTER TABLE users ADD CONSTRAINT chk_age CHECK (age >= 18)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// ADD CHECK constraint should not trigger violations
	assert.Empty(t, violations)
}

func TestHasFKLinter_AlterTableMixedConstraints(t *testing.T) {
	sql := `ALTER TABLE orders 
		ADD CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES users(id),
		ADD CONSTRAINT chk_total CHECK (total >= 0),
		ADD UNIQUE KEY uk_order_number (order_number)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should only detect the FOREIGN KEY constraint
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
	assert.Contains(t, violations[0].Message, "fk_user_id")
}

func TestHasFKLinter_AlterTableModifyColumn(t *testing.T) {
	sql := `ALTER TABLE users MODIFY COLUMN age INT NOT NULL`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// MODIFY COLUMN should not trigger violations
	assert.Empty(t, violations)
}

func TestHasFKLinter_AlterTableChangeColumn(t *testing.T) {
	sql := `ALTER TABLE users CHANGE COLUMN old_name new_name VARCHAR(255)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// CHANGE COLUMN should not trigger violations
	assert.Empty(t, violations)
}

func TestHasFKLinter_AlterTableRenameTable(t *testing.T) {
	sql := `ALTER TABLE old_orders RENAME TO new_orders`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// RENAME should not trigger violations
	assert.Empty(t, violations)
}

func TestHasFKLinter_MultipleAlterTableStatements(t *testing.T) {
	sql1 := `ALTER TABLE orders ADD CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES users(id)`
	stmts1, err := statement.New(sql1)
	require.NoError(t, err)

	sql2 := `ALTER TABLE order_items ADD CONSTRAINT fk_order_id FOREIGN KEY (order_id) REFERENCES orders(id)`
	stmts2, err := statement.New(sql2)
	require.NoError(t, err)

	// Combine statements
	stmts := append(stmts1, stmts2...)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect foreign keys in both ALTER TABLE statements
	require.Len(t, violations, 2)

	tables := []string{violations[0].Location.Table, violations[1].Location.Table}
	assert.Contains(t, tables, "orders")
	assert.Contains(t, tables, "order_items")
}

func TestHasFKLinter_MixedCreateAndAlterStatements(t *testing.T) {
	sql1 := `CREATE TABLE orders (
		id BIGINT UNSIGNED PRIMARY KEY,
		user_id BIGINT UNSIGNED,
		CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id)
	)`
	stmts1, err := statement.New(sql1)
	require.NoError(t, err)

	sql2 := `ALTER TABLE order_items ADD CONSTRAINT fk_order_id FOREIGN KEY (order_id) REFERENCES orders(id)`
	stmts2, err := statement.New(sql2)
	require.NoError(t, err)

	// Combine statements
	stmts := append(stmts1, stmts2...)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect foreign keys in both CREATE TABLE and ALTER TABLE
	require.Len(t, violations, 2)

	tables := []string{violations[0].Location.Table, violations[1].Location.Table}
	assert.Contains(t, tables, "orders")
	assert.Contains(t, tables, "order_items")

	// Verify messages are appropriate for each type
	for _, v := range violations {
		switch v.Location.Table {
		case "orders":
			assert.Contains(t, v.Message, "has FOREIGN KEY constraint")
		case "order_items":
			assert.Contains(t, v.Message, "Adding foreign key constraint")
		}
	}
}

func TestHasFKLinter_AlterTableAddForeignKeyWithSetNull(t *testing.T) {
	sql := `ALTER TABLE orders ADD CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect foreign key with ON DELETE SET NULL
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
}

func TestHasFKLinter_AlterTableAddForeignKeyWithRestrict(t *testing.T) {
	sql := `ALTER TABLE orders ADD CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect foreign key with ON DELETE RESTRICT
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
}

func TestHasFKLinter_AlterTableAddForeignKeyWithNoAction(t *testing.T) {
	sql := `ALTER TABLE orders ADD CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE NO ACTION`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect foreign key with ON DELETE NO ACTION
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
}

func TestHasFKLinter_AlterTableSelfReferencingForeignKey(t *testing.T) {
	sql := `ALTER TABLE employees ADD CONSTRAINT fk_manager FOREIGN KEY (manager_id) REFERENCES employees(id)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect self-referencing foreign key
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
	assert.Contains(t, violations[0].Message, "fk_manager")
}

func TestHasFKLinter_AlterTableForeignKeyReferencingDifferentDatabase(t *testing.T) {
	sql := `ALTER TABLE orders ADD CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES other_db.users(id)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect foreign key even if it references another database
	require.Len(t, violations, 1)
	assert.Equal(t, "has_foreign_key", violations[0].Linter.Name())
}

func TestHasFKLinter_AlterTableViolationStructure(t *testing.T) {
	sql := `ALTER TABLE orders ADD CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES users(id)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFKLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	v := violations[0]

	// Verify violation structure for ALTER TABLE
	assert.NotNil(t, v.Linter)
	assert.Equal(t, "has_foreign_key", v.Linter.Name())
	assert.Equal(t, SeverityWarning, v.Severity)
	assert.NotEmpty(t, v.Message)
	assert.Contains(t, v.Message, "Adding foreign key constraint")
	assert.NotNil(t, v.Location)
	assert.Equal(t, "orders", v.Location.Table)
	assert.NotNil(t, v.Location.Constraint)
	assert.Equal(t, "fk_user_id", *v.Location.Constraint)
	assert.Nil(t, v.Location.Column)
	assert.Nil(t, v.Location.Index)
}

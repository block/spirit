package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests for CREATE TABLE with lowercase names

func TestNameCaseLinter_LowercaseTableName(t *testing.T) {
	sql := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Lowercase table name - no violations
	assert.Empty(t, violations)
}

func TestNameCaseLinter_UppercaseTableName(t *testing.T) {
	sql := `CREATE TABLE USERS (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Uppercase table name should be detected
	require.Len(t, violations, 1)
	assert.Equal(t, "name_case", violations[0].Linter.Name())
	assert.Contains(t, violations[0].Message, "USERS")
	assert.Contains(t, violations[0].Message, "not lowercase")
	assert.Equal(t, "USERS", violations[0].Location.Table)
}

func TestNameCaseLinter_MixedCaseTableName(t *testing.T) {
	sql := `CREATE TABLE UserAccounts (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Mixed case table name should be detected
	require.Len(t, violations, 1)
	assert.Equal(t, "name_case", violations[0].Linter.Name())
	assert.Contains(t, violations[0].Message, "UserAccounts")
	assert.Contains(t, violations[0].Message, "not lowercase")
	assert.Equal(t, "UserAccounts", violations[0].Location.Table)
}

func TestNameCaseLinter_CamelCaseTableName(t *testing.T) {
	sql := `CREATE TABLE userAccounts (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Camel case table name should be detected
	require.Len(t, violations, 1)
	assert.Equal(t, "name_case", violations[0].Linter.Name())
	assert.Contains(t, violations[0].Message, "userAccounts")
	assert.Equal(t, "userAccounts", violations[0].Location.Table)
}

func TestNameCaseLinter_TableNameWithUnderscore(t *testing.T) {
	sql := `CREATE TABLE user_accounts (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Lowercase with underscore - no violations
	assert.Empty(t, violations)
}

func TestNameCaseLinter_TableNameWithNumbers(t *testing.T) {
	sql := `CREATE TABLE users2024 (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Lowercase with numbers - no violations
	assert.Empty(t, violations)
}

func TestNameCaseLinter_TableNameWithNumbersAndUppercase(t *testing.T) {
	sql := `CREATE TABLE Users2024 (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Mixed case with numbers should be detected
	require.Len(t, violations, 1)
	assert.Contains(t, violations[0].Message, "Users2024")
}

// Tests for multiple tables

func TestNameCaseLinter_MultipleTablesAllLowercase(t *testing.T) {
	sql1 := `CREATE TABLE users (id INT PRIMARY KEY)`
	stmts1, err := statement.New(sql1)
	require.NoError(t, err)

	sql2 := `CREATE TABLE orders (id INT PRIMARY KEY)`
	stmts2, err := statement.New(sql2)
	require.NoError(t, err)

	stmts := append(stmts1, stmts2...)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// All lowercase - no violations
	assert.Empty(t, violations)
}

func TestNameCaseLinter_MultipleTablesMixedCase(t *testing.T) {
	sql1 := `CREATE TABLE users (id INT PRIMARY KEY)`
	stmts1, err := statement.New(sql1)
	require.NoError(t, err)

	sql2 := `CREATE TABLE Orders (id INT PRIMARY KEY)`
	stmts2, err := statement.New(sql2)
	require.NoError(t, err)

	sql3 := `CREATE TABLE Products (id INT PRIMARY KEY)`
	stmts3, err := statement.New(sql3)
	require.NoError(t, err)

	stmts := append(stmts1, stmts2...)
	stmts = append(stmts, stmts3...)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect two violations
	require.Len(t, violations, 2)

	tables := []string{violations[0].Location.Table, violations[1].Location.Table}
	assert.Contains(t, tables, "Orders")
	assert.Contains(t, tables, "Products")
}

// Tests for existing tables

func TestNameCaseLinter_ExistingTableLowercase(t *testing.T) {
	sql := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	existingTable, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, nil)

	// Lowercase - no violations
	assert.Empty(t, violations)
}

func TestNameCaseLinter_ExistingTableUppercase(t *testing.T) {
	sql := `CREATE TABLE USERS (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	existingTable, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, nil)

	// Uppercase should be detected
	require.Len(t, violations, 1)
	assert.Equal(t, "USERS", violations[0].Location.Table)
}

func TestNameCaseLinter_ExistingTableMixedCase(t *testing.T) {
	sql := `CREATE TABLE UserAccounts (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	existingTable, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, nil)

	// Mixed case should be detected
	require.Len(t, violations, 1)
	assert.Equal(t, "UserAccounts", violations[0].Location.Table)
}

func TestNameCaseLinter_MultipleExistingTables(t *testing.T) {
	sql1 := `CREATE TABLE users (id INT PRIMARY KEY)`
	table1, err := statement.ParseCreateTable(sql1)
	require.NoError(t, err)

	sql2 := `CREATE TABLE Orders (id INT PRIMARY KEY)`
	table2, err := statement.ParseCreateTable(sql2)
	require.NoError(t, err)

	sql3 := `CREATE TABLE products (id INT PRIMARY KEY)`
	table3, err := statement.ParseCreateTable(sql3)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint([]*statement.CreateTable{table1, table2, table3}, nil)

	// Should detect one violation (Orders)
	require.Len(t, violations, 1)
	assert.Equal(t, "Orders", violations[0].Location.Table)
}

// Tests for ALTER TABLE RENAME

func TestNameCaseLinter_AlterTableRenameLowercase(t *testing.T) {
	sql := `ALTER TABLE users RENAME TO customers`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Renaming to lowercase - no violations
	assert.Empty(t, violations)
}

func TestNameCaseLinter_AlterTableRenameUppercase(t *testing.T) {
	sql := `ALTER TABLE users RENAME TO CUSTOMERS`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Renaming to uppercase should be detected
	require.Len(t, violations, 1)
	assert.Equal(t, "name_case", violations[0].Linter.Name())
	assert.Contains(t, violations[0].Message, "users")
	assert.Contains(t, violations[0].Message, "CUSTOMERS")
	assert.Contains(t, violations[0].Message, "not lowercase")
	assert.Equal(t, "users", violations[0].Location.Table)
}

func TestNameCaseLinter_AlterTableRenameMixedCase(t *testing.T) {
	sql := `ALTER TABLE users RENAME TO UserAccounts`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Renaming to mixed case should be detected
	require.Len(t, violations, 1)
	assert.Contains(t, violations[0].Message, "users")
	assert.Contains(t, violations[0].Message, "UserAccounts")
	assert.Equal(t, "users", violations[0].Location.Table)
}

func TestNameCaseLinter_AlterTableRenameCamelCase(t *testing.T) {
	sql := `ALTER TABLE users RENAME TO userAccounts`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Renaming to camel case should be detected
	require.Len(t, violations, 1)
	assert.Contains(t, violations[0].Message, "userAccounts")
}

func TestNameCaseLinter_AlterTableRenameWithUnderscore(t *testing.T) {
	sql := `ALTER TABLE users RENAME TO user_accounts`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Renaming to lowercase with underscore - no violations
	assert.Empty(t, violations)
}

func TestNameCaseLinter_AlterTableRenameWithNumbers(t *testing.T) {
	sql := `ALTER TABLE users RENAME TO users2024`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Renaming to lowercase with numbers - no violations
	assert.Empty(t, violations)
}

func TestNameCaseLinter_AlterTableRenameToSameName(t *testing.T) {
	sql := `ALTER TABLE users RENAME TO users`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Renaming to same name - no violations (no actual rename)
	assert.Empty(t, violations)
}

func TestNameCaseLinter_AlterTableRenameUppercaseToUppercase(t *testing.T) {
	sql := `ALTER TABLE USERS RENAME TO CUSTOMERS`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Renaming from uppercase to uppercase should be detected
	require.Len(t, violations, 1)
	assert.Contains(t, violations[0].Message, "CUSTOMERS")
}

func TestNameCaseLinter_AlterTableRenameUppercaseToLowercase(t *testing.T) {
	sql := `ALTER TABLE USERS RENAME TO users`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Renaming from uppercase to lowercase - no violations (fixing the issue)
	assert.Empty(t, violations)
}

func TestNameCaseLinter_AlterTableRenameMixedCaseToLowercase(t *testing.T) {
	sql := `ALTER TABLE UserAccounts RENAME TO user_accounts`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Renaming from mixed case to lowercase - no violations (fixing the issue)
	assert.Empty(t, violations)
}

// Tests for other ALTER TABLE operations (should not trigger violations)

func TestNameCaseLinter_AlterTableAddColumn(t *testing.T) {
	sql := `ALTER TABLE users ADD COLUMN email VARCHAR(255)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// ADD COLUMN should not trigger violations
	assert.Empty(t, violations)
}

func TestNameCaseLinter_AlterTableDropColumn(t *testing.T) {
	sql := `ALTER TABLE users DROP COLUMN email`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// DROP COLUMN should not trigger violations
	assert.Empty(t, violations)
}

func TestNameCaseLinter_AlterTableModifyColumn(t *testing.T) {
	sql := `ALTER TABLE users MODIFY COLUMN name VARCHAR(500)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// MODIFY COLUMN should not trigger violations
	assert.Empty(t, violations)
}

func TestNameCaseLinter_AlterTableAddIndex(t *testing.T) {
	sql := `ALTER TABLE users ADD INDEX idx_name (name)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// ADD INDEX should not trigger violations
	assert.Empty(t, violations)
}

// Tests for combined scenarios

func TestNameCaseLinter_ExistingAndNewTables(t *testing.T) {
	// Existing table with uppercase
	existingSQL := `CREATE TABLE USERS (id INT PRIMARY KEY)`
	existingTable, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	// New table with mixed case
	newSQL := `CREATE TABLE Orders (id INT PRIMARY KEY)`
	newStmts, err := statement.New(newSQL)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, newStmts)

	// Should detect both violations
	require.Len(t, violations, 2)

	tables := []string{violations[0].Location.Table, violations[1].Location.Table}
	assert.Contains(t, tables, "USERS")
	assert.Contains(t, tables, "Orders")
}

func TestNameCaseLinter_CreateAndRename(t *testing.T) {
	// Create table with lowercase
	createSQL := `CREATE TABLE users (id INT PRIMARY KEY)`
	createStmts, err := statement.New(createSQL)
	require.NoError(t, err)

	// Rename to uppercase
	renameSQL := `ALTER TABLE products RENAME TO PRODUCTS_NEW`
	renameStmts, err := statement.New(renameSQL)
	require.NoError(t, err)

	allStmts := append(createStmts, renameStmts...)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, allStmts)

	// Should only detect the rename violation
	require.Len(t, violations, 1)
	assert.Contains(t, violations[0].Message, "PRODUCTS_NEW")
}

func TestNameCaseLinter_MultipleRenames(t *testing.T) {
	sql1 := `ALTER TABLE users RENAME TO USERS_NEW`
	stmts1, err := statement.New(sql1)
	require.NoError(t, err)

	sql2 := `ALTER TABLE orders RENAME TO Orders_Archive`
	stmts2, err := statement.New(sql2)
	require.NoError(t, err)

	sql3 := `ALTER TABLE products RENAME TO product_catalog`
	stmts3, err := statement.New(sql3)
	require.NoError(t, err)

	allStmts := append(stmts1, stmts2...)
	allStmts = append(allStmts, stmts3...)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, allStmts)

	// Should detect two violations (not the lowercase one)
	require.Len(t, violations, 2)

	var foundUsers, foundOrders bool
	for _, v := range violations {
		if v.Location.Table == "users" {
			foundUsers = true
			assert.Contains(t, v.Message, "USERS_NEW")
		}
		if v.Location.Table == "orders" {
			foundOrders = true
			assert.Contains(t, v.Message, "Orders_Archive")
		}
	}
	assert.True(t, foundUsers)
	assert.True(t, foundOrders)
}

// Tests for edge cases

func TestNameCaseLinter_EmptyInput(t *testing.T) {
	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, nil)

	// No tables - no violations
	assert.Empty(t, violations)
}

func TestNameCaseLinter_EmptyTableList(t *testing.T) {
	linter := &NameCaseLinter{}
	violations := linter.Lint([]*statement.CreateTable{}, []*statement.AbstractStatement{})

	// Empty lists - no violations
	assert.Empty(t, violations)
}

func TestNameCaseLinter_SingleCharacterTableName(t *testing.T) {
	sql := `CREATE TABLE a (id INT PRIMARY KEY)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Single lowercase character - no violations
	assert.Empty(t, violations)
}

func TestNameCaseLinter_SingleCharacterUppercaseTableName(t *testing.T) {
	sql := `CREATE TABLE A (id INT PRIMARY KEY)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Single uppercase character should be detected
	require.Len(t, violations, 1)
	assert.Equal(t, "A", violations[0].Location.Table)
}

func TestNameCaseLinter_TableNameWithSpecialCharacters(t *testing.T) {
	// MySQL allows backticks for table names with special characters
	sql := "CREATE TABLE `user-accounts` (id INT PRIMARY KEY)"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Lowercase with special characters - no violations
	assert.Empty(t, violations)
}

// Tests for metadata

func TestNameCaseLinter_Metadata(t *testing.T) {
	linter := &NameCaseLinter{}

	assert.Equal(t, "name_case", linter.Name())
	assert.NotEmpty(t, linter.Description())
	assert.Contains(t, linter.Description(), "lowercase")
}

func TestNameCaseLinter_String(t *testing.T) {
	linter := &NameCaseLinter{}
	str := linter.String()

	// String() should return a non-empty string
	assert.NotEmpty(t, str)
}

// Tests for violation structure

func TestNameCaseLinter_ViolationStructure(t *testing.T) {
	sql := `CREATE TABLE USERS (id INT PRIMARY KEY)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	v := violations[0]

	// Verify violation structure
	assert.NotNil(t, v.Linter)
	assert.Equal(t, "name_case", v.Linter.Name())
	assert.NotEmpty(t, v.Message)
	assert.NotNil(t, v.Location)
	assert.Equal(t, "USERS", v.Location.Table)
	assert.Nil(t, v.Location.Column)
	assert.Nil(t, v.Location.Index)
	assert.Nil(t, v.Location.Constraint)
}

func TestNameCaseLinter_RenameViolationStructure(t *testing.T) {
	sql := `ALTER TABLE users RENAME TO USERS_NEW`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	v := violations[0]

	// Verify violation structure for rename
	assert.NotNil(t, v.Linter)
	assert.Equal(t, "name_case", v.Linter.Name())
	assert.NotEmpty(t, v.Message)
	assert.Contains(t, v.Message, "users")
	assert.Contains(t, v.Message, "USERS_NEW")
	assert.Contains(t, v.Message, "renamed")
	assert.NotNil(t, v.Location)
	assert.Equal(t, "users", v.Location.Table)
}

// Tests for complex scenarios

func TestNameCaseLinter_ComplexScenario(t *testing.T) {
	// Existing table 1 with uppercase
	existing1SQL := `CREATE TABLE USERS (id INT PRIMARY KEY)`
	existing1, err := statement.ParseCreateTable(existing1SQL)
	require.NoError(t, err)

	// Existing table 2 with lowercase
	existing2SQL := `CREATE TABLE orders (id INT PRIMARY KEY)`
	existing2, err := statement.ParseCreateTable(existing2SQL)
	require.NoError(t, err)

	// New table with mixed case
	newTableSQL := `CREATE TABLE Products (id INT PRIMARY KEY)`
	newTableStmts, err := statement.New(newTableSQL)
	require.NoError(t, err)

	// Rename to uppercase
	renameSQL := `ALTER TABLE inventory RENAME TO INVENTORY_NEW`
	renameStmts, err := statement.New(renameSQL)
	require.NoError(t, err)

	// Rename to lowercase (fixing)
	fixRenameSQL := `ALTER TABLE BadName RENAME TO good_name`
	fixRenameStmts, err := statement.New(fixRenameSQL)
	require.NoError(t, err)

	allChanges := append(newTableStmts, renameStmts...)
	allChanges = append(allChanges, fixRenameStmts...)

	linter := &NameCaseLinter{}
	violations := linter.Lint([]*statement.CreateTable{existing1, existing2}, allChanges)

	// Should detect:
	// 1. USERS (existing table)
	// 2. Products (new table)
	// 3. INVENTORY_NEW (rename to uppercase)
	// Should NOT detect:
	// - orders (lowercase existing)
	// - good_name (rename to lowercase)
	require.Len(t, violations, 3)

	var foundUsers, foundProducts, foundInventory bool
	for _, v := range violations {
		if v.Location.Table == "USERS" {
			foundUsers = true
		}
		if v.Location.Table == "Products" {
			foundProducts = true
		}
		if v.Location.Table == "inventory" {
			foundInventory = true
			assert.Contains(t, v.Message, "INVENTORY_NEW")
		}
	}
	assert.True(t, foundUsers, "Should find violation in USERS")
	assert.True(t, foundProducts, "Should find violation in Products")
	assert.True(t, foundInventory, "Should find violation in INVENTORY_NEW rename")
}

// Tests for ALTER TABLE with RENAME TABLE syntax variant

func TestNameCaseLinter_RenameTableStatement(t *testing.T) {
	// RENAME TABLE is a separate statement type, not ALTER TABLE
	sql := `RENAME TABLE users TO USERS_NEW`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// The linter currently only checks ALTER TABLE RENAME
	// TABLE statements might not be caught
	// This test documents the current behavior
	// If no violations, it means RENAME TABLE is not currently handled
	t.Logf("RENAME TABLE violations: %d", len(violations))
}

// Tests for table names with prefixes/suffixes

func TestNameCaseLinter_TableNameWithPrefix(t *testing.T) {
	sql := `CREATE TABLE tbl_users (id INT PRIMARY KEY)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Lowercase with prefix - no violations
	assert.Empty(t, violations)
}

func TestNameCaseLinter_TableNameWithPrefixUppercase(t *testing.T) {
	sql := `CREATE TABLE TBL_users (id INT PRIMARY KEY)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Mixed case with prefix should be detected
	require.Len(t, violations, 1)
	assert.Equal(t, "TBL_users", violations[0].Location.Table)
}

func TestNameCaseLinter_TableNameWithSuffix(t *testing.T) {
	sql := `CREATE TABLE users_tmp (id INT PRIMARY KEY)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Lowercase with suffix - no violations
	assert.Empty(t, violations)
}

func TestNameCaseLinter_TableNameWithSuffixUppercase(t *testing.T) {
	sql := `CREATE TABLE users_TMP (id INT PRIMARY KEY)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Mixed case with suffix should be detected
	require.Len(t, violations, 1)
	assert.Equal(t, "users_TMP", violations[0].Location.Table)
}

// Test for very long table names

func TestNameCaseLinter_LongTableNameLowercase(t *testing.T) {
	sql := `CREATE TABLE very_long_table_name_with_many_underscores_and_words (id INT PRIMARY KEY)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Long lowercase name - no violations
	assert.Empty(t, violations)
}

func TestNameCaseLinter_LongTableNameMixedCase(t *testing.T) {
	sql := `CREATE TABLE VeryLongTableNameWithManyCamelCaseWords (id INT PRIMARY KEY)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &NameCaseLinter{}
	violations := linter.Lint(nil, stmts)

	// Long mixed case name should be detected
	require.Len(t, violations, 1)
	assert.Equal(t, "VeryLongTableNameWithManyCamelCaseWords", violations[0].Location.Table)
}

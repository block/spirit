package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/require"
)

func TestAllowedCharacterSet(t *testing.T) {
	sql := `
CREATE TABLE t1 (
	id int unsigned NOT NULL AUTO_INCREMENT,
	col1 varchar(255) character set utf8mb4
) character set utf8mb4`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestDisallowedCharacterSet(t *testing.T) {
	sql := `
	CREATE TABLE t1 (
	id int unsigned NOT NULL
	) character set latin1`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)
	require.Contains(t, violations[0].Message, "Character set \"latin1\" given for table \"t1\" is not allowed")
	require.Contains(t, *violations[0].Suggestion, "Use a supported character set: utf8mb4")
}

// Tests for column-level character sets

func TestAllowedColumnCharacterSet(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255) CHARACTER SET utf8mb4
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestDisallowedColumnCharacterSet(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255) CHARACTER SET latin1
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)
	require.Contains(t, violations[0].Message, "Column \"name\" has unsupported character set: \"latin1\"")
	require.Equal(t, SeverityWarning, violations[0].Severity)
	require.Equal(t, "t1", violations[0].Location.Table)
	require.NotNil(t, violations[0].Location.Column)
	require.Equal(t, "name", *violations[0].Location.Column)
}

func TestMultipleColumnsWithDisallowedCharset(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255) CHARACTER SET latin1,
		description TEXT CHARACTER SET latin1
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 2)

	// Check both columns are reported
	columnNames := []string{*violations[0].Location.Column, *violations[1].Location.Column}
	require.Contains(t, columnNames, "name")
	require.Contains(t, columnNames, "description")
}

func TestMixedColumnCharsets(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255) CHARACTER SET utf8mb4,
		legacy_field VARCHAR(255) CHARACTER SET latin1
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)
	require.Equal(t, "legacy_field", *violations[0].Location.Column)
}

// Tests for table and column character sets together

func TestTableAndColumnBothAllowed(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255) CHARACTER SET utf8mb4
	) CHARACTER SET utf8mb4`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestTableAllowedColumnDisallowed(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255) CHARACTER SET latin1
	) CHARACTER SET utf8mb4`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)
	require.Contains(t, violations[0].Message, "Column \"name\"")
	require.Contains(t, violations[0].Message, "latin1")
}

func TestTableDisallowedColumnAllowed(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255) CHARACTER SET utf8mb4
	) CHARACTER SET latin1`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)
	require.Contains(t, violations[0].Message, "Character set \"latin1\" given for table \"t1\"")
}

func TestTableAndColumnBothDisallowed(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255) CHARACTER SET latin1
	) CHARACTER SET latin1`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 2)

	// One for table, one for column
	var hasTableViolation, hasColumnViolation bool
	for _, v := range violations {
		if v.Location.Column == nil {
			hasTableViolation = true
		} else {
			hasColumnViolation = true
		}
	}
	require.True(t, hasTableViolation)
	require.True(t, hasColumnViolation)
}

// Tests for multiple allowed charsets

func TestMultipleAllowedCharsets(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255) CHARACTER SET utf8mb4,
		legacy VARCHAR(255) CHARACTER SET utf8
	) CHARACTER SET utf8mb4`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4", "utf8"}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestMultipleAllowedCharsetsOneDisallowed(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255) CHARACTER SET utf8mb4,
		legacy VARCHAR(255) CHARACTER SET latin1
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4", "utf8"}}
	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)
	require.Equal(t, "legacy", *violations[0].Location.Column)
}

// Tests for columns without explicit charset

func TestColumnWithoutCharset(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestTableWithoutCharset(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255)
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

// Tests for different column types

func TestCharColumnWithCharset(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		code CHAR(10) CHARACTER SET utf8mb4
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestTextColumnWithCharset(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		content TEXT CHARACTER SET utf8mb4
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestTextColumnWithDisallowedCharset(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		content TEXT CHARACTER SET latin1
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)
	require.Equal(t, "content", *violations[0].Location.Column)
}

func TestEnumColumnWithCharset(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		status ENUM('active', 'inactive') CHARACTER SET utf8mb4
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestSetColumnWithCharset(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		permissions SET('read', 'write', 'delete') CHARACTER SET utf8mb4
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

// Tests for non-string columns (should not have charset)

func TestIntColumnNoCharset(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		count INT
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestDateColumnNoCharset(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		created_at DATETIME
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

// Tests for multiple tables

func TestMultipleTablesAllAllowed(t *testing.T) {
	sql1 := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255) CHARACTER SET utf8mb4
	) CHARACTER SET utf8mb4`
	stmts1, err := statement.New(sql1)
	require.NoError(t, err)

	sql2 := `CREATE TABLE t2 (
		id INT PRIMARY KEY,
		description TEXT CHARACTER SET utf8mb4
	) CHARACTER SET utf8mb4`
	stmts2, err := statement.New(sql2)
	require.NoError(t, err)

	stmts := append(stmts1, stmts2...)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestMultipleTablesSomeDisallowed(t *testing.T) {
	sql1 := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255) CHARACTER SET utf8mb4
	) CHARACTER SET utf8mb4`
	stmts1, err := statement.New(sql1)
	require.NoError(t, err)

	sql2 := `CREATE TABLE t2 (
		id INT PRIMARY KEY,
		description TEXT CHARACTER SET latin1
	) CHARACTER SET latin1`
	stmts2, err := statement.New(sql2)
	require.NoError(t, err)

	stmts := append(stmts1, stmts2...)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 2) // One for table, one for column

	// Check that both violations are for t2
	for _, v := range violations {
		require.Equal(t, "t2", v.Location.Table)
	}
}

// Tests for Configure method

func TestConfigureSingleCharset(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255) CHARACTER SET latin1
	) CHARACTER SET latin1`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{}
	err = linter.Configure(map[string]string{
		"charsets": "latin1",
	})
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestConfigureMultipleCharsets(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255) CHARACTER SET utf8,
		description TEXT CHARACTER SET utf8mb4
	) CHARACTER SET latin1`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{}
	err = linter.Configure(map[string]string{
		"charsets": "utf8,utf8mb4,latin1",
	})
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestConfigureInvalidKey(t *testing.T) {
	linter := AllowCharset{}
	err := linter.Configure(map[string]string{
		"invalid_key": "value",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown configuration key")
}

func TestDefaultConfig(t *testing.T) {
	linter := AllowCharset{}
	config := linter.DefaultConfig()
	require.Contains(t, config, "charsets")
	require.Equal(t, "utf8mb4", config["charsets"])
}

// Tests for existing tables parameter

func TestExistingTableWithAllowedCharset(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255) CHARACTER SET utf8mb4
	) CHARACTER SET utf8mb4`
	existingTable, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, nil)
	require.Empty(t, violations)
}

func TestExistingTableWithDisallowedCharset(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255) CHARACTER SET latin1
	) CHARACTER SET latin1`
	existingTable, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, nil)
	require.Len(t, violations, 2) // Table and column
}

func TestExistingAndNewTables(t *testing.T) {
	existingSQL := `CREATE TABLE existing (
		id INT PRIMARY KEY,
		name VARCHAR(255) CHARACTER SET utf8mb4
	) CHARACTER SET utf8mb4`
	existingTable, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	newSQL := `CREATE TABLE new_table (
		id INT PRIMARY KEY,
		description TEXT CHARACTER SET latin1
	) CHARACTER SET latin1`
	newStmts, err := statement.New(newSQL)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, newStmts)
	require.Len(t, violations, 2) // Both from new_table

	for _, v := range violations {
		require.Equal(t, "new_table", v.Location.Table)
	}
}

// Tests for complex scenarios

func TestComplexTableMixedCharsets(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255) CHARACTER SET utf8mb4,
		legacy_name VARCHAR(255) CHARACTER SET latin1,
		description TEXT CHARACTER SET utf8mb4,
		legacy_desc TEXT CHARACTER SET latin1,
		code CHAR(10) CHARACTER SET ascii
	) CHARACTER SET utf8mb4`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 3) // legacy_name, legacy_desc, code

	columnNames := make([]string, len(violations))
	for i, v := range violations {
		columnNames[i] = *v.Location.Column
	}
	require.Contains(t, columnNames, "legacy_name")
	require.Contains(t, columnNames, "legacy_desc")
	require.Contains(t, columnNames, "code")
}

func TestAllCommonCharsets(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		utf8mb4_col VARCHAR(255) CHARACTER SET utf8mb4,
		utf8_col VARCHAR(255) CHARACTER SET utf8,
		latin1_col VARCHAR(255) CHARACTER SET latin1,
		ascii_col VARCHAR(255) CHARACTER SET ascii
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4", "utf8", "latin1", "ascii"}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestRestrictiveCharsetPolicy(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		utf8mb4_col VARCHAR(255) CHARACTER SET utf8mb4,
		utf8_col VARCHAR(255) CHARACTER SET utf8
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	// Only allow utf8mb4
	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)
	require.Equal(t, "utf8_col", *violations[0].Location.Column)
}

// Tests for violation structure

func TestViolationStructureTableLevel(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY
	) CHARACTER SET latin1`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)

	v := violations[0]
	require.NotNil(t, v.Linter)
	require.Equal(t, "allow_charset", v.Linter.Name())
	require.NotEmpty(t, v.Message)
	require.NotNil(t, v.Location)
	require.Equal(t, "t1", v.Location.Table)
	require.Nil(t, v.Location.Column)
	require.NotNil(t, v.Suggestion)
	require.Contains(t, *v.Suggestion, "utf8mb4")
}

func TestViolationStructureColumnLevel(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255) CHARACTER SET latin1
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)

	v := violations[0]
	require.NotNil(t, v.Linter)
	require.Equal(t, "allow_charset", v.Linter.Name())
	require.Equal(t, SeverityWarning, v.Severity)
	require.NotEmpty(t, v.Message)
	require.NotNil(t, v.Location)
	require.Equal(t, "t1", v.Location.Table)
	require.NotNil(t, v.Location.Column)
	require.Equal(t, "name", *v.Location.Column)
	require.NotNil(t, v.Suggestion)
}

// Tests for metadata

func TestLinterMetadata(t *testing.T) {
	linter := AllowCharset{}
	require.Equal(t, "allow_charset", linter.Name())
	require.NotEmpty(t, linter.Description())
	require.Contains(t, linter.Description(), "character set")
}

func TestLinterString(t *testing.T) {
	linter := AllowCharset{}
	str := linter.String()
	require.NotEmpty(t, str)
}

// Tests for edge cases

func TestEmptyTable(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestTableWithOnlyIntColumns(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		count INT,
		total BIGINT
	) CHARACTER SET utf8mb4`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{"utf8mb4"}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestEmptyCharsetsList(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255) CHARACTER SET utf8mb4
	)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := AllowCharset{charsets: []string{}}
	violations := linter.Lint(nil, stmts)
	// With empty allowed list, any charset should violate
	require.Len(t, violations, 1)
}

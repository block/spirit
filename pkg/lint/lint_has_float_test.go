package lint

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHasFloatLinter_NoFloatOrDouble(t *testing.T) {
	sql := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255),
		age INT,
		balance DECIMAL(10, 2)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// No FLOAT or DOUBLE - no violations
	assert.Empty(t, violations)
}

func TestHasFloatLinter_FloatColumn(t *testing.T) {
	sql := `CREATE TABLE measurements (
		id BIGINT UNSIGNED PRIMARY KEY,
		temperature FLOAT,
		humidity INT
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// FLOAT should be detected
	require.Len(t, violations, 1)
	assert.Equal(t, "has_float", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "temperature")
	assert.Contains(t, violations[0].Message, "measurements")
	// The parser returns lowercase type names
	assert.Contains(t, violations[0].Message, "float")
	assert.Equal(t, "measurements", violations[0].Location.Table)
	assert.NotNil(t, violations[0].Location.Column)
	assert.Equal(t, "temperature", *violations[0].Location.Column)
}

func TestHasFloatLinter_DoubleColumn(t *testing.T) {
	sql := `CREATE TABLE coordinates (
		id BIGINT UNSIGNED PRIMARY KEY,
		latitude DOUBLE,
		longitude DOUBLE
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Both DOUBLE columns should be detected
	require.Len(t, violations, 2)

	// First violation
	assert.Equal(t, "has_float", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "latitude")
	assert.Contains(t, violations[0].Message, "coordinates")
	// The parser returns lowercase type names
	assert.Contains(t, violations[0].Message, "double")
	assert.Equal(t, "coordinates", violations[0].Location.Table)
	assert.NotNil(t, violations[0].Location.Column)
	assert.Equal(t, "latitude", *violations[0].Location.Column)

	// Second violation
	assert.Equal(t, "has_float", violations[1].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[1].Severity)
	assert.Contains(t, violations[1].Message, "longitude")
	assert.Contains(t, violations[1].Message, "coordinates")
	// The parser returns lowercase type names
	assert.Contains(t, violations[1].Message, "double")
	assert.Equal(t, "coordinates", violations[1].Location.Table)
	assert.NotNil(t, violations[1].Location.Column)
	assert.Equal(t, "longitude", *violations[1].Location.Column)
}

func TestHasFloatLinter_MixedFloatAndDouble(t *testing.T) {
	sql := `CREATE TABLE metrics (
		id BIGINT UNSIGNED PRIMARY KEY,
		value1 FLOAT,
		value2 DOUBLE,
		value3 DECIMAL(10, 2)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Both FLOAT and DOUBLE should be detected, but not DECIMAL
	require.Len(t, violations, 2)

	// Check both violations are present
	columnNames := []string{*violations[0].Location.Column, *violations[1].Location.Column}
	assert.Contains(t, columnNames, "value1")
	assert.Contains(t, columnNames, "value2")
}

func TestHasFloatLinter_CaseInsensitive(t *testing.T) {
	// Test lowercase
	sql := `CREATE TABLE test1 (
		id BIGINT UNSIGNED PRIMARY KEY,
		value float
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Should detect lowercase 'float'
	require.Len(t, violations, 1)
	assert.Contains(t, violations[0].Message, "value")
}

func TestHasFloatLinter_CaseInsensitiveDouble(t *testing.T) {
	// Test lowercase double
	sql := `CREATE TABLE test2 (
		id BIGINT UNSIGNED PRIMARY KEY,
		value double
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// Should detect lowercase 'double'
	require.Len(t, violations, 1)
	assert.Contains(t, violations[0].Message, "value")
}

func TestHasFloatLinter_MultipleTables(t *testing.T) {
	sql1 := `CREATE TABLE table1 (
		id BIGINT UNSIGNED PRIMARY KEY,
		value FLOAT
	)`
	ct1, err := statement.ParseCreateTable(sql1)
	require.NoError(t, err)

	sql2 := `CREATE TABLE table2 (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	ct2, err := statement.ParseCreateTable(sql2)
	require.NoError(t, err)

	sql3 := `CREATE TABLE table3 (
		id BIGINT UNSIGNED PRIMARY KEY,
		score DOUBLE
	)`
	ct3, err := statement.ParseCreateTable(sql3)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct1, ct2, ct3}, nil)

	// Should find violations in table1 and table3, but not table2
	require.Len(t, violations, 2)

	tables := []string{violations[0].Location.Table, violations[1].Location.Table}
	assert.Contains(t, tables, "table1")
	assert.Contains(t, tables, "table3")
}

func TestHasFloatLinter_FloatWithPrecision(t *testing.T) {
	sql := `CREATE TABLE measurements (
		id BIGINT UNSIGNED PRIMARY KEY,
		value FLOAT(7, 4)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// FLOAT with precision should still be detected
	require.Len(t, violations, 1)
	assert.Contains(t, violations[0].Message, "value")
	// The parser may normalize the type name
	assert.Contains(t, strings.ToLower(violations[0].Message), "float")
}

func TestHasFloatLinter_DoubleWithPrecision(t *testing.T) {
	sql := `CREATE TABLE measurements (
		id BIGINT UNSIGNED PRIMARY KEY,
		value DOUBLE(15, 8)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// DOUBLE with precision should still be detected
	require.Len(t, violations, 1)
	assert.Contains(t, violations[0].Message, "value")
	// The parser may normalize the type name
	assert.Contains(t, strings.ToLower(violations[0].Message), "double")
}

func TestHasFloatLinter_EmptyInput(t *testing.T) {
	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, nil)

	// No tables - no violations
	assert.Empty(t, violations)
}

func TestHasFloatLinter_EmptyTableList(t *testing.T) {
	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{}, nil)

	// Empty list - no violations
	assert.Empty(t, violations)
}

func TestHasFloatLinter_TableWithNoColumns(t *testing.T) {
	// This is a theoretical test case - in practice, tables always have columns
	ct := &statement.CreateTable{
		TableName: "empty_table",
		Columns:   statement.Columns{},
	}

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// No columns - no violations
	assert.Empty(t, violations)
}

func TestHasFloatLinter_SimilarTypeNames(t *testing.T) {
	// Test that similar type names don't trigger false positives
	sql := `CREATE TABLE test (
		id BIGINT UNSIGNED PRIMARY KEY,
		data VARCHAR(255),
		amount DECIMAL(10, 2),
		count INT,
		flag BOOLEAN
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// None of these types should trigger violations
	assert.Empty(t, violations)
}

func TestHasFloatLinter_Integration(t *testing.T) {
	Reset()
	Register(&HasFloatLinter{})

	sql := `CREATE TABLE measurements (
		id BIGINT UNSIGNED PRIMARY KEY,
		temperature FLOAT
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	violations, err := RunLinters([]*statement.CreateTable{ct}, nil, Config{})
	require.NoError(t, err)

	// Should find at least one violation (from HasFloatLinter)
	var hasFloatViolations []Violation
	for _, v := range violations {
		if v.Linter.Name() == "has_float" {
			hasFloatViolations = append(hasFloatViolations, v)
		}
	}
	require.Len(t, hasFloatViolations, 1)
	assert.Equal(t, "has_float", hasFloatViolations[0].Linter.Name())
}

func TestHasFloatLinter_IntegrationDisabled(t *testing.T) {
	Reset()
	Register(&HasFloatLinter{})

	sql := `CREATE TABLE measurements (
		id BIGINT UNSIGNED PRIMARY KEY,
		temperature FLOAT
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	violations, err := RunLinters([]*statement.CreateTable{ct}, nil, Config{
		Enabled: map[string]bool{
			"has_float": false,
		},
	})
	require.NoError(t, err)

	// HasFloatLinter is disabled, so no violations from it
	for _, v := range violations {
		assert.NotEqual(t, "has_float", v.Linter.Name())
	}
}

func TestHasFloatLinter_Metadata(t *testing.T) {
	linter := &HasFloatLinter{}

	assert.Equal(t, "has_float", linter.Name())
	assert.NotEmpty(t, linter.Description())
	assert.Contains(t, linter.Description(), "FLOAT")
	assert.Contains(t, linter.Description(), "DOUBLE")
}

func TestHasFloatLinter_AllFloatTypes(t *testing.T) {
	// Test all variations of FLOAT and DOUBLE
	sql := `CREATE TABLE all_floats (
		id BIGINT UNSIGNED PRIMARY KEY,
		f1 FLOAT,
		f2 FLOAT(7, 4),
		d1 DOUBLE,
		d2 DOUBLE(15, 8)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// All 4 float/double columns should be detected
	require.Len(t, violations, 4)

	// Verify all violations are warnings
	for _, v := range violations {
		assert.Equal(t, SeverityWarning, v.Severity)
		assert.Equal(t, "all_floats", v.Location.Table)
	}
}

func TestHasFloatLinter_RealType(t *testing.T) {
	// REAL is a synonym for DOUBLE in MySQL
	// However, the parser might normalize it, so this test checks actual behavior
	sql := `CREATE TABLE test (
		id BIGINT UNSIGNED PRIMARY KEY,
		value REAL
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// If the parser normalizes REAL to DOUBLE, it should be detected
	// If not, this test documents that behavior
	// The linter currently only checks for FLOAT and DOUBLE explicitly
	if len(violations) > 0 {
		assert.Contains(t, violations[0].Message, "value")
	}
}

func TestHasFloatLinter_DecimalNotDetected(t *testing.T) {
	// Ensure DECIMAL types are not flagged (they are exact numeric types)
	sql := `CREATE TABLE financial (
		id BIGINT UNSIGNED PRIMARY KEY,
		price DECIMAL(10, 2),
		tax DECIMAL(5, 2),
		total NUMERIC(12, 2)
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// DECIMAL and NUMERIC should not be detected
	assert.Empty(t, violations)
}

func TestHasFloatLinter_MixedCase(t *testing.T) {
	// Test various case combinations
	sql := `CREATE TABLE test (
		id BIGINT UNSIGNED PRIMARY KEY,
		v1 Float,
		v2 FLOAT,
		v3 float,
		v4 Double,
		v5 DOUBLE,
		v6 double
	)`
	ct, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, nil)

	// All 6 columns should be detected regardless of case
	require.Len(t, violations, 6)
}

func TestHasFloatLinter_String(t *testing.T) {
	linter := &HasFloatLinter{}
	str := linter.String()

	// String() should return a non-empty string
	assert.NotEmpty(t, str)
}

// Tests for existingTables parameter

func TestHasFloatLinter_ExistingTableWithFloat(t *testing.T) {
	sql := `CREATE TABLE measurements (
		id BIGINT UNSIGNED PRIMARY KEY,
		temperature FLOAT,
		humidity INT
	)`
	existingTable, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, nil)

	// Should detect FLOAT in existing table
	require.Len(t, violations, 1)
	assert.Equal(t, "has_float", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "temperature")
	assert.Contains(t, violations[0].Message, "measurements")
	assert.Equal(t, "measurements", violations[0].Location.Table)
	assert.Equal(t, "temperature", *violations[0].Location.Column)
}

func TestHasFloatLinter_ExistingTableWithDouble(t *testing.T) {
	sql := `CREATE TABLE coordinates (
		id BIGINT UNSIGNED PRIMARY KEY,
		latitude DOUBLE,
		longitude DOUBLE
	)`
	existingTable, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, nil)

	// Should detect both DOUBLE columns in existing table
	require.Len(t, violations, 2)

	columnNames := []string{*violations[0].Location.Column, *violations[1].Location.Column}
	assert.Contains(t, columnNames, "latitude")
	assert.Contains(t, columnNames, "longitude")
}

func TestHasFloatLinter_ExistingTableNoFloat(t *testing.T) {
	sql := `CREATE TABLE users (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255),
		balance DECIMAL(10, 2)
	)`
	existingTable, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, nil)

	// No FLOAT or DOUBLE - no violations
	assert.Empty(t, violations)
}

func TestHasFloatLinter_MultipleExistingTables(t *testing.T) {
	sql1 := `CREATE TABLE table1 (
		id BIGINT UNSIGNED PRIMARY KEY,
		value FLOAT
	)`
	table1, err := statement.ParseCreateTable(sql1)
	require.NoError(t, err)

	sql2 := `CREATE TABLE table2 (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	table2, err := statement.ParseCreateTable(sql2)
	require.NoError(t, err)

	sql3 := `CREATE TABLE table3 (
		id BIGINT UNSIGNED PRIMARY KEY,
		score DOUBLE
	)`
	table3, err := statement.ParseCreateTable(sql3)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{table1, table2, table3}, nil)

	// Should find violations in table1 and table3
	require.Len(t, violations, 2)

	tables := []string{violations[0].Location.Table, violations[1].Location.Table}
	assert.Contains(t, tables, "table1")
	assert.Contains(t, tables, "table3")
}

// Tests for ALTER TABLE ADD COLUMN

func TestHasFloatLinter_AlterTableAddFloatColumn(t *testing.T) {
	sql := `ALTER TABLE measurements ADD COLUMN temperature FLOAT`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect FLOAT in ALTER TABLE ADD COLUMN
	require.Len(t, violations, 1)
	assert.Equal(t, "has_float", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "temperature")
	assert.Contains(t, violations[0].Message, "measurements")
	assert.Equal(t, "measurements", violations[0].Location.Table)
	assert.Equal(t, "temperature", *violations[0].Location.Column)
}

func TestHasFloatLinter_AlterTableAddDoubleColumn(t *testing.T) {
	sql := `ALTER TABLE coordinates ADD COLUMN latitude DOUBLE`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect DOUBLE in ALTER TABLE ADD COLUMN
	require.Len(t, violations, 1)
	assert.Equal(t, "has_float", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "latitude")
	assert.Equal(t, "coordinates", violations[0].Location.Table)
}

func TestHasFloatLinter_AlterTableAddMultipleFloatColumns(t *testing.T) {
	sql := `ALTER TABLE measurements 
		ADD COLUMN temperature FLOAT,
		ADD COLUMN pressure DOUBLE`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect both FLOAT and DOUBLE columns
	require.Len(t, violations, 2)

	columnNames := []string{*violations[0].Location.Column, *violations[1].Location.Column}
	assert.Contains(t, columnNames, "temperature")
	assert.Contains(t, columnNames, "pressure")
}

func TestHasFloatLinter_AlterTableAddNonFloatColumn(t *testing.T) {
	sql := `ALTER TABLE users ADD COLUMN age INT`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should not detect non-float columns
	assert.Empty(t, violations)
}

func TestHasFloatLinter_AlterTableAddMixedColumns(t *testing.T) {
	sql := `ALTER TABLE measurements 
		ADD COLUMN name VARCHAR(255),
		ADD COLUMN temperature FLOAT,
		ADD COLUMN count INT`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should only detect the FLOAT column
	require.Len(t, violations, 1)
	assert.Equal(t, "temperature", *violations[0].Location.Column)
}

func TestHasFloatLinter_AlterTableAddFloatWithPrecision(t *testing.T) {
	sql := `ALTER TABLE measurements ADD COLUMN value FLOAT(7, 4)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect FLOAT with precision
	require.Len(t, violations, 1)
	assert.Equal(t, "value", *violations[0].Location.Column)
}

func TestHasFloatLinter_AlterTableAddDoubleWithPrecision(t *testing.T) {
	sql := `ALTER TABLE measurements ADD COLUMN value DOUBLE(15, 8)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect DOUBLE with precision
	require.Len(t, violations, 1)
	assert.Equal(t, "value", *violations[0].Location.Column)
}

// Tests for combined scenarios

func TestHasFloatLinter_ExistingTableAndNewTable(t *testing.T) {
	// Existing table with FLOAT
	existingSQL := `CREATE TABLE existing_table (
		id BIGINT UNSIGNED PRIMARY KEY,
		old_value FLOAT
	)`
	existingTable, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	// New table with DOUBLE
	newSQL := `CREATE TABLE new_table (
		id BIGINT UNSIGNED PRIMARY KEY,
		new_value DOUBLE
	)`
	newStmts, err := statement.New(newSQL)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, newStmts)

	// Should detect both violations
	require.Len(t, violations, 2)

	tables := []string{violations[0].Location.Table, violations[1].Location.Table}
	assert.Contains(t, tables, "existing_table")
	assert.Contains(t, tables, "new_table")
}

func TestHasFloatLinter_ExistingTableAndAlterTable(t *testing.T) {
	// Existing table with FLOAT
	existingSQL := `CREATE TABLE measurements (
		id BIGINT UNSIGNED PRIMARY KEY,
		temperature FLOAT
	)`
	existingTable, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	// ALTER TABLE to add DOUBLE column
	alterSQL := `ALTER TABLE measurements ADD COLUMN pressure DOUBLE`
	alterStmts, err := statement.New(alterSQL)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, alterStmts)

	// Should detect both: existing FLOAT and new DOUBLE
	require.Len(t, violations, 2)

	columnNames := []string{*violations[0].Location.Column, *violations[1].Location.Column}
	assert.Contains(t, columnNames, "temperature")
	assert.Contains(t, columnNames, "pressure")
}

func TestHasFloatLinter_MultipleAlterTableStatements(t *testing.T) {
	sql1 := `ALTER TABLE table1 ADD COLUMN value1 FLOAT`
	stmts1, err := statement.New(sql1)
	require.NoError(t, err)

	sql2 := `ALTER TABLE table2 ADD COLUMN value2 DOUBLE`
	stmts2, err := statement.New(sql2)
	require.NoError(t, err)

	sql3 := `ALTER TABLE table3 ADD COLUMN value3 INT`
	stmts3, err := statement.New(sql3)
	require.NoError(t, err)

	stmts := append(stmts1, stmts2...)
	stmts = append(stmts, stmts3...)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect FLOAT and DOUBLE, but not INT
	require.Len(t, violations, 2)

	tables := []string{violations[0].Location.Table, violations[1].Location.Table}
	assert.Contains(t, tables, "table1")
	assert.Contains(t, tables, "table2")
}

func TestHasFloatLinter_ComplexScenario(t *testing.T) {
	// Existing table 1 with FLOAT
	existing1SQL := `CREATE TABLE existing1 (
		id BIGINT UNSIGNED PRIMARY KEY,
		value FLOAT
	)`
	existing1, err := statement.ParseCreateTable(existing1SQL)
	require.NoError(t, err)

	// Existing table 2 without FLOAT
	existing2SQL := `CREATE TABLE existing2 (
		id BIGINT UNSIGNED PRIMARY KEY,
		name VARCHAR(255)
	)`
	existing2, err := statement.ParseCreateTable(existing2SQL)
	require.NoError(t, err)

	// New table with DOUBLE
	newTableSQL := `CREATE TABLE new_table (
		id BIGINT UNSIGNED PRIMARY KEY,
		score DOUBLE
	)`
	newTableStmts, err := statement.New(newTableSQL)
	require.NoError(t, err)

	// ALTER TABLE to add FLOAT
	alterSQL := `ALTER TABLE existing2 ADD COLUMN temperature FLOAT`
	alterStmts, err := statement.New(alterSQL)
	require.NoError(t, err)

	// Combine all changes
	allChanges := append(newTableStmts, alterStmts...)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{existing1, existing2}, allChanges)

	// Should detect:
	// 1. existing1.value (FLOAT)
	// 2. new_table.score (DOUBLE)
	// 3. existing2.temperature (FLOAT via ALTER)
	require.Len(t, violations, 3)

	// Verify all three violations are present
	var foundExisting1, foundNewTable, foundAlter bool
	for _, v := range violations {
		if v.Location.Table == "existing1" && *v.Location.Column == "value" {
			foundExisting1 = true
		}
		if v.Location.Table == "new_table" && *v.Location.Column == "score" {
			foundNewTable = true
		}
		if v.Location.Table == "existing2" && *v.Location.Column == "temperature" {
			foundAlter = true
		}
	}
	assert.True(t, foundExisting1, "Should find violation in existing1")
	assert.True(t, foundNewTable, "Should find violation in new_table")
	assert.True(t, foundAlter, "Should find violation in ALTER TABLE")
}

func TestHasFloatLinter_AlterTableAddColumnWithDefault(t *testing.T) {
	sql := `ALTER TABLE measurements ADD COLUMN temperature FLOAT DEFAULT 0.0`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect FLOAT even with DEFAULT value
	require.Len(t, violations, 1)
	assert.Equal(t, "temperature", *violations[0].Location.Column)
}

func TestHasFloatLinter_AlterTableAddColumnNotNull(t *testing.T) {
	sql := `ALTER TABLE measurements ADD COLUMN temperature FLOAT NOT NULL`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect FLOAT even with NOT NULL constraint
	require.Len(t, violations, 1)
	assert.Equal(t, "temperature", *violations[0].Location.Column)
}

func TestHasFloatLinter_AlterTableAddColumnAfter(t *testing.T) {
	sql := `ALTER TABLE measurements ADD COLUMN temperature FLOAT AFTER id`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect FLOAT even with AFTER clause
	require.Len(t, violations, 1)
	assert.Equal(t, "temperature", *violations[0].Location.Column)
}

func TestHasFloatLinter_AlterTableAddColumnFirst(t *testing.T) {
	sql := `ALTER TABLE measurements ADD COLUMN temperature FLOAT FIRST`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect FLOAT even with FIRST clause
	require.Len(t, violations, 1)
	assert.Equal(t, "temperature", *violations[0].Location.Column)
}

// Tests for ALTER TABLE MODIFY COLUMN

func TestHasFloatLinter_AlterTableModifyColumnToFloat(t *testing.T) {
	sql := `ALTER TABLE measurements MODIFY COLUMN temperature FLOAT`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect MODIFY COLUMN to FLOAT
	require.Len(t, violations, 1)
	assert.Equal(t, "has_float", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "temperature")
	assert.Contains(t, violations[0].Message, "modified")
	assert.Equal(t, "measurements", violations[0].Location.Table)
	assert.Equal(t, "temperature", *violations[0].Location.Column)
}

func TestHasFloatLinter_AlterTableModifyColumnToDouble(t *testing.T) {
	sql := `ALTER TABLE coordinates MODIFY COLUMN latitude DOUBLE`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect MODIFY COLUMN to DOUBLE
	require.Len(t, violations, 1)
	assert.Equal(t, "has_float", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "latitude")
	assert.Contains(t, violations[0].Message, "modified")
	assert.Equal(t, "coordinates", violations[0].Location.Table)
}

func TestHasFloatLinter_AlterTableModifyColumnToNonFloat(t *testing.T) {
	sql := `ALTER TABLE measurements MODIFY COLUMN temperature DECIMAL(10, 2)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should not detect MODIFY COLUMN to DECIMAL
	assert.Empty(t, violations)
}

func TestHasFloatLinter_AlterTableModifyColumnFloatWithPrecision(t *testing.T) {
	sql := `ALTER TABLE measurements MODIFY COLUMN value FLOAT(7, 4)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect MODIFY COLUMN to FLOAT with precision
	require.Len(t, violations, 1)
	assert.Equal(t, "value", *violations[0].Location.Column)
}

func TestHasFloatLinter_AlterTableModifyColumnDoubleWithPrecision(t *testing.T) {
	sql := `ALTER TABLE measurements MODIFY COLUMN value DOUBLE(15, 8)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect MODIFY COLUMN to DOUBLE with precision
	require.Len(t, violations, 1)
	assert.Equal(t, "value", *violations[0].Location.Column)
}

func TestHasFloatLinter_AlterTableModifyColumnWithConstraints(t *testing.T) {
	sql := `ALTER TABLE measurements MODIFY COLUMN temperature FLOAT NOT NULL DEFAULT 0.0`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect MODIFY COLUMN to FLOAT even with constraints
	require.Len(t, violations, 1)
	assert.Equal(t, "temperature", *violations[0].Location.Column)
}

// Tests for ALTER TABLE CHANGE COLUMN

func TestHasFloatLinter_AlterTableChangeColumnToFloat(t *testing.T) {
	sql := `ALTER TABLE measurements CHANGE COLUMN temp temperature FLOAT`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect CHANGE COLUMN to FLOAT
	require.Len(t, violations, 1)
	assert.Equal(t, "has_float", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "temperature")
	assert.Contains(t, violations[0].Message, "modified")
	assert.Equal(t, "measurements", violations[0].Location.Table)
	assert.Equal(t, "temperature", *violations[0].Location.Column)
}

func TestHasFloatLinter_AlterTableChangeColumnToDouble(t *testing.T) {
	sql := `ALTER TABLE coordinates CHANGE COLUMN lat latitude DOUBLE`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect CHANGE COLUMN to DOUBLE
	require.Len(t, violations, 1)
	assert.Equal(t, "has_float", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "latitude")
	assert.Equal(t, "coordinates", violations[0].Location.Table)
}

func TestHasFloatLinter_AlterTableChangeColumnToNonFloat(t *testing.T) {
	sql := `ALTER TABLE measurements CHANGE COLUMN temp temperature DECIMAL(10, 2)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should not detect CHANGE COLUMN to DECIMAL
	assert.Empty(t, violations)
}

func TestHasFloatLinter_AlterTableChangeColumnFloatWithPrecision(t *testing.T) {
	sql := `ALTER TABLE measurements CHANGE COLUMN val value FLOAT(7, 4)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect CHANGE COLUMN to FLOAT with precision
	require.Len(t, violations, 1)
	assert.Equal(t, "value", *violations[0].Location.Column)
}

func TestHasFloatLinter_AlterTableChangeColumnDoubleWithPrecision(t *testing.T) {
	sql := `ALTER TABLE measurements CHANGE COLUMN val value DOUBLE(15, 8)`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect CHANGE COLUMN to DOUBLE with precision
	require.Len(t, violations, 1)
	assert.Equal(t, "value", *violations[0].Location.Column)
}

func TestHasFloatLinter_AlterTableChangeColumnWithConstraints(t *testing.T) {
	sql := `ALTER TABLE measurements CHANGE COLUMN temp temperature FLOAT NOT NULL DEFAULT 0.0`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect CHANGE COLUMN to FLOAT even with constraints
	require.Len(t, violations, 1)
	assert.Equal(t, "temperature", *violations[0].Location.Column)
}

func TestHasFloatLinter_AlterTableChangeColumnSameName(t *testing.T) {
	sql := "ALTER TABLE measurements CHANGE COLUMN temperature temperature FLOAT" //nolint:dupword
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect CHANGE COLUMN even when name doesn't change
	require.Len(t, violations, 1)
	assert.Equal(t, "temperature", *violations[0].Location.Column)
}

// Tests for mixed ALTER TABLE operations

func TestHasFloatLinter_AlterTableMixedAddAndModify(t *testing.T) {
	sql := `ALTER TABLE measurements 
		ADD COLUMN pressure FLOAT,
		MODIFY COLUMN temperature DOUBLE`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect both ADD and MODIFY operations
	require.Len(t, violations, 2)

	columnNames := []string{*violations[0].Location.Column, *violations[1].Location.Column}
	assert.Contains(t, columnNames, "pressure")
	assert.Contains(t, columnNames, "temperature")
}

func TestHasFloatLinter_AlterTableMixedAddModifyChange(t *testing.T) {
	sql := `ALTER TABLE measurements 
		ADD COLUMN pressure FLOAT,
		MODIFY COLUMN temperature DOUBLE,
		CHANGE COLUMN hum humidity FLOAT`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should detect all three operations
	require.Len(t, violations, 3)

	columnNames := []string{*violations[0].Location.Column, *violations[1].Location.Column, *violations[2].Location.Column}
	assert.Contains(t, columnNames, "pressure")
	assert.Contains(t, columnNames, "temperature")
	assert.Contains(t, columnNames, "humidity")
}

func TestHasFloatLinter_AlterTableMixedWithNonFloat(t *testing.T) {
	sql := `ALTER TABLE measurements 
		ADD COLUMN name VARCHAR(255),
		MODIFY COLUMN temperature FLOAT,
		ADD COLUMN count INT`
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, stmts)

	// Should only detect the FLOAT modification
	require.Len(t, violations, 1)
	assert.Equal(t, "temperature", *violations[0].Location.Column)
}

// Tests for comprehensive scenarios with MODIFY/CHANGE

func TestHasFloatLinter_ComplexScenarioWithModify(t *testing.T) {
	// Existing table with INT column
	existingSQL := `CREATE TABLE measurements (
		id BIGINT UNSIGNED PRIMARY KEY,
		temperature INT
	)`
	existingTable, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	// MODIFY column to FLOAT
	modifySQL := `ALTER TABLE measurements MODIFY COLUMN temperature FLOAT`
	modifyStmts, err := statement.New(modifySQL)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, modifyStmts)

	// Should detect the MODIFY to FLOAT
	require.Len(t, violations, 1)
	assert.Equal(t, "temperature", *violations[0].Location.Column)
	assert.Contains(t, violations[0].Message, "modified")
}

func TestHasFloatLinter_ComplexScenarioWithChange(t *testing.T) {
	// Existing table with VARCHAR column
	existingSQL := `CREATE TABLE measurements (
		id BIGINT UNSIGNED PRIMARY KEY,
		temp VARCHAR(50)
	)`
	existingTable, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	// CHANGE column to DOUBLE
	changeSQL := `ALTER TABLE measurements CHANGE COLUMN temp temperature DOUBLE`
	changeStmts, err := statement.New(changeSQL)
	require.NoError(t, err)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, changeStmts)

	// Should detect the CHANGE to DOUBLE
	require.Len(t, violations, 1)
	assert.Equal(t, "temperature", *violations[0].Location.Column)
	assert.Contains(t, violations[0].Message, "modified")
}

func TestHasFloatLinter_AllAlterOperations(t *testing.T) {
	// Test all three ALTER operations in separate statements
	addSQL := `ALTER TABLE t1 ADD COLUMN c1 FLOAT`
	addStmts, err := statement.New(addSQL)
	require.NoError(t, err)

	modifySQL := `ALTER TABLE t2 MODIFY COLUMN c2 DOUBLE`
	modifyStmts, err := statement.New(modifySQL)
	require.NoError(t, err)

	changeSQL := `ALTER TABLE t3 CHANGE COLUMN old_c3 c3 FLOAT`
	changeStmts, err := statement.New(changeSQL)
	require.NoError(t, err)

	allStmts := append(addStmts, modifyStmts...)
	allStmts = append(allStmts, changeStmts...)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, allStmts)

	// Should detect all three operations
	require.Len(t, violations, 3)

	tables := []string{violations[0].Location.Table, violations[1].Location.Table, violations[2].Location.Table}
	assert.Contains(t, tables, "t1")
	assert.Contains(t, tables, "t2")
	assert.Contains(t, tables, "t3")
}

func TestHasFloatLinter_UltraComplexScenario(t *testing.T) {
	// Existing table 1 with FLOAT
	existing1SQL := `CREATE TABLE existing1 (
		id BIGINT UNSIGNED PRIMARY KEY,
		value FLOAT
	)`
	existing1, err := statement.ParseCreateTable(existing1SQL)
	require.NoError(t, err)

	// Existing table 2 with INT (will be modified)
	existing2SQL := `CREATE TABLE existing2 (
		id BIGINT UNSIGNED PRIMARY KEY,
		temperature INT
	)`
	existing2, err := statement.ParseCreateTable(existing2SQL)
	require.NoError(t, err)

	// New table with DOUBLE
	newTableSQL := `CREATE TABLE new_table (
		id BIGINT UNSIGNED PRIMARY KEY,
		score DOUBLE
	)`
	newTableStmts, err := statement.New(newTableSQL)
	require.NoError(t, err)

	// ALTER TABLE to ADD FLOAT
	addSQL := `ALTER TABLE existing2 ADD COLUMN pressure FLOAT`
	addStmts, err := statement.New(addSQL)
	require.NoError(t, err)

	// ALTER TABLE to MODIFY to DOUBLE
	modifySQL := `ALTER TABLE existing2 MODIFY COLUMN temperature DOUBLE`
	modifyStmts, err := statement.New(modifySQL)
	require.NoError(t, err)

	// ALTER TABLE to CHANGE to FLOAT
	changeSQL := `ALTER TABLE existing1 CHANGE COLUMN value measurement FLOAT`
	changeStmts, err := statement.New(changeSQL)
	require.NoError(t, err)

	// Combine all changes
	allChanges := append(newTableStmts, addStmts...)
	allChanges = append(allChanges, modifyStmts...)
	allChanges = append(allChanges, changeStmts...)

	linter := &HasFloatLinter{}
	violations := linter.Lint([]*statement.CreateTable{existing1, existing2}, allChanges)

	// Should detect:
	// 1. existing1.value (FLOAT in existing table)
	// 2. new_table.score (DOUBLE in new table)
	// 3. existing2.pressure (FLOAT via ADD)
	// 4. existing2.temperature (DOUBLE via MODIFY)
	// 5. existing1.measurement (FLOAT via CHANGE)
	require.Len(t, violations, 5)

	// Verify we have violations from all sources
	var foundExisting1, foundNewTable, foundAdd, foundModify, foundChange bool
	for _, v := range violations {
		if v.Location.Table == "existing1" && *v.Location.Column == "value" {
			foundExisting1 = true
		}
		if v.Location.Table == "new_table" && *v.Location.Column == "score" {
			foundNewTable = true
		}
		if v.Location.Table == "existing2" && *v.Location.Column == "pressure" {
			foundAdd = true
		}
		if v.Location.Table == "existing2" && *v.Location.Column == "temperature" {
			foundModify = true
		}
		if v.Location.Table == "existing1" && *v.Location.Column == "measurement" {
			foundChange = true
		}
	}
	assert.True(t, foundExisting1, "Should find violation in existing1.value")
	assert.True(t, foundNewTable, "Should find violation in new_table.score")
	assert.True(t, foundAdd, "Should find violation in ADD COLUMN")
	assert.True(t, foundModify, "Should find violation in MODIFY COLUMN")
	assert.True(t, foundChange, "Should find violation in CHANGE COLUMN")
}

// Test for other ALTER TABLE operations (coverage for default case)

func TestHasFloatLinter_AlterTableOtherOperations(t *testing.T) {
	// Test ALTER TABLE operations that don't involve columns
	sql1 := `ALTER TABLE users ADD INDEX idx_name (name)`
	stmts1, err := statement.New(sql1)
	require.NoError(t, err)

	sql2 := `ALTER TABLE users DROP INDEX idx_name`
	stmts2, err := statement.New(sql2)
	require.NoError(t, err)

	sql3 := `ALTER TABLE users ENGINE=InnoDB`
	stmts3, err := statement.New(sql3)
	require.NoError(t, err)

	allStmts := append(stmts1, stmts2...)
	allStmts = append(allStmts, stmts3...)

	linter := &HasFloatLinter{}
	violations := linter.Lint(nil, allStmts)

	// These operations don't involve column types, so no violations
	assert.Empty(t, violations)
}

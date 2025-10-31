package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/require"
)

// Basic tests for allowed engines

func TestAllowEngine_AllowedEngine(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255)
	) ENGINE=InnoDB`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"innodb": {}}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestAllowEngine_DisallowedEngine(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255)
	) ENGINE=MyISAM`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"innodb": {}}}
	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)
	require.Contains(t, violations[0].Message, "MyISAM")
	require.Contains(t, violations[0].Message, "unsupported engine")
	require.NotNil(t, violations[0].Suggestion)
	require.Contains(t, *violations[0].Suggestion, "innodb")
}

func TestAllowEngine_NoEngineSpecified(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255)
	)`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"innodb": {}}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

// Tests for case insensitivity

func TestAllowEngine_CaseInsensitive_UpperCase(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY
	) ENGINE=INNODB`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"innodb": {}}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestAllowEngine_CaseInsensitive_MixedCase(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY
	) ENGINE=InnoDB`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"innodb": {}}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestAllowEngine_CaseInsensitive_LowerCase(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY
	) ENGINE=innodb`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"innodb": {}}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

// Tests for multiple allowed engines

func TestAllowEngine_MultipleAllowedEngines(t *testing.T) {
	sql1 := `CREATE TABLE t1 (
		id INT PRIMARY KEY
	) ENGINE=InnoDB`

	sql2 := `CREATE TABLE t2 (
		id INT PRIMARY KEY
	) ENGINE=MyISAM`

	stmts1, err := statement.New(sql1)
	require.NoError(t, err)
	stmts2, err := statement.New(sql2)
	require.NoError(t, err)

	stmts := append(stmts1, stmts2...)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{
		"innodb": {},
		"myisam": {},
	}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestAllowEngine_MultipleEnginesOneDisallowed(t *testing.T) {
	sql1 := `CREATE TABLE t1 (
		id INT PRIMARY KEY
	) ENGINE=InnoDB`

	sql2 := `CREATE TABLE t2 (
		id INT PRIMARY KEY
	) ENGINE=Memory`

	stmts1, err := statement.New(sql1)
	require.NoError(t, err)
	stmts2, err := statement.New(sql2)
	require.NoError(t, err)

	stmts := append(stmts1, stmts2...)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"innodb": {}}}
	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)
	require.Equal(t, "t2", violations[0].Location.Table)
	require.Contains(t, violations[0].Message, "Memory")
}

// Tests for different engine types

func TestAllowEngine_MemoryEngine(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY
	) ENGINE=Memory`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"memory": {}}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestAllowEngine_CSVEngine(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY
	) ENGINE=CSV`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"csv": {}}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestAllowEngine_ArchiveEngine(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY
	) ENGINE=Archive`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"archive": {}}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

// Tests for Configure method

func TestAllowEngine_ConfigureSingleEngine(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY
	) ENGINE=MyISAM`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: make(map[string]struct{})}
	err = linter.Configure(map[string]string{
		"allowed_engines": "myisam",
	})
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestAllowEngine_ConfigureMultipleEngines(t *testing.T) {
	sql1 := `CREATE TABLE t1 (id INT) ENGINE=InnoDB`
	sql2 := `CREATE TABLE t2 (id INT) ENGINE=MyISAM`
	sql3 := `CREATE TABLE t3 (id INT) ENGINE=Memory`

	stmts1, err := statement.New(sql1)
	require.NoError(t, err)
	stmts2, err := statement.New(sql2)
	require.NoError(t, err)
	stmts3, err := statement.New(sql3)
	require.NoError(t, err)

	stmts := append(append(stmts1, stmts2...), stmts3...)

	linter := &AllowEngine{allowedEngines: make(map[string]struct{})}
	err = linter.Configure(map[string]string{
		"allowed_engines": "innodb,myisam,memory",
	})
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestAllowEngine_ConfigureWithSpaces(t *testing.T) {
	sql := `CREATE TABLE t1 (id INT) ENGINE=InnoDB`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: make(map[string]struct{})}
	err = linter.Configure(map[string]string{
		"allowed_engines": " innodb , myisam ",
	})
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestAllowEngine_ConfigureInvalidKey(t *testing.T) {
	linter := &AllowEngine{allowedEngines: make(map[string]struct{})}
	err := linter.Configure(map[string]string{
		"invalid_key": "value",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown configuration option")
	require.Contains(t, err.Error(), "invalid_key")
}

func TestAllowEngine_DefaultConfig(t *testing.T) {
	linter := &AllowEngine{}
	config := linter.DefaultConfig()
	require.Contains(t, config, "allowed_engines")
	require.Equal(t, "innodb", config["allowed_engines"])
}

// Tests for existing tables parameter

func TestAllowEngine_ExistingTableAllowed(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY
	) ENGINE=InnoDB`

	existingTable, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"innodb": {}}}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, nil)
	require.Empty(t, violations)
}

func TestAllowEngine_ExistingTableDisallowed(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY
	) ENGINE=MyISAM`

	existingTable, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"innodb": {}}}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, nil)
	require.Len(t, violations, 1)
	require.Equal(t, "t1", violations[0].Location.Table)
}

func TestAllowEngine_ExistingAndNewTables(t *testing.T) {
	existingSQL := `CREATE TABLE existing (
		id INT PRIMARY KEY
	) ENGINE=InnoDB`
	existingTable, err := statement.ParseCreateTable(existingSQL)
	require.NoError(t, err)

	newSQL := `CREATE TABLE new_table (
		id INT PRIMARY KEY
	) ENGINE=MyISAM`
	newStmts, err := statement.New(newSQL)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"innodb": {}}}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, newStmts)
	require.Len(t, violations, 1)
	require.Equal(t, "new_table", violations[0].Location.Table)
}

func TestAllowEngine_MultipleExistingTables(t *testing.T) {
	sql1 := `CREATE TABLE t1 (id INT) ENGINE=InnoDB`
	sql2 := `CREATE TABLE t2 (id INT) ENGINE=MyISAM`

	table1, err := statement.ParseCreateTable(sql1)
	require.NoError(t, err)
	table2, err := statement.ParseCreateTable(sql2)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"innodb": {}}}
	violations := linter.Lint([]*statement.CreateTable{table1, table2}, nil)
	require.Len(t, violations, 1)
	require.Equal(t, "t2", violations[0].Location.Table)
}

// Tests for violation structure

func TestAllowEngine_ViolationStructure(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY
	) ENGINE=MyISAM`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"innodb": {}}}
	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)

	v := violations[0]
	require.NotNil(t, v.Linter)
	require.Equal(t, "allow_engine", v.Linter.Name())
	require.NotEmpty(t, v.Message)
	require.Contains(t, v.Message, "t1")
	require.Contains(t, v.Message, "MyISAM")
	require.NotNil(t, v.Location)
	require.Equal(t, "t1", v.Location.Table)
	require.Nil(t, v.Location.Column)
	require.Nil(t, v.Location.Index)
	require.Nil(t, v.Location.Constraint)
	require.NotNil(t, v.Suggestion)
	require.Contains(t, *v.Suggestion, "innodb")
}

func TestAllowEngine_ViolationSuggestionMultipleEngines(t *testing.T) {
	sql := `CREATE TABLE t1 (id INT) ENGINE=CSV`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{
		"innodb": {},
		"myisam": {},
		"memory": {},
	}}
	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)

	suggestion := *violations[0].Suggestion
	require.Contains(t, suggestion, "supported storage engine")
	// Should contain all allowed engines (sorted)
	require.Contains(t, suggestion, "innodb")
	require.Contains(t, suggestion, "memory")
	require.Contains(t, suggestion, "myisam")
}

// Tests for linter metadata

func TestAllowEngine_Name(t *testing.T) {
	linter := &AllowEngine{}
	require.Equal(t, "allow_engine", linter.Name())
}

func TestAllowEngine_Description(t *testing.T) {
	linter := &AllowEngine{}
	desc := linter.Description()
	require.NotEmpty(t, desc)
	require.Contains(t, desc, "storage engine")
}

func TestAllowEngine_String(t *testing.T) {
	linter := &AllowEngine{}
	str := linter.String()
	require.NotEmpty(t, str)
	require.Contains(t, str, "allow_engine")
}

// Tests for default configuration behavior

func TestAllowEngine_DefaultConfigurationApplied(t *testing.T) {
	sql := `CREATE TABLE t1 (id INT) ENGINE=InnoDB`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	// Linter with nil allowedEngines should use default config
	linter := &AllowEngine{}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestAllowEngine_DefaultConfigurationViolation(t *testing.T) {
	sql := `CREATE TABLE t1 (id INT) ENGINE=MyISAM`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	// Default config only allows InnoDB
	linter := &AllowEngine{}
	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)
	require.Contains(t, violations[0].Message, "MyISAM")
}

// Tests for multiple tables with mixed engines

func TestAllowEngine_MultipleTables_AllAllowed(t *testing.T) {
	sql1 := `CREATE TABLE users (id INT) ENGINE=InnoDB`
	sql2 := `CREATE TABLE orders (id INT) ENGINE=InnoDB`
	sql3 := `CREATE TABLE products (id INT) ENGINE=InnoDB`

	stmts1, err := statement.New(sql1)
	require.NoError(t, err)
	stmts2, err := statement.New(sql2)
	require.NoError(t, err)
	stmts3, err := statement.New(sql3)
	require.NoError(t, err)

	stmts := append(append(stmts1, stmts2...), stmts3...)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"innodb": {}}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestAllowEngine_MultipleTables_SomeDisallowed(t *testing.T) {
	sql1 := `CREATE TABLE users (id INT) ENGINE=InnoDB`
	sql2 := `CREATE TABLE cache (id INT) ENGINE=Memory`
	sql3 := `CREATE TABLE logs (id INT) ENGINE=MyISAM`

	stmts1, err := statement.New(sql1)
	require.NoError(t, err)
	stmts2, err := statement.New(sql2)
	require.NoError(t, err)
	stmts3, err := statement.New(sql3)
	require.NoError(t, err)

	stmts := append(append(stmts1, stmts2...), stmts3...)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"innodb": {}}}
	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 2)

	tableNames := []string{violations[0].Location.Table, violations[1].Location.Table}
	require.Contains(t, tableNames, "cache")
	require.Contains(t, tableNames, "logs")
}

// Tests for edge cases

func TestAllowEngine_EmptyAllowedEngines(t *testing.T) {
	sql := `CREATE TABLE t1 (id INT) ENGINE=InnoDB`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	// Empty allowed engines means nothing is allowed
	linter := &AllowEngine{allowedEngines: map[string]struct{}{}}
	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)
}

func TestAllowEngine_TableWithoutEngine(t *testing.T) {
	sql := `CREATE TABLE t1 (
		id INT PRIMARY KEY,
		name VARCHAR(255)
	)`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"innodb": {}}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestAllowEngine_ComplexTableWithEngine(t *testing.T) {
	sql := `CREATE TABLE users (
		id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
		username VARCHAR(255) NOT NULL,
		email VARCHAR(255) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		PRIMARY KEY (id),
		UNIQUE KEY uk_username (username),
		UNIQUE KEY uk_email (email),
		KEY idx_created_at (created_at)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"innodb": {}}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestAllowEngine_ComplexTableWithDisallowedEngine(t *testing.T) {
	sql := `CREATE TABLE cache_data (
		cache_key VARCHAR(255) NOT NULL,
		cache_value TEXT,
		expires_at TIMESTAMP,
		PRIMARY KEY (cache_key)
	) ENGINE=Memory DEFAULT CHARSET=utf8mb4`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"innodb": {}}}
	violations := linter.Lint(nil, stmts)
	require.Len(t, violations, 1)
	require.Equal(t, "cache_data", violations[0].Location.Table)
	require.Contains(t, violations[0].Message, "Memory")
}

// Tests for configuration with trimming and normalization

func TestAllowEngine_ConfigureEmptyString(t *testing.T) {
	linter := &AllowEngine{allowedEngines: make(map[string]struct{})}
	err := linter.Configure(map[string]string{
		"allowed_engines": "",
	})
	require.NoError(t, err)

	// Empty string should be filtered out, resulting in empty map
	require.Empty(t, linter.allowedEngines)
}

func TestAllowEngine_ConfigureTrailingComma(t *testing.T) {
	sql := `CREATE TABLE t1 (id INT) ENGINE=InnoDB`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: make(map[string]struct{})}
	err = linter.Configure(map[string]string{
		"allowed_engines": "innodb,myisam,",
	})
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestAllowEngine_ConfigureLeadingComma(t *testing.T) {
	sql := `CREATE TABLE t1 (id INT) ENGINE=InnoDB`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: make(map[string]struct{})}
	err = linter.Configure(map[string]string{
		"allowed_engines": ",innodb,myisam",
	})
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

// Tests for nil and empty inputs

func TestAllowEngine_NilExistingTables(t *testing.T) {
	sql := `CREATE TABLE t1 (id INT) ENGINE=InnoDB`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"innodb": {}}}
	violations := linter.Lint(nil, stmts)
	require.Empty(t, violations)
}

func TestAllowEngine_NilChanges(t *testing.T) {
	sql := `CREATE TABLE t1 (id INT) ENGINE=InnoDB`

	existingTable, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"innodb": {}}}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, nil)
	require.Empty(t, violations)
}

func TestAllowEngine_BothNil(t *testing.T) {
	linter := &AllowEngine{allowedEngines: map[string]struct{}{"innodb": {}}}
	violations := linter.Lint(nil, nil)
	require.Empty(t, violations)
}

func TestAllowEngine_EmptyExistingTables(t *testing.T) {
	sql := `CREATE TABLE t1 (id INT) ENGINE=InnoDB`

	stmts, err := statement.New(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"innodb": {}}}
	violations := linter.Lint([]*statement.CreateTable{}, stmts)
	require.Empty(t, violations)
}

func TestAllowEngine_EmptyChanges(t *testing.T) {
	sql := `CREATE TABLE t1 (id INT) ENGINE=InnoDB`

	existingTable, err := statement.ParseCreateTable(sql)
	require.NoError(t, err)

	linter := &AllowEngine{allowedEngines: map[string]struct{}{"innodb": {}}}
	violations := linter.Lint([]*statement.CreateTable{existingTable}, []*statement.AbstractStatement{})
	require.Empty(t, violations)
}

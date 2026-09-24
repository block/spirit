package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/require"
)

func TestInvisibleIndexBeforeDropLinter_DropWithoutInvisible(t *testing.T) {
	sql := "ALTER TABLE users DROP INDEX idx_email"
	stmts, err := statement.New(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	linter := &InvisibleIndexBeforeDropLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	require.Equal(t, "invisible_index_before_drop", violations[0].Linter.Name())
	require.Equal(t, SeverityWarning, violations[0].Severity)
	require.Equal(t, `Index "idx_email" should be made invisible before dropping to ensure it's not needed`, violations[0].Message)
	require.Equal(t, "users", violations[0].Location.Table)
	require.NotNil(t, violations[0].Location.Index)
	require.Equal(t, "idx_email", *violations[0].Location.Index)
	require.NotNil(t, violations[0].Suggestion)
	require.Contains(t, *violations[0].Suggestion, "ALTER INDEX idx_email INVISIBLE")
}

func TestInvisibleIndexBeforeDropLinter_DropAfterInvisibleInSameAlter(t *testing.T) {
	sql := "ALTER TABLE users ALTER INDEX idx_email INVISIBLE, DROP INDEX idx_email"
	stmts, err := statement.New(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	linter := &InvisibleIndexBeforeDropLinter{}
	violations := linter.Lint(nil, stmts)

	// Making an index invisible in the same ALTER statement where you drop it is obviously not good enough
	require.Len(t, violations, 1)
	require.IsType(t, &InvisibleIndexBeforeDropLinter{}, violations[0].Linter)
	require.Equal(t, "invisible_index_before_drop", violations[0].Linter.Name())
}

func TestInvisibleIndexBeforeDropLinter_DropAlreadyInvisibleIndex(t *testing.T) {
	// Create a table with an invisible index
	createSQL := `CREATE TABLE users (
		id INT PRIMARY KEY,
		email VARCHAR(255),
		INDEX idx_email (email) INVISIBLE
	)`
	ct, err := statement.ParseCreateTable(createSQL)
	require.NoError(t, err)

	// Drop the invisible index
	alterSQL := "ALTER TABLE users DROP INDEX idx_email"
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	linter := &InvisibleIndexBeforeDropLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, stmts)

	// Should not have violations since index is already invisible
	require.Empty(t, violations)
}

// TestInvisibleIndexBeforeDropLinter_DropAlreadyInvisibleIndexCaseInsensitive
// verifies that identifier matching is case-insensitive (as MySQL treats table
// and index names). An index defined INVISIBLE as `idx_email` must be
// recognized when dropped as `IDX_EMAIL`, with different table-name casing too,
// so the linter does not falsely flag an already-invisible index.
func TestInvisibleIndexBeforeDropLinter_DropAlreadyInvisibleIndexCaseInsensitive(t *testing.T) {
	createSQL := `CREATE TABLE users (
		id INT PRIMARY KEY,
		email VARCHAR(255),
		INDEX idx_email (email) INVISIBLE
	)`
	ct, err := statement.ParseCreateTable(createSQL)
	require.NoError(t, err)

	alterSQL := "ALTER TABLE Users DROP INDEX IDX_EMAIL"
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	linter := &InvisibleIndexBeforeDropLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, stmts)

	require.Empty(t, violations)
}

func TestInvisibleIndexBeforeDropLinter_DropVisibleIndex(t *testing.T) {
	// Create a table with a visible index
	createSQL := `CREATE TABLE users (
		id INT PRIMARY KEY,
		email VARCHAR(255),
		INDEX idx_email (email)
	)`
	ct, err := statement.ParseCreateTable(createSQL)
	require.NoError(t, err)

	// Drop the visible index
	alterSQL := "ALTER TABLE users DROP INDEX idx_email"
	stmts, err := statement.New(alterSQL)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	linter := &InvisibleIndexBeforeDropLinter{}
	violations := linter.Lint([]*statement.CreateTable{ct}, stmts)

	// Should have violation since index is visible
	require.Len(t, violations, 1)
	require.Equal(t, "invisible_index_before_drop", violations[0].Linter.Name())
}

func TestInvisibleIndexBeforeDropLinter_MultipleDrops(t *testing.T) {
	sql := "ALTER TABLE users DROP INDEX idx_email, DROP INDEX idx_name"
	stmts, err := statement.New(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	linter := &InvisibleIndexBeforeDropLinter{}
	violations := linter.Lint(nil, stmts)

	// Should have violations for both indexes
	require.Len(t, violations, 2)

	indexNames := make(map[string]bool)

	for _, v := range violations {
		require.Equal(t, "invisible_index_before_drop", v.Linter.Name())
		require.Equal(t, SeverityWarning, v.Severity)

		if v.Location.Index != nil {
			indexNames[*v.Location.Index] = true
		}
	}

	require.True(t, indexNames["idx_email"])
	require.True(t, indexNames["idx_name"])
}

func TestInvisibleIndexBeforeDropLinter_NonAlterStatement(t *testing.T) {
	sql := "CREATE TABLE users (id INT PRIMARY KEY)"
	stmts, err := statement.New(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	linter := &InvisibleIndexBeforeDropLinter{}
	violations := linter.Lint(nil, stmts)

	// Should not have violations for non-ALTER statements
	require.Empty(t, violations)
}

func TestInvisibleIndexBeforeDropLinter_AlterWithoutDrop(t *testing.T) {
	sql := "ALTER TABLE users ADD COLUMN age INT"
	stmts, err := statement.New(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	linter := &InvisibleIndexBeforeDropLinter{}
	violations := linter.Lint(nil, stmts)

	// Should not have violations for ALTER without DROP INDEX
	require.Empty(t, violations)
}

func TestInvisibleIndexBeforeDropLinter_Integration(t *testing.T) {
	// Reset registry and register linter
	resetForTest(t)
	Register(&InvisibleIndexBeforeDropLinter{})

	sql := "ALTER TABLE users DROP INDEX idx_email"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations, err := RunLinters(nil, stmts, Config{})
	require.NoError(t, err)

	require.Len(t, violations, 1)
	require.Equal(t, "invisible_index_before_drop", violations[0].Linter.Name())
}

func TestInvisibleIndexBeforeDropLinter_IntegrationDisabled(t *testing.T) {
	// Reset registry and register linter
	resetForTest(t)
	Register(&InvisibleIndexBeforeDropLinter{})

	sql := "ALTER TABLE users DROP INDEX idx_email"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	// Disable the linter
	violations, err := RunLinters(nil, stmts, Config{
		Enabled: map[string]bool{
			"invisible_index_before_drop": false,
		},
	})
	require.NoError(t, err)

	// Should not have violations when disabled
	require.Empty(t, violations)
}

func TestInvisibleIndexBeforeDropLinter_Metadata(t *testing.T) {
	linter := &InvisibleIndexBeforeDropLinter{}

	require.Equal(t, "invisible_index_before_drop", linter.Name())
	require.NotEmpty(t, linter.Description())
}

// Configuration Tests

func TestInvisibleIndexBeforeDropLinter_Configure_ValidRaiseErrorTrue(t *testing.T) {
	linter := &InvisibleIndexBeforeDropLinter{}

	config := map[string]string{
		"raiseError": "true",
	}

	err := linter.Configure(config)
	require.NoError(t, err)
	require.True(t, linter.raiseError)
}

func TestInvisibleIndexBeforeDropLinter_Configure_ValidRaiseErrorFalse(t *testing.T) {
	linter := &InvisibleIndexBeforeDropLinter{}

	config := map[string]string{
		"raiseError": "false",
	}

	err := linter.Configure(config)
	require.NoError(t, err)
	require.False(t, linter.raiseError)
}

func TestInvisibleIndexBeforeDropLinter_Configure_CaseInsensitive(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected bool
	}{
		{"lowercase true", "true", true},
		{"uppercase TRUE", "TRUE", true},
		{"mixed True", "True", true},
		{"lowercase false", "false", false},
		{"uppercase FALSE", "FALSE", false},
		{"mixed False", "False", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			linter := &InvisibleIndexBeforeDropLinter{}

			config := map[string]string{
				"raiseError": tt.value,
			}

			err := linter.Configure(config)
			require.NoError(t, err)
			require.Equal(t, tt.expected, linter.raiseError)
		})
	}
}

func TestInvisibleIndexBeforeDropLinter_Configure_InvalidRaiseErrorValue(t *testing.T) {
	linter := &InvisibleIndexBeforeDropLinter{}

	config := map[string]string{
		"raiseError": "invalid",
	}

	err := linter.Configure(config)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid value for raiseError")
}

func TestInvisibleIndexBeforeDropLinter_Configure_UnknownKey(t *testing.T) {
	linter := &InvisibleIndexBeforeDropLinter{}

	config := map[string]string{
		"unknownKey": "value",
	}

	err := linter.Configure(config)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown config key")
	require.Contains(t, err.Error(), "unknownKey")
}

func TestInvisibleIndexBeforeDropLinter_Configure_MultipleKeys(t *testing.T) {
	linter := &InvisibleIndexBeforeDropLinter{}

	config := map[string]string{
		"raiseError": "false",
		"unknownKey": "value",
	}

	err := linter.Configure(config)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown config key")
}

func TestInvisibleIndexBeforeDropLinter_DefaultConfig(t *testing.T) {
	linter := &InvisibleIndexBeforeDropLinter{}

	defaultConfig := linter.DefaultConfig()
	require.NotNil(t, defaultConfig)
	require.Equal(t, "true", defaultConfig["raiseError"])
}

// Functional Tests with Configuration

func TestInvisibleIndexBeforeDropLinter_RaiseErrorTrue_ProducesError(t *testing.T) {
	sql := "ALTER TABLE users DROP INDEX idx_email"
	stmts, err := statement.New(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	linter := &InvisibleIndexBeforeDropLinter{}
	err = linter.Configure(map[string]string{"raiseError": "true"})
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	require.Equal(t, SeverityError, violations[0].Severity)
}

func TestInvisibleIndexBeforeDropLinter_RaiseErrorFalse_ProducesWarning(t *testing.T) {
	sql := "ALTER TABLE users DROP INDEX idx_email"
	stmts, err := statement.New(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	linter := &InvisibleIndexBeforeDropLinter{}
	err = linter.Configure(map[string]string{"raiseError": "false"})
	require.NoError(t, err)

	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	require.Equal(t, SeverityWarning, violations[0].Severity)
}

func TestInvisibleIndexBeforeDropLinter_DefaultBehavior(t *testing.T) {
	// Without configuration, default should be raiseError=true (warning)
	sql := "ALTER TABLE users DROP INDEX idx_email"
	stmts, err := statement.New(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	linter := &InvisibleIndexBeforeDropLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	// Default behavior is warning (raiseError defaults to false in struct)
	require.Equal(t, SeverityWarning, violations[0].Severity)
}

// Integration Tests with RunLinters

func TestInvisibleIndexBeforeDropLinter_IntegrationWithConfig_RaiseErrorTrue(t *testing.T) {
	resetForTest(t)
	Register(&InvisibleIndexBeforeDropLinter{})

	sql := "ALTER TABLE users DROP INDEX idx_email"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations, err := RunLinters(nil, stmts, Config{
		Settings: map[string]map[string]string{
			"invisible_index_before_drop": {
				"raiseError": "true",
			},
		},
	})
	require.NoError(t, err)

	require.Len(t, violations, 1)
	require.Equal(t, SeverityError, violations[0].Severity)
}

func TestInvisibleIndexBeforeDropLinter_IntegrationWithConfig_RaiseErrorFalse(t *testing.T) {
	resetForTest(t)
	Register(&InvisibleIndexBeforeDropLinter{})

	sql := "ALTER TABLE users DROP INDEX idx_email"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations, err := RunLinters(nil, stmts, Config{
		Settings: map[string]map[string]string{
			"invisible_index_before_drop": {
				"raiseError": "false",
			},
		},
	})
	require.NoError(t, err)

	require.Len(t, violations, 1)
	require.Equal(t, SeverityWarning, violations[0].Severity)
}

func TestInvisibleIndexBeforeDropLinter_IntegrationWithConfig_InvalidConfig(t *testing.T) {
	resetForTest(t)
	Register(&InvisibleIndexBeforeDropLinter{})

	sql := "ALTER TABLE users DROP INDEX idx_email"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	// Invalid configuration should result in error
	violations, err := RunLinters(nil, stmts, Config{
		Settings: map[string]map[string]string{
			"invisible_index_before_drop": {
				"raiseError": "invalid_value",
			},
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid value for raiseError")
	// Linter should be skipped due to configuration error
	require.Empty(t, violations)
}

func TestInvisibleIndexBeforeDropLinter_IntegrationWithConfig_DisabledLinter(t *testing.T) {
	resetForTest(t)
	Register(&InvisibleIndexBeforeDropLinter{})

	sql := "ALTER TABLE users DROP INDEX idx_email"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	// Even with configuration, disabled linter should not run
	violations, err := RunLinters(nil, stmts, Config{
		Enabled: map[string]bool{
			"invisible_index_before_drop": false,
		},
		Settings: map[string]map[string]string{
			"invisible_index_before_drop": {
				"raiseError": "true",
			},
		},
	})
	require.NoError(t, err)
	require.Empty(t, violations)
}

func TestInvisibleIndexBeforeDropLinter_IntegrationWithConfig_EnabledWithConfig(t *testing.T) {
	resetForTest(t)
	Register(&InvisibleIndexBeforeDropLinter{})

	sql := "ALTER TABLE users DROP INDEX idx_email"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	// Explicitly enabled with custom configuration
	violations, err := RunLinters(nil, stmts, Config{
		Enabled: map[string]bool{
			"invisible_index_before_drop": true,
		},
		Settings: map[string]map[string]string{
			"invisible_index_before_drop": {
				"raiseError": "false",
			},
		},
	})
	require.NoError(t, err)

	require.Len(t, violations, 1)
	require.Equal(t, SeverityWarning, violations[0].Severity)
}

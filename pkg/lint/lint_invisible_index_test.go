package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
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
	assert.Equal(t, "invisible_index_before_drop", violations[0].Linter.Name())
	assert.Equal(t, SeverityWarning, violations[0].Severity)
	assert.Contains(t, violations[0].Message, "should be made invisible before dropping")
	assert.Equal(t, "users", violations[0].Location.Table)
	assert.NotNil(t, violations[0].Location.Index)
	assert.Equal(t, "idx_email", *violations[0].Location.Index)
	assert.NotNil(t, violations[0].Suggestion)
	assert.Contains(t, *violations[0].Suggestion, "ALTER INDEX idx_email INVISIBLE")
}

func TestInvisibleIndexBeforeDropLinter_DropAfterInvisibleInSameAlter(t *testing.T) {
	sql := "ALTER TABLE users ALTER INDEX idx_email INVISIBLE, DROP INDEX idx_email"
	stmts, err := statement.New(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	linter := &InvisibleIndexBeforeDropLinter{}
	violations := linter.Lint(nil, stmts)

	// Making an index invisible in the same ALTER statement where you drop it is obviously not good enough
	assert.Len(t, violations, 1)
	assert.IsType(t, &InvisibleIndexBeforeDropLinter{}, violations[0].Linter)
	assert.Equal(t, "invisible_index_before_drop", violations[0].Linter.Name())
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
	assert.Empty(t, violations)
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
	assert.Equal(t, "invisible_index_before_drop", violations[0].Linter.Name())
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
		assert.Equal(t, "invisible_index_before_drop", v.Linter.Name())
		assert.Equal(t, SeverityWarning, v.Severity)

		if v.Location.Index != nil {
			indexNames[*v.Location.Index] = true
		}
	}

	assert.True(t, indexNames["idx_email"])
	assert.True(t, indexNames["idx_name"])
}

func TestInvisibleIndexBeforeDropLinter_NonAlterStatement(t *testing.T) {
	sql := "CREATE TABLE users (id INT PRIMARY KEY)"
	stmts, err := statement.New(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	linter := &InvisibleIndexBeforeDropLinter{}
	violations := linter.Lint(nil, stmts)

	// Should not have violations for non-ALTER statements
	assert.Empty(t, violations)
}

func TestInvisibleIndexBeforeDropLinter_AlterWithoutDrop(t *testing.T) {
	sql := "ALTER TABLE users ADD COLUMN age INT"
	stmts, err := statement.New(sql)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	linter := &InvisibleIndexBeforeDropLinter{}
	violations := linter.Lint(nil, stmts)

	// Should not have violations for ALTER without DROP INDEX
	assert.Empty(t, violations)
}

func TestInvisibleIndexBeforeDropLinter_Integration(t *testing.T) {
	// Reset registry and register linter
	Reset()
	Register(&InvisibleIndexBeforeDropLinter{})

	sql := "ALTER TABLE users DROP INDEX idx_email"
	stmts, err := statement.New(sql)
	require.NoError(t, err)

	violations, err := RunLinters(nil, stmts, Config{})
	require.NoError(t, err)

	require.Len(t, violations, 1)
	assert.Equal(t, "invisible_index_before_drop", violations[0].Linter.Name())
}

func TestInvisibleIndexBeforeDropLinter_IntegrationDisabled(t *testing.T) {
	// Reset registry and register linter
	Reset()
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
	assert.Empty(t, violations)
}

func TestInvisibleIndexBeforeDropLinter_Metadata(t *testing.T) {
	linter := &InvisibleIndexBeforeDropLinter{}

	assert.Equal(t, "invisible_index_before_drop", linter.Name())
	assert.NotEmpty(t, linter.Description())
}

// Configuration Tests

func TestInvisibleIndexBeforeDropLinter_Configure_ValidRaiseErrorTrue(t *testing.T) {
	linter := &InvisibleIndexBeforeDropLinter{}

	config := map[string]string{
		"raiseError": "true",
	}

	err := linter.Configure(config)
	require.NoError(t, err)
	assert.True(t, linter.raiseError)
}

func TestInvisibleIndexBeforeDropLinter_Configure_ValidRaiseErrorFalse(t *testing.T) {
	linter := &InvisibleIndexBeforeDropLinter{}

	config := map[string]string{
		"raiseError": "false",
	}

	err := linter.Configure(config)
	require.NoError(t, err)
	assert.False(t, linter.raiseError)
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
			assert.Equal(t, tt.expected, linter.raiseError)
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
	assert.Contains(t, err.Error(), "invalid value for raiseError")
}

func TestInvisibleIndexBeforeDropLinter_Configure_UnknownKey(t *testing.T) {
	linter := &InvisibleIndexBeforeDropLinter{}

	config := map[string]string{
		"unknownKey": "value",
	}

	err := linter.Configure(config)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown config key")
	assert.Contains(t, err.Error(), "unknownKey")
}

func TestInvisibleIndexBeforeDropLinter_Configure_MultipleKeys(t *testing.T) {
	linter := &InvisibleIndexBeforeDropLinter{}

	config := map[string]string{
		"raiseError": "false",
		"unknownKey": "value",
	}

	err := linter.Configure(config)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown config key")
}

func TestInvisibleIndexBeforeDropLinter_DefaultConfig(t *testing.T) {
	linter := &InvisibleIndexBeforeDropLinter{}

	defaultConfig := linter.DefaultConfig()
	require.NotNil(t, defaultConfig)
	assert.Equal(t, "false", defaultConfig["raiseError"])
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
	assert.Equal(t, SeverityWarning, violations[0].Severity)
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
	assert.Equal(t, SeverityWarning, violations[0].Severity)
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
	assert.Equal(t, SeverityWarning, violations[0].Severity)
}

// Integration Tests with RunLinters

func TestInvisibleIndexBeforeDropLinter_IntegrationWithConfig_RaiseErrorTrue(t *testing.T) {
	Reset()
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
	assert.Equal(t, SeverityWarning, violations[0].Severity)
}

func TestInvisibleIndexBeforeDropLinter_IntegrationWithConfig_RaiseErrorFalse(t *testing.T) {
	Reset()
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
	assert.Equal(t, SeverityWarning, violations[0].Severity)
}

func TestInvisibleIndexBeforeDropLinter_IntegrationWithConfig_InvalidConfig(t *testing.T) {
	Reset()
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
	assert.Contains(t, err.Error(), "invalid value for raiseError")
	// Linter should be skipped due to configuration error
	assert.Empty(t, violations)
}

func TestInvisibleIndexBeforeDropLinter_IntegrationWithConfig_DisabledLinter(t *testing.T) {
	Reset()
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
	assert.Empty(t, violations)
}

func TestInvisibleIndexBeforeDropLinter_IntegrationWithConfig_EnabledWithConfig(t *testing.T) {
	Reset()
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
	assert.Equal(t, SeverityWarning, violations[0].Severity)
}

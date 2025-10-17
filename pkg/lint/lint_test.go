package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock linter for testing
type mockLinter struct {
	name        string
	category    string
	description string
	violations  []Violation
}

func (m *mockLinter) String() string {
	//TODO implement me
	panic("implement me")
}

func (m *mockLinter) Name() string        { return m.name }
func (m *mockLinter) Category() string    { return m.category }
func (m *mockLinter) Description() string { return m.description }
func (m *mockLinter) Lint(createTables []*statement.CreateTable, statements []*statement.AbstractStatement) []Violation {
	return m.violations
}

// Configurable mock linter for testing
type mockConfigurableLinter struct {
	mockLinter

	configCalled bool
	configValue  any
}

func (m *mockConfigurableLinter) String() string {
	//TODO implement me
	panic("implement me")
}

func (m *mockConfigurableLinter) Configure(config any) error {
	m.configCalled = true
	m.configValue = config

	return nil
}

func (m *mockConfigurableLinter) DefaultConfig() any {
	return "default"
}

func TestRegister(t *testing.T) {
	// Reset registry before test
	Reset()

	linter := &mockLinter{
		name:        "test_linter",
		category:    "test",
		description: "A test linter",
	}

	Register(linter)

	// Verify linter was registered
	names := List()
	assert.Contains(t, names, "test_linter")

	// Verify we can get it back
	retrieved, err := Get("test_linter")
	require.NoError(t, err)
	assert.Equal(t, "test_linter", retrieved.Name())
}

func TestRegisterMultiple(t *testing.T) {
	Reset()

	linter1 := &mockLinter{name: "linter1", category: "cat1"}
	linter2 := &mockLinter{name: "linter2", category: "cat2"}
	linter3 := &mockLinter{name: "linter3", category: "cat1"}

	Register(linter1)
	Register(linter2)
	Register(linter3)

	names := List()
	assert.Len(t, names, 3)
	assert.Contains(t, names, "linter1")
	assert.Contains(t, names, "linter2")
	assert.Contains(t, names, "linter3")
}

func TestListByCategory(t *testing.T) {
	Reset()

	linter1 := &mockLinter{name: "linter1", category: "naming"}
	linter2 := &mockLinter{name: "linter2", category: "performance"}
	linter3 := &mockLinter{name: "linter3", category: "naming"}

	Register(linter1)
	Register(linter2)
	Register(linter3)

	namingLinters := ListByCategory("naming")
	assert.Len(t, namingLinters, 2)
	assert.Contains(t, namingLinters, "linter1")
	assert.Contains(t, namingLinters, "linter3")

	perfLinters := ListByCategory("performance")
	assert.Len(t, perfLinters, 1)
	assert.Contains(t, perfLinters, "linter2")

	emptyLinters := ListByCategory("nonexistent")
	assert.Empty(t, emptyLinters)
}

func TestEnableDisable(t *testing.T) {
	Reset()

	linter := &mockLinter{name: "test_linter", category: "test"}
	Register(linter)

	// Linters are enabled by default
	assert.True(t, linters["test_linter"].enabled)

	// Disable it
	err := Disable("test_linter")
	require.NoError(t, err)
	assert.False(t, linters["test_linter"].enabled)

	// Enable it again
	err = Enable("test_linter")
	require.NoError(t, err)
	assert.True(t, linters["test_linter"].enabled)
}

func TestEnableDisableNonexistent(t *testing.T) {
	Reset()

	err := Enable("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	err = Disable("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestGet(t *testing.T) {
	Reset()

	linter := &mockLinter{
		name:        "test_linter",
		category:    "test",
		description: "A test linter",
	}
	Register(linter)

	retrieved, err := Get("test_linter")
	require.NoError(t, err)
	assert.Equal(t, "test_linter", retrieved.Name())
	assert.Equal(t, "test", retrieved.Category())
	assert.Equal(t, "A test linter", retrieved.Description())
}

func TestGetNonexistent(t *testing.T) {
	Reset()

	_, err := Get("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestRunLinters_Empty(t *testing.T) {
	Reset()

	violations := RunLinters(nil, nil, Config{})
	assert.Empty(t, violations)
}

func TestRunLinters_SingleLinter(t *testing.T) {
	Reset()

	linter := &mockLinter{
		name:     "test_linter",
		category: "test",
	}

	expectedViolations := []Violation{
		{
			Linter:   linter,
			Severity: SeverityError,
			Message:  "Test error",
		},
	}
	linter.violations = expectedViolations

	Register(linter)

	violations := RunLinters(nil, nil, Config{})
	assert.Len(t, violations, 1)
	assert.Equal(t, "test_linter", violations[0].Linter.Name())
	assert.Equal(t, SeverityError, violations[0].Severity)
	assert.Equal(t, "Test error", violations[0].Message)
}

func TestRunLinters_MultipleLinters(t *testing.T) {
	Reset()

	linter1 := &mockLinter{
		name:     "linter1",
		category: "test",
	}
	linter1.violations = []Violation{
		{Linter: linter1, Severity: SeverityError, Message: "Error 1"},
	}

	linter2 := &mockLinter{
		name:     "linter2",
		category: "test",
	}
	linter2.violations = []Violation{
		{Linter: linter2, Severity: SeverityWarning, Message: "Warning 1"},
		{Linter: linter2, Severity: SeverityInfo, Message: "Info 1"},
	}

	Register(linter1)
	Register(linter2)

	violations := RunLinters(nil, nil, Config{})
	assert.Len(t, violations, 3)
}

func TestRunLinters_WithConfig_Disabled(t *testing.T) {
	Reset()

	linter := &mockLinter{
		name:     "test_linter",
		category: "test",
	}
	linter.violations = []Violation{
		{Linter: linter, Severity: SeverityError, Message: "Should not see this"},
	}
	Register(linter)

	// Disable the linter via config
	violations := RunLinters(nil, nil, Config{
		Enabled: map[string]bool{
			"test_linter": false,
		},
	})

	assert.Empty(t, violations)
}

func TestRunLinters_WithConfig_Enabled(t *testing.T) {
	Reset()

	linter := &mockLinter{
		name:     "test_linter",
		category: "test",
	}
	linter.violations = []Violation{
		{Linter: linter, Severity: SeverityError, Message: "Should see this"},
	}

	// Disable by default
	Register(linter)
	require.NoError(t, Disable("test_linter"))

	// But explicitly enable via config
	violations := RunLinters(nil, nil, Config{
		Enabled: map[string]bool{
			"test_linter": true,
		},
	})

	assert.Len(t, violations, 1)
	assert.Equal(t, "Should see this", violations[0].Message)
}

func TestRunLinters_ConfigurableLinter(t *testing.T) {
	Reset()

	linter := &mockConfigurableLinter{}
	linter.name = "configurable_linter"
	linter.category = "test"
	linter.violations = []Violation{
		{Linter: linter, Severity: SeverityError, Message: "Test"},
	}
	Register(linter)

	config := map[string]string{"key": "value"}
	violations := RunLinters(nil, nil, Config{
		Settings: map[string]any{
			"configurable_linter": config,
		},
	})

	assert.Len(t, violations, 1)
	assert.True(t, linter.configCalled)
	assert.Equal(t, config, linter.configValue)
}

func TestRunLinters_ConfigurableLinter_NoConfig(t *testing.T) {
	Reset()

	linter := &mockConfigurableLinter{}
	linter.name = "configurable_linter"
	linter.category = "test"
	linter.violations = []Violation{
		{Linter: linter, Severity: SeverityError, Message: "Test"},
	}
	Register(linter)

	violations := RunLinters(nil, nil, Config{})

	assert.Len(t, violations, 1)
	assert.False(t, linter.configCalled)
}

func TestHasErrors(t *testing.T) {
	violations := []Violation{
		{Severity: SeverityWarning},
		{Severity: SeverityInfo},
	}
	assert.False(t, HasErrors(violations))

	violations = append(violations, Violation{Severity: SeverityError})
	assert.True(t, HasErrors(violations))
}

func TestHasWarnings(t *testing.T) {
	violations := []Violation{
		{Severity: SeverityError},
		{Severity: SeverityInfo},
	}
	assert.False(t, HasWarnings(violations))

	violations = append(violations, Violation{Severity: SeverityWarning})
	assert.True(t, HasWarnings(violations))
}

func TestFilterBySeverity(t *testing.T) {
	violations := []Violation{
		{Severity: SeverityError, Message: "Error 1"},
		{Severity: SeverityWarning, Message: "Warning 1"},
		{Severity: SeverityError, Message: "Error 2"},
		{Severity: SeverityInfo, Message: "Info 1"},
	}

	errors := FilterBySeverity(violations, SeverityError)
	assert.Len(t, errors, 2)
	assert.Equal(t, "Error 1", errors[0].Message)
	assert.Equal(t, "Error 2", errors[1].Message)

	warnings := FilterBySeverity(violations, SeverityWarning)
	assert.Len(t, warnings, 1)
	assert.Equal(t, "Warning 1", warnings[0].Message)

	infos := FilterBySeverity(violations, SeverityInfo)
	assert.Len(t, infos, 1)
	assert.Equal(t, "Info 1", infos[0].Message)
}

func TestFilterByLinter(t *testing.T) {
	linter1 := &mockLinter{name: "linter1", category: "test"}
	linter2 := &mockLinter{name: "linter2", category: "test"}

	violations := []Violation{
		{Linter: linter1, Message: "Message 1"},
		{Linter: linter2, Message: "Message 2"},
		{Linter: linter1, Message: "Message 3"},
	}

	linter1Violations := FilterByLinter(violations, "linter1")
	assert.Len(t, linter1Violations, 2)
	assert.Equal(t, "Message 1", linter1Violations[0].Message)
	assert.Equal(t, "Message 3", linter1Violations[1].Message)

	linter2Violations := FilterByLinter(violations, "linter2")
	assert.Len(t, linter2Violations, 1)
	assert.Equal(t, "Message 2", linter2Violations[0].Message)

	nonexistentViolations := FilterByLinter(violations, "nonexistent")
	assert.Empty(t, nonexistentViolations)
}

func TestListSorted(t *testing.T) {
	Reset()

	// Register in non-alphabetical order
	Register(&mockLinter{name: "zebra", category: "test"})
	Register(&mockLinter{name: "alpha", category: "test"})
	Register(&mockLinter{name: "beta", category: "test"})

	names := List()
	assert.Equal(t, []string{"alpha", "beta", "zebra"}, names)
}

func TestListByCategorySorted(t *testing.T) {
	Reset()

	// Register in non-alphabetical order
	Register(&mockLinter{name: "zebra", category: "cat1"})
	Register(&mockLinter{name: "alpha", category: "cat1"})
	Register(&mockLinter{name: "beta", category: "cat2"})
	Register(&mockLinter{name: "gamma", category: "cat1"})

	names := ListByCategory("cat1")
	assert.Equal(t, []string{"alpha", "gamma", "zebra"}, names)
}

func TestReset(t *testing.T) {
	Reset()

	Register(&mockLinter{name: "linter1", category: "test"})
	Register(&mockLinter{name: "linter2", category: "test"})

	assert.Len(t, List(), 2)

	Reset()

	assert.Empty(t, List())
}

func TestViolationWithLocation(t *testing.T) {
	column := "test_column"
	index := "test_index"
	constraint := "test_constraint"
	linter := &mockLinter{name: "test_linter", category: "test"}

	violation := Violation{
		Linter:   linter,
		Severity: SeverityError,
		Message:  "Test message",
		Location: &Location{
			Table:      "test_table",
			Column:     &column,
			Index:      &index,
			Constraint: &constraint,
		},
	}

	assert.Equal(t, "test_table", violation.Location.Table)
	assert.Equal(t, "test_column", *violation.Location.Column)
	assert.Equal(t, "test_index", *violation.Location.Index)
	assert.Equal(t, "test_constraint", *violation.Location.Constraint)
}

func TestViolationWithSuggestion(t *testing.T) {
	suggestion := "Try this instead"
	linter := &mockLinter{name: "test_linter", category: "test"}

	violation := Violation{
		Linter:     linter,
		Severity:   SeverityWarning,
		Message:    "Test message",
		Suggestion: &suggestion,
	}

	assert.NotNil(t, violation.Suggestion)
	assert.Equal(t, "Try this instead", *violation.Suggestion)
}

func TestViolationWithContext(t *testing.T) {
	linter := &mockLinter{name: "test_linter", category: "test"}

	violation := Violation{
		Linter:   linter,
		Severity: SeverityInfo,
		Message:  "Test message",
		Context: map[string]any{
			"key1": "value1",
			"key2": 42,
		},
	}

	assert.Len(t, violation.Context, 2)
	assert.Equal(t, "value1", violation.Context["key1"])
	assert.Equal(t, 42, violation.Context["key2"])
}

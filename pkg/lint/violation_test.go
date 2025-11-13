package lint

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSeverity_DefaultValue tests that the zero value of Severity is SeverityInfo
func TestSeverity_DefaultValue(t *testing.T) {
	var s Severity
	assert.Equal(t, SeverityInfo, s, "default Severity should be SeverityInfo (0)")
	assert.Equal(t, 0, int(s), "default Severity should have underlying value of 0")
}

// TestSeverity_Values tests that the Severity constants have the expected values
func TestSeverity_Values(t *testing.T) {
	assert.Equal(t, 0, int(SeverityInfo), "SeverityInfo should be 0")
	assert.Equal(t, 1, int(SeverityWarning), "SeverityWarning should be 1")
	assert.Equal(t, 2, int(SeverityError), "SeverityError should be 2")
}

// TestSeverity_String tests the String() method for all Severity levels
func TestSeverity_String(t *testing.T) {
	tests := []struct {
		name     string
		severity Severity
		expected string
	}{
		{
			name:     "SeverityInfo",
			severity: SeverityInfo,
			expected: "INFO",
		},
		{
			name:     "SeverityWarning",
			severity: SeverityWarning,
			expected: "WARNING",
		},
		{
			name:     "SeverityError",
			severity: SeverityError,
			expected: "ERROR",
		},
		{
			name:     "Unknown severity",
			severity: Severity(99),
			expected: "UNKNOWN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.severity.String())
		})
	}
}

// TestViolation_DefaultSeverity tests that a Violation without explicit Severity has SeverityInfo
func TestViolation_DefaultSeverity(t *testing.T) {
	linter := &mockLinter{name: "test_linter"}

	// Create violation without setting Severity
	violation := Violation{
		Linter:  linter,
		Message: "Test message",
	}

	assert.Equal(t, SeverityInfo, violation.Severity, "uninitialized Severity should default to SeverityInfo")
}

// TestViolation_String tests the String() method for Violation
func TestViolation_String(t *testing.T) {
	linter := &mockLinter{name: "test_linter"}

	tests := []struct {
		name      string
		violation Violation
		expected  string
	}{
		{
			name: "Basic violation",
			violation: Violation{
				Linter:   linter,
				Severity: SeverityWarning,
				Message:  "Test message",
			},
			expected: "[WARNING] test_linter: Test message",
		},
		{
			name: "Violation with location",
			violation: Violation{
				Linter:   linter,
				Severity: SeverityError,
				Message:  "Test message",
				Location: &Location{
					Table: "users",
				},
			},
			expected: "[ERROR] test_linter: Test message (Table: users)",
		},
		{
			name: "Violation with suggestion",
			violation: Violation{
				Linter:     linter,
				Severity:   SeverityInfo,
				Message:    "Test message",
				Suggestion: stringPtr("Try this fix"),
			},
			expected: "[INFO] test_linter: Test message Suggestion: Try this fix",
		},
		{
			name: "Violation with location and suggestion",
			violation: Violation{
				Linter:   linter,
				Severity: SeverityWarning,
				Message:  "Test message",
				Location: &Location{
					Table: "users",
				},
				Suggestion: stringPtr("Try this fix"),
			},
			expected: "[WARNING] test_linter: Test message (Table: users) Suggestion: Try this fix",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.violation.String())
		})
	}
}

// TestLocation_String tests the String() method for Location
func TestLocation_String(t *testing.T) {
	tests := []struct {
		name     string
		location Location
		expected string
	}{
		{
			name: "Table only",
			location: Location{
				Table: "users",
			},
			expected: "Table: users",
		},
		{
			name: "Table and column",
			location: Location{
				Table:  "users",
				Column: stringPtr("email"),
			},
			expected: "Table: users, Column: email",
		},
		{
			name: "Table and index",
			location: Location{
				Table: "users",
				Index: stringPtr("idx_email"),
			},
			expected: "Table: users, Index: idx_email",
		},
		{
			name: "Table and constraint",
			location: Location{
				Table:      "users",
				Constraint: stringPtr("fk_user_id"),
			},
			expected: "Table: users, Constraint: fk_user_id",
		},
		{
			name: "All fields",
			location: Location{
				Table:      "users",
				Column:     stringPtr("email"),
				Index:      stringPtr("idx_email"),
				Constraint: stringPtr("fk_user_id"),
			},
			expected: "Table: users, Column: email, Index: idx_email, Constraint: fk_user_id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.location.String())
		})
	}
}

// TestViolation_WithContext tests that Context field works correctly
func TestViolation_WithContext(t *testing.T) {
	linter := &mockLinter{name: "test_linter"}

	violation := Violation{
		Linter:   linter,
		Severity: SeverityWarning,
		Message:  "Test message",
		Context: map[string]any{
			"table":         "users",
			"column":        "email",
			"currentType":   "VARCHAR(100)",
			"suggestedType": "VARCHAR(255)",
			"count":         42,
		},
	}

	assert.NotNil(t, violation.Context)
	assert.Len(t, violation.Context, 5)
	assert.Equal(t, "users", violation.Context["table"])
	assert.Equal(t, "email", violation.Context["column"])
	assert.Equal(t, "VARCHAR(100)", violation.Context["currentType"])
	assert.Equal(t, "VARCHAR(255)", violation.Context["suggestedType"])
	assert.Equal(t, 42, violation.Context["count"])
}

// TestViolation_EmptyFields tests violations with nil/empty optional fields
func TestViolation_EmptyFields(t *testing.T) {
	linter := &mockLinter{name: "test_linter"}

	violation := Violation{
		Linter:   linter,
		Severity: SeverityInfo,
		Message:  "Test message",
		// Location, Suggestion, and Context are nil/empty
	}

	assert.Nil(t, violation.Location)
	assert.Nil(t, violation.Suggestion)
	assert.Nil(t, violation.Context)

	// String() should still work
	str := violation.String()
	assert.Contains(t, str, "[INFO]")
	assert.Contains(t, str, "test_linter")
	assert.Contains(t, str, "Test message")
}

// TestSeverity_Comparison tests that Severity values can be compared
func TestSeverity_Comparison(t *testing.T) {
	assert.Less(t, SeverityInfo, SeverityWarning, "SeverityInfo should be less than SeverityWarning")
	assert.Less(t, SeverityWarning, SeverityError, "SeverityWarning should be less than SeverityError")
	assert.Less(t, SeverityInfo, SeverityError, "SeverityInfo should be less than SeverityError")
}

// TestViolation_ArrayDefaultSeverity tests that violations in an array default to SeverityInfo
func TestViolation_ArrayDefaultSeverity(t *testing.T) {
	linter := &mockLinter{name: "test_linter"}

	violations := []Violation{
		{
			Linter:  linter,
			Message: "Message 1",
			// Severity not set
		},
		{
			Linter:   linter,
			Message:  "Message 2",
			Severity: SeverityWarning,
		},
		{
			Linter:  linter,
			Message: "Message 3",
			// Severity not set
		},
	}

	assert.Equal(t, SeverityInfo, violations[0].Severity, "first violation should default to SeverityInfo")
	assert.Equal(t, SeverityWarning, violations[1].Severity, "second violation should be SeverityWarning")
	assert.Equal(t, SeverityInfo, violations[2].Severity, "third violation should default to SeverityInfo")
}

// TestViolation_StructInitialization tests different ways to initialize Violation
func TestViolation_StructInitialization(t *testing.T) {
	linter := &mockLinter{name: "test_linter"}

	t.Run("Zero value", func(t *testing.T) {
		var v Violation
		assert.Equal(t, SeverityInfo, v.Severity)
	})

	t.Run("Partial initialization", func(t *testing.T) {
		v := Violation{
			Linter:  linter,
			Message: "test",
		}
		assert.Equal(t, SeverityInfo, v.Severity)
	})

	t.Run("Explicit SeverityInfo", func(t *testing.T) {
		v := Violation{
			Linter:   linter,
			Message:  "test",
			Severity: SeverityInfo,
		}
		assert.Equal(t, SeverityInfo, v.Severity)
	})

	t.Run("Explicit SeverityWarning", func(t *testing.T) {
		v := Violation{
			Linter:   linter,
			Message:  "test",
			Severity: SeverityWarning,
		}
		assert.Equal(t, SeverityWarning, v.Severity)
	})

	t.Run("Explicit SeverityError", func(t *testing.T) {
		v := Violation{
			Linter:   linter,
			Message:  "test",
			Severity: SeverityError,
		}
		assert.Equal(t, SeverityError, v.Severity)
	})
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}

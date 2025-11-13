package lint

import "fmt"

// Severity represents the severity level of a linting violation
type Severity int

const (
	// SeverityInfo indicates a suggestion or style preference
	// This is the default value if no explicit Severity is given
	SeverityInfo Severity = iota

	// SeverityWarning indicates a best practice violation or potential issue
	SeverityWarning

	// SeverityError indicates a violation that will cause actual problems
	// (syntax errors, MySQL limitations, etc.)
	SeverityError
)

func (s Severity) String() string {
	switch s {
	case SeverityInfo:
		return "INFO"
	case SeverityWarning:
		return "WARNING"
	case SeverityError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// Violation represents a linting violation found during analysis
type Violation struct {
	// Linter is the linter that produced this violation
	Linter Linter

	// Severity is the severity level of the violation
	Severity Severity

	// Message is a human-readable description of the violation
	Message string

	// Location provides information about where the violation occurred
	Location *Location

	// Suggestion is an optional suggestion for fixing the violation
	Suggestion *string

	// Context provides additional context-specific information
	Context map[string]any
}

func (v Violation) String() string {
	msg := fmt.Sprintf("[%s] %s: %s", v.Severity, v.Linter.Name(), v.Message)
	if v.Location != nil {
		msg += fmt.Sprintf(" (%s)", v.Location)
	}

	if v.Suggestion != nil {
		msg += " Suggestion: " + *v.Suggestion
	}

	return msg
}

// Location provides information about where a violation occurred
type Location struct {
	// Table is the name of the table where the violation occurred
	Table string

	// Column is the name of the column (if applicable)
	Column *string

	// Index is the name of the index (if applicable)
	Index *string

	// Constraint is the name of the constraint (if applicable)
	Constraint *string
}

func (l *Location) String() string {
	msg := "Table: " + l.Table
	if l.Column != nil {
		msg += ", Column: " + *l.Column
	}

	if l.Index != nil {
		msg += ", Index: " + *l.Index
	}

	if l.Constraint != nil {
		msg += ", Constraint: " + *l.Constraint
	}

	return msg
}

package lint

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
)

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

// quoteJoin renders a list of SQL tokens (type names, collations, engine
// names) for a violation message, quoting each element the same way %q quotes
// a single identifier so lists follow the same convention as standalone
// tokens.
func quoteJoin(items []string) string {
	quoted := make([]string, len(items))
	for i, item := range items {
		quoted[i] = fmt.Sprintf("%q", item)
	}
	return strings.Join(quoted, ", ")
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

// sortViolations returns a sorted copy of violations: by table name, then
// severity (errors first), then linter name.
func sortViolations(violations []Violation) []Violation {
	sorted := slices.Clone(violations)

	slices.SortFunc(sorted, func(a, b Violation) int {
		ta, tb := "", ""
		if a.Location != nil {
			ta = a.Location.Table
		}
		if b.Location != nil {
			tb = b.Location.Table
		}
		if c := cmp.Compare(ta, tb); c != 0 {
			return c
		}
		if c := cmp.Compare(b.Severity, a.Severity); c != 0 { // errors first
			return c
		}
		return cmp.Compare(a.Linter.Name(), b.Linter.Name())
	})
	return sorted
}

// printViolations prints violations to stdout, sorted by table name then severity.
// Used by the lint command which outputs plain text.
func printViolations(violations []Violation) {
	if len(violations) == 0 {
		return
	}

	for _, v := range sortViolations(violations) {
		fmt.Println(v.String())
	}
}

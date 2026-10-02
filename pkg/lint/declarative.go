package lint

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
)

// PlannedChange represents a single DDL statement produced by declarative
// schema diffing, along with any lint violations associated with it.
type PlannedChange struct {
	// Statement is the semicolon-terminated DDL (ALTER, CREATE, or DROP TABLE).
	Statement string

	// TableName is the table affected by this change.
	TableName string

	// Violations contains lint violations for this table.
	// Linters operate at the table level. When a diff produces multiple
	// statements for the same table, violations are attached only to the
	// last statement to avoid duplication.
	Violations []Violation
}

// Errors returns lint violations with ERROR severity.
func (c *PlannedChange) Errors() []Violation {
	return c.filterBySeverity(SeverityError)
}

// Warnings returns lint violations with WARNING severity.
func (c *PlannedChange) Warnings() []Violation {
	return c.filterBySeverity(SeverityWarning)
}

func (c *PlannedChange) filterBySeverity(severity Severity) []Violation {
	var result []Violation
	for _, v := range c.Violations {
		if v.Severity == severity {
			result = append(result, v)
		}
	}
	return result
}

// Plan holds the complete result of a declarative-to-imperative diff with linting.
type Plan struct {
	// Changes is the ordered list of DDL changes with per-statement lint results.
	Changes []PlannedChange
}

// HasChanges returns true if the plan contains any DDL statements.
func (p *Plan) HasChanges() bool {
	return len(p.Changes) > 0
}

// HasErrors returns true if any change has lint errors.
func (p *Plan) HasErrors() bool {
	for i := range p.Changes {
		if len(p.Changes[i].Errors()) > 0 {
			return true
		}
	}
	return false
}

// HasWarnings returns true if any change has lint warnings.
func (p *Plan) HasWarnings() bool {
	for i := range p.Changes {
		if len(p.Changes[i].Warnings()) > 0 {
			return true
		}
	}
	return false
}

// Statements returns just the DDL strings from all changes.
func (p *Plan) Statements() []string {
	stmts := make([]string, len(p.Changes))
	for i, ch := range p.Changes {
		stmts[i] = ch.Statement
	}
	return stmts
}

// PlanChanges computes the imperative DDL statements needed to transform the
// current schema into the desired schema, and lints each statement against the
// current schema. This combines statement.DeclarativeToImperative with RunLinters
// into a single call, returning per-statement lint results.
//
// LintOnlyChanges is always set to true regardless of the provided lintConfig,
// since PlanChanges only produces lint results for tables that have changes.
//
// Parameters:
//   - current: the current table schemas (CREATE TABLE DDL)
//   - desired: the desired table schemas (CREATE TABLE DDL)
//   - diffOpts: options for the diff (nil uses defaults)
//   - lintConfig: configuration for linting (nil uses defaults; LintOnlyChanges is always overridden to true)
func PlanChanges(current, desired []table.TableSchema, diffOpts *statement.DiffOptions, lintConfig *Config) (*Plan, error) {
	// 1. Compute the diff.
	changes, err := statement.DeclarativeToImperative(current, desired, diffOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to compute schema diff: %w", err)
	}

	// 2. Parse the current schemas for linting.
	var createTables []*statement.CreateTable
	for _, t := range current {
		ct, err := statement.ParseCreateTable(t.Schema)
		if err != nil {
			return nil, fmt.Errorf("failed to parse current schema for table %q: %w", t.Name, err)
		}
		createTables = append(createTables, ct)
	}

	// 3. Run linters.
	cfg := Config{LintOnlyChanges: true}
	if lintConfig != nil {
		cfg = *lintConfig
		cfg.LintOnlyChanges = true
	}
	violations, err := RunLinters(createTables, changes, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to run linters: %w", err)
	}

	// 4. Build per-table violation map.
	// Linters operate at the table level, so violations are grouped by table
	// name. In step 5, they are attached only to the last statement for each
	// table to avoid duplication when a diff produces multiple statements
	// (e.g. partition type changes).
	//
	// Violations are sorted first to ensure deterministic output, since
	// RunLinters iterates over a map of registered linters.
	violationsByTable := make(map[string][]Violation)
	for _, v := range sortViolations(violations) {
		tableName := ""
		if v.Location != nil {
			tableName = v.Location.Table
		}
		violationsByTable[tableName] = append(violationsByTable[tableName], v)
	}

	// 5. Build the plan, attaching violations to the last statement per table.
	// A diff may produce multiple statements for the same table (e.g. ADD
	// PARTITION can't share an ALTER with a column change). Violations
	// are attached only to the last statement for each table so they are not
	// duplicated.
	plan := &Plan{}
	lastIndexByTable := make(map[string]int)
	for i, ch := range changes {
		plan.Changes = append(plan.Changes, PlannedChange{
			Statement: terminatedStmt(ch.Statement),
			TableName: ch.Table,
		})
		lastIndexByTable[ch.Table] = i
	}
	for tableName, vs := range violationsByTable {
		idx, ok := lastIndexByTable[tableName]
		if !ok {
			continue
		}
		plan.Changes[idx].Violations = vs
	}

	return plan, nil
}

// terminatedStmt ensures a SQL statement ends with a semicolon.
func terminatedStmt(stmt string) string {
	stmt = strings.TrimSpace(stmt)
	if len(stmt) == 0 {
		return stmt
	}
	if stmt[len(stmt)-1] != ';' {
		return stmt + ";"
	}
	return stmt
}

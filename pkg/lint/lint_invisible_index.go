package lint

import (
	"fmt"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

func init() {
	Register(&InvisibleIndexBeforeDropLinter{})
}

// InvisibleIndexBeforeDropLinter checks that indexes are made invisible before dropping.
// This is a safety practice to ensure the index is not needed before permanently removing it.
type InvisibleIndexBeforeDropLinter struct {
	raiseError bool
}

func (l *InvisibleIndexBeforeDropLinter) String() string {
	return Stringer(l)
}

func (l *InvisibleIndexBeforeDropLinter) Name() string {
	return "invisible_index_before_drop"
}

func (l *InvisibleIndexBeforeDropLinter) Description() string {
	return "Requires indexes to be made invisible before dropping them as a safety measure"
}

func (l *InvisibleIndexBeforeDropLinter) Configure(config map[string]string) error {
	for k, v := range config {
		switch k {
		case "raiseError":
			boolVal, err := ConfigBool(v, k)
			if err != nil {
				return err
			}

			l.raiseError = boolVal
		default:
			return fmt.Errorf("unknown config key for %s: %s", l.Name(), k)
		}
	}

	return nil
}

func (l *InvisibleIndexBeforeDropLinter) DefaultConfig() map[string]string {
	return map[string]string{
		"raiseError": "false",
	}
}

var _ ConfigurableLinter = &InvisibleIndexBeforeDropLinter{}

func (l *InvisibleIndexBeforeDropLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) []Violation {
	// TODO: Consider re-enabling raiseError configuration to allow SeverityError
	// For now, always use SeverityWarning regardless of raiseError setting
	severity := SeverityWarning

	var violations []Violation

	for _, stmt := range changes {
		// Only check ALTER TABLE statements
		if !stmt.IsAlterTable() {
			continue
		}

		alterStmt, ok := (*stmt.StmtNode).(*ast.AlterTableStmt)
		if !ok {
			continue
		}

		tableName := stmt.Table

		// Check each ALTER specification
		for _, spec := range alterStmt.Specs {
			if spec.Tp != ast.AlterTableDropIndex {
				continue
			}

			indexName := spec.Name

			madeInvisible := false
			// If not made invisible in this ALTER, check if it's invisible in the CREATE TABLE
			if len(existingTables) > 0 {
				for _, ct := range existingTables {
					if ct.GetTableName() == tableName {
						for _, idx := range ct.GetIndexes() {
							if idx.Name == indexName && idx.Invisible != nil && *idx.Invisible {
								madeInvisible = true
								break
							}
						}
					}
				}
			}

			if !madeInvisible {
				suggestion := fmt.Sprintf("First make the index invisible: ALTER TABLE %s ALTER INDEX %s INVISIBLE", tableName, indexName)
				violations = append(violations, Violation{
					Linter:   l,
					Severity: severity,
					Message:  fmt.Sprintf("Index '%s' should be made invisible before dropping to ensure it's not needed", indexName),
					Location: &Location{
						Table: tableName,
						Index: &indexName,
					},
					Suggestion: &suggestion,
				})
			}
		}
	}

	return violations
}

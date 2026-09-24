package lint

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/statement"
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
		"raiseError": "true",
	}
}

var _ ConfigurableLinter = &InvisibleIndexBeforeDropLinter{}

func (l *InvisibleIndexBeforeDropLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) []Violation {
	severity := SeverityWarning
	if l.raiseError {
		severity = SeverityError
	}

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

			// Check whether the index is already invisible in the pre-state.
			// MySQL treats table and index identifiers case-insensitively, so
			// match with EqualFold — otherwise a DROP INDEX whose letter-case
			// differs from the CREATE TABLE definition would falsely flag an
			// index that is already invisible.
			madeInvisible := false
			for _, ct := range existingTables {
				if !strings.EqualFold(ct.GetTableName(), tableName) {
					continue
				}
				for _, idx := range ct.GetIndexes() {
					if strings.EqualFold(idx.Name, indexName) && idx.Invisible != nil && *idx.Invisible {
						madeInvisible = true
						break
					}
				}
			}

			if !madeInvisible {
				suggestion := fmt.Sprintf("First make the index invisible: ALTER TABLE %s ALTER INDEX %s INVISIBLE", tableName, indexName)
				violations = append(violations, Violation{
					Linter:   l,
					Severity: severity,
					Message:  fmt.Sprintf("Index %q should be made invisible before dropping to ensure it's not needed", indexName),
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

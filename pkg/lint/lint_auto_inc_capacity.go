package lint

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

type AutoIncCapacityLinter struct {
	threshold uint64
}

func (l *AutoIncCapacityLinter) Name() string {
	return "auto_inc_capacity"
}

func (l *AutoIncCapacityLinter) Description() string {
	return "Ensures that an auto-inc column is not within a percentage threshold of the maximum capacity of the column type"
}

func (l *AutoIncCapacityLinter) Configure(config map[string]string) error {
	for k, v := range config {
		switch k {
		case "threshold":
			threshold, err := strconv.Atoi(v)
			if err != nil {
				return fmt.Errorf("threshold value could not be parsed: %w", err)
			}
			if threshold <= 0 || threshold > 100 {
				return fmt.Errorf("threshold value must be between 0 and 100, got %d", threshold)
			}
			l.threshold = uint64(threshold)
		}
	}
	return nil
}

func (l *AutoIncCapacityLinter) DefaultConfig() map[string]string {
	return map[string]string{
		"threshold": "85",
	}
}

func (l *AutoIncCapacityLinter) String() string {
	return Stringer(l)
}

func (l *AutoIncCapacityLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	for ct := range CreateTableStatements(existingTables, changes) {
		if ct.TableOptions == nil || ct.TableOptions.AutoIncrement == nil {
			// If the table definition doesn't include an AUTO_INCREMENT clause we can't check anything
			continue
		}
		autoInc := *ct.TableOptions.AutoIncrement
		var maxValue uint64
		var suggestions []string

		// Find the AUTO_INCREMENT column
		for _, col := range ct.Columns {
			if !col.AutoInc {
				continue
			}
			var bytes int

			// Determine max value based on column type
			switch col.Raw.Tp.GetType() {
			case mysql.TypeTiny: // tinyint
				bytes = 8
			case mysql.TypeShort: // smallint
				bytes = 16
			case mysql.TypeInt24: // mediumint
				bytes = 24
			case mysql.TypeLong: // int
				bytes = 32
			case mysql.TypeLonglong: // bigint
				bytes = 64
			default:
				// Unknown type, this can't happen!
				violations = append(violations, Violation{
					Linter: l,
					Location: &Location{
						Table:  ct.TableName,
						Column: &col.Name,
					},
					Message: fmt.Sprintf("unknown column type %q (%d). this is a bug!", col.Type, col.Raw.Tp.GetType()),
				})
			}

			// Adjust for signed types
			if col.Unsigned == nil || !*col.Unsigned {
				// If the column type is signed, it can only hold half of the max value of the type
				bytes--
				suggestions = append(suggestions, "consider using an UNSIGNED data type for auto-increment columns")
			}

			maxValue = 1<<bytes - 1

			// Suggest BIGINT if not already BIGINT
			if strings.ToUpper(col.Type) != "BIGINT" {
				suggestions = append(suggestions, "consider using BIGINT for auto-increment columns")
			}

			// Check if auto_increment value exceeds threshold
			threshold := maxValue * l.threshold / 100
			if autoInc > threshold {
				violations = append(violations, Violation{
					// TODO: Consider changing back to SeverityError - exceeding auto-increment capacity is a serious issue
					Severity: SeverityWarning,
					Linter:   l,
					Location: &Location{
						Table:  ct.TableName,
						Column: &col.Name,
					},
					Message:    fmt.Sprintf("AUTO_INCREMENT value %d is above %d%% of the capacity (%d) of the auto-inc column's %s data type", autoInc, l.threshold, maxValue, col.Type),
					Suggestion: strPtr(strings.Join(suggestions, ". ")),
				})
			}
		}
	}
	return violations
}

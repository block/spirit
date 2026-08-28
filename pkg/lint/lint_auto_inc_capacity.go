package lint

import (
	"fmt"
	"math/bits"
	"strconv"
	"strings"

	"github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/statement"
)

func init() {
	Register(&AutoIncCapacityLinter{threshold: 85})
}

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
		if k == "threshold" {
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

// Lint walks the post-state of the schema so an ALTER that raises
// AUTO_INCREMENT or widens the column type is linted against the table's
// final shape rather than the pre-ALTER snapshot.
func (l *AutoIncCapacityLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	if l.threshold == 0 {
		// A zero threshold means this linter was constructed directly
		// (e.g. &AutoIncCapacityLinter{}) without calling Configure, or the
		// field was manually reset. Instances obtained via Get() always carry
		// the non-zero default registered in init(), and Configure rejects a
		// zero threshold, so this only guards against direct construction.
		// Fall back to the default configuration in that case.
		if err := l.Configure(l.DefaultConfig()); err != nil {
			panic(err)
		}
	}
	for _, ct := range PostState(existingTables, changes) {
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
				// Non-integer auto-increment column (e.g. legacy FLOAT/DOUBLE
				// AUTO_INCREMENT). We can't compute a capacity for it, so
				// report it and skip the capacity check for this column.
				violations = append(violations, Violation{
					Linter: l,
					Location: &Location{
						Table:  ct.TableName,
						Column: &col.Name,
					},
					Message:  fmt.Sprintf("unsupported column type %q (%d) for an auto-increment column; capacity cannot be checked", col.Type, col.Raw.Tp.GetType()),
					Severity: SeverityWarning,
				})

				continue
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
				suggestions = append(suggestions, `consider using "BIGINT" for auto-increment columns`)
			}

			// Check if auto_increment value exceeds threshold. The product
			// maxValue*threshold can exceed 64 bits when maxValue is 2^64-1
			// (BIGINT UNSIGNED), so compute it with a 128-bit intermediate
			// via math/bits. The result is exact: identical to the naive
			// maxValue*l.threshold/100 wherever that doesn't overflow, so
			// the pass/fail boundaries for narrower types are unchanged.
			// bits.Div64 cannot panic here because l.threshold <= 100
			// (validated in Configure), which bounds the high word of the
			// product below the divisor.
			hi, lo := bits.Mul64(maxValue, l.threshold)
			threshold, _ := bits.Div64(hi, lo, 100)
			if autoInc > threshold {
				violations = append(violations, Violation{
					Severity: SeverityError,
					Linter:   l,
					Location: &Location{
						Table:  ct.TableName,
						Column: &col.Name,
					},
					Message:    fmt.Sprintf("AUTO_INCREMENT value %d is above %d%% of the capacity (%d) of the auto-inc column's %q data type", autoInc, l.threshold, maxValue, col.Type),
					Suggestion: new(strings.Join(suggestions, ". ")),
				})
			}
		}
	}
	return violations
}

package linters

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/lint"
	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

func init() {
	lint.Register(&PrimaryKeyTypeLinter{})
}

// PrimaryKeyTypeLinter checks that primary keys use appropriate data types.
// Primary keys should be BIGINT (preferably UNSIGNED) or BINARY/VARBINARY.
// Other types are flagged as errors, and signed BIGINT is flagged as a warning.
type PrimaryKeyTypeLinter struct{}

func (l *PrimaryKeyTypeLinter) String() string {
	return l.Name()
}

func (l *PrimaryKeyTypeLinter) Name() string {
	return "primary_key_type"
}

func (l *PrimaryKeyTypeLinter) Category() string {
	return "schema"
}

func (l *PrimaryKeyTypeLinter) Description() string {
	return "Ensures primary keys use BIGINT (preferably UNSIGNED) or BINARY/VARBINARY types"
}

func (l *PrimaryKeyTypeLinter) Lint(createTables []*statement.CreateTable, _ []*statement.AbstractStatement) []lint.Violation {
	var violations []lint.Violation

	for _, ct := range createTables {
		tableName := ct.GetTableName()

		// Get primary key columns from indexes (this includes both table-level and column-level PRIMARY KEY)
		pkColumns := l.getPrimaryKeyColumnsFromIndexes(ct)
		if len(pkColumns) == 0 {
			continue
		}

		// Check each primary key column's type
		for _, pkCol := range pkColumns {
			column := ct.GetColumns().ByName(pkCol)
			if column == nil {
				continue
			}

			violation := l.checkColumnType(tableName, column)
			if violation != nil {
				violations = append(violations, *violation)
			}
		}
	}

	return violations
}

// getPrimaryKeyColumnsFromIndexes returns the names of columns that are part of the primary key
// by checking the indexes (which includes both table-level and column-level PRIMARY KEY definitions)
func (l *PrimaryKeyTypeLinter) getPrimaryKeyColumnsFromIndexes(ct *statement.CreateTable) []string {
	var pkColumns []string

	// Check for PRIMARY KEY in indexes
	for _, index := range ct.GetIndexes() {
		if index.Type == "PRIMARY KEY" {
			pkColumns = append(pkColumns, index.Columns...)
			break // There can only be one PRIMARY KEY
		}
	}

	return pkColumns
}

// checkColumnType checks if a primary key column has an appropriate type
func (l *PrimaryKeyTypeLinter) checkColumnType(tableName string, column *statement.Column) *lint.Violation {
	columnType := strings.ToUpper(column.Type)

	// Check for BIGINT
	if strings.HasPrefix(columnType, "BIGINT") {
		// Check if it's unsigned (either in type string or Unsigned field)
		isUnsigned := column.Unsigned != nil && *column.Unsigned
		if !isUnsigned {
			isUnsigned = strings.Contains(columnType, "UNSIGNED")
		}

		if isUnsigned {
			// BIGINT UNSIGNED is ideal - no violation
			return nil
		}

		// BIGINT without UNSIGNED is a warning
		suggestion := fmt.Sprintf("Consider using BIGINT UNSIGNED for column '%s' to avoid negative values and increase range", column.Name)

		return &lint.Violation{
			Linter:   l,
			Severity: lint.SeverityWarning,
			Message:  fmt.Sprintf("Primary key column '%s' uses signed BIGINT; UNSIGNED is preferred", column.Name),
			Location: &lint.Location{
				Table:  tableName,
				Column: &column.Name,
			},
			Suggestion: &suggestion,
		}
	}

	// Check for BINARY/VARBINARY
	// Note: The parser returns "char" for BINARY and "varchar" for VARBINARY,
	// so we need to check the raw type and binary flag
	if l.isBinaryType(column) {
		// BINARY/VARBINARY is acceptable - no violation
		return nil
	}

	// Any other type is an error
	suggestion := fmt.Sprintf("Change column '%s' to BIGINT UNSIGNED or BINARY/VARBINARY", column.Name)

	return &lint.Violation{
		Linter:   l,
		Severity: lint.SeverityError,
		Message:  fmt.Sprintf("Primary key column '%s' has type '%s'; must be BIGINT or BINARY/VARBINARY", column.Name, column.Type),
		Location: &lint.Location{
			Table:  tableName,
			Column: &column.Name,
		},
		Suggestion: &suggestion,
		Context: map[string]interface{}{
			"current_type": column.Type,
		},
	}
}

// isBinaryType checks if a column is BINARY or VARBINARY type
// The parser returns "char" for BINARY and "varchar" for VARBINARY, so we need to check the binary flag
func (l *PrimaryKeyTypeLinter) isBinaryType(column *statement.Column) bool {
	if column.Raw == nil || column.Raw.Tp == nil {
		return false
	}

	// BINARY is mysql.TypeString with binary flag
	// VARBINARY is mysql.TypeVarchar with binary flag
	rawType := column.Raw.Tp.GetType()

	// Check if it's a string type with binary flag (BINARY/VARBINARY)

	fmt.Printf("Debug: type=%s rawType=%d, flags=%d, options=%#v\n", column.Type, rawType, column.Raw.Tp.GetFlag(), column.Options)

	return (rawType == mysql.TypeString || rawType == mysql.TypeVarchar) && mysql.HasBinaryFlag(column.Raw.Tp.GetFlag())
}

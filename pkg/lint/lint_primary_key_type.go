package lint

import (
	"fmt"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/statement"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

func init() {
	Register(&PrimaryKeyTypeLinter{})
}

// PrimaryKeyTypeLinter checks that primary keys use appropriate data types.
// Primary keys should be BIGINT (preferably UNSIGNED) or BINARY/VARBINARY.
// Other types are flagged as errors, and signed BIGINT is flagged as a warning.
type PrimaryKeyTypeLinter struct {
	allowedTypes map[string]struct{}
}

var primaryKeyTypeLinterSupportedTypes = []string{
	"BINARY", "VARBINARY", "BIGINT",
	"CHAR", "VARCHAR",
	"BIT", "DECIMAL", "ENUM", "SET",
	"TINYINT", "SMALLINT", "MEDIUMINT", "INT",
	"TIME", "TIMESTAMP", "YEAR", "DATE", "DATETIME",
}

func (l *PrimaryKeyTypeLinter) String() string {
	return Stringer(l)
}

func (l *PrimaryKeyTypeLinter) Name() string {
	return "primary_key_type"
}

func (l *PrimaryKeyTypeLinter) Description() string {
	return "Ensures primary keys use BIGINT (preferably UNSIGNED) or BINARY/VARBINARY types"
}

func (l *PrimaryKeyTypeLinter) Configure(config map[string]string) error {
	if l.allowedTypes == nil {
		l.allowedTypes = make(map[string]struct{})
	}
	for name, value := range config {
		switch name {
		case "allowedTypes":
			allowedTypes := strings.Split(value, ",")
			for _, tp := range allowedTypes {
				t := strings.ToUpper(strings.TrimSpace(tp))
				if !slices.Contains(primaryKeyTypeLinterSupportedTypes, t) {
					return fmt.Errorf("unsupported type %q (not in %s)", tp, primaryKeyTypeLinterSupportedTypes)
				}
				l.allowedTypes[t] = struct{}{}
			}
		default:
			return fmt.Errorf("unknown config key for %s: %s", l.Name(), name)
		}
	}
	return nil
}

func (l *PrimaryKeyTypeLinter) DefaultConfig() map[string]string {
	return map[string]string{
		"allowedTypes": "BIGINT,BINARY,VARBINARY",
	}
}

func (l *PrimaryKeyTypeLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	// TODO: add support for ALTER TABLE statements that try to modify a primary key to an unsupported type

	// If the linter is run with a default allowedTypes configuration, set it to the default value
	if len(l.allowedTypes) == 0 {
		if err := l.Configure(l.DefaultConfig()); err != nil {
			panic(err)
		}
	}

	// We only look over definitions of existing tables and newly created tables.
	for ct := range CreateTableStatements(existingTables, changes) {
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
func (l *PrimaryKeyTypeLinter) checkColumnType(tableName string, column *statement.Column) *Violation {
	columnType := strings.ToUpper(column.Type)

	if _, ok := l.allowedTypes[columnType]; ok {
		if isSignedIntType(column) {
			return &Violation{
				Linter:   l,
				Severity: SeverityWarning,
				Message:  fmt.Sprintf("Primary key column %q uses signed %s; UNSIGNED is preferred", column.Name, columnType),
				Location: &Location{
					Table:  tableName,
					Column: &column.Name,
				},
				Suggestion: strPtr(fmt.Sprintf("Consider using BIGINT UNSIGNED for column '%s' to avoid negative values and increase range", column.Name)),
			}
		}
		return nil
	}

	// The parser rewrites binary->char and varbinary->varchar, so these are handled specially
	if columnType == "CHAR" {
		if _, ok := l.allowedTypes["BINARY"]; ok && l.isBinaryType(column) {
			return nil
		}
	}
	if columnType == "VARCHAR" {
		if _, ok := l.allowedTypes["VARBINARY"]; ok && l.isBinaryType(column) {
			return nil
		}
	}

	// Any other type is an error
	keys := make([]string, 0, len(l.allowedTypes))
	for k := range l.allowedTypes {
		keys = append(keys, k)
	}
	suggestion := fmt.Sprintf("Change column %q to a supported column type (%s)", column.Name, strings.Join(keys, ","))

	return &Violation{
		Linter:   l,
		Severity: SeverityWarning,
		Message:  fmt.Sprintf("Primary key column %q has type %q", column.Name, column.Type),
		Location: &Location{
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

	return (rawType == mysql.TypeString || rawType == mysql.TypeVarchar) && mysql.HasBinaryFlag(column.Raw.Tp.GetFlag())
}

func isSignedIntType(column *statement.Column) bool {
	return mysql.IsIntegerType(column.Raw.Tp.GetType()) && (column.Unsigned == nil || !*column.Unsigned)
}

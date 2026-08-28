package lint

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/statement"
)

func init() {
	Register(&PrimaryKeyLinter{})
}

// PrimaryKeyLinter checks that primary keys are defined and use appropriate data types.
// Primary keys should be BIGINT (preferably UNSIGNED) or BINARY/VARBINARY, but the linter can be configured to allow other types.
// Severity is scoped by source: existing tables produce SeverityWarning (don't block ALTERs on legacy schemas),
// while CREATE TABLE statements in the incoming changes produce SeverityError (enforce standards on new tables).
type PrimaryKeyLinter struct {
	allowedTypes map[string]struct{}
}

var primaryKeyTypeLinterSupportedTypes = []string{
	"BINARY", "VARBINARY", "BIGINT",
	"CHAR", "VARCHAR",
	"BIT", "DECIMAL", "ENUM", "SET",
	"TINYINT", "SMALLINT", "MEDIUMINT", "INT",
	"TIME", "TIMESTAMP", "YEAR", "DATE", "DATETIME",
}

func (l *PrimaryKeyLinter) String() string {
	return Stringer(l)
}

func (l *PrimaryKeyLinter) Name() string {
	return "primary_key"
}

func (l *PrimaryKeyLinter) Description() string {
	return "Ensures primary keys use BIGINT (preferably UNSIGNED) or BINARY/VARBINARY types"
}

func (l *PrimaryKeyLinter) Configure(config map[string]string) error {
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

func (l *PrimaryKeyLinter) DefaultConfig() map[string]string {
	return map[string]string{
		"allowedTypes": "BIGINT,BINARY,VARBINARY",
	}
}

// Lint operates on a post-state view of the schema, so an ALTER that fixes a
// legacy primary key (e.g. swapping a VARCHAR PK for a new BIGINT id, as in
// `ADD COLUMN id BIGINT AUTO_INCREMENT, DROP PRIMARY KEY, ADD PRIMARY KEY(id)`)
// doesn't produce a false positive against the pre-state. Severity is scoped by
// source:
//
//   - Error, if the table is created in this changeset, or the PK column is
//     added/modified by this changeset — enforce standards on new schema.
//   - Warning, if the PK column pre-existed on a legacy table and no incoming
//     change touches it — don't block ALTERs on legacy schemas.
func (l *PrimaryKeyLinter) Lint(existingTables []*statement.CreateTable, changes []*statement.AbstractStatement) (violations []Violation) {
	// If the linter is run without a default allowedTypes configuration, set it to the default value
	if len(l.allowedTypes) == 0 {
		err := l.Configure(l.DefaultConfig())
		if err != nil {
			panic(err)
		}
	}

	pre := PreStateColumns(existingTables)
	createdInChanges := newTablesInChanges(changes)
	addedOrModified := columnsAddedOrModifiedInChanges(changes)

	for _, ct := range PostState(existingTables, changes) {
		violations = append(violations, l.checkTable(ct, pre, createdInChanges, addedOrModified)...)
	}

	return violations
}

// checkTable checks a single post-state table's primary key, scoping the
// severity of each violation to whether the offending column is new/modified
// (Error) or pre-existing legacy schema (Warning).
func (l *PrimaryKeyLinter) checkTable(ct *statement.CreateTable, pre map[string]map[string]*statement.Column, createdTables map[string]bool, addedOrModified map[string]map[string]bool) []Violation {
	var violations []Violation
	tableName := ct.GetTableName()
	tKey := strings.ToLower(tableName)

	// Get primary key columns from indexes (this includes both table-level and column-level PRIMARY KEY)
	pkColumns := l.getPrimaryKeyColumnsFromIndexes(ct)
	if len(pkColumns) == 0 {
		// A newly created table with no PK is an Error; a pre-existing table
		// missing a PK is a legacy Warning.
		severity := SeverityWarning
		if createdTables[tKey] {
			severity = SeverityError
		}
		violations = append(violations, Violation{
			Linter:     l,
			Location:   &Location{Table: tableName},
			Message:    "No primary key defined",
			Severity:   severity,
			Suggestion: new("Every table should have an explicit primary key"),
		})
		return violations
	}

	// Check each primary key column's type
	for _, pkCol := range pkColumns {
		// MySQL column identifiers are case-insensitive, and the PK column
		// name here can come from a source (a table-level PRIMARY KEY(...)
		// clause, or an ALTER spec in post-state) whose letter-case differs
		// from the column definition. Resolve case-insensitively so the type
		// check isn't silently skipped (false negative).
		column := columnByNameFold(ct.GetColumns(), pkCol)
		if column == nil {
			continue
		}

		severity := l.severityForColumn(tKey, pkCol, pre, createdTables, addedOrModified)
		violation := l.checkColumnType(tableName, column, severity)
		if violation != nil {
			violations = append(violations, *violation)
		}
	}

	return violations
}

// severityForColumn returns Error for PK columns that are new (in a created
// table, or added/modified by the changeset) and Warning for PK columns that
// pre-existed on a legacy table untouched by the changeset.
func (l *PrimaryKeyLinter) severityForColumn(tableKey, colName string, pre map[string]map[string]*statement.Column, createdTables map[string]bool, addedOrModified map[string]map[string]bool) Severity {
	if createdTables[tableKey] {
		return SeverityError
	}
	colKey := strings.ToLower(colName)
	if addedOrModified[tableKey][colKey] {
		return SeverityError
	}
	if _, ok := pre[tableKey][colKey]; ok {
		return SeverityWarning
	}
	return SeverityError
}

// getPrimaryKeyColumnsFromIndexes returns the names of columns that are part of the primary key
// by checking the indexes (which includes both table-level and column-level PRIMARY KEY definitions)
func (l *PrimaryKeyLinter) getPrimaryKeyColumnsFromIndexes(ct *statement.CreateTable) []string {
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

// columnByNameFold resolves a column by name case-insensitively, matching
// MySQL's case-insensitive column identifiers. It returns nil if no column
// matches.
func columnByNameFold(columns statement.Columns, name string) *statement.Column {
	for i := range columns {
		if strings.EqualFold(columns[i].Name, name) {
			return &columns[i]
		}
	}
	return nil
}

// checkColumnType checks if a primary key column has an appropriate type
func (l *PrimaryKeyLinter) checkColumnType(tableName string, column *statement.Column, severity Severity) *Violation {
	columnType := strings.ToUpper(column.Type)

	if _, ok := l.allowedTypes[columnType]; ok {
		// A PK on an allowed type passes. We deliberately do NOT emit a
		// "prefer UNSIGNED" warning for signed integer PKs — it was judged
		// too noisy for real-world schemas. isSignedIntType (with its unit
		// tests) is retained should that policy be revisited.
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
	keys := slices.Sorted(maps.Keys(l.allowedTypes))

	return &Violation{
		Linter:   l,
		Severity: severity,
		Message:  fmt.Sprintf("Primary key column %q has type %q", column.Name, column.Type),
		Location: &Location{
			Table:  tableName,
			Column: &column.Name,
		},
		Suggestion: new(fmt.Sprintf("Change column %q to a supported column type (%s)", column.Name, quoteJoin(keys))),
		Context: map[string]any{
			"current_type": column.Type,
		},
	}
}

// isBinaryType checks if a column is BINARY or VARBINARY type
// The parser returns "char" for BINARY and "varchar" for VARBINARY, so we need to check the binary flag
func (l *PrimaryKeyLinter) isBinaryType(column *statement.Column) bool {
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

package table

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/utils"
)

// TableSchema represents a table's name and its raw CREATE TABLE DDL statement.
// This is the common representation used across spirit, strata, and gap for
// passing schema information between components.
type TableSchema struct {
	Name   string // Table name
	Schema string // CREATE TABLE DDL statement
}

// FilterOption controls which tables are excluded and which DDL
// transformations are applied when loading schema from a database.
type FilterOption int

const (
	// WithoutUnderscoreTables filters out tables whose name begins with "_".
	// This is commonly used to exclude Spirit's internal shadow/checkpoint tables
	// and other tool-generated temporary tables.
	WithoutUnderscoreTables FilterOption = iota + 1

	// WithoutArchiveTables filters out tables matching the archive naming convention:
	// <name>_archive_YYYY, <name>_archive_YYYY_MM, or <name>_archive_YYYY_MM_DD.
	WithoutArchiveTables

	// WithStrippedAutoIncrement removes the AUTO_INCREMENT=N table option from
	// CREATE TABLE statements, leaving every other byte as MySQL emitted it, so
	// a table that carries a counter and one that does not come back in the same
	// format. Use it when the DDL text is itself the output — a schema file, a
	// stored snapshot — and one instance's counter must not be baked into it.
	//
	// It is not needed to compare two schemas. statement.Diff already ignores
	// the counter, via DiffOptions.IgnoreAutoIncrement (default true).
	//
	// See StripAutoIncrement.
	WithStrippedAutoIncrement
)

// archiveTableRegexp matches table names following the archive convention:
// <name>_archive_YYYY, <name>_archive_YYYY_MM, or <name>_archive_YYYY_MM_DD.
var archiveTableRegexp = regexp.MustCompile(`^.*_archive_[0-9]{4}(_[0-9]{2}(_[0-9]{2})?)?$`)

// IsArchiveTable returns true if the table name matches the archive naming
// convention: <name>_archive_YYYY, <name>_archive_YYYY_MM, or
// <name>_archive_YYYY_MM_DD.
func IsArchiveTable(name string) bool {
	return archiveTableRegexp.MatchString(name)
}

// ExcludedTable is a table a filter option kept out of the schema
// LoadSchemaAndExcludedTablesFromDB returned, paired with the option that
// excluded it.
type ExcludedTable struct {
	Name   string       // Table name
	Filter FilterOption // The filter option that excluded the table
}

// TableObject is one entry SHOW FULL TABLES lists for a database: a base
// table, a view, or any other object type MySQL lists alongside tables.
type TableObject struct {
	Name string // Object name
	Type string // Table_type as MySQL reports it, e.g. BASE TABLE or VIEW
}

// IsBaseTable reports whether the object is a base table, as opposed to a
// view or another object type sharing the table namespace.
func (o TableObject) IsBaseTable() bool {
	return strings.EqualFold(o.Type, "BASE TABLE")
}

// ListTableObjects returns every object in the connection's selected database
// that occupies the table namespace, with its type, in the order SHOW FULL
// TABLES returns them. Callers that must know whether a name is free, rather
// than which base tables exist, need the views as well: a view blocks a
// CREATE TABLE of the same name and accepts writes aimed at it.
//
// The statement stays in the plain form that any MySQL-protocol endpoint
// supports; callers filter on Type in Go.
func ListTableObjects(ctx context.Context, db *sql.DB) ([]TableObject, error) {
	rows, err := db.QueryContext(ctx, "SHOW FULL TABLES")
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer utils.CloseAndLog(rows)
	var objects []TableObject
	for rows.Next() {
		var o TableObject
		if err := rows.Scan(&o.Name, &o.Type); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		objects = append(objects, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tables: %w", err)
	}
	return objects, nil
}

// ListBaseTables returns the base tables in the connection's selected
// database, in the order SHOW FULL TABLES returns them. Views, and any other
// object type MySQL lists alongside tables, are skipped: they hold no rows of
// their own, and SHOW CREATE TABLE describes a view in a different shape
// (four columns instead of two) and requires the SHOW VIEW privilege.
func ListBaseTables(ctx context.Context, db *sql.DB) ([]string, error) {
	objects, err := ListTableObjects(ctx, db)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, o := range objects {
		if o.IsBaseTable() {
			names = append(names, o.Name)
		}
	}
	return names, nil
}

// LoadSchemaFromDB retrieves all table schemas from the database using the
// provided connection. Only base tables are loaded (see ListBaseTables):
// views are never returned, as tables or as exclusions. The returned tables
// and DDL are filtered according to the supplied options. With no options the
// raw DDL is returned unmodified.
func LoadSchemaFromDB(ctx context.Context, db *sql.DB, opts ...FilterOption) ([]TableSchema, error) {
	tables, _, err := LoadSchemaAndExcludedTablesFromDB(ctx, db, opts...)
	return tables, err
}

// LoadSchemaAndExcludedTablesFromDB is LoadSchemaFromDB, additionally
// reporting every table the filter options excluded and which option excluded
// it. A caller that has to account for what is missing from the schema it
// loaded — a declarative tool disclosing the tables it is declining to look
// at, say — cannot recover those names from the returned schema, and
// recovering them by re-implementing the predicates puts a second copy of them
// outside this package, to drift the next time a filter changes.
//
// A table is reported once, under the first option that excluded it, and the
// exclusions keep the order ListBaseTables returned them in. WithStrippedAutoIncrement
// rewrites DDL rather than excluding a table, so it never appears.
func LoadSchemaAndExcludedTablesFromDB(ctx context.Context, db *sql.DB, opts ...FilterOption) ([]TableSchema, []ExcludedTable, error) {
	optSet := make(map[FilterOption]bool, len(opts))
	for _, o := range opts {
		optSet[o] = true
	}

	tableNames, err := ListBaseTables(ctx, db)
	if err != nil {
		return nil, nil, err
	}
	var tables []TableSchema
	var excluded []ExcludedTable
	for _, name := range tableNames {
		if optSet[WithoutUnderscoreTables] && strings.HasPrefix(name, "_") {
			excluded = append(excluded, ExcludedTable{Name: name, Filter: WithoutUnderscoreTables})
			continue
		}
		if optSet[WithoutArchiveTables] && IsArchiveTable(name) {
			excluded = append(excluded, ExcludedTable{Name: name, Filter: WithoutArchiveTables})
			continue
		}
		var tbl, createStmt string
		err := db.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE %s", sqlescape.EscapeIdentifier(name))).Scan(&tbl, &createStmt)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get CREATE TABLE for %s: %w", name, err)
		}
		if optSet[WithStrippedAutoIncrement] {
			createStmt, err = StripAutoIncrement(createStmt)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to strip AUTO_INCREMENT from CREATE TABLE for %s: %w", name, err)
			}
		}
		tables = append(tables, TableSchema{Name: tbl, Schema: createStmt})
	}
	return tables, excluded, nil
}

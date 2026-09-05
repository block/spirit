package table

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/format"
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
	// CREATE TABLE statements. This is useful when comparing schemas to avoid
	// spurious diffs caused by differing auto-increment counters.
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

// StripAutoIncrement removes the AUTO_INCREMENT=N table option from a
// CREATE TABLE statement. This is useful when comparing schemas to avoid
// spurious diffs caused by differing auto-increment counters.
func StripAutoIncrement(stmt string) string {
	node, err := parser.New().ParseOneStmt(stmt, "", "")
	if err != nil {
		return stmt
	}
	create, ok := node.(*ast.CreateTableStmt)
	if !ok {
		return stmt
	}
	originalCount := len(create.Options)
	create.Options = slices.DeleteFunc(create.Options, func(opt *ast.TableOption) bool {
		return opt.Tp == ast.TableOptionAutoIncrement
	})
	if len(create.Options) == originalCount {
		return stmt
	}
	var restored strings.Builder
	if err := create.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &restored)); err != nil {
		return stmt
	}
	return restored.String()
}

// LoadSchemaFromDB retrieves all table schemas from the database using the
// provided connection. The returned tables and DDL are filtered according to
// the supplied options. With no options the raw DDL is returned unmodified.
func LoadSchemaFromDB(ctx context.Context, db *sql.DB, opts ...FilterOption) ([]TableSchema, error) {
	optSet := make(map[FilterOption]bool, len(opts))
	for _, o := range opts {
		optSet[o] = true
	}

	rows, err := db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var tableNames []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tableNames = append(tableNames, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tables: %w", err)
	}
	var tables []TableSchema
	for _, name := range tableNames {
		if optSet[WithoutUnderscoreTables] && strings.HasPrefix(name, "_") {
			continue
		}
		if optSet[WithoutArchiveTables] && IsArchiveTable(name) {
			continue
		}
		var tbl, createStmt string
		err := db.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE %s", sqlescape.EscapeIdentifier(name))).Scan(&tbl, &createStmt)
		if err != nil {
			return nil, fmt.Errorf("failed to get CREATE TABLE for %s: %w", name, err)
		}
		if optSet[WithStrippedAutoIncrement] {
			createStmt = StripAutoIncrement(createStmt)
		}
		tables = append(tables, TableSchema{Name: tbl, Schema: createStmt})
	}
	return tables, nil
}

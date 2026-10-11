package statement

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

// DeclarativeToImperative compares current and desired schemas and returns the
// imperative DDL statements (ALTER, CREATE, DROP) needed to transform current
// into desired.
//
// This is the core of declarative schema management: given two sets of table
// definitions, compute the minimal set of changes. It is used by spirit's diff
// subcommand, strata, and GAP.
//
// The returned statements are ordered as CREATE → ALTER → DROP. This ordering
// is a correctness property: it ensures the output is safe to execute
// sequentially (e.g. an ALTER that adds a foreign key referencing a
// newly-created table will run after the CREATE, and a table referenced by a
// FK won't be dropped before the referencing ALTER runs). Within the CREATE
// and DROP groups, tables follow their foreign keys: a table is created after
// the tables its foreign keys reference (MySQL error 1824 otherwise) and
// dropped before the tables that reference it (error 3730). Tables with no
// such dependency between them are sorted alphabetically, and ALTERs always
// are. A reference is matched by table name alone; a reference to a table
// outside the group, or a table's reference to itself, imposes no order. Two
// orders the output cannot satisfy are left to MySQL: a cycle of references
// among new tables (MySQL creates neither table without FOREIGN_KEY_CHECKS=0)
// and an ALTER that depends on another table's ALTER.
//
// Tables are matched by name the way the target server compares them (see
// DiffOptions.LowerCaseTableNames). A schema that names the same table twice
// is refused rather than planned from one of its definitions: the caller's
// intent is ambiguous, and the definition left out would never be applied.
//
// If opts is nil, NewDiffOptions() defaults are used for table diffs.
func DeclarativeToImperative(current, desired []table.TableSchema, opts *DiffOptions) ([]*AbstractStatement, error) {
	key := tableNameKey(opts)
	currentMap, err := tablesByName(current, key, opts, "current")
	if err != nil {
		return nil, err
	}
	desiredMap, err := tablesByName(desired, key, opts, "desired")
	if err != nil {
		return nil, err
	}

	// Collect sorted table names for deterministic output.
	desiredNames := slices.Sorted(maps.Keys(desiredMap))

	var creates []*AbstractStatement
	var alters []*AbstractStatement
	var drops []*AbstractStatement

	// New tables are collected first and emitted parent before child.
	var createNames []string
	createStmts := make(map[string][]*AbstractStatement)
	createDeps := make(map[string][]string)

	// Tables in desired: create if new, diff if existing.
	for _, k := range desiredNames {
		desiredTable := desiredMap[k]
		name := desiredTable.Name
		existingTable, exists := currentMap[k]
		if !exists {
			// New table — emit CREATE TABLE.
			stmts, err := New(desiredTable.Schema)
			if err != nil {
				return nil, fmt.Errorf("failed to parse CREATE TABLE for new table %q: %w", name, err)
			}
			for _, stmt := range stmts {
				if !stmt.IsCreateTable() {
					continue
				}
				ct, err := stmt.ParseCreateTable()
				if err != nil {
					return nil, fmt.Errorf("failed to parse CREATE TABLE for new table %q: %w", name, err)
				}
				if err := checkPrimaryKeyNullability(ct); err != nil {
					return nil, fmt.Errorf("invalid desired schema for table %q: %w", name, err)
				}
				for _, parent := range referencedTables(ct) {
					createDeps[k] = append(createDeps[k], key(parent))
				}
			}
			createNames = append(createNames, k)
			createStmts[k] = stmts
			continue
		}

		// Both exist — compute ALTER TABLE diff.
		diffs, err := diffTable(existingTable.Name, existingTable.Schema, desiredTable.Schema, opts)
		if err != nil {
			return nil, err
		}
		alters = append(alters, diffs...)
	}

	for _, k := range utils.TopologicalOrder(createNames, createDeps) {
		creates = append(creates, createStmts[k]...)
	}

	// Tables in current but not in desired — emit DROP TABLE, child before
	// parent. A dropped table's dependencies come from its current schema;
	// the DROP itself needs no definition, so a schema that does not parse
	// only loses its place in the order.
	dropNames := make([]string, 0)
	for k := range currentMap {
		if _, exists := desiredMap[k]; !exists {
			dropNames = append(dropNames, k)
		}
	}
	slices.Sort(dropNames)
	droppedBefore := make(map[string][]string)
	for _, k := range dropNames {
		ct, err := ParseCreateTable(currentMap[k].Schema)
		if err != nil {
			continue
		}
		for _, parent := range referencedTables(ct) {
			droppedBefore[key(parent)] = append(droppedBefore[key(parent)], k)
		}
	}

	for _, k := range utils.TopologicalOrder(dropNames, droppedBefore) {
		name := currentMap[k].Name
		stmts, err := New(fmt.Sprintf("DROP TABLE %s", sqlescape.EscapeIdentifier(name)))
		if err != nil {
			return nil, fmt.Errorf("failed to parse DROP TABLE for %q: %w", name, err)
		}
		drops = append(drops, stmts...)
	}

	// Order: CREATE first, then ALTER, then DROP.
	result := make([]*AbstractStatement, 0, len(creates)+len(alters)+len(drops))
	result = append(result, creates...)
	result = append(result, alters...)
	result = append(result, drops...)
	return result, nil
}

// tableNameKey returns how DeclarativeToImperative keys a table name: folded to
// lower case when the target compares names case-insensitively, as is.
func tableNameKey(opts *DiffOptions) func(string) string {
	if opts != nil && opts.LowerCaseTableNames != 0 {
		return strings.ToLower
	}
	return func(name string) string { return name }
}

// tablesByName indexes tables by key(name), refusing two tables with the same
// key. side names the schema ("current" or "desired") in the error.
func tablesByName(tables []table.TableSchema, key func(string) string, opts *DiffOptions, side string) (map[string]table.TableSchema, error) {
	byName := make(map[string]table.TableSchema, len(tables))
	for _, t := range tables {
		k := key(t.Name)
		earlier, seen := byName[k]
		switch {
		case !seen:
			byName[k] = t
		case earlier.Name == t.Name:
			return nil, fmt.Errorf("%s schema declares table %q more than once", side, t.Name)
		default:
			return nil, fmt.Errorf("%s schema declares tables %q and %q, which are the same table when lower_case_table_names=%d",
				side, earlier.Name, t.Name, opts.LowerCaseTableNames)
		}
	}
	return byName, nil
}

// referencedTables returns the names of the tables ct's foreign keys
// reference, without their schema: DeclarativeToImperative orders one
// schema's tables, which carry no schema of their own to compare against.
func referencedTables(ct *CreateTable) []string {
	var names []string
	for i := range ct.Constraints {
		if ref := ct.Constraints[i].References; ref != nil {
			names = append(names, ref.Table)
		}
	}
	return names
}

// diffTable computes the ALTER TABLE diff for a single table, recovering from
// panics in CreateTable.Diff(). Diff() can panic on certain edge cases (e.g.
// formatting differences between MySQL's SHOW CREATE TABLE output and embedded
// schema files). This recovery ensures DeclarativeToImperative is at least as
// safe as callers who previously wrapped Diff() in recover() themselves.
func diffTable(name, currentSchema, desiredSchema string, opts *DiffOptions) (stmts []*AbstractStatement, err error) {
	a, err := ParseCreateTable(currentSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse current schema for table %q: %w", name, err)
	}
	b, err := ParseCreateTable(desiredSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse desired schema for table %q: %w", name, err)
	}
	// DeclarativeToImperative matched the two by tableNameKey, so under a
	// case-insensitive target they may differ in case only. The ALTER names
	// the table as it exists.
	if tableNameKey(opts)(b.TableName) == tableNameKey(opts)(a.TableName) {
		b.TableName = a.TableName
	}
	defer func() {
		if r := recover(); r != nil {
			stmts = nil
			err = fmt.Errorf("panic diffing table %q: %v", name, r)
		}
	}()

	diffs, diffErr := a.Diff(b, opts)
	if diffErr != nil {
		return nil, fmt.Errorf("failed to diff table %q: %w", name, diffErr)
	}
	return diffs, nil
}

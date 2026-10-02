package statement

import (
	"fmt"
	"maps"
	"slices"

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
// If opts is nil, NewDiffOptions() defaults are used for table diffs.
func DeclarativeToImperative(current, desired []table.TableSchema, opts *DiffOptions) ([]*AbstractStatement, error) {
	currentMap := make(map[string]table.TableSchema, len(current))
	desiredMap := make(map[string]table.TableSchema, len(desired))
	for _, t := range current {
		currentMap[t.Name] = t
	}
	for _, t := range desired {
		desiredMap[t.Name] = t
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
	for _, name := range desiredNames {
		desiredTable := desiredMap[name]
		existingTable, exists := currentMap[name]
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
				createDeps[name] = append(createDeps[name], referencedTables(ct)...)
			}
			createNames = append(createNames, name)
			createStmts[name] = stmts
			continue
		}

		// Both exist — compute ALTER TABLE diff.
		diffs, err := diffTable(name, existingTable.Schema, desiredTable.Schema, opts)
		if err != nil {
			return nil, err
		}
		alters = append(alters, diffs...)
	}

	for _, name := range utils.TopologicalOrder(createNames, createDeps) {
		creates = append(creates, createStmts[name]...)
	}

	// Tables in current but not in desired — emit DROP TABLE, child before
	// parent. A dropped table's dependencies come from its current schema;
	// the DROP itself needs no definition, so a schema that does not parse
	// only loses its place in the order.
	dropNames := make([]string, 0)
	for name := range currentMap {
		if _, exists := desiredMap[name]; !exists {
			dropNames = append(dropNames, name)
		}
	}
	slices.Sort(dropNames)
	droppedBefore := make(map[string][]string)
	for _, name := range dropNames {
		ct, err := ParseCreateTable(currentMap[name].Schema)
		if err != nil {
			continue
		}
		for _, parent := range referencedTables(ct) {
			droppedBefore[parent] = append(droppedBefore[parent], name)
		}
	}

	for _, name := range utils.TopologicalOrder(dropNames, droppedBefore) {
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

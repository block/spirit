package statement

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/parser/format"
	"github.com/block/spirit/pkg/table"
)

// DeclarativeToImperative compares current and desired schemas and returns the
// imperative DDL statements (ALTER, CREATE, DROP) needed to transform current
// into desired.
//
// This is the core of declarative schema management: given two sets of table
// definitions, compute the minimal set of changes. It is used by spirit's diff
// subcommand, strata, and GAP.
//
// The returned statements are ordered as CREATE → ALTER → DROP (within each
// group, tables are sorted alphabetically). This ordering is a correctness
// property: it ensures the output is safe to execute sequentially (e.g. an
// ALTER that adds a foreign key referencing a newly-created table will run
// after the CREATE, and a table referenced by a FK won't be dropped before
// the referencing ALTER runs).
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
			creates = append(creates, stmts...)
			continue
		}

		// Both exist — compute ALTER TABLE diff.
		diffs, err := diffTable(name, existingTable.Schema, desiredTable.Schema, opts)
		if err != nil {
			return nil, err
		}
		alters = append(alters, diffs...)
	}

	// Tables in current but not in desired — emit DROP TABLE.
	dropNames := make([]string, 0)
	for name := range currentMap {
		if _, exists := desiredMap[name]; !exists {
			dropNames = append(dropNames, name)
		}
	}
	slices.Sort(dropNames)

	for _, name := range dropNames {
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

// ToTableSchema converts a parsed CreateTable back to a table.TableSchema
// by restoring the AST to SQL. This is useful when callers have already parsed
// schemas (e.g. for linting) but need to pass them to DeclarativeToImperative.
func (ct *CreateTable) ToTableSchema() (table.TableSchema, error) {
	var sb strings.Builder
	rCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := ct.Raw.Restore(rCtx); err != nil {
		return table.TableSchema{}, fmt.Errorf("failed to restore CREATE TABLE for %q: %w", ct.TableName, err)
	}
	return table.TableSchema{
		Name:   ct.TableName,
		Schema: sb.String(),
	}, nil
}

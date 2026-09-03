package lint

import (
	"maps"
	"slices"
	"strings"

	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/types"
	"github.com/block/spirit/pkg/statement"
)

// PostState returns a deterministic post-state view of the schema: the existing
// tables augmented with CREATE TABLE statements from changes, and existing tables
// patched with ADD/DROP/MODIFY/CHANGE COLUMN and ADD/DROP INDEX (and DROP PRIMARY
// KEY) specs from ALTER TABLE statements. Other ALTER specs are ignored.
//
// The result is sorted by table name. The input tables are not mutated.
//
// Linters that need to evaluate the schema as it will exist *after* a set of
// pending changes (rather than as it exists today) should iterate this slice
// rather than existingTables directly — otherwise they will produce false
// positives for ALTER statements that fix legacy issues.
func PostState(existing []*statement.CreateTable, changes []*statement.AbstractStatement) []*statement.CreateTable {
	byName := make(map[string]*statement.CreateTable, len(existing))
	for _, t := range existing {
		byName[strings.ToLower(t.TableName)] = t
	}

	for _, change := range changes {
		if change == nil {
			continue
		}
		if change.IsCreateTable() {
			ct, err := change.ParseCreateTable()
			if err == nil && ct != nil {
				byName[strings.ToLower(ct.TableName)] = ct
			}
			continue
		}
		at, ok := change.AsAlterTable()
		if !ok {
			continue
		}
		key := strings.ToLower(change.Table)
		base, found := byName[key]
		if !found {
			// Synthesize a placeholder for ALTERs whose target isn't in
			// existingTables. applyAlter still produces a best-effort
			// post-state — column adds/modifies/changes show up; drops are
			// harmless against an empty column list. This is the right
			// behavior for linters that evaluate the affected columns
			// individually (has_timestamp, has_float, zero_date) even
			// without full schema context.
			base = &statement.CreateTable{TableName: change.Table}
		}
		result := applyAlter(base, at)
		// RENAME TABLE changes the key the post-state map should index by.
		newKey := strings.ToLower(result.TableName)
		if newKey != key {
			delete(byName, key)
		}
		byName[newKey] = result
	}

	out := slices.Collect(maps.Values(byName))
	slices.SortFunc(out, func(a, b *statement.CreateTable) int { return strings.Compare(a.TableName, b.TableName) })
	return out
}

// newTablesInChanges returns the set of (lowercased) table names that are
// created by a CREATE TABLE statement in changes. Columns inside these tables
// are considered "new", not legacy.
func newTablesInChanges(changes []*statement.AbstractStatement) map[string]bool {
	out := make(map[string]bool)
	for _, change := range changes {
		if change == nil || !change.IsCreateTable() {
			continue
		}
		ct, err := change.ParseCreateTable()
		if err != nil || ct == nil {
			continue
		}
		out[strings.ToLower(ct.TableName)] = true
	}
	return out
}

// columnsAddedOrModifiedInChanges returns, for each table, the set of
// (lowercased) column names that are added or modified by ALTER TABLE
// statements in changes. CHANGE COLUMN records both the old name and the
// new name as added/modified — neither is "pre-existing untouched". MODIFY
// COLUMN records the column name. ADD COLUMN records the new column name.
// DROP COLUMN is not recorded here (dropped columns don't appear in
// post-state at all).
func columnsAddedOrModifiedInChanges(changes []*statement.AbstractStatement) map[string]map[string]bool {
	out := make(map[string]map[string]bool)
	mark := func(table, col string) {
		tKey := strings.ToLower(table)
		if out[tKey] == nil {
			out[tKey] = make(map[string]bool)
		}
		out[tKey][strings.ToLower(col)] = true
	}
	for _, change := range changes {
		if change == nil {
			continue
		}
		at, ok := change.AsAlterTable()
		if !ok {
			continue
		}
		for _, spec := range at.Specs {
			switch spec.Tp { //nolint:exhaustive
			case ast.AlterTableAddColumns, ast.AlterTableModifyColumn:
				for _, col := range spec.NewColumns {
					if col.Name != nil {
						mark(change.Table, col.Name.Name.O)
					}
				}
			case ast.AlterTableChangeColumn:
				if spec.OldColumnName != nil {
					mark(change.Table, spec.OldColumnName.Name.O)
				}
				for _, col := range spec.NewColumns {
					if col.Name != nil {
						mark(change.Table, col.Name.Name.O)
					}
				}
			}
		}
	}
	return out
}

// columnsModifiedInChanges returns, for each table, the (lowercased) column
// names that are retyped via MODIFY COLUMN or CHANGE COLUMN — i.e. operations
// that act on a column that should already exist. The map distinguishes
// retypes from ADD COLUMN, which is useful when a linter wants different
// messaging for "new column" vs "existing column being changed".
func columnsModifiedInChanges(changes []*statement.AbstractStatement) map[string]map[string]bool {
	out := make(map[string]map[string]bool)
	mark := func(table, col string) {
		tKey := strings.ToLower(table)
		if out[tKey] == nil {
			out[tKey] = make(map[string]bool)
		}
		out[tKey][strings.ToLower(col)] = true
	}
	for _, change := range changes {
		if change == nil {
			continue
		}
		at, ok := change.AsAlterTable()
		if !ok {
			continue
		}
		for _, spec := range at.Specs {
			switch spec.Tp { //nolint:exhaustive
			case ast.AlterTableModifyColumn:
				for _, col := range spec.NewColumns {
					if col.Name != nil {
						mark(change.Table, col.Name.Name.O)
					}
				}
			case ast.AlterTableChangeColumn:
				if spec.OldColumnName != nil {
					mark(change.Table, spec.OldColumnName.Name.O)
				}
				for _, col := range spec.NewColumns {
					if col.Name != nil {
						mark(change.Table, col.Name.Name.O)
					}
				}
			}
		}
	}
	return out
}

// PreStateColumns returns a lookup of (lowercased table name) → (lowercased
// column name) → column from the existing tables. Linters use this to
// distinguish columns that pre-existed (severity Warning) from columns added
// or modified by changes (severity Error).
func PreStateColumns(existing []*statement.CreateTable) map[string]map[string]*statement.Column {
	out := make(map[string]map[string]*statement.Column, len(existing))
	for _, t := range existing {
		tKey := strings.ToLower(t.TableName)
		cols := make(map[string]*statement.Column, len(t.Columns))
		for i := range t.Columns {
			c := &t.Columns[i]
			cols[strings.ToLower(c.Name)] = c
		}
		out[tKey] = cols
	}
	return out
}

// applyAlter returns a shallow clone of t with the relevant subset of alter
// specs applied. The original table is not mutated.
//
// Note: GetIndexes() synthesizes PRIMARY KEY / UNIQUE entries from inline
// column-level flags (col.PrimaryKey, col.Unique), so the drop paths below
// must clear those flags in addition to scrubbing cloned.Indexes — otherwise
// the synthesis would resurrect the dropped index in the post-state.
func applyAlter(t *statement.CreateTable, at *ast.AlterTableStmt) *statement.CreateTable {
	cloned := *t
	cloned.Columns = append(statement.Columns(nil), t.Columns...)
	cloned.Indexes = append(statement.Indexes(nil), t.Indexes...)
	cloned.Constraints = append(statement.Constraints(nil), t.Constraints...)
	if t.TableOptions != nil {
		opts := *t.TableOptions
		cloned.TableOptions = &opts
	}

	for _, spec := range at.Specs {
		switch spec.Tp { //nolint:exhaustive
		case ast.AlterTableAddColumns:
			for _, colDef := range spec.NewColumns {
				cloned.Columns = append(cloned.Columns, columnFromAst(colDef))
			}
		case ast.AlterTableDropColumn:
			if spec.OldColumnName != nil {
				cloned.Columns = removeColumn(cloned.Columns, spec.OldColumnName.Name.O)
			}
		case ast.AlterTableModifyColumn:
			if len(spec.NewColumns) > 0 {
				colDef := spec.NewColumns[0]
				cloned.Columns = replaceColumn(cloned.Columns, colDef.Name.Name.O, columnFromAst(colDef))
			}
		case ast.AlterTableChangeColumn:
			if len(spec.NewColumns) > 0 && spec.OldColumnName != nil {
				colDef := spec.NewColumns[0]
				cloned.Columns = replaceColumn(cloned.Columns, spec.OldColumnName.Name.O, columnFromAst(colDef))
			}
		case ast.AlterTableAddConstraint:
			if spec.Constraint == nil {
				continue
			}
			if idx, ok := indexFromConstraint(spec.Constraint); ok {
				cloned.Indexes = append(cloned.Indexes, idx)
				// An inline-flag column whose constraint we just promoted to
				// the table-level Indexes list could be double-counted. The
				// duplication is harmless for the indexed-column set (a set,
				// not a multiset), so we don't dedupe here.
			}
			if c, ok := nonIndexConstraint(spec.Constraint); ok {
				cloned.Constraints = append(cloned.Constraints, c)
			}
		case ast.AlterTableDropIndex:
			before := len(cloned.Indexes)
			cloned.Indexes = removeIndex(cloned.Indexes, spec.Name, "")
			if len(cloned.Indexes) == before {
				// Nothing removed at the table level — this may target an
				// implicit index created by an inline column-level UNIQUE,
				// whose server-assigned index name is the column's name.
				cloned.Columns = clearInlineUniqueByName(cloned.Columns, spec.Name)
			}
		case ast.AlterTableDropPrimaryKey:
			cloned.Indexes = removeIndex(cloned.Indexes, "", "PRIMARY KEY")
			// Inline `col TYPE PRIMARY KEY` never appears in t.Indexes; it
			// only surfaces via GetIndexes() synthesizing from this flag.
			// Clearing it here makes the post-state honor DROP PRIMARY KEY.
			cloned.Columns = clearAllInlinePrimaryKey(cloned.Columns)
		case ast.AlterTableDropForeignKey:
			cloned.Constraints = removeConstraint(cloned.Constraints, spec.Name, "FOREIGN KEY")
		case ast.AlterTableDropCheck:
			// The DROP CHECK production carries the name on spec.Constraint,
			// not spec.Name.
			if spec.Constraint != nil {
				cloned.Constraints = removeConstraint(cloned.Constraints, spec.Constraint.Name, "CHECK")
			}
		case ast.AlterTableDropConstraint:
			// DROP CONSTRAINT resolves the name against the table's CHECK,
			// FOREIGN KEY and UNIQUE constraints, so drop whichever one owns
			// it. MySQL errors if the name matches none of them; the post
			// state just leaves the table unchanged in that case.
			if spec.Constraint != nil {
				name := spec.Constraint.Name
				cloned.Constraints = removeConstraint(cloned.Constraints, name, "CHECK")
				cloned.Constraints = removeConstraint(cloned.Constraints, name, "FOREIGN KEY")
				before := len(cloned.Indexes)
				cloned.Indexes = removeUniqueIndexByName(cloned.Indexes, name)
				if len(cloned.Indexes) == before {
					// As in DROP INDEX: an inline column-level UNIQUE has no
					// table-level index, and takes the column's name.
					cloned.Columns = clearInlineUniqueByName(cloned.Columns, name)
				}
			}
		case ast.AlterTableRenameTable:
			if spec.NewTable != nil {
				cloned.TableName = spec.NewTable.Name.O
			}
		case ast.AlterTableOption:
			cloned.TableOptions = applyTableOptions(cloned.TableOptions, spec.Options)
		}
	}
	return &cloned
}

// applyTableOptions returns a TableOptions with the changes from opts merged in.
// Options not set in opts are preserved from the input; setting an empty string
// or zero value leaves the existing option untouched (matches MySQL semantics —
// table options without a clause are not reset to defaults).
func applyTableOptions(existing *statement.TableOptions, opts []*ast.TableOption) *statement.TableOptions {
	if len(opts) == 0 {
		return existing
	}
	var out statement.TableOptions
	if existing != nil {
		out = *existing
	}
	for _, opt := range opts {
		switch opt.Tp { //nolint:exhaustive
		case ast.TableOptionEngine:
			if opt.StrValue != "" {
				v := opt.StrValue
				out.Engine = &v
			}
		case ast.TableOptionCharset:
			if opt.StrValue != "" {
				v := opt.StrValue
				out.Charset = &v
			}
		case ast.TableOptionCollate:
			if opt.StrValue != "" {
				v := opt.StrValue
				out.Collation = &v
			}
		case ast.TableOptionAutoIncrement:
			if opt.UintValue > 0 {
				v := opt.UintValue
				out.AutoIncrement = &v
			}
		case ast.TableOptionComment:
			if opt.StrValue != "" {
				v := opt.StrValue
				out.Comment = &v
			}
		}
	}
	return &out
}

// nonIndexConstraint converts an ast.Constraint that is *not* an index-like
// constraint (FK, CHECK) into a statement.Constraint suitable for the post-
// state Constraints slice. Returns (_, false) for index/PK/UNIQUE constraints,
// which are already handled by indexFromConstraint.
func nonIndexConstraint(c *ast.Constraint) (statement.Constraint, bool) {
	switch c.Tp { //nolint:exhaustive
	case ast.ConstraintForeignKey:
		return statement.Constraint{Raw: c, Name: c.Name, Type: "FOREIGN KEY"}, true
	case ast.ConstraintCheck:
		return statement.Constraint{Raw: c, Name: c.Name, Type: "CHECK"}, true
	}
	return statement.Constraint{}, false
}

func removeConstraint(cs statement.Constraints, name, typeMatch string) statement.Constraints {
	out := cs[:0]
	for _, c := range cs {
		if name != "" && strings.EqualFold(c.Name, name) && c.Type == typeMatch {
			continue
		}
		out = append(out, c)
	}
	return out
}

// columnFromAst constructs a minimal statement.Column from an AST column def.
// Raw (for type information), Name, the base Type string, and inline
// PrimaryKey/Unique flags are populated — that's enough for the linters that
// need post-state. The base Type mirrors CreateTable.parseColumn so that
// linters comparing against Column.Type (e.g. primary_key) work on
// ADD/MODIFY/CHANGE COLUMN specs, not just fully-parsed existing tables.
// Binary/spatial nuances aren't recovered here; linters that care read Raw.
func columnFromAst(colDef *ast.ColumnDef) statement.Column {
	col := statement.Column{
		Raw:  colDef,
		Name: colDef.Name.Name.O,
	}
	if colDef.Tp != nil {
		col.Type = types.TypeStr(colDef.Tp.GetType())
		// Charset/collation come off the type first and may be overridden by a
		// COLLATE option below — the same order CreateTable.parseColumn uses,
		// so a MODIFY/CHANGE COLUMN that converges a collation is visible to
		// type_pedantic's collation rule. Spatial and VECTOR types carry a
		// synthetic "binary" charset here that parseColumn's normalizer would
		// strip; nothing reads charset for those types, so it is left alone.
		if charset := colDef.Tp.GetCharset(); charset != "" {
			col.Charset = &charset
		}
		if collation := colDef.Tp.GetCollate(); collation != "" {
			col.Collation = &collation
		}
	}
	for _, opt := range colDef.Options {
		switch opt.Tp { //nolint:exhaustive
		case ast.ColumnOptionPrimaryKey:
			col.PrimaryKey = true
		case ast.ColumnOptionUniqKey:
			col.Unique = true
		case ast.ColumnOptionCollate:
			if opt.StrValue != "" {
				col.Collation = &opt.StrValue
			}
		}
	}
	return col
}

func removeColumn(cols statement.Columns, name string) statement.Columns {
	out := cols[:0]
	for _, c := range cols {
		if !strings.EqualFold(c.Name, name) {
			out = append(out, c)
		}
	}
	return out
}

// replaceColumn substitutes newCol in place of the column named oldName,
// preserving any inline PRIMARY KEY / UNIQUE flags from the old definition.
// MySQL semantics: MODIFY / CHANGE COLUMN re-types a column but does not drop
// existing constraints on it; those are removed only via DROP PRIMARY KEY /
// DROP INDEX. The new column def from the parser doesn't carry those over.
func replaceColumn(cols statement.Columns, oldName string, newCol statement.Column) statement.Columns {
	for i, c := range cols {
		if strings.EqualFold(c.Name, oldName) {
			if c.PrimaryKey && !newCol.PrimaryKey {
				newCol.PrimaryKey = true
			}
			if c.Unique && !newCol.Unique {
				newCol.Unique = true
			}
			cols[i] = newCol
			return cols
		}
	}
	return append(cols, newCol)
}

// clearAllInlinePrimaryKey clears the inline PrimaryKey flag from every
// column. Used by DROP PRIMARY KEY since inline `col TYPE PRIMARY KEY` only
// shows up via GetIndexes() synthesizing from this flag.
func clearAllInlinePrimaryKey(cols statement.Columns) statement.Columns {
	for i := range cols {
		cols[i].PrimaryKey = false
	}
	return cols
}

// clearInlineUniqueByName clears col.Unique on the column whose name matches
// the dropped index name. MySQL names the implicit index for an inline
// `col TYPE UNIQUE` after the column itself, so DROP INDEX <colname> can
// target it. We only run this when no table-level index of that name existed.
func clearInlineUniqueByName(cols statement.Columns, indexName string) statement.Columns {
	if indexName == "" {
		return cols
	}
	for i := range cols {
		if strings.EqualFold(cols[i].Name, indexName) && cols[i].Unique {
			cols[i].Unique = false
			break
		}
	}
	return cols
}

func indexFromConstraint(c *ast.Constraint) (statement.Index, bool) {
	var typeStr string
	switch c.Tp { //nolint:exhaustive
	case ast.ConstraintPrimaryKey:
		typeStr = "PRIMARY KEY"
	case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
		typeStr = "UNIQUE"
	case ast.ConstraintKey, ast.ConstraintIndex:
		typeStr = "INDEX"
	default:
		return statement.Index{}, false
	}
	cols := make([]string, 0, len(c.Keys))
	for _, k := range c.Keys {
		if k.Column != nil {
			cols = append(cols, k.Column.Name.O)
		}
	}
	return statement.Index{
		Name:    c.Name,
		Type:    typeStr,
		Columns: cols,
	}, true
}

// removeUniqueIndexByName removes the UNIQUE index of the given name, and only
// that: DROP CONSTRAINT does not resolve against a non-unique index, so it must
// not remove one that happens to share the name.
func removeUniqueIndexByName(indexes statement.Indexes, name string) statement.Indexes {
	if name == "" {
		return indexes
	}
	out := indexes[:0]
	for _, idx := range indexes {
		if strings.EqualFold(idx.Name, name) && idx.Type == "UNIQUE" {
			continue
		}
		out = append(out, idx)
	}
	return out
}

func removeIndex(indexes statement.Indexes, name, typeMatch string) statement.Indexes {
	out := indexes[:0]
	for _, idx := range indexes {
		if name != "" && strings.EqualFold(idx.Name, name) {
			continue
		}
		if typeMatch != "" && idx.Type == typeMatch {
			continue
		}
		out = append(out, idx)
	}
	return out
}

package statement

import (
	"strings"

	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/parser/ast"
)

func init() { registerNormalizer(columnReferenceCaseNormalizer{}) }

// columnReferenceCaseNormalizer rewrites every column reference in a stored
// expression to the case the column is declared in, which is what MySQL
// reports: a generated column written AS (C + 1) over a column declared `c`
// reads back from SHOW CREATE TABLE as ((`c` + 1)), and so do a functional
// index key part and a partition expression. Column names are
// case-insensitive, so the two spellings are the same expression, but the
// texts differ, and a declarative diff emits a MODIFY COLUMN (a full table
// copy), a DROP+ADD INDEX or a PARTITION BY on every run.
//
// A CHECK constraint is included although MySQL keeps the written case there
// at CREATE TABLE: it re-renders the constraint in declared case after a later
// ALTER of the table, so a definition that matched the live one stops matching
// once an unrelated change has been applied. Rewriting both sides makes the
// comparison independent of which has happened.
//
// Only a reference that names a declared column is rewritten; anything else (a
// name MySQL would reject) is left as written. Expression DEFAULTs are not
// covered, because MySQL rejects a column reference in one.
type columnReferenceCaseNormalizer struct{}

func (columnReferenceCaseNormalizer) Name() string { return "column-reference-case" }

func (columnReferenceCaseNormalizer) Normalize(ct *CreateTable) *CreateTable {
	declared := make(map[string]string, len(ct.Columns))
	for _, col := range ct.Columns {
		declared[strings.ToLower(col.Name)] = col.Name
	}
	p := parser.New()
	rewrite := func(text *string) bool {
		return rewriteExpressionText(p, text, restoreExpressionText, &columnReferenceCaser{declared: declared})
	}
	for i := range ct.Columns {
		col := &ct.Columns[i]
		rewrite(col.GeneratedExpr)
		for j := range col.Checks {
			rewrite(&col.Checks[j].Expression)
		}
	}
	for i := range ct.Constraints {
		c := &ct.Constraints[i]
		if c.Type != "CHECK" || c.Expression == nil {
			continue
		}
		if rewrite(c.Expression) {
			// The rendered definition is what diffConstraints emits, so it has
			// to track the expression it was built from.
			definition := checkConstraintDefinition(c)
			c.Definition = &definition
		}
	}
	for i := range ct.Indexes {
		for j := range ct.Indexes[i].ColumnList {
			rewrite(ct.Indexes[i].ColumnList[j].Expression)
		}
	}
	if ct.Partition != nil {
		rewrite(ct.Partition.Expression)
		if ct.Partition.SubPartition != nil {
			rewrite(ct.Partition.SubPartition.Expression)
		}
	}
	return ct
}

// columnReferenceCaser is the exprRewriter behind columnReferenceCaseNormalizer:
// it respells each reference to a declared column on the way back up the tree.
type columnReferenceCaser struct {
	declared map[string]string // lowercased column name → declared spelling
	changed  bool
}

func (r *columnReferenceCaser) Enter(n ast.Node) (ast.Node, bool) { return n, false }

func (r *columnReferenceCaser) Leave(n ast.Node) (ast.Node, bool) {
	ref, ok := n.(*ast.ColumnNameExpr)
	if !ok || ref.Name == nil {
		return n, true
	}
	want, ok := r.declared[ref.Name.Name.L]
	if !ok || ref.Name.Name.O == want {
		return n, true
	}
	ref.Name.Name = ast.NewCIStr(want)
	r.changed = true
	return n, true
}

func (r *columnReferenceCaser) Changed() bool { return r.changed }

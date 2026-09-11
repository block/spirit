package statement

import (
	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/parser/ast"
)

func init() { registerNormalizer(functionAliasNormalizer{}) }

// mysqlFunctionAliases maps a function name MySQL accepts on input to the name
// it stores. MySQL resolves an alias when it parses the expression and keeps
// only the resolved item, so SHOW CREATE TABLE reports the stored name — a
// column written as DEFAULT (STRING_TO_VECTOR('[1,2,3]')) reads back as
// DEFAULT (to_vector(_latin1'[1,2,3]')).
//
// Every entry here was verified against MySQL 9.7 by creating the column and
// reading SHOW CREATE TABLE back. No key is also a value, so the mapping is
// idempotent: a live definition never contains a name that needs rewriting
// (asserted by TestFunctionAliasesAreIdempotent).
//
// MySQL also rewrites some functions *structurally*, which a rename cannot
// express and which this rule deliberately leaves alone: MOD(a,b) and
// ADDDATE(d, INTERVAL ...) become operators, INSTR(a,b) becomes
// locate(b,a) with its arguments swapped, WEEKOFYEAR(d) becomes week(d,3),
// BIN(n) becomes conv(n,10,2). Those defaults still re-emit on every diff.
// The timestamp family (current_timestamp, localtime, localtimestamp → now) is
// only reachable here from inside an expression default: the literal-style
// DEFAULT LOCALTIME / ON UPDATE NOW() forms are folded to CURRENT_TIMESTAMP by
// the parser, which is the spelling MySQL reports for them.
var mysqlFunctionAliases = map[string]string{
	"ceil":              "ceiling",
	"character_length":  "char_length",
	"current_date":      "curdate",
	"current_time":      "curtime",
	"current_timestamp": "now",
	"day":               "dayofmonth",
	"lcase":             "lower",
	"localtime":         "now",
	"localtimestamp":    "now",
	"mid":               "substr",
	"octet_length":      "length",
	"position":          "locate",
	"power":             "pow",
	"session_user":      "user",
	"string_to_vector":  "to_vector",
	"substring":         "substr",
	"system_user":       "user",
	"ucase":             "upper",
	"vector_to_string":  "from_vector",
}

// functionAliasNormalizer rewrites every function name in a stored expression
// to the name MySQL reports for it. Without this, an expression that spells a
// function by one of its aliases never converges: the authored text and the
// SHOW CREATE TABLE text differ forever, so a declarative diff emits the same
// no-op statement on every run — and for a column DEFAULT that no-op is a
// MODIFY COLUMN, i.e. a full table copy each time (issue #1152).
//
// It covers every expression MySQL stores and reports back: column expression
// DEFAULTs, generated columns, CHECK constraints (column-level and table-level),
// functional index key parts, and partition expressions.
//
// The rule renames only; it does not touch the shape of the expression. The
// other half of the rewriting MySQL does to a stored expression — charset
// introducers, so that 'x' reads back as _latin1'x' — needs no rule, because
// the introducer is dropped when the expression is restored to text
// (format.RestoreStringWithoutCharset) on both sides of the diff.
type functionAliasNormalizer struct{}

func (functionAliasNormalizer) Name() string { return "function-aliases" }

func (functionAliasNormalizer) Normalize(ct *CreateTable) *CreateTable {
	p := parser.New()
	for i := range ct.Columns {
		col := &ct.Columns[i]
		// A string-literal default holds a value, not an expression, even in
		// the parenthesized DEFAULT ('{}') form.
		if col.DefaultIsExpr && col.DefaultKind != DefaultKindString {
			canonicalizeFuncAliases(p, col.Default, restoreExprDefaultText)
		}
		canonicalizeFuncAliases(p, col.GeneratedExpr, restoreExpressionText)
		canonicalizeFuncAliases(p, col.Check, restoreExpressionText)
	}
	for i := range ct.Constraints {
		c := &ct.Constraints[i]
		if c.Type != "CHECK" || c.Expression == nil {
			continue
		}
		if canonicalizeFuncAliases(p, c.Expression, restoreExpressionText) {
			// The rendered definition is what diffConstraints emits, so it has
			// to track the expression it was built from.
			definition := checkConstraintDefinition(c)
			c.Definition = &definition
		}
	}
	for i := range ct.Indexes {
		for j := range ct.Indexes[i].ColumnList {
			canonicalizeFuncAliases(p, ct.Indexes[i].ColumnList[j].Expression, restoreExpressionText)
		}
	}
	if ct.Partition != nil {
		canonicalizeFuncAliases(p, ct.Partition.Expression, restoreExpressionText)
		if ct.Partition.SubPartition != nil {
			// A subpartition expression is parsed through parseExpression, not
			// the generic expression restore the partition expression uses.
			canonicalizeFuncAliases(p, ct.Partition.SubPartition.Expression, restoreLiteralStyleText)
		}
	}
	return ct
}

// functionAliasRewriter is the ast.Visitor behind canonicalizeFuncAliases: it
// renames each aliased function call on the way back up the tree.
type functionAliasRewriter struct{ renamed bool }

func (r *functionAliasRewriter) Enter(n ast.Node) (ast.Node, bool) { return n, false }

func (r *functionAliasRewriter) Leave(n ast.Node) (ast.Node, bool) {
	call, ok := n.(*ast.FuncCallExpr)
	// A schema-qualified call is a stored function, not a builtin: db.day(x)
	// is whatever the user defined, and renaming it would call something else.
	if !ok || call.Schema.L != "" {
		return n, true
	}
	canonical, ok := mysqlFunctionAliases[call.FnName.L]
	if !ok {
		return n, true
	}
	call.FnName = ast.NewCIStr(canonical)
	r.renamed = true
	return n, true
}

// canonicalizeFuncAliases rewrites the aliased function names in an expression
// text, in place, and reports whether anything changed. A nil or empty text is
// left alone.
//
// render must be the same restore the text was originally produced with, so
// that an expression holding an alias and one already spelling the stored name
// render identically — that identity is the whole point of the rule. An
// expression with no alias in it is left byte-for-byte untouched rather than
// re-rendered, so this rule cannot perturb a form another rule established
// (which is what keeps it order-independent).
func canonicalizeFuncAliases(p *parser.Parser, text *string, render func(ast.ExprNode) (string, bool)) bool {
	if text == nil || *text == "" {
		return false
	}
	expr, ok := parseExpressionText(p, *text)
	if !ok {
		return false
	}
	rewriter := &functionAliasRewriter{}
	node, ok := expr.Accept(rewriter)
	if !ok || !rewriter.renamed {
		return false
	}
	// The rewriter only ever renames a call in place, so the node it hands
	// back is the expression it was given. Check rather than assert anyway:
	// normalization runs on every parse, and leaving the text alone beats
	// panicking if a future visitor change breaks that.
	rewritten, ok := node.(ast.ExprNode)
	if !ok {
		return false
	}
	rendered, ok := render(rewritten)
	if !ok {
		return false
	}
	*text = rendered
	return true
}

// restoreExprDefaultText and restoreLiteralStyleText are restoreValueExprText's
// two modes, in the shape canonicalizeFuncAliases takes: each renders an
// expression exactly as the parse that produced the text rendered it.
func restoreExprDefaultText(expr ast.ExprNode) (string, bool) {
	text, ok := restoreValueExprText(expr, false).(string)
	return text, ok
}

func restoreLiteralStyleText(expr ast.ExprNode) (string, bool) {
	text, ok := restoreValueExprText(expr, true).(string)
	return text, ok
}

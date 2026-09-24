// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ast

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/block/spirit/pkg/parser/format"
	"github.com/block/spirit/pkg/parser/opcode"
)

var (
	_ ExprNode = &BetweenExpr{}
	_ ExprNode = &BinaryOperationExpr{}
	_ ExprNode = &CaseExpr{}
	_ ExprNode = &ColumnNameExpr{}
	_ ExprNode = &CompareSubqueryExpr{}
	_ ExprNode = &DefaultExpr{}
	_ ExprNode = &ExistsSubqueryExpr{}
	_ ExprNode = &IsNullExpr{}
	_ ExprNode = &IsTruthExpr{}
	_ ExprNode = &ParenthesesExpr{}
	_ ExprNode = &PatternInExpr{}
	_ ExprNode = &PatternLikeExpr{}
	_ ExprNode = &PatternRegexpExpr{}
	_ ExprNode = &PositionExpr{}
	_ ExprNode = &RowExpr{}
	_ ExprNode = &SubqueryExpr{}
	_ ExprNode = &UnaryOperationExpr{}
	_ ExprNode = &ValuesExpr{}
	_ ExprNode = &VariableExpr{}
	_ ExprNode = &MatchAgainst{}
	_ ExprNode = &SetCollationExpr{}

	_ Node = &ColumnName{}
	_ Node = &WhenClause{}
)

// BetweenExpr is for "between and" or "not between and" expression.
type BetweenExpr struct {
	exprNode
	// Expr is the expression to be checked.
	Expr ExprNode
	// Left is the expression for minimal value in the range.
	Left ExprNode
	// Right is the expression for maximum value in the range.
	Right ExprNode
	// Not is true, the expression is "not between and".
	Not bool
}

// Restore implements Node interface.
func (n *BetweenExpr) Restore(ctx *format.RestoreCtx) error {
	if ctx.Flags.HasRestoreBracketAroundBetweenExpr() {
		ctx.WritePlain("(")
	}
	// There is no opcode for BETWEEN. Use a restore-only opcode so BETWEEN
	// operands can be checked against MySQL precedence without conflating
	// BETWEEN with comparison operators.
	if err := restoreExprWithBinaryOpParent(ctx, n.Expr, restoreOpBetween, binaryOpLeftSide); err != nil {
		return fmt.Errorf("an error occurred while restore BetweenExpr.Expr: %w", err)
	}
	if n.Not {
		ctx.WriteKeyWord(" NOT BETWEEN ")
	} else {
		ctx.WriteKeyWord(" BETWEEN ")
	}
	if err := restoreExprWithBinaryOpParent(ctx, n.Left, restoreOpBetween, binaryOpRightSide); err != nil {
		return fmt.Errorf("an error occurred while restore BetweenExpr.Left: %w", err)
	}
	ctx.WriteKeyWord(" AND ")
	if err := restoreExprWithBinaryOpParent(ctx, n.Right, restoreOpBetween, binaryOpRightSide); err != nil {
		return fmt.Errorf("an error occurred while restore BetweenExpr.Right : %w", err)
	}
	if ctx.Flags.HasRestoreBracketAroundBetweenExpr() {
		ctx.WritePlain(")")
	}
	return nil
}

// Accept implements Node interface.
func (n *BetweenExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	n = newNode.(*BetweenExpr)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)

	node, ok = n.Left.Accept(v)
	if !ok {
		return n, false
	}
	n.Left = node.(ExprNode)

	node, ok = n.Right.Accept(v)
	if !ok {
		return n, false
	}
	n.Right = node.(ExprNode)

	return v.Leave(n)
}

// BinaryOperationExpr is for binary operation like `1 + 1`, `1 - 1`, etc.
type BinaryOperationExpr struct {
	exprNode
	// Op is the operator code for BinaryOperation.
	Op opcode.Op
	// L is the left expression in BinaryOperation.
	L ExprNode
	// R is the right expression in BinaryOperation.
	R ExprNode
}

func restoreBinaryOpWithSpacesAround(ctx *format.RestoreCtx, op opcode.Op) error {
	shouldInsertSpace := ctx.Flags.HasSpacesAroundBinaryOperationFlag() || op.IsKeyword()
	if shouldInsertSpace {
		ctx.WritePlain(" ")
	}
	if err := op.Restore(ctx); err != nil {
		return err // no need to annotate, the caller will annotate.
	}
	if shouldInsertSpace {
		ctx.WritePlain(" ")
	}
	return nil
}

// restoreExprWithBinaryOpParent restores expr as a child of parentOp, on
// parentSide, so a ParenthesesExpr below it can tell whether its parentheses
// are redundant under the surrounding operator.
func restoreExprWithBinaryOpParent(ctx *format.RestoreCtx, expr ExprNode, parentOp opcode.Op, parentSide int) error {
	originalParentOp, originalParentSide := ctx.ParentBinaryOp, ctx.ParentBinarySide
	defer func() {
		ctx.ParentBinaryOp, ctx.ParentBinarySide = originalParentOp, originalParentSide
	}()
	ctx.ParentBinaryOp, ctx.ParentBinarySide = int(parentOp), parentSide
	return expr.Restore(ctx)
}

// restoreExprWithUnaryOpParent restores expr as the operand of a unary
// operator, where no parentheses may be dropped.
func restoreExprWithUnaryOpParent(ctx *format.RestoreCtx, expr ExprNode) error {
	originalInUnaryOperation := ctx.InUnaryOperation
	defer func() {
		ctx.InUnaryOperation = originalInUnaryOperation
	}()
	ctx.InUnaryOperation = true
	return expr.Restore(ctx)
}

// restoreWithResetParentContext restores below a syntactic boundary — the
// parentheses of a subquery, or a pair of parentheses that is being kept —
// where the enclosing operator no longer constrains the child.
func restoreWithResetParentContext(ctx *format.RestoreCtx, restore func() error) error {
	inUnaryOperation, parentOp, parentSide := ctx.InUnaryOperation, ctx.ParentBinaryOp, ctx.ParentBinarySide
	defer func() {
		ctx.InUnaryOperation, ctx.ParentBinaryOp, ctx.ParentBinarySide = inUnaryOperation, parentOp, parentSide
	}()
	ctx.InUnaryOperation, ctx.ParentBinaryOp, ctx.ParentBinarySide = false, 0, 0
	return restore()
}

// Restore implements Node interface.
func (n *BinaryOperationExpr) Restore(ctx *format.RestoreCtx) error {
	originalFlags := ctx.Flags
	if ctx.Flags.HasRestoreBracketAroundBinaryOperation() {
		ctx.WritePlain("(")
		ctx.Flags |= format.RestoreBracketAroundBetweenExpr
	}
	parentOp, parentSide := ctx.ParentBinaryOp, ctx.ParentBinarySide
	defer func() {
		ctx.ParentBinaryOp, ctx.ParentBinarySide = parentOp, parentSide
	}()
	ctx.ParentBinaryOp, ctx.ParentBinarySide = int(n.Op), binaryOpLeftSide
	if err := n.L.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore BinaryOperationExpr.L: %w", err)
	}
	if err := restoreBinaryOpWithSpacesAround(ctx, n.Op); err != nil {
		return fmt.Errorf("an error occurred while restore BinaryOperationExpr.Op: %w", err)
	}
	ctx.ParentBinaryOp, ctx.ParentBinarySide = int(n.Op), binaryOpRightSide
	if err := n.R.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore BinaryOperationExpr.R: %w", err)
	}
	if ctx.Flags.HasRestoreBracketAroundBinaryOperation() {
		ctx.WritePlain(")")
		ctx.Flags = originalFlags
	}
	return nil
}

// Accept implements Node interface.
func (n *BinaryOperationExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	n = newNode.(*BinaryOperationExpr)
	node, ok := n.L.Accept(v)
	if !ok {
		return n, false
	}
	n.L = node.(ExprNode)

	node, ok = n.R.Accept(v)
	if !ok {
		return n, false
	}
	n.R = node.(ExprNode)

	return v.Leave(n)
}

// WhenClause is the when clause in Case expression for "when condition then result".
type WhenClause struct {
	node
	// Expr is the condition expression in WhenClause.
	Expr ExprNode
	// Result is the result expression in WhenClause.
	Result ExprNode
}

// Restore implements Node interface.
func (n *WhenClause) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("WHEN ")
	if err := n.Expr.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore WhenClauses.Expr: %w", err)
	}
	ctx.WriteKeyWord(" THEN ")
	if err := n.Result.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore WhenClauses.Result: %w", err)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *WhenClause) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	n = newNode.(*WhenClause)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)

	node, ok = n.Result.Accept(v)
	if !ok {
		return n, false
	}
	n.Result = node.(ExprNode)
	return v.Leave(n)
}

// CaseExpr is the case expression.
type CaseExpr struct {
	exprNode
	// Value is the compare value expression.
	Value ExprNode
	// WhenClauses is the condition check expression.
	WhenClauses []*WhenClause
	// ElseClause is the else result expression.
	ElseClause ExprNode
}

// Restore implements Node interface.
func (n *CaseExpr) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CASE")
	if n.Value != nil {
		ctx.WritePlain(" ")
		if err := n.Value.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore CaseExpr.Value: %w", err)
		}
	}
	for _, clause := range n.WhenClauses {
		ctx.WritePlain(" ")
		if err := clause.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore CaseExpr.WhenClauses: %w", err)
		}
	}
	if n.ElseClause != nil {
		ctx.WriteKeyWord(" ELSE ")
		if err := n.ElseClause.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore CaseExpr.ElseClause: %w", err)
		}
	}
	ctx.WriteKeyWord(" END")

	return nil
}

// Accept implements Node Accept interface.
func (n *CaseExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	n = newNode.(*CaseExpr)
	if n.Value != nil {
		node, ok := n.Value.Accept(v)
		if !ok {
			return n, false
		}
		n.Value = node.(ExprNode)
	}
	for i, val := range n.WhenClauses {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.WhenClauses[i] = node.(*WhenClause)
	}
	if n.ElseClause != nil {
		node, ok := n.ElseClause.Accept(v)
		if !ok {
			return n, false
		}
		n.ElseClause = node.(ExprNode)
	}
	return v.Leave(n)
}

// SubqueryExpr represents a subquery.
type SubqueryExpr struct {
	exprNode
	// Query is the query SelectNode.
	Query      ResultSetNode
	Evaluated  bool
	Correlated bool
	MultiRows  bool
	Exists     bool
}

func (*SubqueryExpr) resultSet() {}

// Restore implements Node interface.
func (n *SubqueryExpr) Restore(ctx *format.RestoreCtx) error {
	ctx.WritePlain("(")
	if err := restoreWithResetParentContext(ctx, func() error {
		return n.Query.Restore(ctx)
	}); err != nil {
		return fmt.Errorf("an error occurred while restore SubqueryExpr.Query: %w", err)
	}
	ctx.WritePlain(")")
	return nil
}

// Accept implements Node Accept interface.
func (n *SubqueryExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SubqueryExpr)
	node, ok := n.Query.Accept(v)
	if !ok {
		return n, false
	}
	n.Query = node.(ResultSetNode)
	return v.Leave(n)
}

// CompareSubqueryExpr is the expression for "expr cmp (select ...)".
// See https://dev.mysql.com/doc/refman/5.7/en/comparisons-using-subqueries.html
// See https://dev.mysql.com/doc/refman/5.7/en/any-in-some-subqueries.html
// See https://dev.mysql.com/doc/refman/5.7/en/all-subqueries.html
type CompareSubqueryExpr struct {
	exprNode
	// L is the left expression
	L ExprNode
	// Op is the comparison opcode.
	Op opcode.Op
	// R is the subquery for right expression, may be rewritten to other type of expression.
	R ExprNode
	// All is true, we should compare all records in subquery.
	All bool
}

// Restore implements Node interface.
func (n *CompareSubqueryExpr) Restore(ctx *format.RestoreCtx) error {
	if err := restoreExprWithBinaryOpParent(ctx, n.L, n.Op, binaryOpLeftSide); err != nil {
		return fmt.Errorf("an error occurred while restore CompareSubqueryExpr.L: %w", err)
	}
	if err := restoreBinaryOpWithSpacesAround(ctx, n.Op); err != nil {
		return fmt.Errorf("an error occurred while restore CompareSubqueryExpr.Op: %w", err)
	}
	if n.All {
		ctx.WriteKeyWord("ALL ")
	} else {
		ctx.WriteKeyWord("ANY ")
	}
	if err := n.R.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore CompareSubqueryExpr.R: %w", err)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *CompareSubqueryExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CompareSubqueryExpr)
	node, ok := n.L.Accept(v)
	if !ok {
		return n, false
	}
	n.L = node.(ExprNode)
	node, ok = n.R.Accept(v)
	if !ok {
		return n, false
	}
	n.R = node.(ExprNode)
	return v.Leave(n)
}

// ColumnName represents column name.
type ColumnName struct {
	node
	Schema CIStr
	Table  CIStr
	Name   CIStr
}

// Restore implements Node interface.
func (n *ColumnName) Restore(ctx *format.RestoreCtx) error {
	if n.Schema.O != "" && !ctx.IsCTETableName(n.Table.L) && !ctx.Flags.HasWithoutSchemaNameFlag() {
		ctx.WriteName(n.Schema.O)
		ctx.WritePlain(".")
	}
	if n.Table.O != "" && !ctx.Flags.HasWithoutTableNameFlag() {
		ctx.WriteName(n.Table.O)
		ctx.WritePlain(".")
	}
	ctx.WriteName(n.Name.O)
	return nil
}

// Accept implements Node Accept interface.
func (n *ColumnName) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ColumnName)
	return v.Leave(n)
}

// String implements Stringer interface.
func (n *ColumnName) String() string {
	result := n.Name.L
	if n.Table.L != "" {
		result = n.Table.L + "." + result
	}
	if n.Schema.L != "" {
		result = n.Schema.L + "." + result
	}
	return result
}

// OrigColName returns the full original column name.
func (n *ColumnName) OrigColName() (ret string) {
	ret = n.Name.O
	if n.Table.O == "" {
		return
	}
	ret = n.Table.O + "." + ret
	if n.Schema.O == "" {
		return
	}
	ret = n.Schema.O + "." + ret
	return
}

// Match means that if a match b, e.g. t.a can match test.t.a but test.t.a can't match t.a.
// Because column a want column from database test exactly.
func (n *ColumnName) Match(b *ColumnName) bool {
	if n.Schema.L == "" || n.Schema.L == b.Schema.L {
		if n.Table.L == "" || n.Table.L == b.Table.L {
			return n.Name.L == b.Name.L
		}
	}
	return false
}

// ColumnNameExpr represents a column name expression.
type ColumnNameExpr struct {
	exprNode

	// Name is the referenced column name.
	Name *ColumnName
}

// Restore implements Node interface.
func (n *ColumnNameExpr) Restore(ctx *format.RestoreCtx) error {
	if err := n.Name.Restore(ctx); err != nil {
		return err
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ColumnNameExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ColumnNameExpr)
	node, ok := n.Name.Accept(v)
	if !ok {
		return n, false
	}
	n.Name = node.(*ColumnName)
	return v.Leave(n)
}

// DefaultExpr is the default expression using default value for a column.
type DefaultExpr struct {
	exprNode
	// Name is the column name.
	Name *ColumnName
}

// Restore implements Node interface.
func (n *DefaultExpr) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DEFAULT")
	if n.Name != nil {
		ctx.WritePlain("(")
		if err := n.Name.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore DefaultExpr.Name: %w", err)
		}
		ctx.WritePlain(")")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *DefaultExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DefaultExpr)
	return v.Leave(n)
}

// FuncCallArgAliasExpr is a function call argument carrying MySQL's
// optional UDF attribute alias: udf_expr: expr [[AS] ident]. The server
// accepts the alias syntactically for any function and rejects non-UDF
// uses during resolution.
type FuncCallArgAliasExpr struct {
	exprNode

	Expr   ExprNode
	AsName CIStr
}

// Restore implements Node interface.
func (n *FuncCallArgAliasExpr) Restore(ctx *format.RestoreCtx) error {
	if err := n.Expr.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore FuncCallArgAliasExpr.Expr: %w", err)
	}
	ctx.WriteKeyWord(" AS ")
	ctx.WriteName(n.AsName.O)
	return nil
}

// Accept implements Node Accept interface.
func (n *FuncCallArgAliasExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FuncCallArgAliasExpr)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)
	return v.Leave(n)
}

// JSONDualityObjectPair is one 'key' : value member of a JSON_DUALITY_OBJECT
// constructor.
type JSONDualityObjectPair struct {
	Key   string
	Value ExprNode
}

// JSONDualityObjectExpr is the JSON_DUALITY_OBJECT(...) constructor used in
// JSON duality view definitions.
// See https://dev.mysql.com/doc/refman/9.4/en/create-json-duality-view.html
type JSONDualityObjectExpr struct {
	exprNode

	// With lists the operation annotations: WITH (INSERT, UPDATE, DELETE).
	With  []string
	Pairs []*JSONDualityObjectPair
}

// Restore implements Node interface.
func (n *JSONDualityObjectExpr) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("JSON_DUALITY_OBJECT")
	ctx.WritePlain("(")
	if len(n.With) > 0 {
		ctx.WriteKeyWord("WITH ")
		ctx.WritePlain("(")
		for i, op := range n.With {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			ctx.WriteKeyWord(op)
		}
		ctx.WritePlain(") ")
	}
	for i, pair := range n.Pairs {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		ctx.WriteString(pair.Key)
		ctx.WritePlain(" : ")
		if err := pair.Value.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore JSONDualityObjectExpr.Pairs[%d]: %w", i, err)
		}
	}
	ctx.WritePlain(")")
	return nil
}

// Accept implements Node Accept interface.
func (n *JSONDualityObjectExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*JSONDualityObjectExpr)
	for i, pair := range n.Pairs {
		node, ok := pair.Value.Accept(v)
		if !ok {
			return n, false
		}
		n.Pairs[i].Value = node.(ExprNode)
	}
	return v.Leave(n)
}

// ExistsSubqueryExpr is the expression for "exists (select ...)".
// See https://dev.mysql.com/doc/refman/5.7/en/exists-and-not-exists-subqueries.html
type ExistsSubqueryExpr struct {
	exprNode
	// Sel is the subquery, may be rewritten to other type of expression.
	Sel ExprNode
	// Not is true, the expression is "not exists".
	Not bool
}

// Restore implements Node interface.
func (n *ExistsSubqueryExpr) Restore(ctx *format.RestoreCtx) error {
	if n.Not {
		ctx.WriteKeyWord("NOT EXISTS ")
	} else {
		ctx.WriteKeyWord("EXISTS ")
	}
	if err := n.Sel.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore ExistsSubqueryExpr.Sel: %w", err)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ExistsSubqueryExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ExistsSubqueryExpr)
	node, ok := n.Sel.Accept(v)
	if !ok {
		return n, false
	}
	n.Sel = node.(ExprNode)
	return v.Leave(n)
}

// PatternInExpr is the expression for in operator, like "expr in (1, 2, 3)" or "expr in (select c from t)".
type PatternInExpr struct {
	exprNode
	// Expr is the value expression to be compared.
	Expr ExprNode
	// List is the list expression in compare list.
	List []ExprNode
	// Not is true, the expression is "not in".
	Not bool
	// Sel is the subquery, may be rewritten to other type of expression.
	Sel ExprNode
}

// Restore implements Node interface.
func (n *PatternInExpr) Restore(ctx *format.RestoreCtx) error {
	if err := restoreExprWithBinaryOpParent(ctx, n.Expr, opcode.In, binaryOpLeftSide); err != nil {
		return fmt.Errorf("an error occurred while restore PatternInExpr.Expr: %w", err)
	}
	if n.Not {
		ctx.WriteKeyWord(" NOT IN ")
	} else {
		ctx.WriteKeyWord(" IN ")
	}
	if n.Sel != nil {
		if err := n.Sel.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore PatternInExpr.Sel: %w", err)
		}
	} else {
		ctx.WritePlain("(")
		for i, expr := range n.List {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := expr.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore PatternInExpr.List[%d]: %w", i, err)
			}
		}
		ctx.WritePlain(")")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *PatternInExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*PatternInExpr)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)
	for i, val := range n.List {
		node, ok = val.Accept(v)
		if !ok {
			return n, false
		}
		n.List[i] = node.(ExprNode)
	}
	if n.Sel != nil {
		node, ok = n.Sel.Accept(v)
		if !ok {
			return n, false
		}
		n.Sel = node.(ExprNode)
	}
	return v.Leave(n)
}

// IsNullExpr is the expression for null check.
type IsNullExpr struct {
	exprNode
	// Expr is the expression to be checked.
	Expr ExprNode
	// Not is true, the expression is "is not null".
	Not bool
}

// Restore implements Node interface.
func (n *IsNullExpr) Restore(ctx *format.RestoreCtx) error {
	if err := restoreExprWithBinaryOpParent(ctx, n.Expr, opcode.IsNull, binaryOpLeftSide); err != nil {
		return err
	}
	if n.Not {
		ctx.WriteKeyWord(" IS NOT NULL")
	} else {
		ctx.WriteKeyWord(" IS NULL")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *IsNullExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*IsNullExpr)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)
	return v.Leave(n)
}

// IsTruthExpr is the expression for true/false check.
type IsTruthExpr struct {
	exprNode
	// Expr is the expression to be checked.
	Expr ExprNode
	// Not is true, the expression is "is not true/false".
	Not bool
	// True indicates checking true or false.
	True int64
}

// Restore implements Node interface.
func (n *IsTruthExpr) Restore(ctx *format.RestoreCtx) error {
	parentOp := opcode.IsFalsity
	if n.True > 0 {
		parentOp = opcode.IsTruth
	}
	if err := restoreExprWithBinaryOpParent(ctx, n.Expr, parentOp, binaryOpLeftSide); err != nil {
		return err
	}
	if n.Not {
		ctx.WriteKeyWord(" IS NOT")
	} else {
		ctx.WriteKeyWord(" IS")
	}
	if n.True > 0 {
		ctx.WriteKeyWord(" TRUE")
	} else {
		ctx.WriteKeyWord(" FALSE")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *IsTruthExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*IsTruthExpr)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)
	return v.Leave(n)
}

// PatternLikeExpr is the expression for the LIKE operator, e.g. expr LIKE "%123%".
type PatternLikeExpr struct {
	exprNode
	// Expr is the expression to be checked.
	Expr ExprNode
	// Pattern is the like expression.
	Pattern ExprNode
	// Not is true, the expression is "not like".
	Not bool

	Escape byte
	// EscapeExpr is a non-literal ESCAPE expression (MySQL accepts any simple
	// expression and validates the one-character requirement at execution
	// time). When set, Escape is 0 and the expression is restored verbatim.
	EscapeExpr ExprNode
	// EscapeExplicit indicates whether ESCAPE clause is specified explicitly.
	EscapeExplicit bool

	PatChars []byte
	PatTypes []byte
}

// Restore implements Node interface.
func (n *PatternLikeExpr) Restore(ctx *format.RestoreCtx) error {
	if err := restoreExprWithBinaryOpParent(ctx, n.Expr, opcode.Like, binaryOpLeftSide); err != nil {
		return fmt.Errorf("an error occurred while restore PatternLikeExpr.Expr: %w", err)
	}

	if n.Not {
		ctx.WriteKeyWord(" NOT LIKE ")
	} else {
		ctx.WriteKeyWord(" LIKE ")
	}

	if err := restoreExprWithBinaryOpParent(ctx, n.Pattern, opcode.Like, binaryOpRightSide); err != nil {
		return fmt.Errorf("an error occurred while restore PatternLikeExpr.Pattern: %w", err)
	}

	if n.EscapeExpr != nil {
		ctx.WriteKeyWord(" ESCAPE ")
		if err := n.EscapeExpr.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore PatternLikeExpr.EscapeExpr: %w", err)
		}
	} else if n.EscapeExplicit && n.Escape != '\\' {
		ctx.WriteKeyWord(" ESCAPE ")
		if n.Escape == 0 {
			// ESCAPE '' means no escape character
			ctx.WriteString("")
		} else {
			ctx.WriteString(string(n.Escape))
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *PatternLikeExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*PatternLikeExpr)
	if n.Expr != nil {
		node, ok := n.Expr.Accept(v)
		if !ok {
			return n, false
		}
		n.Expr = node.(ExprNode)
	}
	if n.Pattern != nil {
		node, ok := n.Pattern.Accept(v)
		if !ok {
			return n, false
		}
		n.Pattern = node.(ExprNode)
	}
	if n.EscapeExpr != nil {
		node, ok := n.EscapeExpr.Accept(v)
		if !ok {
			return n, false
		}
		n.EscapeExpr = node.(ExprNode)
	}
	return v.Leave(n)
}

// ParenthesesExpr is the parentheses' expression.
type ParenthesesExpr struct {
	exprNode
	// Expr is the expression in parentheses.
	Expr ExprNode
}

// Restore implements Node interface.
func (n *ParenthesesExpr) Restore(ctx *format.RestoreCtx) error {
	if ctx.Flags.HasRestoreSkipRedundantParentheses() && canRestoreWithoutParentheses(ctx, n.Expr) {
		if err := n.Expr.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ParenthesesExpr.Expr: %w", err)
		}
		return nil
	}
	ctx.WritePlain("(")
	if err := restoreWithResetParentContext(ctx, func() error {
		return n.Expr.Restore(ctx)
	}); err != nil {
		return fmt.Errorf("an error occurred while restore ParenthesesExpr.Expr: %w", err)
	}
	ctx.WritePlain(")")
	return nil
}

// Accept implements Node Accept interface.
func (n *ParenthesesExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ParenthesesExpr)
	if n.Expr != nil {
		node, ok := n.Expr.Accept(v)
		if !ok {
			return n, false
		}
		n.Expr = node.(ExprNode)
	}
	return v.Leave(n)
}

const (
	binaryOpLeftSide = iota + 1
	binaryOpRightSide
)

// Restore-only opcodes for constructs that behave like binary operators when it
// comes to precedence but have no opcode.Op of their own. They are negative so
// they cannot collide with a real opcode.
const (
	restoreOpMemberOf opcode.Op = -1 - iota
	restoreOpCollate
	restoreOpBetween
)

// canRestoreWithoutParentheses decides whether removing a pair of parentheses
// keeps the expression under the same surrounding parse boundary. Unknown or
// ambiguous cases keep parentheses. Unary parents are conservative because
// expressions like -(a + b) are not equivalent to -a + b.
func canRestoreWithoutParentheses(ctx *format.RestoreCtx, expr ExprNode) bool {
	if ctx.InUnaryOperation {
		return false
	}
	// Unary expressions need conservative handling under a binary parent because
	// dropping their parentheses can expose a different parse boundary to the
	// surrounding binary operator.
	if _, ok := expr.(*UnaryOperationExpr); ok && ctx.ParentBinaryOp != 0 {
		return false
	}
	childOp, ok := restoreParenthesizedExprOp(expr)
	if !ok {
		return true
	}
	parentOp := opcode.Op(ctx.ParentBinaryOp)
	if parentOp == 0 {
		return true
	}
	return canRestoreBinaryChildWithoutParentheses(parentOp, childOp, ctx.ParentBinarySide)
}

// restoreParenthesizedExprOp reports the operator a parenthesized expression is
// built around, if it is one whose precedence matters to the parent. The second
// return value is false for expressions that are self-delimiting (a literal, a
// column reference, a function call), whose parentheses are always redundant.
func restoreParenthesizedExprOp(expr ExprNode) (opcode.Op, bool) {
	switch x := expr.(type) {
	case *BinaryOperationExpr:
		return x.Op, true
	case *BetweenExpr:
		return restoreOpBetween, true
	case *CompareSubqueryExpr:
		return x.Op, true
	case *IsNullExpr:
		return opcode.IsNull, true
	case *IsTruthExpr:
		if x.True > 0 {
			return opcode.IsTruth, true
		}
		return opcode.IsFalsity, true
	case *PatternInExpr:
		return opcode.In, true
	case *PatternLikeExpr:
		return opcode.Like, true
	case *PatternRegexpExpr:
		return opcode.Regexp, true
	case *SetCollationExpr:
		return restoreOpCollate, true
	case *FuncCallExpr:
		if x.FnName.L == JSONMemberOf {
			return restoreOpMemberOf, true
		}
	}
	return 0, false
}

func canRestoreBinaryChildWithoutParentheses(parentOp, childOp opcode.Op, side int) bool {
	parentPrecedence := restoreBinaryPrecedence(parentOp)
	childPrecedence := restoreBinaryPrecedence(childOp)
	if parentPrecedence == 0 || childPrecedence == 0 {
		return false
	}
	if restoreOpTakesFixedOperands(parentOp) {
		return childPrecedence >= restoreFixedOperandPrecedence(parentOp, side)
	}
	if childPrecedence > parentPrecedence {
		return true
	}
	if childPrecedence < parentPrecedence {
		return false
	}
	return side == binaryOpLeftSide || isAssociativeRestoreOp(parentOp, childOp)
}

// restoreOpTakesFixedOperands reports whether op is one of the predicate
// operators whose operands are fixed productions in MySQL's grammar rather than
// expressions at op's own precedence level. They look like ordinary infix
// operators but do not nest like them: dropping an operand's parentheses either
// rebinds the expression, since MySQL reads `a = b BETWEEN 1 AND 10` as
// `a = (b BETWEEN 1 AND 10)`, or produces text the server rejects outright, as
// it does `a IN (1,2) IN (3,4)`.
func restoreOpTakesFixedOperands(op opcode.Op) bool {
	switch op { //nolint:exhaustive
	case restoreOpBetween, opcode.In, opcode.Like, opcode.Regexp, restoreOpMemberOf:
		return true
	default:
		return false
	}
}

// restoreFixedOperandPrecedence returns the precedence a child must reach to
// drop its parentheses in the given operand position of a predicate operator.
// All five take a bit_expr subject, so a subject binding at least as tightly as
// bitwise OR fits there. The other operands differ: BETWEEN's bounds and
// REGEXP's pattern are bit_exprs too, while LIKE's pattern and MEMBER OF's
// document are simple_exprs, a level only COLLATE reaches here. BETWEEN's upper
// bound is really a predicate, which is looser again, but it shares a side with
// the lower bound, so both are held to the stricter of the two. IN's list is
// restored as a fresh expression and never reaches this function.
func restoreFixedOperandPrecedence(op opcode.Op, side int) int {
	if side == binaryOpLeftSide {
		return restoreBinaryPrecedence(opcode.Or)
	}
	switch op { //nolint:exhaustive
	case restoreOpBetween, opcode.Regexp:
		return restoreBinaryPrecedence(opcode.Or)
	default:
		return restoreBinaryPrecedence(restoreOpCollate)
	}
}

// restoreBinaryPrecedence follows MySQL operator precedence: larger values bind
// tighter, and 0 means unknown so parentheses must be kept. Binary operators are
// left-associative, so same-precedence right children can drop parentheses only
// for operators that preserve SQL evaluation semantics after regrouping.
// Arithmetic operators are intentionally excluded: subtraction, division,
// integer division, and modulo are not associative, while addition and
// multiplication can still produce different finite-precision numeric results
// after reassociation.
//
// Examples:
//   - `(a + b) * c` must keep parentheses.
//   - `a + (b * c)` can become `a + b * c`.
//   - `(a BETWEEN b AND c) = d` must keep parentheses because BETWEEN binds
//     weaker than comparison operators.
//
// See https://dev.mysql.com/doc/refman/8.4/en/operator-precedence.html.
func restoreBinaryPrecedence(op opcode.Op) int {
	// The unary opcodes (Not, Not2, BitNeg) and Case are not binary operators;
	// they fall through to the unknown-precedence default on purpose.
	switch op { //nolint:exhaustive
	case opcode.LogicOr:
		return 1
	case opcode.LogicXor:
		return 2
	case opcode.LogicAnd:
		return 3
	case restoreOpBetween:
		return 4
	case opcode.EQ, opcode.NE, opcode.NullEQ, opcode.LT, opcode.LE, opcode.GT, opcode.GE,
		opcode.In, opcode.Like, opcode.Regexp, opcode.IsNull, opcode.IsTruth, opcode.IsFalsity,
		restoreOpMemberOf:
		return 5
	case opcode.Or:
		return 6
	case opcode.And:
		return 7
	case opcode.LeftShift, opcode.RightShift:
		return 8
	case opcode.Plus, opcode.Minus:
		return 9
	case opcode.Mul, opcode.Div, opcode.IntDiv, opcode.Mod:
		return 10
	case opcode.Xor:
		return 11
	case restoreOpCollate:
		return 12
	default:
		return 0
	}
}

func isAssociativeRestoreOp(parentOp, childOp opcode.Op) bool {
	if parentOp != childOp {
		return false
	}
	// Every other operator is either non-associative or not worth regrouping.
	switch parentOp { //nolint:exhaustive
	case opcode.LogicAnd, opcode.LogicOr, opcode.And, opcode.Or, opcode.Xor:
		return true
	default:
		return false
	}
}

// PositionExpr is the expression for order by and group by position.
// MySQL use position expression started from 1, it looks a little confused inner.
// maybe later we will use 0 at first.
type PositionExpr struct {
	exprNode
	// N is the position, started from 1 now.
	N int
	// P is the parameterized position.
	P ExprNode
}

// Restore implements Node interface.
func (n *PositionExpr) Restore(ctx *format.RestoreCtx) error {
	ctx.WritePlainf("%d", n.N)
	return nil
}

// Accept implements Node Accept interface.
func (n *PositionExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*PositionExpr)
	if n.P != nil {
		node, ok := n.P.Accept(v)
		if !ok {
			return n, false
		}
		n.P = node.(ExprNode)
	}
	return v.Leave(n)
}

// PatternRegexpExpr is the pattern expression for pattern match.
type PatternRegexpExpr struct {
	exprNode
	// Expr is the expression to be checked.
	Expr ExprNode
	// Pattern is the expression for pattern.
	Pattern ExprNode
	// Not is true, the expression is "not rlike",
	Not bool

	// Re is the compiled regexp.
	Re *regexp.Regexp
	// Sexpr is the string for Expr expression.
	Sexpr *string
}

// Restore implements Node interface.
func (n *PatternRegexpExpr) Restore(ctx *format.RestoreCtx) error {
	if err := restoreExprWithBinaryOpParent(ctx, n.Expr, opcode.Regexp, binaryOpLeftSide); err != nil {
		return fmt.Errorf("an error occurred while restore PatternRegexpExpr.Expr: %w", err)
	}

	if n.Not {
		ctx.WriteKeyWord(" NOT REGEXP ")
	} else {
		ctx.WriteKeyWord(" REGEXP ")
	}

	if err := restoreExprWithBinaryOpParent(ctx, n.Pattern, opcode.Regexp, binaryOpRightSide); err != nil {
		return fmt.Errorf("an error occurred while restore PatternRegexpExpr.Pattern: %w", err)
	}

	return nil
}

// Accept implements Node Accept interface.
func (n *PatternRegexpExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*PatternRegexpExpr)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)
	node, ok = n.Pattern.Accept(v)
	if !ok {
		return n, false
	}
	n.Pattern = node.(ExprNode)
	return v.Leave(n)
}

// RowExpr is the expression for row constructor.
// See https://dev.mysql.com/doc/refman/5.7/en/row-subqueries.html
type RowExpr struct {
	exprNode

	Values []ExprNode
}

// Restore implements Node interface.
func (n *RowExpr) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ROW")
	ctx.WritePlain("(")
	for i, v := range n.Values {
		if i != 0 {
			ctx.WritePlain(",")
		}
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore RowExpr.Values[%v]: %w", i, err)
		}
	}
	ctx.WritePlain(")")
	return nil
}

// Accept implements Node Accept interface.
func (n *RowExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RowExpr)
	for i, val := range n.Values {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Values[i] = node.(ExprNode)
	}
	return v.Leave(n)
}

// UnaryOperationExpr is the expression for unary operator.
type UnaryOperationExpr struct {
	exprNode
	// Op is the operator opcode.
	Op opcode.Op
	// V is the unary expression.
	V ExprNode
}

// Restore implements Node interface.
func (n *UnaryOperationExpr) Restore(ctx *format.RestoreCtx) error {
	if err := n.Op.Restore(ctx); err != nil {
		return err
	}
	if err := restoreExprWithUnaryOpParent(ctx, n.V); err != nil {
		return err
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *UnaryOperationExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*UnaryOperationExpr)
	node, ok := n.V.Accept(v)
	if !ok {
		return n, false
	}
	n.V = node.(ExprNode)
	return v.Leave(n)
}

// ValuesExpr is the expression used in INSERT VALUES.
type ValuesExpr struct {
	exprNode
	// Column is column name.
	Column *ColumnNameExpr
}

// Restore implements Node interface.
func (n *ValuesExpr) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("VALUES")
	ctx.WritePlain("(")
	if err := n.Column.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore ValuesExpr.Column: %w", err)
	}
	ctx.WritePlain(")")

	return nil
}

// Accept implements Node Accept interface.
func (n *ValuesExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ValuesExpr)
	node, ok := n.Column.Accept(v)
	if !ok {
		return n, false
	}
	// `node` may be *ast.ValueExpr, to avoid panic, we write `_` and do not use
	// it.
	n.Column, _ = node.(*ColumnNameExpr)
	return v.Leave(n)
}

// VariableExpr is the expression for variable.
type VariableExpr struct {
	exprNode
	// Name is the variable name.
	Name string
	// IsGlobal indicates whether this variable is global.
	IsGlobal bool
	// IsInstance indicates whether this variable is instance.
	IsInstance bool
	// IsSystem indicates whether this variable is a system variable in current session.
	IsSystem bool
	// ExplicitScope indicates whether this variable scope is set explicitly.
	ExplicitScope bool
	// Value is the variable value.
	Value ExprNode
}

// Restore implements Node interface.
func (n *VariableExpr) Restore(ctx *format.RestoreCtx) error {
	if n.IsSystem {
		ctx.WritePlain("@@")
		if n.ExplicitScope {
			switch {
			case n.IsGlobal:
				ctx.WriteKeyWord("GLOBAL")
			case n.IsInstance:
				ctx.WriteKeyWord("INSTANCE")
			default:
				ctx.WriteKeyWord("SESSION")
			}
			ctx.WritePlain(".")
		}
	} else {
		ctx.WritePlain("@")
	}
	ctx.WriteName(n.Name)

	if n.Value != nil {
		ctx.WritePlain(":=")
		if err := n.Value.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore VariableExpr.Value: %w", err)
		}
	}

	return nil
}

// Accept implements Node Accept interface.
func (n *VariableExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*VariableExpr)
	if n.Value == nil {
		return v.Leave(n)
	}

	node, ok := n.Value.Accept(v)
	if !ok {
		return n, false
	}
	n.Value = node.(ExprNode)
	return v.Leave(n)
}

// MaxValueExpr is the expression for "maxvalue" used in partition.
type MaxValueExpr struct {
	exprNode
}

// Restore implements Node interface.
func (n *MaxValueExpr) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("MAXVALUE")
	return nil
}

// Accept implements Node Accept interface.
func (n *MaxValueExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(n)
}

// MatchAgainst is the expression for matching against fulltext index.
type MatchAgainst struct {
	exprNode
	// ColumnNames are the columns to match.
	ColumnNames []*ColumnName
	// Against
	Against ExprNode
	// Modifier
	Modifier FulltextSearchModifier
}

func (n *MatchAgainst) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("MATCH")
	ctx.WritePlain(" (")
	for i, v := range n.ColumnNames {
		if i != 0 {
			ctx.WritePlain(",")
		}
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore MatchAgainst.ColumnNames[%d]: %w", i, err)
		}
	}
	ctx.WritePlain(") ")
	ctx.WriteKeyWord("AGAINST")
	ctx.WritePlain(" (")
	if err := n.Against.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore MatchAgainst.Against: %w", err)
	}
	if n.Modifier.IsBooleanMode() {
		ctx.WritePlain(" IN BOOLEAN MODE")
		if n.Modifier.WithQueryExpansion() {
			return errors.New("BOOLEAN MODE doesn't support QUERY EXPANSION")
		}
	} else if n.Modifier.WithQueryExpansion() {
		ctx.WritePlain(" WITH QUERY EXPANSION")
	}
	ctx.WritePlain(")")
	return nil
}

func (n *MatchAgainst) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*MatchAgainst)
	for i, colName := range n.ColumnNames {
		newColName, ok := colName.Accept(v)
		if !ok {
			return n, false
		}
		n.ColumnNames[i] = newColName.(*ColumnName)
	}
	newAgainst, ok := n.Against.Accept(v)
	if !ok {
		return n, false
	}
	n.Against = newAgainst.(ExprNode)
	return v.Leave(n)
}

// SetCollationExpr is the expression for the `COLLATE collation_name` clause.
type SetCollationExpr struct {
	exprNode
	// Expr is the expression to be set.
	Expr ExprNode
	// Collate is the name of collation to set.
	Collate string
}

// Restore implements Node interface.
func (n *SetCollationExpr) Restore(ctx *format.RestoreCtx) error {
	if err := restoreExprWithBinaryOpParent(ctx, n.Expr, restoreOpCollate, binaryOpLeftSide); err != nil {
		return err
	}
	ctx.WriteKeyWord(" COLLATE ")
	ctx.WritePlain(n.Collate)
	return nil
}

// Accept implements Node Accept interface.
func (n *SetCollationExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SetCollationExpr)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)
	return v.Leave(n)
}

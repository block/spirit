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
	"strings"

	"github.com/block/spirit/pkg/parser/auth"
	"github.com/block/spirit/pkg/parser/format"
	"github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/parser/types"
)

var (
	_ DMLNode = &DeleteStmt{}
	_ DMLNode = &InsertStmt{}
	_ DMLNode = &SetOprStmt{}
	_ DMLNode = &UpdateStmt{}
	_ DMLNode = &SelectStmt{}
	_ DMLNode = &CallStmt{}
	_ DMLNode = &ShowStmt{}
	_ DMLNode = &LoadDataStmt{}

	_ Node = &Assignment{}
	_ Node = &ByItem{}
	_ Node = &FieldList{}
	_ Node = &GroupByClause{}
	_ Node = &HavingClause{}
	_ Node = &Join{}
	_ Node = &Limit{}
	_ Node = &OnCondition{}
	_ Node = &OrderByClause{}
	_ Node = &SelectField{}
	_ Node = &TableName{}
	_ Node = &TableRefsClause{}
	_ Node = &TableSource{}
	_ Node = &SetOprSelectList{}
	_ Node = &WildCardField{}
	_ Node = &WindowSpec{}
	_ Node = &PartitionByClause{}
	_ Node = &FrameClause{}
	_ Node = &FrameBound{}
)

// JoinType is join type, including cross/left/right/full.
type JoinType int

const (
	// CrossJoin is cross join type.
	CrossJoin JoinType = iota + 1
	// LeftJoin is left Join type.
	LeftJoin
	// RightJoin is right Join type.
	RightJoin
)

// Join represents table join.
type Join struct {
	node

	// Left table can be TableSource or JoinNode.
	Left ResultSetNode
	// Right table can be TableSource or JoinNode or nil.
	Right ResultSetNode
	// Tp represents join type.
	Tp JoinType
	// On represents join on condition.
	On *OnCondition
	// Using represents join using clause.
	Using []*ColumnName
	// NaturalJoin represents join is natural join.
	NaturalJoin bool
	// StraightJoin represents a straight join.
	StraightJoin   bool
	ExplicitParens bool
}

func (*Join) resultSet() {}

// NewCrossJoin builds a cross join without `on` or `using` clause.
// If the right child is a join tree, we need to handle it differently to make the precedence get right.
// Here is the example: t1 join t2 join t3
//
//	               JOIN ON t2.a = t3.a
//	t1    join    /    \
//	            t2      t3
//
// (left)         (right)
//
// We can not build it directly to:
//
//	  JOIN
//	 /    \
//	t1	   JOIN ON t2.a = t3.a
//	      /   \
//	     t2    t3
//
// The precedence would be t1 join (t2 join t3 on t2.a=t3.a), not (t1 join t2) join t3 on t2.a=t3.a
// We need to find the left-most child of the right child, and build a cross join of the left-hand side
// of the left child(t1), and the right hand side with the original left-most child of the right child(t2).
//
//	    JOIN t2.a = t3.a
//	   /    \
//	 JOIN    t3
//	 /  \
//	t1  t2
//
// Besides, if the right handle side join tree's join type is right join and has explicit parentheses, we need to rewrite it to left join.
// So t1 join t2 right join t3 would be rewrite to t1 join t3 left join t2.
// If not, t1 join (t2 right join t3) would be (t1 join t2) right join t3. After rewrite the right join to left join.
// We get (t1 join t3) left join t2, the semantics is correct.
func NewCrossJoin(left, right ResultSetNode) (n *Join) {
	rj, ok := right.(*Join)
	// don't break the explicit parents name scope constraints.
	// this kind of join re-order can be done in logical-phase after the name resolution.
	if !ok || rj.Right == nil || rj.ExplicitParens {
		return &Join{Left: left, Right: right, Tp: CrossJoin}
	}

	var leftMostLeafFatherOfRight = rj
	// Walk down the right hand side.
	for {
		if leftMostLeafFatherOfRight.Tp == RightJoin && leftMostLeafFatherOfRight.ExplicitParens {
			// Rewrite right join to left join.
			leftMostLeafFatherOfRight.Right, leftMostLeafFatherOfRight.Left = leftMostLeafFatherOfRight.Left, leftMostLeafFatherOfRight.Right
			leftMostLeafFatherOfRight.Tp = LeftJoin
		}
		leftChild := leftMostLeafFatherOfRight.Left
		join, ok := leftChild.(*Join)
		if !ok || join.Right == nil {
			break
		}
		leftMostLeafFatherOfRight = join
	}

	newCrossJoin := &Join{Left: left, Right: leftMostLeafFatherOfRight.Left, Tp: CrossJoin}
	leftMostLeafFatherOfRight.Left = newCrossJoin
	return rj
}

// Restore implements Node interface.
func (n *Join) Restore(ctx *format.RestoreCtx) error {
	useCommaJoin := false
	_, leftIsJoin := n.Left.(*Join)

	if leftIsJoin && n.Left.(*Join).Right == nil {
		if ts, ok := n.Left.(*Join).Left.(*TableSource); ok {
			switch ts.Source.(type) {
			case *SelectStmt, *SetOprStmt:
				useCommaJoin = true
			}
		}
	}

	if leftIsJoin && !useCommaJoin {
		ctx.WritePlain("(")
	}
	if err := n.Left.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore Join.Left: %w", err)
	}
	if leftIsJoin && !useCommaJoin {
		ctx.WritePlain(")")
	}
	if n.Right == nil {
		return nil
	}
	if n.NaturalJoin {
		ctx.WriteKeyWord(" NATURAL")
	}
	switch n.Tp { //nolint:exhaustive
	case LeftJoin:
		ctx.WriteKeyWord(" LEFT")
	case RightJoin:
		ctx.WriteKeyWord(" RIGHT")
	}
	if n.StraightJoin {
		ctx.WriteKeyWord(" STRAIGHT_JOIN ")
	} else {
		if useCommaJoin {
			ctx.WritePlain(", ")
		} else {
			ctx.WriteKeyWord(" JOIN ")
		}
	}
	_, rightIsJoin := n.Right.(*Join)
	if rightIsJoin {
		ctx.WritePlain("(")
	}
	if err := n.Right.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore Join.Right: %w", err)
	}
	if rightIsJoin {
		ctx.WritePlain(")")
	}

	if n.On != nil {
		ctx.WritePlain(" ")
		if err := n.On.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore Join.On: %w", err)
		}
	}
	if len(n.Using) != 0 {
		ctx.WriteKeyWord(" USING ")
		ctx.WritePlain("(")
		for i, v := range n.Using {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := v.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore Join.Using: %w", err)
			}
		}
		ctx.WritePlain(")")
	}

	return nil
}

// Accept implements Node Accept interface.
func (n *Join) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*Join)
	node, ok := n.Left.Accept(v)
	if !ok {
		return n, false
	}
	n.Left = node.(ResultSetNode)
	if n.Right != nil {
		node, ok = n.Right.Accept(v)
		if !ok {
			return n, false
		}
		n.Right = node.(ResultSetNode)
	}
	if n.On != nil {
		node, ok = n.On.Accept(v)
		if !ok {
			return n, false
		}
		n.On = node.(*OnCondition)
	}
	for i, col := range n.Using {
		node, ok = col.Accept(v)
		if !ok {
			return n, false
		}
		n.Using[i] = node.(*ColumnName)
	}
	return v.Leave(n)
}

// TableName represents a table name.
type TableName struct {
	node

	Schema CIStr
	Name   CIStr

	IndexHints     []*IndexHint
	PartitionNames []CIStr
	// IsAlias is true if this table name is an alias.
	//  sometime, we need to distinguish the table name is an alias or not.
	//   for example ```delete tt1 from t1 tt1,(select max(id) id from t2)tt2 where tt1.id<=tt2.id```
	//   ```tt1``` is a alias name. so we need to set IsAlias to true and restore the table name without database name.
	IsAlias bool
}

func (*TableName) resultSet() {}

// Restore implements Node interface.
func (n *TableName) restoreName(ctx *format.RestoreCtx) {
	if !ctx.Flags.HasWithoutSchemaNameFlag() {
		// restore db name
		if n.Schema.String() != "" {
			ctx.WriteName(n.Schema.String())
			ctx.WritePlain(".")
		} else if ctx.DefaultDB != "" && !n.IsAlias {
			// Try CTE, for a CTE table name, we shouldn't write the database name.
			if !ctx.IsCTETableName(n.Name.L) {
				ctx.WriteName(ctx.DefaultDB)
				ctx.WritePlain(".")
			}
		}
	}
	// restore table name
	ctx.WriteName(n.Name.String())
}

func (n *TableName) restorePartitions(ctx *format.RestoreCtx) {
	if len(n.PartitionNames) > 0 {
		ctx.WriteKeyWord(" PARTITION")
		ctx.WritePlain("(")
		for i, v := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			ctx.WriteName(v.String())
		}
		ctx.WritePlain(")")
	}
}

func (n *TableName) restoreIndexHints(ctx *format.RestoreCtx) error {
	for _, value := range n.IndexHints {
		ctx.WritePlain(" ")
		if err := value.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while splicing IndexHints: %w", err)
		}
	}
	return nil
}

func (n *TableName) Restore(ctx *format.RestoreCtx) error {
	n.restoreName(ctx)
	n.restorePartitions(ctx)
	if err := n.restoreIndexHints(ctx); err != nil {
		return err
	}
	return nil
}

// IndexHintType is the type for index hint use, ignore or force.
type IndexHintType int

// IndexHintUseType values.
const (
	HintUse IndexHintType = iota + 1
	HintIgnore
	HintForce
	HintOrderIndex
	HintNoOrderIndex
)

// IndexHintScope is the type for index hint for join, order by or group by.
type IndexHintScope int

// Index hint scopes.
const (
	HintForScan IndexHintScope = iota + 1
	HintForJoin
	HintForOrderBy
	HintForGroupBy
)

// IndexHint represents a hint for optimizer to use/ignore/force for join/order by/group by.
type IndexHint struct {
	IndexNames []CIStr
	HintType   IndexHintType
	HintScope  IndexHintScope
}

// IndexHint Restore (The const field uses switch to facilitate understanding)
func (n *IndexHint) Restore(ctx *format.RestoreCtx) error {
	indexHintType := ""
	switch n.HintType {
	case HintUse:
		indexHintType = "USE INDEX"
	case HintIgnore:
		indexHintType = "IGNORE INDEX"
	case HintForce:
		indexHintType = "FORCE INDEX"
	case HintOrderIndex:
		indexHintType = "ORDER INDEX"
	case HintNoOrderIndex:
		indexHintType = "NO ORDER INDEX"
	default: // Prevent accidents
		return errors.New("IndexHintType has an error while matching")
	}

	indexHintScope := ""
	switch n.HintScope {
	case HintForScan:
		indexHintScope = ""
	case HintForJoin:
		indexHintScope = " FOR JOIN"
	case HintForOrderBy:
		indexHintScope = " FOR ORDER BY"
	case HintForGroupBy:
		indexHintScope = " FOR GROUP BY"
	default: // Prevent accidents
		return errors.New("IndexHintScope has an error while matching")
	}
	ctx.WriteKeyWord(indexHintType)
	ctx.WriteKeyWord(indexHintScope)
	ctx.WritePlain(" (")
	for i, value := range n.IndexNames {
		if i > 0 {
			ctx.WritePlain(", ")
		}
		ctx.WriteName(value.O)
	}
	ctx.WritePlain(")")

	return nil
}

// Accept implements Node Accept interface.
func (n *TableName) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*TableName)
	return v.Leave(n)
}

// DeleteTableList is the tablelist used in delete statement multi-table mode.
type DeleteTableList struct {
	node
	Tables []*TableName
}

// Restore implements Node interface.
func (n *DeleteTableList) Restore(ctx *format.RestoreCtx) error {
	for i, t := range n.Tables {
		if i != 0 {
			ctx.WritePlain(",")
		}
		if err := t.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore DeleteTableList.Tables[%v]: %w", i, err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *DeleteTableList) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DeleteTableList)
	if n != nil {
		for i, t := range n.Tables {
			node, ok := t.Accept(v)
			if !ok {
				return n, false
			}
			n.Tables[i] = node.(*TableName)
		}
	}
	return v.Leave(n)
}

// OnCondition represents JOIN on condition.
type OnCondition struct {
	node

	Expr ExprNode
}

// Restore implements Node interface.
func (n *OnCondition) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ON ")
	if err := n.Expr.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore OnCondition.Expr: %w", err)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *OnCondition) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*OnCondition)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)
	return v.Leave(n)
}

// TableSource represents table source with a name.
type TableSource struct {
	node

	// Source is the source of the data, can be a TableName,
	// a SelectStmt, a SetOprStmt, or a JoinNode.
	Source ResultSetNode

	// AsName is the alias name of the table source.
	AsName CIStr

	// Lateral indicates whether this is a LATERAL derived table.
	// MySQL 8.0+ syntax: FROM t1, LATERAL (SELECT ...) AS dt
	// LATERAL allows the derived table to reference columns from tables to its left.
	Lateral bool

	// ColumnNames is the optional column alias list for derived tables.
	// e.g. LATERAL (SELECT ...) AS dt(c1, c2)
	ColumnNames []CIStr
}

func (*TableSource) resultSet() {}

// Restore implements Node interface.
func (n *TableSource) Restore(ctx *format.RestoreCtx) error {
	// Validate AST invariants before emitting any SQL.
	// Source can be TableName, SelectStmt, SetOprStmt, or JoinNode (parenthesized join).
	// LATERAL and ColumnNames are only valid on derived tables (SelectStmt, SetOprStmt);
	// TableName and JoinNode are both excluded.
	isDerived := false
	switch n.Source.(type) {
	case *SelectStmt, *SetOprStmt:
		isDerived = true
	}
	if n.Lateral && !isDerived {
		return errors.New("LATERAL cannot be applied to a table name, only to derived tables")
	}
	if len(n.ColumnNames) > 0 && !isDerived {
		return errors.New("column alias list cannot be applied to a table name")
	}
	if len(n.ColumnNames) > 0 && n.AsName.String() == "" {
		return errors.New("column list provided without alias for derived table")
	}

	needParen := false
	switch n.Source.(type) {
	case *SelectStmt, *SetOprStmt:
		needParen = true
	}

	// Output LATERAL keyword if this is a LATERAL derived table
	if n.Lateral {
		ctx.WriteKeyWord("LATERAL ")
	}

	if tn, tnCase := n.Source.(*TableName); tnCase {
		if needParen {
			ctx.WritePlain("(")
		}

		tn.restoreName(ctx)
		tn.restorePartitions(ctx)

		if asName := n.AsName.String(); asName != "" {
			ctx.WriteKeyWord(" AS ")
			ctx.WriteName(asName)
		}

		if err := tn.restoreIndexHints(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore TableSource.Source.(*TableName).IndexHints: %w", err)
		}

		if needParen {
			ctx.WritePlain(")")
		}
	} else {
		if needParen {
			ctx.WritePlain("(")
		}
		if err := n.Source.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore TableSource.Source: %w", err)
		}
		if needParen {
			ctx.WritePlain(")")
		}
		if asName := n.AsName.String(); asName != "" {
			ctx.WriteKeyWord(" AS ")
			ctx.WriteName(asName)
			if len(n.ColumnNames) > 0 {
				ctx.WritePlain("(")
				for i, col := range n.ColumnNames {
					if i > 0 {
						ctx.WritePlain(", ")
					}
					ctx.WriteName(col.String())
				}
				ctx.WritePlain(")")
			}
		}
	}

	return nil
}

// Accept implements Node Accept interface.
func (n *TableSource) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*TableSource)
	node, ok := n.Source.Accept(v)
	if !ok {
		return n, false
	}
	n.Source = node.(ResultSetNode)
	return v.Leave(n)
}

// JSONTableColumn is one column definition in the COLUMNS clause of
// JSON_TABLE. Exactly one of ForOrdinality, Tp (value column) and
// NestedColumns (NESTED PATH entry) is set.
type JSONTableColumn struct {
	// Name is the column name; empty for NESTED entries.
	Name CIStr
	// ForOrdinality marks a `name FOR ORDINALITY` counter column.
	ForOrdinality bool
	// Tp is the column type of a value column; nil otherwise.
	Tp *types.FieldType
	// Exists marks an `EXISTS PATH` value column.
	Exists bool
	// Path is the JSON path literal of value and nested entries.
	Path string
	// OnEmpty is the ON EMPTY behavior of a value column; nil when absent.
	OnEmpty *JSONValueOnBehavior
	// OnError is the ON ERROR behavior of a value column; nil when absent.
	OnError *JSONValueOnBehavior
	// NestedColumns is non-nil for `NESTED [PATH] '...' COLUMNS (...)` entries.
	NestedColumns []*JSONTableColumn
}

// Restore implements Node interface.
func (n *JSONTableColumn) Restore(ctx *format.RestoreCtx) error {
	if n.NestedColumns != nil {
		ctx.WriteKeyWord("NESTED PATH ")
		ctx.WriteString(n.Path)
		ctx.WriteKeyWord(" COLUMNS ")
		return restoreJSONTableColumns(ctx, n.NestedColumns)
	}
	ctx.WriteName(n.Name.O)
	if n.ForOrdinality {
		ctx.WriteKeyWord(" FOR ORDINALITY")
		return nil
	}
	ctx.WritePlain(" ")
	if err := n.Tp.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore JSONTableColumn.Tp: %w", err)
	}
	if n.Exists {
		ctx.WriteKeyWord(" EXISTS")
	}
	ctx.WriteKeyWord(" PATH ")
	ctx.WriteString(n.Path)
	if n.OnEmpty != nil {
		ctx.WritePlain(" ")
		if err := n.OnEmpty.Restore(ctx); err != nil {
			return err
		}
		ctx.WriteKeyWord(" ON EMPTY")
	}
	if n.OnError != nil {
		ctx.WritePlain(" ")
		if err := n.OnError.Restore(ctx); err != nil {
			return err
		}
		ctx.WriteKeyWord(" ON ERROR")
	}
	return nil
}

func restoreJSONTableColumns(ctx *format.RestoreCtx, cols []*JSONTableColumn) error {
	ctx.WritePlain("(")
	for i, col := range cols {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := col.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore JSONTableColumn: %w", err)
		}
	}
	ctx.WritePlain(")")
	return nil
}

// acceptJSONTableColumns visits the behavior default expressions of all
// (recursively nested) JSON_TABLE columns.
func acceptJSONTableColumns(v Visitor, cols []*JSONTableColumn) bool {
	for _, col := range cols {
		if col.OnEmpty != nil && col.OnEmpty.Default != nil {
			node, ok := col.OnEmpty.Default.Accept(v)
			if !ok {
				return false
			}
			col.OnEmpty.Default = node.(ExprNode)
		}
		if col.OnError != nil && col.OnError.Default != nil {
			node, ok := col.OnError.Default.Accept(v)
			if !ok {
				return false
			}
			col.OnError.Default = node.(ExprNode)
		}
		if col.NestedColumns != nil && !acceptJSONTableColumns(v, col.NestedColumns) {
			return false
		}
	}
	return true
}

// JSONTableExpr is the JSON_TABLE(doc, path COLUMNS (...)) table function
// used as a table factor.
type JSONTableExpr struct {
	node

	// Doc is the JSON document argument.
	Doc ExprNode
	// Path is the row path literal.
	Path string
	// Columns is the COLUMNS clause.
	Columns []*JSONTableColumn
}

func (*JSONTableExpr) resultSet() {}

// Restore implements Node interface.
func (n *JSONTableExpr) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("JSON_TABLE")
	ctx.WritePlain("(")
	if err := n.Doc.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore JSONTableExpr.Doc: %w", err)
	}
	ctx.WritePlain(", ")
	ctx.WriteString(n.Path)
	ctx.WriteKeyWord(" COLUMNS ")
	if err := restoreJSONTableColumns(ctx, n.Columns); err != nil {
		return err
	}
	ctx.WritePlain(")")
	return nil
}

// Accept implements Node Accept interface.
func (n *JSONTableExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*JSONTableExpr)
	node, ok := n.Doc.Accept(v)
	if !ok {
		return n, false
	}
	n.Doc = node.(ExprNode)
	if !acceptJSONTableColumns(v, n.Columns) {
		return n, false
	}
	return v.Leave(n)
}

// SelectLockType is the lock type for SelectStmt.
type SelectLockType int

// Select lock types.
const (
	SelectLockNone SelectLockType = iota
	SelectLockForUpdate
	SelectLockForShare
	SelectLockForUpdateNoWait
	SelectLockForUpdateWaitN
	SelectLockForShareNoWait
	SelectLockForUpdateSkipLocked
	SelectLockForShareSkipLocked
)

type SelectLockInfo struct {
	LockType SelectLockType
	WaitSec  uint64
	Tables   []*TableName
}

// String implements fmt.Stringer.
func (n SelectLockType) String() string {
	switch n {
	case SelectLockNone:
		return "none"
	case SelectLockForUpdate:
		return "for update"
	case SelectLockForShare:
		return "for share"
	case SelectLockForUpdateNoWait:
		return "for update nowait"
	case SelectLockForUpdateWaitN:
		return "for update wait"
	case SelectLockForShareNoWait:
		return "for share nowait"
	case SelectLockForUpdateSkipLocked:
		return "for update skip locked"
	case SelectLockForShareSkipLocked:
		return "for share skip locked"
	}
	return "unsupported select lock type"
}

// WildCardField is a special type of select field content.
type WildCardField struct {
	node

	Table  CIStr
	Schema CIStr
}

// Restore implements Node interface.
func (n *WildCardField) Restore(ctx *format.RestoreCtx) error {
	if schema := n.Schema.String(); schema != "" {
		ctx.WriteName(schema)
		ctx.WritePlain(".")
	}
	if table := n.Table.String(); table != "" {
		ctx.WriteName(table)
		ctx.WritePlain(".")
	}
	ctx.WritePlain("*")
	return nil
}

// Accept implements Node Accept interface.
func (n *WildCardField) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*WildCardField)
	return v.Leave(n)
}

// SelectField represents fields in select statement.
// There are two type of select field: wildcard
// and expression with optional alias name.
type SelectField struct {
	node

	// Offset is used to get original text.
	Offset int
	// WildCard is not nil, Expr will be nil.
	WildCard *WildCardField
	// Expr is not nil, WildCard will be nil.
	Expr ExprNode
	// AsName is alias name for Expr.
	AsName CIStr
	// Auxiliary stands for if this field is auxiliary.
	// When we add a Field into SelectField list which is used for having/orderby clause but the field is not in select clause,
	// we should set its Auxiliary to true. Then the TrimExec will trim the field.
	Auxiliary             bool
	AuxiliaryColInAgg     bool
	AuxiliaryColInOrderBy bool
}

// Restore implements Node interface.
func (n *SelectField) Restore(ctx *format.RestoreCtx) error {
	if n.WildCard != nil {
		if err := n.WildCard.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore SelectField.WildCard: %w", err)
		}
	}
	if n.Expr != nil {
		if err := n.Expr.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore SelectField.Expr: %w", err)
		}
	}
	if asName := n.AsName.String(); asName != "" {
		ctx.WriteKeyWord(" AS ")
		ctx.WriteName(asName)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *SelectField) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SelectField)
	if n.Expr != nil {
		node, ok := n.Expr.Accept(v)
		if !ok {
			return n, false
		}
		n.Expr = node.(ExprNode)
	}
	return v.Leave(n)
}

func (n *SelectField) Match(col *ColumnNameExpr, ignoreAsName bool) bool {
	// if col specify a table name, resolve from table source directly.
	if col.Name.Table.L == "" {
		if n.AsName.L == "" || ignoreAsName {
			if curCol, isCol := n.Expr.(*ColumnNameExpr); isCol {
				return curCol.Name.Name.L == col.Name.Name.L
			} else if _, isFunc := n.Expr.(*FuncCallExpr); isFunc {
				// Fix issue 7331
				// If there are some function calls in SelectField, we check if
				// ColumnNameExpr in GroupByClause matches one of these function calls.
				// Example: select concat(k1,k2) from t group by `concat(k1,k2)`,
				// `concat(k1,k2)` matches with function call concat(k1, k2).
				return strings.ToLower(n.Text()) == col.Name.Name.L
			}
			// a expression without as name can't be matched.
			return false
		}
		return n.AsName.L == col.Name.Name.L
	}
	return false
}

// FieldList represents field list in select statement.
type FieldList struct {
	node

	Fields []*SelectField
}

// Restore implements Node interface.
func (n *FieldList) Restore(ctx *format.RestoreCtx) error {
	for i, v := range n.Fields {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore FieldList.Fields[%d]: %w", i, err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *FieldList) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FieldList)
	for i, val := range n.Fields {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Fields[i] = node.(*SelectField)
	}
	return v.Leave(n)
}

// TableRefsClause represents table references clause in dml statement.
type TableRefsClause struct {
	node

	TableRefs *Join
}

// Restore implements Node interface.
func (n *TableRefsClause) Restore(ctx *format.RestoreCtx) error {
	if err := n.TableRefs.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore TableRefsClause.TableRefs: %w", err)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *TableRefsClause) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*TableRefsClause)
	node, ok := n.TableRefs.Accept(v)
	if !ok {
		return n, false
	}
	n.TableRefs = node.(*Join)
	return v.Leave(n)
}

// ByItem represents an item in order by or group by.
type ByItem struct {
	node

	Expr      ExprNode
	Desc      bool
	NullOrder bool
}

// Restore implements Node interface.
func (n *ByItem) Restore(ctx *format.RestoreCtx) error {
	if err := n.Expr.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore ByItem.Expr: %w", err)
	}
	if n.Desc {
		ctx.WriteKeyWord(" DESC")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ByItem) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ByItem)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)
	return v.Leave(n)
}

// GroupByClause represents group by clause.
type GroupByClause struct {
	node
	Items  []*ByItem
	Rollup bool
	// GroupingSets is non-nil for GROUP BY GROUPING SETS ((...), ...);
	// Items is empty in that case. An empty inner slice is the empty set ().
	GroupingSets [][]ExprNode
}

// Restore implements Node interface.
func (n *GroupByClause) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("GROUP BY ")
	if n.GroupingSets != nil {
		ctx.WriteKeyWord("GROUPING SETS ")
		ctx.WritePlain("(")
		for i, set := range n.GroupingSets {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WritePlain("(")
			for j, expr := range set {
				if j != 0 {
					ctx.WritePlain(",")
				}
				if err := expr.Restore(ctx); err != nil {
					return fmt.Errorf("an error occurred while restore GroupByClause.GroupingSets[%d][%d]: %w", i, j, err)
				}
			}
			ctx.WritePlain(")")
		}
		ctx.WritePlain(")")
		return nil
	}
	for i, v := range n.Items {
		if i != 0 {
			ctx.WritePlain(",")
		}
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore GroupByClause.Items[%d]: %w", i, err)
		}
	}
	if n.Rollup {
		ctx.WriteKeyWord(" WITH ROLLUP")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *GroupByClause) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*GroupByClause)
	for i, val := range n.Items {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Items[i] = node.(*ByItem)
	}
	for i, set := range n.GroupingSets {
		for j, expr := range set {
			node, ok := expr.Accept(v)
			if !ok {
				return n, false
			}
			n.GroupingSets[i][j] = node.(ExprNode)
		}
	}
	return v.Leave(n)
}

// HavingClause represents having clause.
type HavingClause struct {
	node
	Expr ExprNode
}

// Restore implements Node interface.
func (n *HavingClause) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("HAVING ")
	if err := n.Expr.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore HavingClause.Expr: %w", err)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *HavingClause) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*HavingClause)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)
	return v.Leave(n)
}

// OrderByClause represents order by clause.
type OrderByClause struct {
	node
	Items    []*ByItem
	ForUnion bool
}

// Restore implements Node interface.
func (n *OrderByClause) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ORDER BY ")
	for i, item := range n.Items {
		if i != 0 {
			ctx.WritePlain(",")
		}
		if err := item.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore OrderByClause.Items[%d]: %w", i, err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *OrderByClause) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*OrderByClause)
	for i, val := range n.Items {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Items[i] = node.(*ByItem)
	}
	return v.Leave(n)
}

type SelectStmtKind uint8

const (
	SelectStmtKindSelect SelectStmtKind = iota
	SelectStmtKindTable
	SelectStmtKindValues
)

func (s *SelectStmtKind) String() string {
	switch *s {
	case SelectStmtKindSelect:
		return "SELECT"
	case SelectStmtKindTable:
		return "TABLE"
	case SelectStmtKindValues:
		return "VALUES"
	}
	return ""
}

type CommonTableExpression struct {
	node

	Name        CIStr
	Query       *SubqueryExpr
	ColNameList []CIStr
	IsRecursive bool

	// Record how many consumers the current cte has
	ConsumerCount int
}

// Restore implements Node interface
func (c *CommonTableExpression) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteName(c.Name.String())
	if c.IsRecursive {
		// If the CTE is recursive, we should make it visible for the CTE's query.
		// Otherwise, we should put it to stack after building the CTE's query.
		ctx.RecordCTEName(c.Name.L)
	}
	if len(c.ColNameList) > 0 {
		ctx.WritePlain(" (")
		for j, name := range c.ColNameList {
			if j != 0 {
				ctx.WritePlain(", ")
			}
			ctx.WriteName(name.String())
		}
		ctx.WritePlain(")")
	}
	ctx.WriteKeyWord(" AS ")
	err := c.Query.Restore(ctx)
	if err != nil {
		return err
	}
	if !c.IsRecursive {
		ctx.RecordCTEName(c.Name.L)
	}
	return nil
}

// Accept implements Node interface
func (c *CommonTableExpression) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(c)
	if skipChildren {
		return v.Leave(newNode)
	}

	node, ok := c.Query.Accept(v)
	if !ok {
		return c, false
	}
	c.Query = node.(*SubqueryExpr)
	return v.Leave(c)
}

type WithClause struct {
	node

	IsRecursive bool
	CTEs        []*CommonTableExpression
}

// SelectStmt represents the select query node.
// See https://dev.mysql.com/doc/refman/5.7/en/select.html
type SelectStmt struct {
	dmlNode

	// SelectStmtOpts wraps around select hints and switches.
	*SelectStmtOpts
	// Distinct represents whether the select has distinct option.
	Distinct bool
	// From is the from clause of the query.
	From *TableRefsClause
	// Where is the where clause in select statement.
	Where ExprNode
	// Fields is the select expression list.
	Fields *FieldList
	// GroupBy is the group by expression list.
	GroupBy *GroupByClause
	// Having is the having condition.
	Having *HavingClause
	// WindowSpecs is the window specification list.
	WindowSpecs []WindowSpec
	// Qualify is the QUALIFY window-filter condition (MySQL 9.7+).
	Qualify ExprNode
	// OrderBy is the ordering expression list.
	OrderBy *OrderByClause
	// Limit is the limit clause.
	Limit *Limit
	// LockInfos are the locking clauses (FOR UPDATE, FOR SHARE, LOCK IN SHARE
	// MODE); MySQL allows several locking clauses in one query block.
	LockInfos []*SelectLockInfo
	// TableHints represents the table level Optimizer Hint for join type
	TableHints []*TableOptimizerHint
	// IsInBraces indicates whether it's a stmt in brace.
	IsInBraces bool
	// WithBeforeBraces indicates whether stmt's with clause is before the brace.
	// It's used to distinguish (with xxx select xxx) and with xxx (select xxx)
	WithBeforeBraces bool
	// QueryBlockOffset indicates the order of this SelectStmt if counted from left to right in the sql text.
	QueryBlockOffset int
	// SelectIntoOpt is the select-into option.
	SelectIntoOpt *SelectIntoOption
	// AfterSetOperator indicates the SelectStmt after which type of set operator
	AfterSetOperator *SetOprType
	// Kind refer to three kind of statement: SelectStmt, TableStmt and ValuesStmt
	Kind SelectStmtKind
	// Lists is filled only when Kind == SelectStmtKindValues
	Lists []*RowExpr
	With  *WithClause
	// AsViewSchema indicates if this stmt provides the schema for the view. It is only used when creating the view
	AsViewSchema bool
}

func (*SelectStmt) resultSet() {}

func (n *WithClause) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("WITH ")
	if n.IsRecursive {
		ctx.WriteKeyWord("RECURSIVE ")
	}
	for i, cte := range n.CTEs {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := cte.Restore(ctx); err != nil {
			return err
		}
	}
	ctx.WritePlain(" ")
	return nil
}

func (n *WithClause) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	for _, cte := range n.CTEs {
		if _, ok := cte.Accept(v); !ok {
			return n, false
		}
	}
	return v.Leave(n)
}

// Restore implements Node interface.
func (n *SelectStmt) Restore(ctx *format.RestoreCtx) error {
	if n.WithBeforeBraces {
		defer ctx.RestoreCTEFunc()()
		err := n.With.Restore(ctx)
		if err != nil {
			return err
		}
	}
	if n.IsInBraces {
		ctx.WritePlain("(")
		defer func() {
			ctx.WritePlain(")")
		}()
	}
	if !n.WithBeforeBraces && n.With != nil {
		defer ctx.RestoreCTEFunc()()
		err := n.With.Restore(ctx)
		if err != nil {
			return err
		}
	}

	ctx.WriteKeyWord(n.Kind.String())
	ctx.WritePlain(" ")
	switch n.Kind {
	case SelectStmtKindSelect:
		if n.Priority > 0 {
			ctx.WriteKeyWord(mysql.Priority2Str[n.Priority])
			ctx.WritePlain(" ")
		}

		if n.SQLSmallResult {
			ctx.WriteKeyWord("SQL_SMALL_RESULT ")
		}

		if n.SQLBigResult {
			ctx.WriteKeyWord("SQL_BIG_RESULT ")
		}

		if n.SQLBufferResult {
			ctx.WriteKeyWord("SQL_BUFFER_RESULT ")
		}

		if !n.SQLCache {
			ctx.WriteKeyWord("SQL_NO_CACHE ")
		}

		if n.CalcFoundRows {
			ctx.WriteKeyWord("SQL_CALC_FOUND_ROWS ")
		}

		if len(n.TableHints) != 0 {
			ctx.WritePlain("/*+ ")
			for i, tableHint := range n.TableHints {
				if i != 0 {
					ctx.WritePlain(" ")
				}
				if err := tableHint.Restore(ctx); err != nil {
					return fmt.Errorf("an error occurred while restore SelectStmt.TableHints[%d]: %w", i, err)
				}
			}
			ctx.WritePlain("*/ ")
		}

		if n.Distinct {
			ctx.WriteKeyWord("DISTINCT ")
		} else if n.ExplicitAll {
			ctx.WriteKeyWord("ALL ")
		}
		if n.StraightJoin {
			ctx.WriteKeyWord("STRAIGHT_JOIN ")
		}
		if n.Fields != nil {
			for i, field := range n.Fields.Fields {
				if i != 0 {
					ctx.WritePlain(",")
				}
				if err := field.Restore(ctx); err != nil {
					return fmt.Errorf("an error occurred while restore SelectStmt.Fields[%d]: %w", i, err)
				}
			}
		}

		if n.From != nil {
			ctx.WriteKeyWord(" FROM ")
			if err := n.From.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore SelectStmt.From: %w", err)
			}
		}

		if n.From == nil && n.Where != nil {
			ctx.WriteKeyWord(" FROM DUAL")
		}

		if n.Where != nil {
			ctx.WriteKeyWord(" WHERE ")
			if err := n.Where.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore SelectStmt.Where: %w", err)
			}
		}

		if n.GroupBy != nil {
			ctx.WritePlain(" ")
			if err := n.GroupBy.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore SelectStmt.GroupBy: %w", err)
			}
		}

		if n.Having != nil {
			ctx.WritePlain(" ")
			if err := n.Having.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore SelectStmt.Having: %w", err)
			}
		}

		if n.WindowSpecs != nil {
			ctx.WriteKeyWord(" WINDOW ")
			for i, windowsSpec := range n.WindowSpecs {
				if i != 0 {
					ctx.WritePlain(",")
				}
				if err := windowsSpec.Restore(ctx); err != nil {
					return fmt.Errorf("an error occurred while restore SelectStmt.WindowSpec[%d]: %w", i, err)
				}
			}
		}

		if n.Qualify != nil {
			ctx.WriteKeyWord(" QUALIFY ")
			if err := n.Qualify.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore SelectStmt.Qualify: %w", err)
			}
		}
	case SelectStmtKindTable:
		if err := n.From.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore SelectStmt.From: %w", err)
		}
	case SelectStmtKindValues:
		for i, v := range n.Lists {
			if err := v.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore SelectStmt.Lists[%d]: %w", i, err)
			}
			if i != len(n.Lists)-1 {
				ctx.WritePlain(", ")
			}
		}
	}

	if n.OrderBy != nil {
		ctx.WritePlain(" ")
		if err := n.OrderBy.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore SelectStmt.OrderBy: %w", err)
		}
	}

	if n.Limit != nil {
		ctx.WritePlain(" ")
		if err := n.Limit.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore SelectStmt.Limit: %w", err)
		}
	}

	for _, lock := range n.LockInfos {
		ctx.WritePlain(" ")
		if err := restoreSelectLockInfo(ctx, lock); err != nil {
			return err
		}
	}

	if n.SelectIntoOpt != nil {
		ctx.WritePlain(" ")
		if err := n.SelectIntoOpt.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore SelectStmt.SelectIntoOpt: %w", err)
		}
	}
	return nil
}

func restoreSelectLockInfo(ctx *format.RestoreCtx, lock *SelectLockInfo) error {
	switch lock.LockType { //nolint:exhaustive
	case SelectLockNone:
	case SelectLockForUpdateNoWait:
		ctx.WriteKeyWord("for update")
		if len(lock.Tables) != 0 {
			ctx.WriteKeyWord(" OF ")
			if err := restoreTables(ctx, lock.Tables); err != nil {
				return err
			}
		}
		ctx.WriteKeyWord(" nowait")
	case SelectLockForUpdateWaitN:
		ctx.WriteKeyWord("for update")
		if len(lock.Tables) != 0 {
			ctx.WriteKeyWord(" OF ")
			if err := restoreTables(ctx, lock.Tables); err != nil {
				return err
			}
		}
		ctx.WriteKeyWord(" wait")
		ctx.WritePlainf(" %d", lock.WaitSec)
	case SelectLockForShareNoWait:
		ctx.WriteKeyWord("for share")
		if len(lock.Tables) != 0 {
			ctx.WriteKeyWord(" OF ")
			if err := restoreTables(ctx, lock.Tables); err != nil {
				return err
			}
		}
		ctx.WriteKeyWord(" nowait")
	case SelectLockForUpdateSkipLocked:
		ctx.WriteKeyWord("for update")
		if len(lock.Tables) != 0 {
			ctx.WriteKeyWord(" OF ")
			if err := restoreTables(ctx, lock.Tables); err != nil {
				return err
			}
		}
		ctx.WriteKeyWord(" skip locked")
	case SelectLockForShareSkipLocked:
		ctx.WriteKeyWord("for share")
		if len(lock.Tables) != 0 {
			ctx.WriteKeyWord(" OF ")
			if err := restoreTables(ctx, lock.Tables); err != nil {
				return err
			}
		}
		ctx.WriteKeyWord(" skip locked")
	default:
		ctx.WriteKeyWord(lock.LockType.String())
		if len(lock.Tables) != 0 {
			ctx.WriteKeyWord(" OF ")
			if err := restoreTables(ctx, lock.Tables); err != nil {
				return err
			}
		}
	}
	return nil
}

func restoreTables(ctx *format.RestoreCtx, ts []*TableName) error {
	for i, v := range ts {
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore SelectStmt.LockInfo: %w", err)
		}
		if i != len(ts)-1 {
			ctx.WritePlain(", ")
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *SelectStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	n = newNode.(*SelectStmt)

	if n.With != nil {
		node, ok := n.With.Accept(v)
		if !ok {
			return n, false
		}
		n.With = node.(*WithClause)
	}

	if len(n.TableHints) != 0 {
		newHints := make([]*TableOptimizerHint, len(n.TableHints))
		for i, hint := range n.TableHints {
			node, ok := hint.Accept(v)
			if !ok {
				return n, false
			}
			newHints[i] = node.(*TableOptimizerHint)
		}
		n.TableHints = newHints
	}

	if n.Fields != nil {
		node, ok := n.Fields.Accept(v)
		if !ok {
			return n, false
		}
		n.Fields = node.(*FieldList)
	}

	if n.From != nil {
		node, ok := n.From.Accept(v)
		if !ok {
			return n, false
		}
		n.From = node.(*TableRefsClause)
	}

	if n.Where != nil {
		node, ok := n.Where.Accept(v)
		if !ok {
			return n, false
		}
		n.Where = node.(ExprNode)
	}

	if n.GroupBy != nil {
		node, ok := n.GroupBy.Accept(v)
		if !ok {
			return n, false
		}
		n.GroupBy = node.(*GroupByClause)
	}

	if n.Having != nil {
		node, ok := n.Having.Accept(v)
		if !ok {
			return n, false
		}
		n.Having = node.(*HavingClause)
	}

	for i, list := range n.Lists {
		node, ok := list.Accept(v)
		if !ok {
			return n, false
		}
		n.Lists[i] = node.(*RowExpr)
	}

	for i, spec := range n.WindowSpecs {
		node, ok := spec.Accept(v)
		if !ok {
			return n, false
		}
		n.WindowSpecs[i] = *node.(*WindowSpec)
	}
	if n.Qualify != nil {
		node, ok := n.Qualify.Accept(v)
		if !ok {
			return n, false
		}
		n.Qualify = node.(ExprNode)
	}

	if n.OrderBy != nil {
		node, ok := n.OrderBy.Accept(v)
		if !ok {
			return n, false
		}
		n.OrderBy = node.(*OrderByClause)
	}

	if n.Limit != nil {
		node, ok := n.Limit.Accept(v)
		if !ok {
			return n, false
		}
		n.Limit = node.(*Limit)
	}

	for _, lock := range n.LockInfos {
		for i, t := range lock.Tables {
			node, ok := t.Accept(v)
			if !ok {
				return n, false
			}
			lock.Tables[i] = node.(*TableName)
		}
	}

	if n.SelectIntoOpt != nil {
		node, ok := n.SelectIntoOpt.Accept(v)
		if !ok {
			return n, false
		}
		n.SelectIntoOpt = node.(*SelectIntoOption)
	}

	return v.Leave(n)
}

// SetOprSelectList represents the SelectStmt/TableStmt/ValuesStmt list in a union statement.
type SetOprSelectList struct {
	node

	With             *WithClause
	AfterSetOperator *SetOprType
	Selects          []Node
	Limit            *Limit
	OrderBy          *OrderByClause
}

// Restore implements Node interface.
func (n *SetOprSelectList) Restore(ctx *format.RestoreCtx) error {
	if n.With != nil {
		defer ctx.RestoreCTEFunc()()
		if err := n.With.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore SetOprSelectList.With: %w", err)
		}
	}

	for i, stmt := range n.Selects {
		switch selectStmt := stmt.(type) {
		case *SelectStmt:
			if i != 0 {
				ctx.WriteKeyWord(" " + selectStmt.AfterSetOperator.String() + " ")
			}
			if err := selectStmt.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore SetOprSelectList.SelectStmt: %w", err)
			}
		case *SetOprSelectList:
			if i != 0 {
				ctx.WriteKeyWord(" " + selectStmt.AfterSetOperator.String() + " ")
			}
			ctx.WritePlain("(")
			err := selectStmt.Restore(ctx)
			if err != nil {
				return err
			}
			ctx.WritePlain(")")
		}
	}

	if n.OrderBy != nil {
		ctx.WritePlain(" ")
		if err := n.OrderBy.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore SetOprSelectList.OrderBy: %w", err)
		}
	}

	if n.Limit != nil {
		ctx.WritePlain(" ")
		if err := n.Limit.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore SetOprSelectList.Limit: %w", err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *SetOprSelectList) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SetOprSelectList)
	if n.With != nil {
		node, ok := n.With.Accept(v)
		if !ok {
			return n, false
		}
		n.With = node.(*WithClause)
	}
	for i, sel := range n.Selects {
		node, ok := sel.Accept(v)
		if !ok {
			return n, false
		}
		n.Selects[i] = node
	}
	if n.OrderBy != nil {
		node, ok := n.OrderBy.Accept(v)
		if !ok {
			return n, false
		}
		n.OrderBy = node.(*OrderByClause)
	}
	if n.Limit != nil {
		node, ok := n.Limit.Accept(v)
		if !ok {
			return n, false
		}
		n.Limit = node.(*Limit)
	}
	return v.Leave(n)
}

type SetOprType uint8

const (
	Union SetOprType = iota
	UnionAll
	Except
	ExceptAll
	Intersect
	IntersectAll
)

func (s *SetOprType) String() string {
	switch *s {
	case Union:
		return "UNION"
	case UnionAll:
		return "UNION ALL"
	case Except:
		return "EXCEPT"
	case ExceptAll:
		return "EXCEPT ALL"
	case Intersect:
		return "INTERSECT"
	case IntersectAll:
		return "INTERSECT ALL"
	}
	return ""
}

// SetOprStmt represents "union/except/intersect statement"
// See https://dev.mysql.com/doc/refman/5.7/en/union.html
// INTERSECT and EXCEPT are supported by MySQL 8.0.31+.
// See https://dev.mysql.com/doc/refman/8.0/en/set-operations.html
type SetOprStmt struct {
	dmlNode

	IsInBraces bool
	SelectList *SetOprSelectList
	OrderBy    *OrderByClause
	Limit      *Limit
	With       *WithClause
	// IntoOpt is the trailing INTO of a parenthesized query expression:
	// (SELECT ...) [ORDER BY ...] [LIMIT ...] INTO var_list.
	IntoOpt *SelectIntoOption
}

func (*SetOprStmt) resultSet() {}

// Restore implements Node interface.
func (n *SetOprStmt) Restore(ctx *format.RestoreCtx) error {
	if n.With != nil {
		defer ctx.RestoreCTEFunc()()
		if err := n.With.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore SetOprStmt.With: %w", err)
		}
	}
	if n.IsInBraces {
		ctx.WritePlain("(")
		defer func() {
			ctx.WritePlain(")")
		}()
	}

	if err := n.SelectList.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore SetOprStmt.SelectList: %w", err)
	}

	if n.OrderBy != nil {
		ctx.WritePlain(" ")
		if err := n.OrderBy.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore SetOprStmt.OrderBy: %w", err)
		}
	}

	if n.Limit != nil {
		ctx.WritePlain(" ")
		if err := n.Limit.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore SetOprStmt.Limit: %w", err)
		}
	}
	if n.IntoOpt != nil {
		ctx.WritePlain(" ")
		if err := n.IntoOpt.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore SetOprStmt.IntoOpt: %w", err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *SetOprStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	if n.With != nil {
		node, ok := n.With.Accept(v)
		if !ok {
			return n, false
		}
		n.With = node.(*WithClause)
	}
	if n.SelectList != nil {
		node, ok := n.SelectList.Accept(v)
		if !ok {
			return n, false
		}
		n.SelectList = node.(*SetOprSelectList)
	}
	if n.OrderBy != nil {
		node, ok := n.OrderBy.Accept(v)
		if !ok {
			return n, false
		}
		n.OrderBy = node.(*OrderByClause)
	}
	if n.Limit != nil {
		node, ok := n.Limit.Accept(v)
		if !ok {
			return n, false
		}
		n.Limit = node.(*Limit)
	}
	if n.IntoOpt != nil {
		node, ok := n.IntoOpt.Accept(v)
		if !ok {
			return n, false
		}
		n.IntoOpt = node.(*SelectIntoOption)
	}
	return v.Leave(n)
}

// Assignment is the expression for assignment, like a = 1.
type Assignment struct {
	node
	// Column is the column name to be assigned.
	Column *ColumnName
	// Expr is the expression assigning to ColName.
	Expr ExprNode
}

// Restore implements Node interface.
func (n *Assignment) Restore(ctx *format.RestoreCtx) error {
	if err := n.Column.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore Assignment.Column: %w", err)
	}
	ctx.WritePlain("=")
	if err := n.Expr.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore Assignment.Expr: %w", err)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *Assignment) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*Assignment)
	node, ok := n.Column.Accept(v)
	if !ok {
		return n, false
	}
	n.Column = node.(*ColumnName)
	node, ok = n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)
	return v.Leave(n)
}

type ColumnNameOrUserVar struct {
	node
	ColumnName *ColumnName
	UserVar    *VariableExpr
}

func (n *ColumnNameOrUserVar) Restore(ctx *format.RestoreCtx) error {
	if n.ColumnName != nil {
		if err := n.ColumnName.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ColumnNameOrUserVar.ColumnName: %w", err)
		}
	}
	if n.UserVar != nil {
		if err := n.UserVar.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ColumnNameOrUserVar.UserVar: %w", err)
		}
	}
	return nil
}

func (n *ColumnNameOrUserVar) Accept(v Visitor) (node Node, ok bool) {
	newNode, skipChild := v.Enter(n)
	if skipChild {
		return v.Leave(newNode)
	}
	n = newNode.(*ColumnNameOrUserVar)
	if n.ColumnName != nil {
		node, ok = n.ColumnName.Accept(v)
		if !ok {
			return node, false
		}
		n.ColumnName = node.(*ColumnName)
	}
	if n.UserVar != nil {
		node, ok = n.UserVar.Accept(v)
		if !ok {
			return node, false
		}
		n.UserVar = node.(*VariableExpr)
	}
	return v.Leave(n)
}

// FileLocRefTp indicates where the LOAD DATA file is located.
// See https://dev.mysql.com/doc/refman/8.0/en/load-data.html
type FileLocRefTp int

const (
	// FileLocServer is used when there is no LOCAL keyword, meaning the data
	// file is located on the server host.
	FileLocServer FileLocRefTp = iota
	// FileLocClient is used when there's LOCAL keyword in SQL, which means the data file should be located on the MySQL
	// client.
	FileLocClient
)

type LoadDataStmt struct {
	dmlNode

	Xml               bool   // LOAD XML instead of LOAD DATA.
	XmlRowTag         string // ROWS IDENTIFIED BY '<tag>' of LOAD XML; empty when absent.
	LowPriority       bool
	Concurrent        bool
	InPrimaryKeyOrder bool // NDB-only IN PRIMARY KEY ORDER load hint.
	FileLocRef        FileLocRefTp
	Path              string
	OnDuplicate       OnDuplicateKeyHandlingType
	Table             *TableName
	Charset           *string
	Columns           []*ColumnName
	FieldsInfo        *FieldsClause
	LinesInfo         *LinesClause
	IgnoreLines       *uint64
	ColumnAssignments []*Assignment

	ColumnsAndUserVars []*ColumnNameOrUserVar
}

// Restore implements Node interface.
func (n *LoadDataStmt) Restore(ctx *format.RestoreCtx) error {
	if n.Xml {
		ctx.WriteKeyWord("LOAD XML ")
	} else {
		ctx.WriteKeyWord("LOAD DATA ")
	}
	if n.LowPriority {
		ctx.WriteKeyWord("LOW_PRIORITY ")
	}
	if n.Concurrent {
		ctx.WriteKeyWord("CONCURRENT ")
	}
	switch n.FileLocRef {
	case FileLocServer:
	case FileLocClient:
		ctx.WriteKeyWord("LOCAL ")
	}
	ctx.WriteKeyWord("INFILE ")
	ctx.WriteString(n.Path)
	if n.InPrimaryKeyOrder {
		ctx.WriteKeyWord(" IN PRIMARY KEY ORDER")
	}
	switch n.OnDuplicate { //nolint:exhaustive
	case OnDuplicateKeyHandlingReplace:
		ctx.WriteKeyWord(" REPLACE")
	case OnDuplicateKeyHandlingIgnore:
		ctx.WriteKeyWord(" IGNORE")
	}
	ctx.WriteKeyWord(" INTO TABLE ")
	if err := n.Table.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore LoadDataStmt.Table: %w", err)
	}
	if n.Charset != nil {
		ctx.WriteKeyWord(" CHARACTER SET ")
		ctx.WritePlain(*n.Charset)
	}
	if n.XmlRowTag != "" {
		ctx.WriteKeyWord(" ROWS IDENTIFIED BY ")
		ctx.WriteString(n.XmlRowTag)
	}
	if n.FieldsInfo != nil {
		if err := n.FieldsInfo.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore LoadDataStmt.FieldsInfo: %w", err)
		}
	}
	if n.LinesInfo != nil {
		if err := n.LinesInfo.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore LoadDataStmt.LinesInfo: %w", err)
		}
	}
	if n.IgnoreLines != nil {
		ctx.WriteKeyWord(" IGNORE ")
		ctx.WritePlainf("%d", *n.IgnoreLines)
		ctx.WriteKeyWord(" LINES")
	}
	if len(n.ColumnsAndUserVars) != 0 {
		ctx.WritePlain(" (")
		for i, c := range n.ColumnsAndUserVars {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := c.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore LoadDataStmt.ColumnsAndUserVars: %w", err)
			}
		}
		ctx.WritePlain(")")
	}

	if n.ColumnAssignments != nil {
		ctx.WriteKeyWord(" SET")
		for i, assign := range n.ColumnAssignments {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WritePlain(" ")
			if err := assign.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore LoadDataStmt.ColumnAssignments: %w", err)
			}
		}
	}

	return nil
}

// Accept implements Node Accept interface.
func (n *LoadDataStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*LoadDataStmt)
	if n.Table != nil {
		node, ok := n.Table.Accept(v)
		if !ok {
			return n, false
		}
		n.Table = node.(*TableName)
	}
	for i, val := range n.Columns {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Columns[i] = node.(*ColumnName)
	}

	for i, assignment := range n.ColumnAssignments {
		node, ok := assignment.Accept(v)
		if !ok {
			return n, false
		}
		n.ColumnAssignments[i] = node.(*Assignment)
	}
	for i, cuVars := range n.ColumnsAndUserVars {
		node, ok := cuVars.Accept(v)
		if !ok {
			return n, false
		}
		n.ColumnsAndUserVars[i] = node.(*ColumnNameOrUserVar)
	}
	return v.Leave(n)
}

const (
	Terminated = iota
	Enclosed
	Escaped
	DefinedNullBy
)

type FieldItem struct {
	Type        int
	Value       string
	OptEnclosed bool
}

// FieldsClause represents fields references clause in load data statement.
type FieldsClause struct {
	Terminated           *string
	Enclosed             *string // length always <= 1 if not nil, see parser.y
	Escaped              *string // length always <= 1 if not nil, see parser.y
	OptEnclosed          bool
	DefinedNullBy        *string
	NullValueOptEnclosed bool
}

// Restore for FieldsClause
func (n *FieldsClause) Restore(ctx *format.RestoreCtx) error {
	if n.Terminated == nil && n.Enclosed == nil && n.Escaped == nil && n.DefinedNullBy == nil {
		return nil
	}

	ctx.WriteKeyWord(" FIELDS")
	if n.Terminated != nil {
		ctx.WriteKeyWord(" TERMINATED BY ")
		ctx.WriteString(*n.Terminated)
	}
	if n.Enclosed != nil {
		if n.OptEnclosed {
			ctx.WriteKeyWord(" OPTIONALLY")
		}
		ctx.WriteKeyWord(" ENCLOSED BY ")
		ctx.WriteString(*n.Enclosed)
	}
	if n.Escaped != nil {
		ctx.WriteKeyWord(" ESCAPED BY ")
		ctx.WriteString(*n.Escaped)
	}
	if n.DefinedNullBy != nil {
		ctx.WriteKeyWord(" DEFINED NULL BY ")
		ctx.WriteString(*n.DefinedNullBy)
		if n.NullValueOptEnclosed {
			ctx.WriteKeyWord(" OPTIONALLY ENCLOSED")
		}
	}
	return nil
}

// LinesClause represents lines references clause in load data statement.
type LinesClause struct {
	Starting   *string
	Terminated *string
}

// Restore for LinesClause
func (n *LinesClause) Restore(ctx *format.RestoreCtx) error {
	if n.Starting == nil && n.Terminated == nil {
		return nil
	}
	ctx.WriteKeyWord(" LINES")
	if n.Starting != nil {
		ctx.WriteKeyWord(" STARTING BY ")
		ctx.WriteString(*n.Starting)
	}
	if n.Terminated != nil {
		ctx.WriteKeyWord(" TERMINATED BY ")
		ctx.WriteString(*n.Terminated)
	}
	return nil
}

// CallStmt represents a call procedure query node.
// See https://dev.mysql.com/doc/refman/5.7/en/call.html
type CallStmt struct {
	dmlNode

	Procedure *FuncCallExpr
}

// Restore implements Node interface.
func (n *CallStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CALL ")

	if err := n.Procedure.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore CallStmt.Procedure: %w", err)
	}

	return nil
}

// Accept implements Node Accept interface.
func (n *CallStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	n = newNode.(*CallStmt)

	if n.Procedure != nil {
		node, ok := n.Procedure.Accept(v)
		if !ok {
			return n, false
		}

		n.Procedure = node.(*FuncCallExpr)
	}

	return v.Leave(n)
}

// InsertStmt is a statement to insert new rows into an existing table.
// See https://dev.mysql.com/doc/refman/5.7/en/insert.html
type InsertStmt struct {
	dmlNode

	IsReplace   bool
	IgnoreErr   bool
	Table       *TableRefsClause
	Columns     []*ColumnName
	Lists       [][]ExprNode
	Setlist     bool
	Priority    mysql.PriorityEnum
	OnDuplicate []*Assignment
	Select      ResultSetNode
	// TableHints represents the table level Optimizer Hint for join type.
	TableHints     []*TableOptimizerHint
	PartitionNames []CIStr
	// RowAlias is the optional row alias for VALUES/SET clause (MySQL 8.0.19+).
	// e.g. INSERT INTO t VALUES (1,2) AS new ON DUPLICATE KEY UPDATE b = new.b
	RowAlias CIStr
	// ColumnAliases is the optional column alias list for the row alias.
	// e.g. INSERT INTO t VALUES (1,2) AS new(m, n) ON DUPLICATE KEY UPDATE b = m
	ColumnAliases []CIStr
}

// Restore implements Node interface.
func (n *InsertStmt) Restore(ctx *format.RestoreCtx) error {
	if n.IsReplace {
		ctx.WriteKeyWord("REPLACE ")
	} else {
		ctx.WriteKeyWord("INSERT ")
	}

	if len(n.TableHints) != 0 {
		ctx.WritePlain("/*+ ")
		for i, tableHint := range n.TableHints {
			if i != 0 {
				ctx.WritePlain(" ")
			}
			if err := tableHint.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore InsertStmt.TableHints[%d]: %w", i, err)
			}
		}
		ctx.WritePlain("*/ ")
	}

	if err := n.Priority.Restore(ctx); err != nil {
		return err
	}
	if n.Priority != mysql.NoPriority {
		ctx.WritePlain(" ")
	}
	if n.IgnoreErr {
		ctx.WriteKeyWord("IGNORE ")
	}
	ctx.WriteKeyWord("INTO ")
	if err := n.Table.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore InsertStmt.Table: %w", err)
	}
	if len(n.PartitionNames) != 0 {
		ctx.WriteKeyWord(" PARTITION")
		ctx.WritePlain("(")
		for i := range n.PartitionNames {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			ctx.WriteName(n.PartitionNames[i].String())
		}
		ctx.WritePlain(")")
	}
	if n.Setlist {
		if len(n.Lists) != 1 {
			return fmt.Errorf("expect len(InsertStmt.Lists)[%d] == 1 for SET x=y", len(n.Lists))
		}
		if len(n.Lists[0]) != len(n.Columns) {
			return fmt.Errorf("expect len(InsertStmt.Columns)[%d] == len(InsertStmt.Lists[0])[%d] for SET x=y", len(n.Columns), len(n.Lists[0]))
		}
		ctx.WriteKeyWord(" SET ")
		for i, v := range n.Lists[0] {
			if i != 0 {
				ctx.WritePlain(",")
			}
			v := &Assignment{
				Column: n.Columns[i],
				Expr:   v,
			}
			if err := v.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore InsertStmt.Set (Columns = Expr)[%d]: %w", i, err)
			}
		}
	} else {
		if n.Columns != nil {
			ctx.WritePlain(" (")
			for i, v := range n.Columns {
				if i != 0 {
					ctx.WritePlain(",")
				}
				if err := v.Restore(ctx); err != nil {
					return fmt.Errorf("an error occurred while restore InsertStmt.Columns[%d]: %w", i, err)
				}
			}
			ctx.WritePlain(")")
		}
		if n.Lists != nil {
			ctx.WriteKeyWord(" VALUES ")
			for i, row := range n.Lists {
				if i != 0 {
					ctx.WritePlain(",")
				}
				ctx.WritePlain("(")
				for j, v := range row {
					if j != 0 {
						ctx.WritePlain(",")
					}
					if err := v.Restore(ctx); err != nil {
						return fmt.Errorf("an error occurred while restore InsertStmt.Lists[%d][%d]: %w", i, j, err)
					}
				}
				ctx.WritePlain(")")
			}
		}
	}
	if n.Select != nil {
		ctx.WritePlain(" ")
		switch v := n.Select.(type) {
		case *SelectStmt, *SetOprStmt:
			if err := v.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore InsertStmt.Select: %w", err)
			}
		default:
			return fmt.Errorf("incorrect type for InsertStmt.Select: %T", v)
		}
	}
	if asName := n.RowAlias.String(); asName != "" {
		ctx.WriteKeyWord(" AS ")
		ctx.WriteName(asName)
		if len(n.ColumnAliases) > 0 {
			ctx.WritePlain("(")
			for i, col := range n.ColumnAliases {
				if i > 0 {
					ctx.WritePlain(", ")
				}
				ctx.WriteName(col.String())
			}
			ctx.WritePlain(")")
		}
	}
	if n.OnDuplicate != nil {
		ctx.WriteKeyWord(" ON DUPLICATE KEY UPDATE ")
		for i, v := range n.OnDuplicate {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := v.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore InsertStmt.OnDuplicate[%d]: %w", i, err)
			}
		}
	}

	return nil
}

// Accept implements Node Accept interface.
func (n *InsertStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	n = newNode.(*InsertStmt)
	if n.Select != nil {
		node, ok := n.Select.Accept(v)
		if !ok {
			return n, false
		}
		n.Select = node.(ResultSetNode)
	}

	node, ok := n.Table.Accept(v)
	if !ok {
		return n, false
	}
	n.Table = node.(*TableRefsClause)

	for i, val := range n.Columns {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Columns[i] = node.(*ColumnName)
	}
	for i, list := range n.Lists {
		for j, val := range list {
			node, ok := val.Accept(v)
			if !ok {
				return n, false
			}
			n.Lists[i][j] = node.(ExprNode)
		}
	}
	for i, val := range n.OnDuplicate {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.OnDuplicate[i] = node.(*Assignment)
	}
	return v.Leave(n)
}

// WhereExpr implements ShardableDMLStmt interface.
func (n *InsertStmt) WhereExpr() ExprNode {
	if n.Select == nil {
		return nil
	}
	s, ok := n.Select.(*SelectStmt)
	if !ok {
		return nil
	}
	return s.Where
}

// SetWhereExpr implements ShardableDMLStmt interface.
func (n *InsertStmt) SetWhereExpr(e ExprNode) {
	if n.Select == nil {
		return
	}
	s, ok := n.Select.(*SelectStmt)
	if !ok {
		return
	}
	s.Where = e
}

// TableRefsJoin implements ShardableDMLStmt interface.
func (n *InsertStmt) TableRefsJoin() (*Join, bool) {
	if n.Select == nil {
		return nil, false
	}
	s, ok := n.Select.(*SelectStmt)
	if !ok {
		return nil, false
	}
	return s.From.TableRefs, true
}

// DeleteStmt is a statement to delete rows from table.
// See https://dev.mysql.com/doc/refman/5.7/en/delete.html
type DeleteStmt struct {
	dmlNode

	// TableRefs is used in both single table and multiple table delete statement.
	TableRefs *TableRefsClause
	// Tables is only used in multiple table delete statement.
	Tables       *DeleteTableList
	Where        ExprNode
	Order        *OrderByClause
	Limit        *Limit
	Priority     mysql.PriorityEnum
	IgnoreErr    bool
	Quick        bool
	IsMultiTable bool
	BeforeFrom   bool
	// TableHints represents the table level Optimizer Hint for join type.
	TableHints []*TableOptimizerHint
	With       *WithClause
}

// Restore implements Node interface.
func (n *DeleteStmt) Restore(ctx *format.RestoreCtx) error {
	if n.With != nil {
		defer ctx.RestoreCTEFunc()()
		err := n.With.Restore(ctx)
		if err != nil {
			return err
		}
	}

	ctx.WriteKeyWord("DELETE ")

	if len(n.TableHints) != 0 {
		ctx.WritePlain("/*+ ")
		for i, tableHint := range n.TableHints {
			if i != 0 {
				ctx.WritePlain(" ")
			}
			if err := tableHint.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore UpdateStmt.TableHints[%d]: %w", i, err)
			}
		}
		ctx.WritePlain("*/ ")
	}

	if err := n.Priority.Restore(ctx); err != nil {
		return err
	}
	if n.Priority != mysql.NoPriority {
		ctx.WritePlain(" ")
	}
	if n.Quick {
		ctx.WriteKeyWord("QUICK ")
	}
	if n.IgnoreErr {
		ctx.WriteKeyWord("IGNORE ")
	}

	if n.IsMultiTable { // Multiple-Table Syntax
		if n.BeforeFrom {
			if err := n.Tables.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore DeleteStmt.Tables: %w", err)
			}

			ctx.WriteKeyWord(" FROM ")
			if err := n.TableRefs.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore DeleteStmt.TableRefs: %w", err)
			}
		} else {
			ctx.WriteKeyWord("FROM ")
			if err := n.Tables.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore DeleteStmt.Tables: %w", err)
			}

			ctx.WriteKeyWord(" USING ")
			if err := n.TableRefs.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore DeleteStmt.TableRefs: %w", err)
			}
		}
	} else { // Single-Table Syntax
		ctx.WriteKeyWord("FROM ")

		if err := n.TableRefs.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore DeleteStmt.TableRefs: %w", err)
		}
	}

	if n.Where != nil {
		ctx.WriteKeyWord(" WHERE ")
		if err := n.Where.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore DeleteStmt.Where: %w", err)
		}
	}

	if n.Order != nil {
		ctx.WritePlain(" ")
		if err := n.Order.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore DeleteStmt.Order: %w", err)
		}
	}

	if n.Limit != nil {
		ctx.WritePlain(" ")
		if err := n.Limit.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore DeleteStmt.Limit: %w", err)
		}
	}

	return nil
}

// Accept implements Node Accept interface.
func (n *DeleteStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	n = newNode.(*DeleteStmt)
	if n.With != nil {
		node, ok := n.With.Accept(v)
		if !ok {
			return n, false
		}
		n.With = node.(*WithClause)
	}
	node, ok := n.TableRefs.Accept(v)
	if !ok {
		return n, false
	}
	n.TableRefs = node.(*TableRefsClause)

	if n.Tables != nil {
		node, ok = n.Tables.Accept(v)
		if !ok {
			return n, false
		}
		n.Tables = node.(*DeleteTableList)
	}

	if n.Where != nil {
		node, ok = n.Where.Accept(v)
		if !ok {
			return n, false
		}
		n.Where = node.(ExprNode)
	}
	if n.Order != nil {
		node, ok = n.Order.Accept(v)
		if !ok {
			return n, false
		}
		n.Order = node.(*OrderByClause)
	}
	if n.Limit != nil {
		node, ok = n.Limit.Accept(v)
		if !ok {
			return n, false
		}
		n.Limit = node.(*Limit)
	}
	return v.Leave(n)
}

// WhereExpr implements ShardableDMLStmt interface.
func (n *DeleteStmt) WhereExpr() ExprNode {
	return n.Where
}

// SetWhereExpr implements ShardableDMLStmt interface.
func (n *DeleteStmt) SetWhereExpr(e ExprNode) {
	n.Where = e
}

// TableRefsJoin implements ShardableDMLStmt interface.
func (n *DeleteStmt) TableRefsJoin() (*Join, bool) {
	return n.TableRefs.TableRefs, true
}

// UpdateStmt is a statement to update columns of existing rows in tables with new values.
// See https://dev.mysql.com/doc/refman/5.7/en/update.html
type UpdateStmt struct {
	dmlNode

	TableRefs     *TableRefsClause
	List          []*Assignment
	Where         ExprNode
	Order         *OrderByClause
	Limit         *Limit
	Priority      mysql.PriorityEnum
	IgnoreErr     bool
	MultipleTable bool
	TableHints    []*TableOptimizerHint
	With          *WithClause
}

// Restore implements Node interface.
func (n *UpdateStmt) Restore(ctx *format.RestoreCtx) error {
	if n.With != nil {
		defer ctx.RestoreCTEFunc()()
		err := n.With.Restore(ctx)
		if err != nil {
			return err
		}
	}

	ctx.WriteKeyWord("UPDATE ")

	if len(n.TableHints) != 0 {
		ctx.WritePlain("/*+ ")
		for i, tableHint := range n.TableHints {
			if i != 0 {
				ctx.WritePlain(" ")
			}
			if err := tableHint.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore UpdateStmt.TableHints[%d]: %w", i, err)
			}
		}
		ctx.WritePlain("*/ ")
	}

	if err := n.Priority.Restore(ctx); err != nil {
		return err
	}
	if n.Priority != mysql.NoPriority {
		ctx.WritePlain(" ")
	}
	if n.IgnoreErr {
		ctx.WriteKeyWord("IGNORE ")
	}

	if err := n.TableRefs.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore UpdateStmt.TableRefs: %w", err)
	}

	ctx.WriteKeyWord(" SET ")
	for i, assignment := range n.List {
		if i != 0 {
			ctx.WritePlain(", ")
		}

		if err := assignment.Column.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore UpdateStmt.List[%d].Column: %w", i, err)
		}

		ctx.WritePlain("=")

		if err := assignment.Expr.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore UpdateStmt.List[%d].Expr: %w", i, err)
		}
	}

	if n.Where != nil {
		ctx.WriteKeyWord(" WHERE ")
		if err := n.Where.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore UpdateStmt.Where: %w", err)
		}
	}

	if n.Order != nil {
		ctx.WritePlain(" ")
		if err := n.Order.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore UpdateStmt.Order: %w", err)
		}
	}

	if n.Limit != nil {
		ctx.WritePlain(" ")
		if err := n.Limit.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore UpdateStmt.Limit: %w", err)
		}
	}

	return nil
}

// Accept implements Node Accept interface.
func (n *UpdateStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*UpdateStmt)
	if n.With != nil {
		node, ok := n.With.Accept(v)
		if !ok {
			return n, false
		}
		n.With = node.(*WithClause)
	}
	node, ok := n.TableRefs.Accept(v)
	if !ok {
		return n, false
	}
	n.TableRefs = node.(*TableRefsClause)
	for i, val := range n.List {
		node, ok = val.Accept(v)
		if !ok {
			return n, false
		}
		n.List[i] = node.(*Assignment)
	}
	if n.Where != nil {
		node, ok = n.Where.Accept(v)
		if !ok {
			return n, false
		}
		n.Where = node.(ExprNode)
	}
	if n.Order != nil {
		node, ok = n.Order.Accept(v)
		if !ok {
			return n, false
		}
		n.Order = node.(*OrderByClause)
	}
	if n.Limit != nil {
		node, ok = n.Limit.Accept(v)
		if !ok {
			return n, false
		}
		n.Limit = node.(*Limit)
	}
	return v.Leave(n)
}

// WhereExpr implements ShardableDMLStmt interface.
func (n *UpdateStmt) WhereExpr() ExprNode {
	return n.Where
}

// SetWhereExpr implements ShardableDMLStmt interface.
func (n *UpdateStmt) SetWhereExpr(e ExprNode) {
	n.Where = e
}

// TableRefsJoin implements ShardableDMLStmt interface.
func (n *UpdateStmt) TableRefsJoin() (*Join, bool) {
	return n.TableRefs.TableRefs, true
}

// Limit is the limit clause.
type Limit struct {
	node

	Count  ExprNode
	Offset ExprNode
}

// Restore implements Node interface.
func (n *Limit) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("LIMIT ")
	if n.Offset != nil {
		if err := n.Offset.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore Limit.Offset: %w", err)
		}
		ctx.WritePlain(",")
	}
	if err := n.Count.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore Limit.Count: %w", err)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *Limit) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	if n.Count != nil {
		node, ok := n.Count.Accept(v)
		if !ok {
			return n, false
		}
		n.Count = node.(ExprNode)
	}
	if n.Offset != nil {
		node, ok := n.Offset.Accept(v)
		if !ok {
			return n, false
		}
		n.Offset = node.(ExprNode)
	}

	n = newNode.(*Limit)
	return v.Leave(n)
}

// ShowStmtType is the type for SHOW statement.
type ShowStmtType int

// Show statement types.
const (
	ShowNone = iota
	ShowEngines
	ShowDatabases
	ShowTables
	ShowTableStatus
	ShowColumns
	ShowWarnings
	ShowCharset
	ShowVariables
	ShowStatus
	ShowCollation
	ShowCreateTable
	ShowCreateView
	ShowCreateUser
	ShowGrants
	ShowTriggers
	ShowIndex
	ShowProcessList
	ShowCreateDatabase
	ShowEvents
	ShowPlugins
	ShowProfile
	ShowProfiles
	ShowMasterStatus
	ShowPrivileges
	ShowErrors
	ShowOpenTables
	ShowBinlogStatus
	ShowReplicaStatus
	ShowBinaryLogs
	ShowBinlogEvents
	ShowReplicas
	ShowCreateProcedure
	ShowCreateFunction
	ShowCreateTrigger
	ShowCreateEvent
	ShowCreateLibrary
	ShowProcedureStatus
	ShowFunctionStatus
	ShowLibraryStatus
	ShowProcedureCode
	ShowFunctionCode
	ShowParseTree
	ShowEngineStatus
	ShowEngineLogs
	ShowEngineMutex
	ShowRelaylogEvents
)

const (
	ProfileTypeInvalid = iota
	ProfileTypeCPU
	ProfileTypeMemory
	ProfileTypeBlockIo
	ProfileTypeContextSwitch
	ProfileTypePageFaults
	ProfileTypeIpc
	ProfileTypeSwaps
	ProfileTypeSource
	ProfileTypeAll
)

// ShowStmt is a statement to provide information about databases, tables, columns and so on.
// See https://dev.mysql.com/doc/refman/5.7/en/show.html
type ShowStmt struct {
	dmlNode

	Tp          ShowStmtType // Databases/Tables/Columns/....
	DBName      string
	EngineName  string      // Used for SHOW ENGINE <engine> {STATUS|LOGS|MUTEX}.
	Table       *TableName  // Used for showing columns.
	Partition   CIStr       // Used for showing partition.
	Column      *ColumnName // Used for `desc table column`.
	IndexName   CIStr
	Flag        int // Some flag parsed from sql, such as FULL.
	Full        bool
	User        *auth.UserIdentity   // Used for show grants/create user.
	Roles       []*auth.RoleIdentity // Used for show grants .. using
	IfNotExists bool                 // Used for `show create database if not exists`
	Extended    bool                 // Used for `show extended columns from ...`
	Limit       *Limit               // Used for partial Show STMTs to limit Result Set row numbers.

	CountWarningsOrErrors bool // Used for showing count(*) warnings | errors

	// GlobalScope is used by `show variables`
	GlobalScope bool
	Pattern     *PatternLikeExpr
	Where       ExprNode

	ShowProfileTypes []int  // Used for `SHOW PROFILE` syntax
	ShowProfileArgs  *int64 // Used for `SHOW PROFILE` syntax
	ShowProfileLimit *Limit // Used for `SHOW PROFILE` syntax

	LogName    string // Used for `SHOW BINLOG EVENTS IN 'name'`
	Pos        uint64 // Used for `SHOW BINLOG EVENTS ... FROM pos`
	HasPos     bool
	Channel    string // Used for `SHOW REPLICA STATUS FOR CHANNEL 'name'`
	HasChannel bool
	// ParseTreeStmt is the statement whose parse tree is requested by
	// `SHOW PARSE_TREE <stmt>` (available in debug builds of MySQL).
	ParseTreeStmt StmtNode
}

// Restore implements Node interface.
func (n *ShowStmt) Restore(ctx *format.RestoreCtx) error {
	restoreOptFull := func() {
		if n.Full {
			ctx.WriteKeyWord("FULL ")
		}
	}
	restoreShowDatabaseNameOpt := func() {
		if n.DBName != "" {
			// FROM OR IN
			ctx.WriteKeyWord(" IN ")
			ctx.WriteName(n.DBName)
		}
	}
	restoreGlobalScope := func() {
		if n.GlobalScope {
			ctx.WriteKeyWord("GLOBAL ")
		} else {
			ctx.WriteKeyWord("SESSION ")
		}
	}
	restoreShowLikeOrWhereOpt := func() error {
		if n.Pattern != nil && n.Pattern.Pattern != nil {
			ctx.WriteKeyWord(" LIKE ")
			if err := n.Pattern.Pattern.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore ShowStmt.Pattern: %w", err)
			}
		} else if n.Where != nil {
			ctx.WriteKeyWord(" WHERE ")
			if err := n.Where.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore ShowStmt.Where: %w", err)
			}
		}
		return nil
	}

	ctx.WriteKeyWord("SHOW ")
	switch n.Tp {
	case ShowBinlogStatus:
		ctx.WriteKeyWord("BINARY LOG STATUS")
	case ShowBinaryLogs:
		ctx.WriteKeyWord("BINARY LOGS")
	case ShowBinlogEvents, ShowRelaylogEvents:
		if n.Tp == ShowBinlogEvents {
			ctx.WriteKeyWord("BINLOG EVENTS")
		} else {
			ctx.WriteKeyWord("RELAYLOG EVENTS")
		}
		if n.LogName != "" {
			ctx.WriteKeyWord(" IN ")
			ctx.WriteString(n.LogName)
		}
		if n.HasPos {
			ctx.WriteKeyWord(" FROM ")
			ctx.WritePlainf("%d", n.Pos)
		}
		if n.Limit != nil {
			ctx.WritePlain(" ")
			if err := n.Limit.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore ShowStmt.Limit: %w", err)
			}
		}
	case ShowReplicas:
		ctx.WriteKeyWord("REPLICAS")
	case ShowCreateProcedure:
		ctx.WriteKeyWord("CREATE PROCEDURE ")
		if err := n.Table.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ShowStmt.Table: %w", err)
		}
	case ShowCreateFunction:
		ctx.WriteKeyWord("CREATE FUNCTION ")
		if err := n.Table.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ShowStmt.Table: %w", err)
		}
	case ShowCreateTrigger:
		ctx.WriteKeyWord("CREATE TRIGGER ")
		if err := n.Table.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ShowStmt.Table: %w", err)
		}
	case ShowCreateEvent:
		ctx.WriteKeyWord("CREATE EVENT ")
		if err := n.Table.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ShowStmt.Table: %w", err)
		}
	case ShowCreateLibrary:
		ctx.WriteKeyWord("CREATE LIBRARY ")
		if err := n.Table.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ShowStmt.Table: %w", err)
		}
	case ShowProcedureCode:
		ctx.WriteKeyWord("PROCEDURE CODE ")
		if err := n.Table.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ShowStmt.Table: %w", err)
		}
	case ShowFunctionCode:
		ctx.WriteKeyWord("FUNCTION CODE ")
		if err := n.Table.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ShowStmt.Table: %w", err)
		}
	case ShowParseTree:
		ctx.WriteKeyWord("PARSE_TREE ")
		if err := n.ParseTreeStmt.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ShowStmt.ParseTreeStmt: %w", err)
		}
	case ShowCreateTable:
		ctx.WriteKeyWord("CREATE TABLE ")
		if err := n.Table.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ShowStmt.Table: %w", err)
		}
	case ShowCreateView:
		ctx.WriteKeyWord("CREATE VIEW ")
		if err := n.Table.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ShowStmt.VIEW: %w", err)
		}
	case ShowCreateDatabase:
		ctx.WriteKeyWord("CREATE DATABASE ")
		if n.IfNotExists {
			ctx.WriteKeyWord("IF NOT EXISTS ")
		}
		ctx.WriteName(n.DBName)
	case ShowCreateUser:
		ctx.WriteKeyWord("CREATE USER ")
		if err := n.User.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ShowStmt.User: %w", err)
		}
	case ShowGrants:
		ctx.WriteKeyWord("GRANTS")
		if n.User != nil {
			ctx.WriteKeyWord(" FOR ")
			if err := n.User.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore ShowStmt.User: %w", err)
			}
		}
		if n.Roles != nil {
			ctx.WriteKeyWord(" USING ")
			for i, r := range n.Roles {
				if err := r.Restore(ctx); err != nil {
					return fmt.Errorf("an error occurred while restore ShowStmt.User: %w", err)
				}
				if i != len(n.Roles)-1 {
					ctx.WritePlain(", ")
				}
			}
		}
	case ShowMasterStatus:
		ctx.WriteKeyWord("MASTER STATUS")
	case ShowProcessList:
		restoreOptFull()
		ctx.WriteKeyWord("PROCESSLIST")
	case ShowProfiles:
		ctx.WriteKeyWord("PROFILES")
	case ShowProfile:
		ctx.WriteKeyWord("PROFILE")
		if len(n.ShowProfileTypes) > 0 {
			for i, tp := range n.ShowProfileTypes {
				if i != 0 {
					ctx.WritePlain(",")
				}
				ctx.WritePlain(" ")
				switch tp {
				case ProfileTypeCPU:
					ctx.WriteKeyWord("CPU")
				case ProfileTypeMemory:
					ctx.WriteKeyWord("MEMORY")
				case ProfileTypeBlockIo:
					ctx.WriteKeyWord("BLOCK IO")
				case ProfileTypeContextSwitch:
					ctx.WriteKeyWord("CONTEXT SWITCHES")
				case ProfileTypeIpc:
					ctx.WriteKeyWord("IPC")
				case ProfileTypePageFaults:
					ctx.WriteKeyWord("PAGE FAULTS")
				case ProfileTypeSource:
					ctx.WriteKeyWord("SOURCE")
				case ProfileTypeSwaps:
					ctx.WriteKeyWord("SWAPS")
				case ProfileTypeAll:
					ctx.WriteKeyWord("ALL")
				}
			}
		}
		if n.ShowProfileArgs != nil {
			ctx.WriteKeyWord(" FOR QUERY ")
			ctx.WritePlainf("%d", *n.ShowProfileArgs)
		}
		if n.ShowProfileLimit != nil {
			ctx.WritePlain(" ")
			if err := n.ShowProfileLimit.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore ShowStmt.WritePlain: %w", err)
			}
		}

	case ShowPrivileges:
		ctx.WriteKeyWord("PRIVILEGES")
	default:
		switch n.Tp {
		case ShowEngines:
			ctx.WriteKeyWord("ENGINES")
		case ShowEngineStatus:
			ctx.WriteKeyWord("ENGINE ")
			ctx.WriteName(n.EngineName)
			ctx.WriteKeyWord(" STATUS")
		case ShowEngineLogs:
			ctx.WriteKeyWord("ENGINE ")
			ctx.WriteName(n.EngineName)
			ctx.WriteKeyWord(" LOGS")
		case ShowEngineMutex:
			ctx.WriteKeyWord("ENGINE ")
			ctx.WriteName(n.EngineName)
			ctx.WriteKeyWord(" MUTEX")
		case ShowDatabases:
			ctx.WriteKeyWord("DATABASES")
		case ShowCharset:
			ctx.WriteKeyWord("CHARSET")
		case ShowTables:
			if n.Extended {
				ctx.WriteKeyWord("EXTENDED ")
			}
			restoreOptFull()
			ctx.WriteKeyWord("TABLES")
			restoreShowDatabaseNameOpt()
		case ShowOpenTables:
			ctx.WriteKeyWord("OPEN TABLES")
			restoreShowDatabaseNameOpt()
		case ShowTableStatus:
			ctx.WriteKeyWord("TABLE STATUS")
			restoreShowDatabaseNameOpt()
		case ShowIndex:
			// here can be INDEX INDEXES KEYS
			// FROM or IN
			if n.Extended {
				ctx.WriteKeyWord("EXTENDED ")
			}
			ctx.WriteKeyWord("INDEX IN ")
			if err := n.Table.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore ShowStmt.Table: %w", err)
			} // TODO: remember to check this case
		case ShowColumns: // equivalent to SHOW FIELDS
			if n.Extended {
				ctx.WriteKeyWord("EXTENDED ")
			}
			restoreOptFull()
			ctx.WriteKeyWord("COLUMNS")
			if n.Table != nil {
				// FROM or IN
				ctx.WriteKeyWord(" IN ")
				if err := n.Table.Restore(ctx); err != nil {
					return fmt.Errorf("an error occurred while restore ShowStmt.Table: %w", err)
				}
			}
			restoreShowDatabaseNameOpt()
		case ShowWarnings:
			if n.CountWarningsOrErrors {
				ctx.WriteKeyWord("COUNT(*) WARNINGS")
			} else {
				ctx.WriteKeyWord("WARNINGS")
				if n.Limit != nil {
					ctx.WritePlain(" ")
					if err := n.Limit.Restore(ctx); err != nil {
						return fmt.Errorf("an error occurred while restore ShowStmt.Limit: %w", err)
					}
				}
			}
		case ShowErrors:
			if n.CountWarningsOrErrors {
				ctx.WriteKeyWord("COUNT(*) ERRORS")
			} else {
				ctx.WriteKeyWord("ERRORS")
				if n.Limit != nil {
					ctx.WritePlain(" ")
					if err := n.Limit.Restore(ctx); err != nil {
						return fmt.Errorf("an error occurred while restore ShowStmt.Limit: %w", err)
					}
				}
			}
		case ShowVariables:
			restoreGlobalScope()
			ctx.WriteKeyWord("VARIABLES")
		case ShowStatus:
			restoreGlobalScope()
			ctx.WriteKeyWord("STATUS")
		case ShowCollation:
			ctx.WriteKeyWord("COLLATION")
		case ShowTriggers:
			ctx.WriteKeyWord("TRIGGERS")
			restoreShowDatabaseNameOpt()
		case ShowEvents:
			ctx.WriteKeyWord("EVENTS")
			restoreShowDatabaseNameOpt()
		case ShowPlugins:
			ctx.WriteKeyWord("PLUGINS")
		case ShowProcedureStatus:
			ctx.WriteKeyWord("PROCEDURE STATUS")
		case ShowFunctionStatus:
			ctx.WriteKeyWord("FUNCTION STATUS")
		case ShowLibraryStatus:
			ctx.WriteKeyWord("LIBRARY STATUS")
		case ShowReplicaStatus:
			ctx.WriteKeyWord("REPLICA STATUS")
			if n.HasChannel {
				ctx.WriteKeyWord(" FOR CHANNEL ")
				ctx.WriteString(n.Channel)
			}
		default:
			return errors.New("unknown ShowStmt type")
		}
		if err := restoreShowLikeOrWhereOpt(); err != nil {
			return err
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ShowStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ShowStmt)
	if n.Table != nil {
		node, ok := n.Table.Accept(v)
		if !ok {
			return n, false
		}
		n.Table = node.(*TableName)
	}
	if n.Column != nil {
		node, ok := n.Column.Accept(v)
		if !ok {
			return n, false
		}
		n.Column = node.(*ColumnName)
	}
	if n.Pattern != nil {
		node, ok := n.Pattern.Accept(v)
		if !ok {
			return n, false
		}
		n.Pattern = node.(*PatternLikeExpr)
	}

	if n.Where != nil {
		node, ok := n.Where.Accept(v)
		if !ok {
			return n, false
		}
		n.Where = node.(ExprNode)
	}
	if n.Limit != nil {
		node, ok := n.Limit.Accept(v)
		if !ok {
			return n, false
		}
		n.Limit = node.(*Limit)
	}
	if n.ParseTreeStmt != nil {
		node, ok := n.ParseTreeStmt.Accept(v)
		if !ok {
			return n, false
		}
		n.ParseTreeStmt = node.(StmtNode)
	}
	return v.Leave(n)
}

// WindowSpec is the specification of a window.
type WindowSpec struct {
	node

	Name CIStr
	// Ref is the reference window of this specification. For example, in `w2 as (w1 order by a)`,
	// the definition of `w2` references `w1`.
	Ref CIStr

	PartitionBy *PartitionByClause
	OrderBy     *OrderByClause
	Frame       *FrameClause

	// OnlyAlias will set to true of the first following case.
	// To make compatible with MySQL, we need to distinguish `select func over w` from `select func over (w)`.
	OnlyAlias bool
}

// Restore implements Node interface.
func (n *WindowSpec) Restore(ctx *format.RestoreCtx) error {
	if name := n.Name.String(); name != "" {
		ctx.WriteName(name)
		if n.OnlyAlias {
			return nil
		}
		ctx.WriteKeyWord(" AS ")
	}
	ctx.WritePlain("(")
	sep := ""
	if refName := n.Ref.String(); refName != "" {
		ctx.WriteName(refName)
		sep = " "
	}
	if n.PartitionBy != nil {
		ctx.WritePlain(sep)
		if err := n.PartitionBy.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore WindowSpec.PartitionBy: %w", err)
		}
		sep = " "
	}
	if n.OrderBy != nil {
		ctx.WritePlain(sep)
		if err := n.OrderBy.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore WindowSpec.OrderBy: %w", err)
		}
		sep = " "
	}
	if n.Frame != nil {
		ctx.WritePlain(sep)
		if err := n.Frame.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore WindowSpec.Frame: %w", err)
		}
	}
	ctx.WritePlain(")")

	return nil
}

// Accept implements Node Accept interface.
func (n *WindowSpec) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*WindowSpec)
	if n.PartitionBy != nil {
		node, ok := n.PartitionBy.Accept(v)
		if !ok {
			return n, false
		}
		n.PartitionBy = node.(*PartitionByClause)
	}
	if n.OrderBy != nil {
		node, ok := n.OrderBy.Accept(v)
		if !ok {
			return n, false
		}
		n.OrderBy = node.(*OrderByClause)
	}
	if n.Frame != nil {
		node, ok := n.Frame.Accept(v)
		if !ok {
			return n, false
		}
		n.Frame = node.(*FrameClause)
	}
	return v.Leave(n)
}

type SelectIntoType int

const (
	SelectIntoOutfile SelectIntoType = iota + 1
	SelectIntoDumpfile
	SelectIntoVars
)

type SelectIntoOption struct {
	node

	Tp       SelectIntoType
	FileName string
	// Charset is the optional CHARACTER SET clause of INTO OUTFILE.
	Charset    string
	FieldsInfo *FieldsClause
	LinesInfo  *LinesClause
	// Variables is the user variable list of SELECT ... INTO @var [, @var] ...
	Variables []ExprNode
}

// Restore implements Node interface.
func (n *SelectIntoOption) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp {
	case SelectIntoOutfile:
		ctx.WriteKeyWord("INTO OUTFILE ")
		ctx.WriteString(n.FileName)
		if n.Charset != "" {
			ctx.WriteKeyWord(" CHARACTER SET ")
			ctx.WriteKeyWord(n.Charset)
		}
		if n.FieldsInfo != nil {
			if err := n.FieldsInfo.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore SelectInto.FieldsInfo: %w", err)
			}
		}
		if n.LinesInfo != nil {
			if err := n.LinesInfo.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore SelectInto.LinesInfo: %w", err)
			}
		}
	case SelectIntoDumpfile:
		ctx.WriteKeyWord("INTO DUMPFILE ")
		ctx.WriteString(n.FileName)
	case SelectIntoVars:
		ctx.WriteKeyWord("INTO ")
		for i, v := range n.Variables {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := v.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore SelectInto.Variables[%d]: %w", i, err)
			}
		}
	default:
		return errors.New("unsupported SelectionInto type")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *SelectIntoOption) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SelectIntoOption)
	for i, val := range n.Variables {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Variables[i] = node.(ExprNode)
	}
	return v.Leave(n)
}

// PartitionByClause represents partition by clause.
type PartitionByClause struct {
	node

	Items []*ByItem
}

// Restore implements Node interface.
func (n *PartitionByClause) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("PARTITION BY ")
	for i, v := range n.Items {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore PartitionByClause.Items[%d]: %w", i, err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *PartitionByClause) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*PartitionByClause)
	for i, val := range n.Items {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Items[i] = node.(*ByItem)
	}
	return v.Leave(n)
}

// FrameType is the type of window function frame.
type FrameType int

// Window function frame types.
// MySQL only supports `ROWS` and `RANGES`.
const (
	Rows = iota
	Ranges
	Groups
)

// FrameClause represents frame clause.
type FrameClause struct {
	node

	Type   FrameType
	Extent FrameExtent
}

// Restore implements Node interface.
func (n *FrameClause) Restore(ctx *format.RestoreCtx) error {
	switch n.Type {
	case Rows:
		ctx.WriteKeyWord("ROWS")
	case Ranges:
		ctx.WriteKeyWord("RANGE")
	default:
		return errors.New("unsupported window function frame type")
	}
	ctx.WriteKeyWord(" BETWEEN ")
	if err := n.Extent.Start.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore FrameClause.Extent.Start: %w", err)
	}
	ctx.WriteKeyWord(" AND ")
	if err := n.Extent.End.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore FrameClause.Extent.End: %w", err)
	}

	return nil
}

// Accept implements Node Accept interface.
func (n *FrameClause) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FrameClause)
	node, ok := n.Extent.Start.Accept(v)
	if !ok {
		return n, false
	}
	n.Extent.Start = *node.(*FrameBound)
	node, ok = n.Extent.End.Accept(v)
	if !ok {
		return n, false
	}
	n.Extent.End = *node.(*FrameBound)
	return v.Leave(n)
}

// FrameExtent represents frame extent.
type FrameExtent struct {
	Start FrameBound
	End   FrameBound
}

// FrameType is the type of window function frame bound.
type BoundType int

// Frame bound types.
const (
	Following = iota
	Preceding
	CurrentRow
)

// FrameBound represents frame bound.
type FrameBound struct {
	node

	Type      BoundType
	UnBounded bool
	Expr      ExprNode
	// `Unit` is used to indicate the units in which the `Expr` should be interpreted.
	// For example: '2:30' MINUTE_SECOND.
	Unit TimeUnitType
}

// Restore implements Node interface.
func (n *FrameBound) Restore(ctx *format.RestoreCtx) error {
	if n.UnBounded {
		ctx.WriteKeyWord("UNBOUNDED")
	}
	switch n.Type {
	case CurrentRow:
		ctx.WriteKeyWord("CURRENT ROW")
	case Preceding, Following:
		if n.Unit != TimeUnitInvalid {
			ctx.WriteKeyWord("INTERVAL ")
		}
		if n.Expr != nil {
			if err := n.Expr.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore FrameBound.Expr: %w", err)
			}
		}
		if n.Unit != TimeUnitInvalid {
			ctx.WritePlain(" ")
			ctx.WriteKeyWord(n.Unit.String())
		}
		if n.Type == Preceding {
			ctx.WriteKeyWord(" PRECEDING")
		} else {
			ctx.WriteKeyWord(" FOLLOWING")
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *FrameBound) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FrameBound)
	if n.Expr != nil {
		node, ok := n.Expr.Accept(v)
		if !ok {
			return n, false
		}
		n.Expr = node.(ExprNode)
	}
	return v.Leave(n)
}

type FulltextSearchModifier int

const (
	FulltextSearchModifierNaturalLanguageMode = 0
	FulltextSearchModifierBooleanMode         = 1
	FulltextSearchModifierModeMask            = 0xF
	FulltextSearchModifierWithQueryExpansion  = 1 << 4
)

func (m FulltextSearchModifier) IsBooleanMode() bool {
	return m&FulltextSearchModifierModeMask == FulltextSearchModifierBooleanMode
}

func (m FulltextSearchModifier) IsNaturalLanguageMode() bool {
	return m&FulltextSearchModifierModeMask == FulltextSearchModifierNaturalLanguageMode
}

func (m FulltextSearchModifier) WithQueryExpansion() bool {
	return m&FulltextSearchModifierWithQueryExpansion == FulltextSearchModifierWithQueryExpansion
}

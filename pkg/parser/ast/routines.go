// Copyright 2026 Block, Inc.
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

// Package ast: stored program statements (procedures, functions, triggers,
// events) and the compound statement language used in their bodies.
// See https://dev.mysql.com/doc/refman/8.4/en/sql-compound-statements.html

package ast

import (
	"fmt"

	"github.com/block/spirit/pkg/parser/auth"
	"github.com/block/spirit/pkg/parser/format"
	"github.com/block/spirit/pkg/parser/types"
)

var (
	_ DDLNode = &CreateProcedureStmt{}
	_ DDLNode = &CreateFunctionStmt{}
	_ DDLNode = &CreateLoadableFunctionStmt{}
	_ DDLNode = &CreateTriggerStmt{}
	_ DDLNode = &CreateEventStmt{}
	_ DDLNode = &AlterProcedureStmt{}
	_ DDLNode = &AlterFunctionStmt{}
	_ DDLNode = &AlterEventStmt{}
	_ DDLNode = &DropRoutineStmt{}

	_ StmtNode = &BeginEndStmt{}
	_ StmtNode = &DeclareVarStmt{}
	_ StmtNode = &DeclareConditionStmt{}
	_ StmtNode = &DeclareCursorStmt{}
	_ StmtNode = &DeclareHandlerStmt{}
	_ StmtNode = &ProcIfStmt{}
	_ StmtNode = &ProcCaseStmt{}
	_ StmtNode = &WhileStmt{}
	_ StmtNode = &RepeatStmt{}
	_ StmtNode = &LoopStmt{}
	_ StmtNode = &IterateStmt{}
	_ StmtNode = &LeaveStmt{}
	_ StmtNode = &ReturnStmt{}
	_ StmtNode = &OpenCursorStmt{}
	_ StmtNode = &FetchCursorStmt{}
	_ StmtNode = &CloseCursorStmt{}
	_ StmtNode = &SignalStmt{}
	_ StmtNode = &ResignalStmt{}
)

// restoreDefinerOpt writes "DEFINER = user " when the definer differs from
// what an absent clause parses to (the current user).
func restoreDefinerOpt(ctx *format.RestoreCtx, definer *auth.UserIdentity) {
	if definer == nil || definer.CurrentUser {
		return
	}
	ctx.WriteKeyWord("DEFINER")
	ctx.WritePlain(" = ")
	ctx.WriteName(definer.Username)
	if definer.Hostname != "" {
		ctx.WritePlain("@")
		ctx.WriteName(definer.Hostname)
	}
	ctx.WritePlain(" ")
}

// restoreStmtList writes each statement of a compound body followed by "; ".
func restoreStmtList(ctx *format.RestoreCtx, stmts []StmtNode, what string) error {
	for i, s := range stmts {
		if err := s.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore %s[%d]: %w", what, i, err)
		}
		ctx.WritePlain("; ")
	}
	return nil
}

// acceptStmtList visits each statement of a compound body.
func acceptStmtList(v Visitor, stmts []StmtNode) bool {
	for i, s := range stmts {
		node, ok := s.Accept(v)
		if !ok {
			return false
		}
		stmts[i] = node.(StmtNode)
	}
	return true
}

// RoutineParamDirection is the parameter mode of a stored procedure
// parameter.
type RoutineParamDirection int

// RoutineParamDirection values.
const (
	RoutineParamIn RoutineParamDirection = iota
	RoutineParamOut
	RoutineParamInOut
)

// RoutineParam is one parameter of a stored procedure or function.
type RoutineParam struct {
	Name      CIStr
	Direction RoutineParamDirection // procedures only; functions are always IN
	Type      *types.FieldType
}

// Restore implements Node interface.
func (n *RoutineParam) Restore(ctx *format.RestoreCtx, withDirection bool) error {
	if withDirection {
		switch n.Direction { //nolint:exhaustive // RoutineParamIn is the default branch
		case RoutineParamOut:
			ctx.WriteKeyWord("OUT ")
		case RoutineParamInOut:
			ctx.WriteKeyWord("INOUT ")
		default:
			ctx.WriteKeyWord("IN ")
		}
	}
	ctx.WriteName(n.Name.O)
	ctx.WritePlain(" ")
	if err := n.Type.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore RoutineParam.Type: %w", err)
	}
	return nil
}

func restoreRoutineParams(ctx *format.RestoreCtx, params []*RoutineParam, withDirection bool) error {
	ctx.WritePlain("(")
	for i, p := range params {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := p.Restore(ctx, withDirection); err != nil {
			return err
		}
	}
	ctx.WritePlain(")")
	return nil
}

// RoutineLibrary is one library reference of a USING clause.
type RoutineLibrary struct {
	Name  *TableName
	Alias CIStr
}

// RoutineOptionType is the kind of a routine characteristic.
type RoutineOptionType int

// RoutineOptionType values.
const (
	RoutineOptionComment RoutineOptionType = iota
	RoutineOptionLanguageSQL
	RoutineOptionLanguage
	RoutineOptionDeterministic
	RoutineOptionNotDeterministic
	RoutineOptionContainsSQL
	RoutineOptionNoSQL
	RoutineOptionReadsSQLData
	RoutineOptionModifiesSQLData
	RoutineOptionSecurityDefiner
	RoutineOptionSecurityInvoker
	RoutineOptionUsing
	RoutineOptionDropComment
)

// RoutineOption is one characteristic of a CREATE/ALTER routine statement.
// The parsed order is preserved so statements restore in their written form.
type RoutineOption struct {
	Tp        RoutineOptionType
	StrValue  string            // COMMENT text or LANGUAGE name
	Libraries []*RoutineLibrary // USING (...) entries
}

// Restore implements Node interface.
func (n *RoutineOption) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp {
	case RoutineOptionComment:
		ctx.WriteKeyWord("COMMENT ")
		ctx.WriteString(n.StrValue)
	case RoutineOptionLanguageSQL:
		ctx.WriteKeyWord("LANGUAGE SQL")
	case RoutineOptionLanguage:
		ctx.WriteKeyWord("LANGUAGE ")
		ctx.WriteKeyWord(n.StrValue)
	case RoutineOptionDeterministic:
		ctx.WriteKeyWord("DETERMINISTIC")
	case RoutineOptionNotDeterministic:
		ctx.WriteKeyWord("NOT DETERMINISTIC")
	case RoutineOptionContainsSQL:
		ctx.WriteKeyWord("CONTAINS SQL")
	case RoutineOptionNoSQL:
		ctx.WriteKeyWord("NO SQL")
	case RoutineOptionReadsSQLData:
		ctx.WriteKeyWord("READS SQL DATA")
	case RoutineOptionModifiesSQLData:
		ctx.WriteKeyWord("MODIFIES SQL DATA")
	case RoutineOptionSecurityDefiner:
		ctx.WriteKeyWord("SQL SECURITY DEFINER")
	case RoutineOptionSecurityInvoker:
		ctx.WriteKeyWord("SQL SECURITY INVOKER")
	case RoutineOptionDropComment:
		ctx.WriteKeyWord("DROP COMMENT")
	case RoutineOptionUsing:
		ctx.WriteKeyWord("USING ")
		ctx.WritePlain("(")
		for i, lib := range n.Libraries {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := lib.Name.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore RoutineOption.Libraries[%d]: %w", i, err)
			}
			if lib.Alias.O != "" {
				ctx.WriteKeyWord(" AS ")
				ctx.WriteName(lib.Alias.O)
			}
		}
		ctx.WritePlain(")")
	}
	return nil
}

func restoreRoutineOptions(ctx *format.RestoreCtx, options []*RoutineOption) error {
	for _, opt := range options {
		ctx.WritePlain(" ")
		if err := opt.Restore(ctx); err != nil {
			return err
		}
	}
	return nil
}

// restoreRoutineBody writes either the AS 'text' body of an external
// language routine or the parsed SQL body.
func restoreRoutineBody(ctx *format.RestoreCtx, body StmtNode, bodyStr string, hasBodyStr bool, what string) error {
	if hasBodyStr {
		ctx.WriteKeyWord(" AS ")
		ctx.WriteString(bodyStr)
		return nil
	}
	ctx.WritePlain(" ")
	if err := body.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore %s.Body: %w", what, err)
	}
	return nil
}

// CreateProcedureStmt is a statement to create a stored procedure.
// See https://dev.mysql.com/doc/refman/8.4/en/create-procedure.html
type CreateProcedureStmt struct {
	ddlNode

	IfNotExists bool
	Definer     *auth.UserIdentity
	Name        *TableName
	Params      []*RoutineParam
	Options     []*RoutineOption
	Body        StmtNode // nil when HasBodyStr
	BodyStr     string   // AS 'text' body of an external language routine
	HasBodyStr  bool
}

// Restore implements Node interface.
func (n *CreateProcedureStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE ")
	restoreDefinerOpt(ctx, n.Definer)
	ctx.WriteKeyWord("PROCEDURE ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	if err := n.Name.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore CreateProcedureStmt.Name: %w", err)
	}
	if err := restoreRoutineParams(ctx, n.Params, true); err != nil {
		return err
	}
	if err := restoreRoutineOptions(ctx, n.Options); err != nil {
		return err
	}
	return restoreRoutineBody(ctx, n.Body, n.BodyStr, n.HasBodyStr, "CreateProcedureStmt")
}

// Accept implements Node Accept interface.
func (n *CreateProcedureStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateProcedureStmt)
	if n.Body != nil {
		node, ok := n.Body.Accept(v)
		if !ok {
			return n, false
		}
		n.Body = node.(StmtNode)
	}
	return v.Leave(n)
}

// CreateFunctionStmt is a statement to create a stored function.
// See https://dev.mysql.com/doc/refman/8.4/en/create-procedure.html
type CreateFunctionStmt struct {
	ddlNode

	IfNotExists bool
	Definer     *auth.UserIdentity
	Name        *TableName
	Params      []*RoutineParam
	ReturnType  *types.FieldType
	Options     []*RoutineOption
	Body        StmtNode
	BodyStr     string
	HasBodyStr  bool
}

// Restore implements Node interface.
func (n *CreateFunctionStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE ")
	restoreDefinerOpt(ctx, n.Definer)
	ctx.WriteKeyWord("FUNCTION ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	if err := n.Name.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore CreateFunctionStmt.Name: %w", err)
	}
	if err := restoreRoutineParams(ctx, n.Params, false); err != nil {
		return err
	}
	ctx.WriteKeyWord(" RETURNS ")
	if err := n.ReturnType.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore CreateFunctionStmt.ReturnType: %w", err)
	}
	if err := restoreRoutineOptions(ctx, n.Options); err != nil {
		return err
	}
	return restoreRoutineBody(ctx, n.Body, n.BodyStr, n.HasBodyStr, "CreateFunctionStmt")
}

// Accept implements Node Accept interface.
func (n *CreateFunctionStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateFunctionStmt)
	if n.Body != nil {
		node, ok := n.Body.Accept(v)
		if !ok {
			return n, false
		}
		n.Body = node.(StmtNode)
	}
	return v.Leave(n)
}

// CreateLoadableFunctionStmt is a statement to register a loadable (UDF)
// function. See https://dev.mysql.com/doc/refman/8.4/en/create-function-loadable.html
type CreateLoadableFunctionStmt struct {
	ddlNode

	IfNotExists bool
	Aggregate   bool
	Name        *TableName
	ReturnType  string // STRING | INTEGER | REAL | DECIMAL
	Soname      string
}

// Restore implements Node interface.
func (n *CreateLoadableFunctionStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE ")
	if n.Aggregate {
		ctx.WriteKeyWord("AGGREGATE ")
	}
	ctx.WriteKeyWord("FUNCTION ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	if err := n.Name.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore CreateLoadableFunctionStmt.Name: %w", err)
	}
	ctx.WriteKeyWord(" RETURNS ")
	ctx.WriteKeyWord(n.ReturnType)
	ctx.WriteKeyWord(" SONAME ")
	ctx.WriteString(n.Soname)
	return nil
}

// Accept implements Node Accept interface.
func (n *CreateLoadableFunctionStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateLoadableFunctionStmt)
	return v.Leave(n)
}

// TriggerTime is when a trigger fires relative to the row operation.
type TriggerTime int

// TriggerTime values.
const (
	TriggerTimeBefore TriggerTime = iota
	TriggerTimeAfter
)

// TriggerEvent is the row operation a trigger fires on.
type TriggerEvent int

// TriggerEvent values.
const (
	TriggerEventInsert TriggerEvent = iota
	TriggerEventUpdate
	TriggerEventDelete
)

// TriggerOrder is the FOLLOWS/PRECEDES placement of a trigger.
type TriggerOrder int

// TriggerOrder values.
const (
	TriggerOrderNone TriggerOrder = iota
	TriggerOrderFollows
	TriggerOrderPrecedes
)

// CreateTriggerStmt is a statement to create a trigger.
// See https://dev.mysql.com/doc/refman/8.4/en/create-trigger.html
type CreateTriggerStmt struct {
	ddlNode

	IfNotExists  bool
	Definer      *auth.UserIdentity
	Name         *TableName
	Time         TriggerTime
	Event        TriggerEvent
	Table        *TableName
	Order        TriggerOrder
	OtherTrigger CIStr
	Body         StmtNode
}

// Restore implements Node interface.
func (n *CreateTriggerStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE ")
	restoreDefinerOpt(ctx, n.Definer)
	ctx.WriteKeyWord("TRIGGER ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	if err := n.Name.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore CreateTriggerStmt.Name: %w", err)
	}
	if n.Time == TriggerTimeBefore {
		ctx.WriteKeyWord(" BEFORE ")
	} else {
		ctx.WriteKeyWord(" AFTER ")
	}
	switch n.Event {
	case TriggerEventInsert:
		ctx.WriteKeyWord("INSERT")
	case TriggerEventUpdate:
		ctx.WriteKeyWord("UPDATE")
	case TriggerEventDelete:
		ctx.WriteKeyWord("DELETE")
	}
	ctx.WriteKeyWord(" ON ")
	if err := n.Table.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore CreateTriggerStmt.Table: %w", err)
	}
	ctx.WriteKeyWord(" FOR EACH ROW")
	switch n.Order { //nolint:exhaustive // TriggerOrderNone restores nothing
	case TriggerOrderFollows:
		ctx.WriteKeyWord(" FOLLOWS ")
		ctx.WriteName(n.OtherTrigger.O)
	case TriggerOrderPrecedes:
		ctx.WriteKeyWord(" PRECEDES ")
		ctx.WriteName(n.OtherTrigger.O)
	}
	ctx.WritePlain(" ")
	if err := n.Body.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore CreateTriggerStmt.Body: %w", err)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *CreateTriggerStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateTriggerStmt)
	node, ok := n.Table.Accept(v)
	if !ok {
		return n, false
	}
	n.Table = node.(*TableName)
	body, ok := n.Body.Accept(v)
	if !ok {
		return n, false
	}
	n.Body = body.(StmtNode)
	return v.Leave(n)
}

// EventSchedule is the ON SCHEDULE clause of an event.
type EventSchedule struct {
	At     ExprNode // AT timestamp form
	Every  ExprNode // EVERY interval form
	Unit   TimeUnitType
	Starts ExprNode
	Ends   ExprNode
}

// accept visits the schedule expressions on behalf of the enclosing
// statement; EventSchedule itself is not a Node.
func (n *EventSchedule) accept(v Visitor) bool {
	for _, e := range []*ExprNode{&n.At, &n.Every, &n.Starts, &n.Ends} {
		if *e == nil {
			continue
		}
		node, ok := (*e).Accept(v)
		if !ok {
			return false
		}
		*e = node.(ExprNode)
	}
	return true
}

// Restore implements Node interface.
func (n *EventSchedule) Restore(ctx *format.RestoreCtx) error {
	if n.At != nil {
		ctx.WriteKeyWord("AT ")
		if err := n.At.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore EventSchedule.At: %w", err)
		}
		return nil
	}
	ctx.WriteKeyWord("EVERY ")
	if err := n.Every.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore EventSchedule.Every: %w", err)
	}
	ctx.WritePlain(" ")
	ctx.WriteKeyWord(n.Unit.String())
	if n.Starts != nil {
		ctx.WriteKeyWord(" STARTS ")
		if err := n.Starts.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore EventSchedule.Starts: %w", err)
		}
	}
	if n.Ends != nil {
		ctx.WriteKeyWord(" ENDS ")
		if err := n.Ends.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore EventSchedule.Ends: %w", err)
		}
	}
	return nil
}

// EventCompletion is the ON COMPLETION behavior of an event.
type EventCompletion int

// EventCompletion values.
const (
	EventCompletionDefault EventCompletion = iota
	EventCompletionPreserve
	EventCompletionNotPreserve
)

func restoreEventCompletion(ctx *format.RestoreCtx, c EventCompletion) {
	switch c { //nolint:exhaustive // EventCompletionDefault restores nothing
	case EventCompletionPreserve:
		ctx.WriteKeyWord(" ON COMPLETION PRESERVE")
	case EventCompletionNotPreserve:
		ctx.WriteKeyWord(" ON COMPLETION NOT PRESERVE")
	}
}

// EventStatus is the ENABLE/DISABLE state of an event.
type EventStatus int

// EventStatus values.
const (
	EventStatusDefault EventStatus = iota
	EventStatusEnable
	EventStatusDisable
	EventStatusDisableOnReplica
)

func restoreEventStatus(ctx *format.RestoreCtx, s EventStatus) {
	switch s { //nolint:exhaustive // EventStatusDefault restores nothing
	case EventStatusEnable:
		ctx.WriteKeyWord(" ENABLE")
	case EventStatusDisable:
		ctx.WriteKeyWord(" DISABLE")
	case EventStatusDisableOnReplica:
		ctx.WriteKeyWord(" DISABLE ON REPLICA")
	}
}

// CreateEventStmt is a statement to create an event.
// See https://dev.mysql.com/doc/refman/8.4/en/create-event.html
type CreateEventStmt struct {
	ddlNode

	IfNotExists bool
	Definer     *auth.UserIdentity
	Name        *TableName
	Schedule    *EventSchedule
	Completion  EventCompletion
	Status      EventStatus
	HasComment  bool
	Comment     string
	Body        StmtNode
}

// Restore implements Node interface.
func (n *CreateEventStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE ")
	restoreDefinerOpt(ctx, n.Definer)
	ctx.WriteKeyWord("EVENT ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	if err := n.Name.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore CreateEventStmt.Name: %w", err)
	}
	ctx.WriteKeyWord(" ON SCHEDULE ")
	if err := n.Schedule.Restore(ctx); err != nil {
		return err
	}
	restoreEventCompletion(ctx, n.Completion)
	restoreEventStatus(ctx, n.Status)
	if n.HasComment {
		ctx.WriteKeyWord(" COMMENT ")
		ctx.WriteString(n.Comment)
	}
	ctx.WriteKeyWord(" DO ")
	if err := n.Body.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore CreateEventStmt.Body: %w", err)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *CreateEventStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateEventStmt)
	if !n.Schedule.accept(v) {
		return n, false
	}
	body, ok := n.Body.Accept(v)
	if !ok {
		return n, false
	}
	n.Body = body.(StmtNode)
	return v.Leave(n)
}

// AlterProcedureStmt is a statement to change stored procedure
// characteristics. See https://dev.mysql.com/doc/refman/8.4/en/alter-procedure.html
type AlterProcedureStmt struct {
	ddlNode

	Name    *TableName
	Options []*RoutineOption
}

// Restore implements Node interface.
func (n *AlterProcedureStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER PROCEDURE ")
	if err := n.Name.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore AlterProcedureStmt.Name: %w", err)
	}
	return restoreRoutineOptions(ctx, n.Options)
}

// Accept implements Node Accept interface.
func (n *AlterProcedureStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterProcedureStmt)
	return v.Leave(n)
}

// AlterFunctionStmt is a statement to change stored function
// characteristics. See https://dev.mysql.com/doc/refman/8.4/en/alter-function.html
type AlterFunctionStmt struct {
	ddlNode

	Name    *TableName
	Options []*RoutineOption
}

// Restore implements Node interface.
func (n *AlterFunctionStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER FUNCTION ")
	if err := n.Name.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore AlterFunctionStmt.Name: %w", err)
	}
	return restoreRoutineOptions(ctx, n.Options)
}

// Accept implements Node Accept interface.
func (n *AlterFunctionStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterFunctionStmt)
	return v.Leave(n)
}

// AlterEventStmt is a statement to change an event.
// See https://dev.mysql.com/doc/refman/8.4/en/alter-event.html
type AlterEventStmt struct {
	ddlNode

	Definer    *auth.UserIdentity
	Name       *TableName
	Schedule   *EventSchedule // nil when unchanged
	Completion EventCompletion
	RenameTo   *TableName
	Status     EventStatus
	HasComment bool
	Comment    string
	Body       StmtNode // nil when unchanged
}

// Restore implements Node interface.
func (n *AlterEventStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER ")
	restoreDefinerOpt(ctx, n.Definer)
	ctx.WriteKeyWord("EVENT ")
	if err := n.Name.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore AlterEventStmt.Name: %w", err)
	}
	if n.Schedule != nil {
		ctx.WriteKeyWord(" ON SCHEDULE ")
		if err := n.Schedule.Restore(ctx); err != nil {
			return err
		}
	}
	restoreEventCompletion(ctx, n.Completion)
	if n.RenameTo != nil {
		ctx.WriteKeyWord(" RENAME TO ")
		if err := n.RenameTo.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterEventStmt.RenameTo: %w", err)
		}
	}
	restoreEventStatus(ctx, n.Status)
	if n.HasComment {
		ctx.WriteKeyWord(" COMMENT ")
		ctx.WriteString(n.Comment)
	}
	if n.Body != nil {
		ctx.WriteKeyWord(" DO ")
		if err := n.Body.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterEventStmt.Body: %w", err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *AlterEventStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterEventStmt)
	if n.Schedule != nil && !n.Schedule.accept(v) {
		return n, false
	}
	if n.Body != nil {
		body, ok := n.Body.Accept(v)
		if !ok {
			return n, false
		}
		n.Body = body.(StmtNode)
	}
	return v.Leave(n)
}

// RoutineType is the object kind of a DROP routine statement.
type RoutineType int

// RoutineType values.
const (
	RoutineTypeProcedure RoutineType = iota
	RoutineTypeFunction
	RoutineTypeTrigger
	RoutineTypeEvent
)

// DropRoutineStmt is a statement to drop a procedure, function, trigger or
// event.
type DropRoutineStmt struct {
	ddlNode

	Tp       RoutineType
	IfExists bool
	Name     *TableName
}

// Restore implements Node interface.
func (n *DropRoutineStmt) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp {
	case RoutineTypeProcedure:
		ctx.WriteKeyWord("DROP PROCEDURE ")
	case RoutineTypeFunction:
		ctx.WriteKeyWord("DROP FUNCTION ")
	case RoutineTypeTrigger:
		ctx.WriteKeyWord("DROP TRIGGER ")
	case RoutineTypeEvent:
		ctx.WriteKeyWord("DROP EVENT ")
	}
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	if err := n.Name.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore DropRoutineStmt.Name: %w", err)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *DropRoutineStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropRoutineStmt)
	return v.Leave(n)
}

// BeginEndStmt is a BEGIN ... END compound statement.
type BeginEndStmt struct {
	stmtNode

	Label       CIStr
	HasEndLabel bool
	Stmts       []StmtNode
}

// Restore implements Node interface.
func (n *BeginEndStmt) Restore(ctx *format.RestoreCtx) error {
	if n.Label.O != "" {
		ctx.WriteName(n.Label.O)
		ctx.WritePlain(": ")
	}
	ctx.WriteKeyWord("BEGIN ")
	if err := restoreStmtList(ctx, n.Stmts, "BeginEndStmt.Stmts"); err != nil {
		return err
	}
	ctx.WriteKeyWord("END")
	if n.HasEndLabel {
		ctx.WritePlain(" ")
		ctx.WriteName(n.Label.O)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *BeginEndStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*BeginEndStmt)
	if !acceptStmtList(v, n.Stmts) {
		return n, false
	}
	return v.Leave(n)
}

// DeclareVarStmt is a DECLARE variable statement in a compound body.
type DeclareVarStmt struct {
	stmtNode

	Names   []CIStr
	Type    *types.FieldType
	Default ExprNode
}

// Restore implements Node interface.
func (n *DeclareVarStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DECLARE ")
	for i, name := range n.Names {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		ctx.WriteName(name.O)
	}
	ctx.WritePlain(" ")
	if err := n.Type.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore DeclareVarStmt.Type: %w", err)
	}
	if n.Default != nil {
		ctx.WriteKeyWord(" DEFAULT ")
		if err := n.Default.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore DeclareVarStmt.Default: %w", err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *DeclareVarStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DeclareVarStmt)
	if n.Default != nil {
		node, ok := n.Default.Accept(v)
		if !ok {
			return n, false
		}
		n.Default = node.(ExprNode)
	}
	return v.Leave(n)
}

// HandlerConditionType is the kind of one handler condition value.
type HandlerConditionType int

// HandlerConditionType values.
const (
	HandlerConditionErrorCode HandlerConditionType = iota
	HandlerConditionSQLState
	HandlerConditionName
	HandlerConditionSQLWarning
	HandlerConditionNotFound
	HandlerConditionSQLException
)

// HandlerCondition is one condition value of a DECLARE HANDLER statement.
type HandlerCondition struct {
	Tp    HandlerConditionType
	Code  uint64
	State string
	Name  CIStr
}

// Restore implements Node interface.
func (n *HandlerCondition) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp {
	case HandlerConditionErrorCode:
		ctx.WritePlainf("%d", n.Code)
	case HandlerConditionSQLState:
		ctx.WriteKeyWord("SQLSTATE ")
		ctx.WriteString(n.State)
	case HandlerConditionName:
		ctx.WriteName(n.Name.O)
	case HandlerConditionSQLWarning:
		ctx.WriteKeyWord("SQLWARNING")
	case HandlerConditionNotFound:
		ctx.WriteKeyWord("NOT FOUND")
	case HandlerConditionSQLException:
		ctx.WriteKeyWord("SQLEXCEPTION")
	}
	return nil
}

// HandlerAction is what a handler does after its statement runs.
type HandlerAction int

// HandlerAction values.
const (
	HandlerActionContinue HandlerAction = iota
	HandlerActionExit
	HandlerActionUndo
)

// DeclareHandlerStmt is a DECLARE ... HANDLER statement in a compound body.
type DeclareHandlerStmt struct {
	stmtNode

	Action     HandlerAction
	Conditions []*HandlerCondition
	Handler    StmtNode
}

// Restore implements Node interface.
func (n *DeclareHandlerStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DECLARE ")
	switch n.Action {
	case HandlerActionContinue:
		ctx.WriteKeyWord("CONTINUE")
	case HandlerActionExit:
		ctx.WriteKeyWord("EXIT")
	case HandlerActionUndo:
		ctx.WriteKeyWord("UNDO")
	}
	ctx.WriteKeyWord(" HANDLER FOR ")
	for i, cond := range n.Conditions {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := cond.Restore(ctx); err != nil {
			return err
		}
	}
	ctx.WritePlain(" ")
	if err := n.Handler.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore DeclareHandlerStmt.Handler: %w", err)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *DeclareHandlerStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DeclareHandlerStmt)
	node, ok := n.Handler.Accept(v)
	if !ok {
		return n, false
	}
	n.Handler = node.(StmtNode)
	return v.Leave(n)
}

// DeclareConditionStmt is a DECLARE ... CONDITION statement in a compound
// body.
type DeclareConditionStmt struct {
	stmtNode

	Name      CIStr
	Condition *HandlerCondition // error code or SQLSTATE form
}

// Restore implements Node interface.
func (n *DeclareConditionStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DECLARE ")
	ctx.WriteName(n.Name.O)
	ctx.WriteKeyWord(" CONDITION FOR ")
	return n.Condition.Restore(ctx)
}

// Accept implements Node Accept interface.
func (n *DeclareConditionStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DeclareConditionStmt)
	return v.Leave(n)
}

// DeclareCursorStmt is a DECLARE ... CURSOR statement in a compound body.
type DeclareCursorStmt struct {
	stmtNode

	Name   CIStr
	Select StmtNode
}

// Restore implements Node interface.
func (n *DeclareCursorStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DECLARE ")
	ctx.WriteName(n.Name.O)
	ctx.WriteKeyWord(" CURSOR FOR ")
	if err := n.Select.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore DeclareCursorStmt.Select: %w", err)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *DeclareCursorStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DeclareCursorStmt)
	node, ok := n.Select.Accept(v)
	if !ok {
		return n, false
	}
	n.Select = node.(StmtNode)
	return v.Leave(n)
}

// ProcIfBranch is one IF/ELSEIF branch of a procedure IF statement.
type ProcIfBranch struct {
	Cond  ExprNode
	Stmts []StmtNode
}

// ProcIfStmt is an IF ... END IF statement in a compound body.
type ProcIfStmt struct {
	stmtNode

	Branches []*ProcIfBranch
	Else     []StmtNode
}

// Restore implements Node interface.
func (n *ProcIfStmt) Restore(ctx *format.RestoreCtx) error {
	for i, b := range n.Branches {
		if i == 0 {
			ctx.WriteKeyWord("IF ")
		} else {
			ctx.WriteKeyWord("ELSEIF ")
		}
		if err := b.Cond.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ProcIfStmt.Branches[%d].Cond: %w", i, err)
		}
		ctx.WriteKeyWord(" THEN ")
		if err := restoreStmtList(ctx, b.Stmts, "ProcIfStmt.Branches.Stmts"); err != nil {
			return err
		}
	}
	if n.Else != nil {
		ctx.WriteKeyWord("ELSE ")
		if err := restoreStmtList(ctx, n.Else, "ProcIfStmt.Else"); err != nil {
			return err
		}
	}
	ctx.WriteKeyWord("END IF")
	return nil
}

// Accept implements Node Accept interface.
func (n *ProcIfStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcIfStmt)
	for _, b := range n.Branches {
		node, ok := b.Cond.Accept(v)
		if !ok {
			return n, false
		}
		b.Cond = node.(ExprNode)
		if !acceptStmtList(v, b.Stmts) {
			return n, false
		}
	}
	if !acceptStmtList(v, n.Else) {
		return n, false
	}
	return v.Leave(n)
}

// ProcWhenClause is one WHEN arm of a procedure CASE statement.
type ProcWhenClause struct {
	Expr  ExprNode
	Stmts []StmtNode
}

// ProcCaseStmt is a CASE ... END CASE statement in a compound body.
type ProcCaseStmt struct {
	stmtNode

	Expr        ExprNode // nil for the searched CASE form
	WhenClauses []*ProcWhenClause
	Else        []StmtNode
}

// Restore implements Node interface.
func (n *ProcCaseStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CASE")
	if n.Expr != nil {
		ctx.WritePlain(" ")
		if err := n.Expr.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ProcCaseStmt.Expr: %w", err)
		}
	}
	for i, w := range n.WhenClauses {
		ctx.WriteKeyWord(" WHEN ")
		if err := w.Expr.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ProcCaseStmt.WhenClauses[%d]: %w", i, err)
		}
		ctx.WriteKeyWord(" THEN ")
		if err := restoreStmtList(ctx, w.Stmts, "ProcCaseStmt.WhenClauses.Stmts"); err != nil {
			return err
		}
		// restoreStmtList leaves a trailing space before the next keyword.
	}
	if n.Else != nil {
		ctx.WriteKeyWord(" ELSE ")
		if err := restoreStmtList(ctx, n.Else, "ProcCaseStmt.Else"); err != nil {
			return err
		}
	}
	ctx.WriteKeyWord(" END CASE")
	return nil
}

// Accept implements Node Accept interface.
func (n *ProcCaseStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ProcCaseStmt)
	if n.Expr != nil {
		node, ok := n.Expr.Accept(v)
		if !ok {
			return n, false
		}
		n.Expr = node.(ExprNode)
	}
	for _, w := range n.WhenClauses {
		node, ok := w.Expr.Accept(v)
		if !ok {
			return n, false
		}
		w.Expr = node.(ExprNode)
		if !acceptStmtList(v, w.Stmts) {
			return n, false
		}
	}
	if !acceptStmtList(v, n.Else) {
		return n, false
	}
	return v.Leave(n)
}

// WhileStmt is a WHILE ... END WHILE loop in a compound body.
type WhileStmt struct {
	stmtNode

	Label       CIStr
	HasEndLabel bool
	Cond        ExprNode
	Stmts       []StmtNode
}

// Restore implements Node interface.
func (n *WhileStmt) Restore(ctx *format.RestoreCtx) error {
	if n.Label.O != "" {
		ctx.WriteName(n.Label.O)
		ctx.WritePlain(": ")
	}
	ctx.WriteKeyWord("WHILE ")
	if err := n.Cond.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore WhileStmt.Cond: %w", err)
	}
	ctx.WriteKeyWord(" DO ")
	if err := restoreStmtList(ctx, n.Stmts, "WhileStmt.Stmts"); err != nil {
		return err
	}
	ctx.WriteKeyWord("END WHILE")
	if n.HasEndLabel {
		ctx.WritePlain(" ")
		ctx.WriteName(n.Label.O)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *WhileStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*WhileStmt)
	node, ok := n.Cond.Accept(v)
	if !ok {
		return n, false
	}
	n.Cond = node.(ExprNode)
	if !acceptStmtList(v, n.Stmts) {
		return n, false
	}
	return v.Leave(n)
}

// RepeatStmt is a REPEAT ... UNTIL ... END REPEAT loop in a compound body.
type RepeatStmt struct {
	stmtNode

	Label       CIStr
	HasEndLabel bool
	Stmts       []StmtNode
	Until       ExprNode
}

// Restore implements Node interface.
func (n *RepeatStmt) Restore(ctx *format.RestoreCtx) error {
	if n.Label.O != "" {
		ctx.WriteName(n.Label.O)
		ctx.WritePlain(": ")
	}
	ctx.WriteKeyWord("REPEAT ")
	if err := restoreStmtList(ctx, n.Stmts, "RepeatStmt.Stmts"); err != nil {
		return err
	}
	ctx.WriteKeyWord("UNTIL ")
	if err := n.Until.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore RepeatStmt.Until: %w", err)
	}
	ctx.WriteKeyWord(" END REPEAT")
	if n.HasEndLabel {
		ctx.WritePlain(" ")
		ctx.WriteName(n.Label.O)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *RepeatStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RepeatStmt)
	if !acceptStmtList(v, n.Stmts) {
		return n, false
	}
	node, ok := n.Until.Accept(v)
	if !ok {
		return n, false
	}
	n.Until = node.(ExprNode)
	return v.Leave(n)
}

// LoopStmt is a LOOP ... END LOOP loop in a compound body.
type LoopStmt struct {
	stmtNode

	Label       CIStr
	HasEndLabel bool
	Stmts       []StmtNode
}

// Restore implements Node interface.
func (n *LoopStmt) Restore(ctx *format.RestoreCtx) error {
	if n.Label.O != "" {
		ctx.WriteName(n.Label.O)
		ctx.WritePlain(": ")
	}
	ctx.WriteKeyWord("LOOP ")
	if err := restoreStmtList(ctx, n.Stmts, "LoopStmt.Stmts"); err != nil {
		return err
	}
	ctx.WriteKeyWord("END LOOP")
	if n.HasEndLabel {
		ctx.WritePlain(" ")
		ctx.WriteName(n.Label.O)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *LoopStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*LoopStmt)
	if !acceptStmtList(v, n.Stmts) {
		return n, false
	}
	return v.Leave(n)
}

// IterateStmt is an ITERATE statement in a compound body.
type IterateStmt struct {
	stmtNode

	Label CIStr
}

// Restore implements Node interface.
func (n *IterateStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ITERATE ")
	ctx.WriteName(n.Label.O)
	return nil
}

// Accept implements Node Accept interface.
func (n *IterateStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*IterateStmt)
	return v.Leave(n)
}

// LeaveStmt is a LEAVE statement in a compound body.
type LeaveStmt struct {
	stmtNode

	Label CIStr
}

// Restore implements Node interface.
func (n *LeaveStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("LEAVE ")
	ctx.WriteName(n.Label.O)
	return nil
}

// Accept implements Node Accept interface.
func (n *LeaveStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*LeaveStmt)
	return v.Leave(n)
}

// ReturnStmt is a RETURN statement in a stored function body.
type ReturnStmt struct {
	stmtNode

	Expr ExprNode
}

// Restore implements Node interface.
func (n *ReturnStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("RETURN ")
	if err := n.Expr.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore ReturnStmt.Expr: %w", err)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ReturnStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ReturnStmt)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)
	return v.Leave(n)
}

// OpenCursorStmt is an OPEN cursor statement in a compound body.
type OpenCursorStmt struct {
	stmtNode

	Name CIStr
}

// Restore implements Node interface.
func (n *OpenCursorStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("OPEN ")
	ctx.WriteName(n.Name.O)
	return nil
}

// Accept implements Node Accept interface.
func (n *OpenCursorStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*OpenCursorStmt)
	return v.Leave(n)
}

// FetchCursorStmt is a FETCH cursor statement in a compound body.
type FetchCursorStmt struct {
	stmtNode

	Name CIStr
	Vars []ExprNode
}

// Restore implements Node interface.
func (n *FetchCursorStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("FETCH ")
	ctx.WriteName(n.Name.O)
	ctx.WriteKeyWord(" INTO ")
	for i, v := range n.Vars {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore FetchCursorStmt.Vars[%d]: %w", i, err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *FetchCursorStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FetchCursorStmt)
	for i, e := range n.Vars {
		node, ok := e.Accept(v)
		if !ok {
			return n, false
		}
		n.Vars[i] = node.(ExprNode)
	}
	return v.Leave(n)
}

// CloseCursorStmt is a CLOSE cursor statement in a compound body.
type CloseCursorStmt struct {
	stmtNode

	Name CIStr
}

// Restore implements Node interface.
func (n *CloseCursorStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CLOSE ")
	ctx.WriteName(n.Name.O)
	return nil
}

// Accept implements Node Accept interface.
func (n *CloseCursorStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CloseCursorStmt)
	return v.Leave(n)
}

// SignalCondition is the condition value of a SIGNAL/RESIGNAL statement.
type SignalCondition struct {
	IsSQLState bool
	State      string
	Name       CIStr
}

// Restore implements Node interface.
func (n *SignalCondition) Restore(ctx *format.RestoreCtx) error {
	if n.IsSQLState {
		ctx.WriteKeyWord("SQLSTATE ")
		ctx.WriteString(n.State)
	} else {
		ctx.WriteName(n.Name.O)
	}
	return nil
}

// SignalItem is one name = value entry of a SIGNAL/RESIGNAL SET clause.
type SignalItem struct {
	Name  string // condition information item name, e.g. MESSAGE_TEXT
	Value ExprNode
}

func restoreSignalItems(ctx *format.RestoreCtx, items []*SignalItem) error {
	for i, item := range items {
		if i == 0 {
			ctx.WriteKeyWord(" SET ")
		} else {
			ctx.WritePlain(", ")
		}
		ctx.WriteKeyWord(item.Name)
		ctx.WritePlain(" = ")
		if err := item.Value.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore SignalItem[%d]: %w", i, err)
		}
	}
	return nil
}

func acceptSignalItems(v Visitor, items []*SignalItem) bool {
	for _, item := range items {
		node, ok := item.Value.Accept(v)
		if !ok {
			return false
		}
		item.Value = node.(ExprNode)
	}
	return true
}

// SignalStmt is a SIGNAL statement.
// See https://dev.mysql.com/doc/refman/8.4/en/signal.html
type SignalStmt struct {
	stmtNode

	Condition *SignalCondition
	Items     []*SignalItem
}

// Restore implements Node interface.
func (n *SignalStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SIGNAL ")
	if err := n.Condition.Restore(ctx); err != nil {
		return err
	}
	return restoreSignalItems(ctx, n.Items)
}

// Accept implements Node Accept interface.
func (n *SignalStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SignalStmt)
	if !acceptSignalItems(v, n.Items) {
		return n, false
	}
	return v.Leave(n)
}

// ResignalStmt is a RESIGNAL statement.
// See https://dev.mysql.com/doc/refman/8.4/en/resignal.html
type ResignalStmt struct {
	stmtNode

	Condition *SignalCondition // nil for the bare RESIGNAL form
	Items     []*SignalItem
}

// Restore implements Node interface.
func (n *ResignalStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("RESIGNAL")
	if n.Condition != nil {
		ctx.WritePlain(" ")
		if err := n.Condition.Restore(ctx); err != nil {
			return err
		}
	}
	return restoreSignalItems(ctx, n.Items)
}

// Accept implements Node Accept interface.
func (n *ResignalStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ResignalStmt)
	if !acceptSignalItems(v, n.Items) {
		return n, false
	}
	return v.Leave(n)
}

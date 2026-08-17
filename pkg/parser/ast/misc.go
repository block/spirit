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
	"strconv"
	"strings"

	"github.com/block/spirit/pkg/parser/auth"
	"github.com/block/spirit/pkg/parser/format"
	"github.com/block/spirit/pkg/parser/mysql"
)

var (
	_ StmtNode = &AlterUserStmt{}
	_ StmtNode = &BeginStmt{}
	_ StmtNode = &BinlogStmt{}
	_ StmtNode = &CommitStmt{}
	_ StmtNode = &CreateUserStmt{}
	_ StmtNode = &DeallocateStmt{}
	_ StmtNode = &DoStmt{}
	_ StmtNode = &ExecuteStmt{}
	_ StmtNode = &ExplainStmt{}
	_ StmtNode = &GrantStmt{}
	_ StmtNode = &PrepareStmt{}
	_ StmtNode = &RollbackStmt{}
	_ StmtNode = &SetPwdStmt{}
	_ StmtNode = &SetRoleStmt{}
	_ StmtNode = &SetDefaultRoleStmt{}
	_ StmtNode = &SetStmt{}
	_ StmtNode = &UseStmt{}
	_ StmtNode = &FlushStmt{}
	_ StmtNode = &KillStmt{}
	_ StmtNode = &HelpStmt{}
	_ StmtNode = &GetDiagnosticsStmt{}
	_ StmtNode = &ShutdownStmt{}
	_ StmtNode = &RestartStmt{}
	_ StmtNode = &RenameUserStmt{}

	_ Node = &PrivElem{}
	_ Node = &VariableAssignment{}
)

// Isolation level constants.
const (
	ReadCommitted   = "READ-COMMITTED"
	ReadUncommitted = "READ-UNCOMMITTED"
	Serializable    = "SERIALIZABLE"
	RepeatableRead  = "REPEATABLE-READ"
)

// TypeOpt is used for parsing data type option from SQL.
type TypeOpt struct {
	IsUnsigned bool
	IsZerofill bool
}

// FloatOpt is used for parsing floating-point type option from SQL.
// See http://dev.mysql.com/doc/refman/5.7/en/floating-point-types.html
type FloatOpt struct {
	Flen    int
	Decimal int
}

// AuthOption is used for parsing create use statement.
type AuthOption struct {
	// ByAuthString set as true, if AuthString is used for authorization. Otherwise, authorization is done by HashString.
	ByAuthString bool
	AuthString   string
	ByHashString bool
	HashString   string
	AuthPlugin   string
	// ByRandomPassword is the IDENTIFIED BY RANDOM PASSWORD form (MySQL 8.0.18+).
	ByRandomPassword bool
	// HasReplace / ReplaceString carry REPLACE 'current password'
	// (MySQL 8.0.14+ password verification). HasReplace distinguishes an
	// absent clause from REPLACE ''.
	HasReplace    bool
	ReplaceString string
	// InitialAuth is the IDENTIFIED WITH plugin INITIAL AUTHENTICATION ...
	// form used for passwordless authentication (MySQL 8.0.27+).
	InitialAuth *AuthOption
}

// Restore implements Node interface.
func (n *AuthOption) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("IDENTIFIED")
	if n.AuthPlugin != "" {
		ctx.WriteKeyWord(" WITH ")
		ctx.WriteString(n.AuthPlugin)
	}
	switch {
	case n.ByRandomPassword:
		ctx.WriteKeyWord(" BY RANDOM PASSWORD")
	case n.ByAuthString:
		ctx.WriteKeyWord(" BY ")
		ctx.WriteString(n.AuthString)
	case n.ByHashString:
		ctx.WriteKeyWord(" AS ")
		ctx.WriteString(n.HashString)
	}
	if n.HasReplace {
		ctx.WriteKeyWord(" REPLACE ")
		ctx.WriteString(n.ReplaceString)
	}
	if n.InitialAuth != nil {
		ctx.WriteKeyWord(" INITIAL AUTHENTICATION ")
		if err := n.InitialAuth.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AuthOption.InitialAuth: %w", err)
		}
	}
	return nil
}

// ExplainForStmt is a statement to provite information about how is SQL statement executeing
// in connection #ConnectionID
// See https://dev.mysql.com/doc/refman/5.7/en/explain.html
type ExplainForStmt struct {
	stmtNode

	Format       string
	ConnectionID uint64
}

// Restore implements Node interface.
func (n *ExplainForStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("EXPLAIN ")
	ctx.WriteKeyWord("FORMAT ")
	ctx.WritePlain("= ")
	ctx.WriteString(n.Format)
	ctx.WritePlain(" ")
	ctx.WriteKeyWord("FOR ")
	ctx.WriteKeyWord("CONNECTION ")
	ctx.WritePlain(strconv.FormatUint(n.ConnectionID, 10))
	return nil
}

// Accept implements Node Accept interface.
func (n *ExplainForStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ExplainForStmt)
	return v.Leave(n)
}

// ExplainStmt is a statement to provide information about how is SQL statement executed
// or get columns information in a table.
// See https://dev.mysql.com/doc/refman/5.7/en/explain.html
type ExplainStmt struct {
	stmtNode

	Stmt    StmtNode
	Format  string
	Analyze bool

	// IntoVar is the raw @variable token of `EXPLAIN ... INTO @var`,
	// including the leading '@'. Empty when no INTO clause was given.
	IntoVar string

	// ForSchema is the schema name of `EXPLAIN ... FOR {SCHEMA|DATABASE} name`.
	// Empty when no FOR SCHEMA clause was given.
	ForSchema string

	// Explore indicates whether to use EXPLAIN EXPLORE.
	Explore bool
	// SQLDigest to explain, used in `EXPLAIN EXPLORE <sql_digest>`.
	SQLDigest string
	// PlanDigest to explain, used in `EXPLAIN [ANALYZE] <plan_digest>`.
	PlanDigest string
}

// Restore implements Node interface.
func (n *ExplainStmt) Restore(ctx *format.RestoreCtx) error {
	if showStmt, ok := n.Stmt.(*ShowStmt); ok {
		ctx.WriteKeyWord("DESC ")
		if err := showStmt.Table.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ExplainStmt.ShowStmt.Table: %w", err)
		}
		if showStmt.Column != nil {
			ctx.WritePlain(" ")
			if err := showStmt.Column.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore ExplainStmt.ShowStmt.Column: %w", err)
			}
		}
		return nil
	}
	ctx.WriteKeyWord("EXPLAIN ")
	if n.Analyze {
		ctx.WriteKeyWord("ANALYZE ")
	}
	if n.Explore {
		ctx.WriteKeyWord("EXPLORE ")
		if n.SQLDigest != "" {
			ctx.WriteString(n.SQLDigest)
		}
	} else if !n.Analyze || strings.ToLower(n.Format) != "row" {
		ctx.WriteKeyWord("FORMAT ")
		ctx.WritePlain("= ")
		ctx.WriteString(n.Format)
		ctx.WritePlain(" ")
	}
	if n.IntoVar != "" {
		ctx.WriteKeyWord("INTO ")
		ctx.WritePlain(n.IntoVar)
		ctx.WritePlain(" ")
	}
	if n.ForSchema != "" {
		ctx.WriteKeyWord("FOR SCHEMA ")
		ctx.WriteName(n.ForSchema)
		ctx.WritePlain(" ")
	}
	if n.PlanDigest != "" {
		ctx.WriteString(n.PlanDigest)
	}
	if n.Stmt != nil {
		if err := n.Stmt.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ExplainStmt.Stmt: %w", err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ExplainStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ExplainStmt)
	if n.Stmt != nil {
		node, ok := n.Stmt.Accept(v)
		if !ok {
			return n, false
		}
		n.Stmt = node.(StmtNode)
	}
	return v.Leave(n)
}

// PrepareStmt is a statement to prepares a SQL statement which contains placeholders,
// and it is executed with ExecuteStmt and released with DeallocateStmt.
// See https://dev.mysql.com/doc/refman/5.7/en/prepare.html
type PrepareStmt struct {
	stmtNode

	Name    string
	SQLText string
	SQLVar  *VariableExpr
}

// Restore implements Node interface.
func (n *PrepareStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("PREPARE ")
	ctx.WriteName(n.Name)
	ctx.WriteKeyWord(" FROM ")
	if n.SQLText != "" {
		ctx.WriteString(n.SQLText)
		return nil
	}
	if n.SQLVar != nil {
		if err := n.SQLVar.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore PrepareStmt.SQLVar: %w", err)
		}
		return nil
	}
	return errors.New("an error occurred while restore PrepareStmt")
}

// Accept implements Node Accept interface.
func (n *PrepareStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*PrepareStmt)
	if n.SQLVar != nil {
		node, ok := n.SQLVar.Accept(v)
		if !ok {
			return n, false
		}
		n.SQLVar = node.(*VariableExpr)
	}
	return v.Leave(n)
}

// DeallocateStmt is a statement to release PreparedStmt.
// See https://dev.mysql.com/doc/refman/5.7/en/deallocate-prepare.html
type DeallocateStmt struct {
	stmtNode

	Name string
}

// Restore implements Node interface.
func (n *DeallocateStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DEALLOCATE PREPARE ")
	ctx.WriteName(n.Name)
	return nil
}

// Accept implements Node Accept interface.
func (n *DeallocateStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DeallocateStmt)
	return v.Leave(n)
}

// ExecuteStmt is a statement to execute PreparedStmt.
// See https://dev.mysql.com/doc/refman/5.7/en/execute.html
type ExecuteStmt struct {
	stmtNode

	Name       string
	UsingVars  []ExprNode
	BinaryArgs any
	PrepStmt   any // the corresponding prepared statement
	PrepStmtId uint32
	IdxInMulti int

	// FromGeneralStmt indicates whether this execute-stmt is converted from a general query.
	// e.g. select * from t where a>2 --> execute 'select * from t where a>?' using 2
	FromGeneralStmt bool
}

// Restore implements Node interface.
func (n *ExecuteStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("EXECUTE ")
	ctx.WriteName(n.Name)
	if len(n.UsingVars) > 0 {
		ctx.WriteKeyWord(" USING ")
		for i, val := range n.UsingVars {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := val.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore ExecuteStmt.UsingVars index %d: %w", i, err)
			}
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ExecuteStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ExecuteStmt)
	for i, val := range n.UsingVars {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.UsingVars[i] = node.(ExprNode)
	}
	return v.Leave(n)
}

// BeginStmt is a statement to start a new transaction.
// See https://dev.mysql.com/doc/refman/5.7/en/commit.html
type BeginStmt struct {
	stmtNode
	ReadOnly bool
}

// Restore implements Node interface.
func (n *BeginStmt) Restore(ctx *format.RestoreCtx) error {
	if n.ReadOnly {
		ctx.WriteKeyWord("START TRANSACTION READ ONLY")
	} else {
		ctx.WriteKeyWord("START TRANSACTION")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *BeginStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}

	n = newNode.(*BeginStmt)
	return v.Leave(n)
}

// BinlogStmt is an internal-use statement.
// We just parse and ignore it.
// See http://dev.mysql.com/doc/refman/5.7/en/binlog.html
type BinlogStmt struct {
	stmtNode
	Str string
}

// Restore implements Node interface.
func (n *BinlogStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("BINLOG ")
	ctx.WriteString(n.Str)
	return nil
}

// Accept implements Node Accept interface.
func (n *BinlogStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*BinlogStmt)
	return v.Leave(n)
}

// CompletionType defines completion_type used in COMMIT and ROLLBACK statements
type CompletionType int8

const (
	// CompletionTypeDefault refers to NO_CHAIN
	CompletionTypeDefault CompletionType = iota
	CompletionTypeChain
	CompletionTypeRelease
)

func (n CompletionType) Restore(ctx *format.RestoreCtx) error {
	switch n {
	case CompletionTypeDefault:
	case CompletionTypeChain:
		ctx.WriteKeyWord(" AND CHAIN")
	case CompletionTypeRelease:
		ctx.WriteKeyWord(" RELEASE")
	}
	return nil
}

// CommitStmt is a statement to commit the current transaction.
// See https://dev.mysql.com/doc/refman/5.7/en/commit.html
type CommitStmt struct {
	stmtNode
	// CompletionType overwrites system variable `completion_type` within transaction
	CompletionType CompletionType
}

// Restore implements Node interface.
func (n *CommitStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("COMMIT")
	if err := n.CompletionType.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore CommitStmt.CompletionType: %w", err)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *CommitStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CommitStmt)
	return v.Leave(n)
}

// RollbackStmt is a statement to roll back the current transaction.
// See https://dev.mysql.com/doc/refman/5.7/en/commit.html
type RollbackStmt struct {
	stmtNode
	// CompletionType overwrites system variable `completion_type` within transaction
	CompletionType CompletionType
	// SavepointName is the savepoint name.
	SavepointName string
}

// Restore implements Node interface.
func (n *RollbackStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ROLLBACK")
	if n.SavepointName != "" {
		ctx.WritePlain(" TO ")
		ctx.WritePlain(n.SavepointName)
	}
	if err := n.CompletionType.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore RollbackStmt.CompletionType: %w", err)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *RollbackStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RollbackStmt)
	return v.Leave(n)
}

// UseStmt is a statement to use the DBName database as the current database.
// See https://dev.mysql.com/doc/refman/5.7/en/use.html
type UseStmt struct {
	stmtNode

	DBName string
}

// Restore implements Node interface.
func (n *UseStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("USE ")
	ctx.WriteName(n.DBName)
	return nil
}

// Accept implements Node Accept interface.
func (n *UseStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*UseStmt)
	return v.Leave(n)
}

const (
	// SetNames is the const for set names stmt.
	// If VariableAssignment.Name == Names, it should be set names stmt.
	SetNames = "SetNAMES"
	// SetCharset is the const for set charset stmt.
	SetCharset = "SetCharset"
)

// VariableAssignment is a variable assignment struct.
type VariableAssignment struct {
	node
	Name       string
	Value      ExprNode
	IsInstance bool
	IsGlobal   bool
	IsSystem   bool
	// IsPersist and IsPersistOnly are the SET PERSIST and
	// SET PERSIST_ONLY variable scopes.
	IsPersist     bool
	IsPersistOnly bool

	// ExtendValue is a way to store extended info.
	// VariableAssignment should be able to store information for SetCharset/SetPWD Stmt.
	// For SetCharsetStmt, Value is charset, ExtendValue is collation.
	// TODO: Use SetStmt to implement set password statement.
	ExtendValue *ValueExpr
}

// Restore implements Node interface.
func (n *VariableAssignment) Restore(ctx *format.RestoreCtx) error {
	if n.IsSystem {
		ctx.WritePlain("@@")
		switch {
		case n.IsGlobal:
			ctx.WriteKeyWord("GLOBAL")
		case n.IsInstance:
			ctx.WriteKeyWord("INSTANCE")
		case n.IsPersist:
			ctx.WriteKeyWord("PERSIST")
		case n.IsPersistOnly:
			ctx.WriteKeyWord("PERSIST_ONLY")
		default:
			ctx.WriteKeyWord("SESSION")
		}
		ctx.WritePlain(".")
	} else if n.Name != SetNames && n.Name != SetCharset {
		ctx.WriteKeyWord("@")
	}
	switch n.Name {
	case SetNames:
		ctx.WriteKeyWord("NAMES ")
	case SetCharset:
		ctx.WriteKeyWord("CHARSET ")
	default:
		ctx.WriteName(n.Name)
		ctx.WritePlain("=")
	}
	if err := n.Value.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore VariableAssignment.Value: %w", err)
	}
	if n.ExtendValue != nil {
		ctx.WriteKeyWord(" COLLATE ")
		if err := n.ExtendValue.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore VariableAssignment.ExtendValue: %w", err)
		}
	}
	return nil
}

// Accept implements Node interface.
func (n *VariableAssignment) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*VariableAssignment)
	node, ok := n.Value.Accept(v)
	if !ok {
		return n, false
	}
	n.Value = node.(ExprNode)
	return v.Leave(n)
}

// FlushStmtType is the type for FLUSH statement.
type FlushStmtType int

// Flush statement types.
const (
	FlushNone FlushStmtType = iota
	FlushTables
	FlushPrivileges
	FlushStatus
	FlushHosts
	FlushLogs
	FlushUserResources
	FlushOptimizerCosts
)

// LogType is the log type used in FLUSH statement.
type LogType int8

const (
	LogTypeDefault LogType = iota
	LogTypeBinary
	LogTypeEngine
	LogTypeError
	LogTypeGeneral
	LogTypeSlow
	LogTypeRelay
)

// FlushStmt is a statement to flush tables/privileges/optimizer costs and so on.
type FlushStmt struct {
	stmtNode

	Tp              FlushStmtType // Privileges/Tables/...
	NoWriteToBinLog bool
	LogType         LogType
	Tables          []*TableName // For FlushTableStmt, if Tables is empty, it means flush all tables.
	ReadLock        bool
	ForExport       bool
	// Channel is the FOR CHANNEL of FLUSH RELAY LOGS.
	Channel string
	// ExtraTargets are the additional comma-separated flush targets when the
	// statement lists several, e.g. FLUSH STATUS, USER_RESOURCES. Only the
	// Tp, LogType and Channel fields of each entry are meaningful: the table
	// form cannot appear in a list.
	ExtraTargets []*FlushStmt
}

// Restore implements Node interface.
func (n *FlushStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("FLUSH ")
	if n.NoWriteToBinLog {
		ctx.WriteKeyWord("NO_WRITE_TO_BINLOG ")
	}
	if err := n.restoreTarget(ctx); err != nil {
		return err
	}
	for _, extra := range n.ExtraTargets {
		ctx.WritePlain(", ")
		if err := extra.restoreTarget(ctx); err != nil {
			return err
		}
	}
	return nil
}

// restoreTarget restores one flush target, without the FLUSH keyword and
// NO_WRITE_TO_BINLOG prefix that belong to the statement as a whole.
func (n *FlushStmt) restoreTarget(ctx *format.RestoreCtx) error {
	switch n.Tp { //nolint:exhaustive
	case FlushTables:
		ctx.WriteKeyWord("TABLES")
		for i, v := range n.Tables {
			if i == 0 {
				ctx.WritePlain(" ")
			} else {
				ctx.WritePlain(", ")
			}
			if err := v.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore FlushStmt.Tables[%d]: %w", i, err)
			}
		}
		if n.ReadLock {
			ctx.WriteKeyWord(" WITH READ LOCK")
		}
		if n.ForExport {
			ctx.WriteKeyWord(" FOR EXPORT")
		}
	case FlushPrivileges:
		ctx.WriteKeyWord("PRIVILEGES")
	case FlushStatus:
		ctx.WriteKeyWord("STATUS")
	case FlushHosts:
		ctx.WriteKeyWord("HOSTS")
	case FlushUserResources:
		ctx.WriteKeyWord("USER_RESOURCES")
	case FlushOptimizerCosts:
		ctx.WriteKeyWord("OPTIMIZER_COSTS")
	case FlushLogs:
		var logType string
		switch n.LogType {
		case LogTypeDefault:
			logType = "LOGS"
		case LogTypeBinary:
			logType = "BINARY LOGS"
		case LogTypeEngine:
			logType = "ENGINE LOGS"
		case LogTypeError:
			logType = "ERROR LOGS"
		case LogTypeGeneral:
			logType = "GENERAL LOGS"
		case LogTypeSlow:
			logType = "SLOW LOGS"
		case LogTypeRelay:
			logType = "RELAY LOGS"
		}
		ctx.WriteKeyWord(logType)
		if n.LogType == LogTypeRelay && n.Channel != "" {
			ctx.WriteKeyWord(" FOR CHANNEL ")
			ctx.WriteString(n.Channel)
		}
	default:
		return errors.New("unsupported type of FlushStmt")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *FlushStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FlushStmt)
	for i, t := range n.Tables {
		node, ok := t.Accept(v)
		if !ok {
			return n, false
		}
		n.Tables[i] = node.(*TableName)
	}
	return v.Leave(n)
}

// KillStmt is a statement to kill a query or connection.
type KillStmt struct {
	stmtNode

	// Query indicates whether terminate a single query on this connection or the whole connection.
	// If Query is true, terminates the statement the connection is currently executing, but leaves the connection itself intact.
	// If Query is false, terminates the connection associated with the given ConnectionID, after terminating any statement the connection is executing.
	Query        bool
	ConnectionID uint64

	Expr ExprNode
}

// Restore implements Node interface.
func (n *KillStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("KILL")
	if n.Query {
		ctx.WriteKeyWord(" QUERY")
	}
	if n.Expr != nil {
		ctx.WriteKeyWord(" ")
		if err := n.Expr.Restore(ctx); err != nil {
			return err
		}
	} else {
		ctx.WritePlainf(" %d", n.ConnectionID)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *KillStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*KillStmt)
	if n.Expr != nil {
		node, ok := n.Expr.Accept(v)
		if !ok {
			return n, false
		}
		n.Expr = node.(ExprNode)
	}
	return v.Leave(n)
}

// HelpStmt is the HELP statement.
// See https://dev.mysql.com/doc/refman/8.0/en/help.html
type HelpStmt struct {
	stmtNode

	Topic string
}

// Restore implements Node interface.
func (n *HelpStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("HELP ")
	ctx.WriteString(n.Topic)
	return nil
}

// Accept implements Node Accept interface.
func (n *HelpStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*HelpStmt)
	return v.Leave(n)
}

// DiagnosticsScope is the optional scope of a GET DIAGNOSTICS statement.
type DiagnosticsScope int

// DiagnosticsScope values.
const (
	DiagnosticsScopeNone DiagnosticsScope = iota
	DiagnosticsScopeCurrent
	DiagnosticsScopeStacked
)

// DiagnosticsItem is one `target = item_name` element of GET DIAGNOSTICS.
type DiagnosticsItem struct {
	Target ExprNode
	Name   string
}

var statementInformationItems = map[string]struct{}{
	"NUMBER":    {},
	"ROW_COUNT": {},
}

var conditionInformationItems = map[string]struct{}{
	"CLASS_ORIGIN":       {},
	"SUBCLASS_ORIGIN":    {},
	"RETURNED_SQLSTATE":  {},
	"MESSAGE_TEXT":       {},
	"MYSQL_ERRNO":        {},
	"CONSTRAINT_CATALOG": {},
	"CONSTRAINT_SCHEMA":  {},
	"CONSTRAINT_NAME":    {},
	"CATALOG_NAME":       {},
	"SCHEMA_NAME":        {},
	"TABLE_NAME":         {},
	"COLUMN_NAME":        {},
	"CURSOR_NAME":        {},
}

// IsStatementInformationItem reports whether name is a valid GET DIAGNOSTICS
// statement information item. MySQL keeps the statement and condition item
// sets disjoint and rejects mixing them with a syntax error.
func IsStatementInformationItem(name string) bool {
	_, ok := statementInformationItems[name]
	return ok
}

// IsConditionInformationItem reports whether name is a valid GET DIAGNOSTICS
// condition information item.
func IsConditionInformationItem(name string) bool {
	_, ok := conditionInformationItems[name]
	return ok
}

// GetDiagnosticsStmt is the GET [CURRENT|STACKED] DIAGNOSTICS statement.
// See https://dev.mysql.com/doc/refman/8.0/en/get-diagnostics.html
type GetDiagnosticsStmt struct {
	stmtNode

	Scope DiagnosticsScope
	// ConditionNumber is non-nil for the CONDITION form.
	ConditionNumber ExprNode
	Items           []*DiagnosticsItem
}

// Restore implements Node interface.
func (n *GetDiagnosticsStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("GET ")
	switch n.Scope {
	case DiagnosticsScopeCurrent:
		ctx.WriteKeyWord("CURRENT ")
	case DiagnosticsScopeStacked:
		ctx.WriteKeyWord("STACKED ")
	case DiagnosticsScopeNone:
	}
	ctx.WriteKeyWord("DIAGNOSTICS")
	if n.ConditionNumber != nil {
		ctx.WriteKeyWord(" CONDITION ")
		if err := n.ConditionNumber.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore GetDiagnosticsStmt.ConditionNumber: %w", err)
		}
	}
	for i, item := range n.Items {
		if i != 0 {
			ctx.WritePlain(",")
		}
		ctx.WritePlain(" ")
		if err := item.Target.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore GetDiagnosticsStmt.Items: %w", err)
		}
		ctx.WritePlain(" = ")
		ctx.WriteKeyWord(item.Name)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *GetDiagnosticsStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*GetDiagnosticsStmt)
	if n.ConditionNumber != nil {
		node, ok := n.ConditionNumber.Accept(v)
		if !ok {
			return n, false
		}
		n.ConditionNumber = node.(ExprNode)
	}
	for _, item := range n.Items {
		node, ok := item.Target.Accept(v)
		if !ok {
			return n, false
		}
		item.Target = node.(ExprNode)
	}
	return v.Leave(n)
}

// SavepointStmt is the statement of SAVEPOINT.
type SavepointStmt struct {
	stmtNode
	// Name is the savepoint name.
	Name string
}

// Restore implements Node interface.
func (n *SavepointStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SAVEPOINT ")
	ctx.WritePlain(n.Name)
	return nil
}

// Accept implements Node Accept interface.
func (n *SavepointStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	n = newNode.(*SavepointStmt)
	return v.Leave(n)
}

// ReleaseSavepointStmt is the statement of RELEASE SAVEPOINT.
type ReleaseSavepointStmt struct {
	stmtNode
	// Name is the savepoint name.
	Name string
}

// Restore implements Node interface.
func (n *ReleaseSavepointStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("RELEASE SAVEPOINT ")
	ctx.WritePlain(n.Name)
	return nil
}

// Accept implements Node Accept interface.
func (n *ReleaseSavepointStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	n = newNode.(*ReleaseSavepointStmt)
	return v.Leave(n)
}

// SetStmt is the statement to set variables.
type SetStmt struct {
	stmtNode
	// Variables is the list of variable assignment.
	Variables []*VariableAssignment
}

// Restore implements Node interface.
func (n *SetStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SET ")
	for i, v := range n.Variables {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore SetStmt.Variables[%d]: %w", i, err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *SetStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SetStmt)
	for i, val := range n.Variables {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Variables[i] = node.(*VariableAssignment)
	}
	return v.Leave(n)
}

// SetPwdStmt is a statement to assign a password to user account.
// See https://dev.mysql.com/doc/refman/5.7/en/set-password.html
type SetPwdStmt struct {
	stmtNode

	User                  *auth.UserIdentity
	Password              string
	RetainCurrentPassword bool
	// Random is the SET PASSWORD ... TO RANDOM form (MySQL 8.0.18+).
	Random bool
	// HasReplace / ReplaceString carry REPLACE 'current password'
	// (MySQL 8.0.14+ password verification).
	HasReplace    bool
	ReplaceString string
}

// Restore implements Node interface.
func (n *SetPwdStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SET PASSWORD")
	if n.User != nil {
		ctx.WriteKeyWord(" FOR ")
		if err := n.User.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore SetPwdStmt.User: %w", err)
		}
	}
	if n.Random {
		ctx.WriteKeyWord(" TO RANDOM")
	} else {
		ctx.WritePlain("=")
		ctx.WriteString(n.Password)
	}
	if n.HasReplace {
		ctx.WriteKeyWord(" REPLACE ")
		ctx.WriteString(n.ReplaceString)
	}
	if n.RetainCurrentPassword {
		ctx.WriteKeyWord(" RETAIN CURRENT PASSWORD")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *SetPwdStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SetPwdStmt)
	return v.Leave(n)
}

// SetRoleStmtType is the type for FLUSH statement.
type SetRoleStmtType int

// SetRole statement types.
const (
	SetRoleDefault SetRoleStmtType = iota
	SetRoleNone
	SetRoleAll
	SetRoleAllExcept
	SetRoleRegular
)

type SetRoleStmt struct {
	stmtNode

	SetRoleOpt SetRoleStmtType
	RoleList   []*auth.RoleIdentity
}

func (n *SetRoleStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SET ROLE")
	switch n.SetRoleOpt { //nolint:exhaustive
	case SetRoleDefault:
		ctx.WriteKeyWord(" DEFAULT")
	case SetRoleNone:
		ctx.WriteKeyWord(" NONE")
	case SetRoleAll:
		ctx.WriteKeyWord(" ALL")
	case SetRoleAllExcept:
		ctx.WriteKeyWord(" ALL EXCEPT")
	}
	for i, role := range n.RoleList {
		ctx.WritePlain(" ")
		err := role.Restore(ctx)
		if err != nil {
			return fmt.Errorf("an error occurred while restore SetRoleStmt.RoleList: %w", err)
		}
		if i != len(n.RoleList)-1 {
			ctx.WritePlain(",")
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *SetRoleStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SetRoleStmt)
	return v.Leave(n)
}

type SetDefaultRoleStmt struct {
	stmtNode

	SetRoleOpt SetRoleStmtType
	RoleList   []*auth.RoleIdentity
	UserList   []*auth.UserIdentity
}

func (n *SetDefaultRoleStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SET DEFAULT ROLE")
	switch n.SetRoleOpt { //nolint:exhaustive
	case SetRoleNone:
		ctx.WriteKeyWord(" NONE")
	case SetRoleAll:
		ctx.WriteKeyWord(" ALL")
	default:
	}
	for i, role := range n.RoleList {
		ctx.WritePlain(" ")
		err := role.Restore(ctx)
		if err != nil {
			return fmt.Errorf("an error occurred while restore SetDefaultRoleStmt.RoleList: %w", err)
		}
		if i != len(n.RoleList)-1 {
			ctx.WritePlain(",")
		}
	}
	ctx.WritePlain(" TO")
	for i, user := range n.UserList {
		ctx.WritePlain(" ")
		err := user.Restore(ctx)
		if err != nil {
			return fmt.Errorf("an error occurred while restore SetDefaultRoleStmt.UserList: %w", err)
		}
		if i != len(n.UserList)-1 {
			ctx.WritePlain(",")
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *SetDefaultRoleStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*SetDefaultRoleStmt)
	return v.Leave(n)
}

// UserSpec is used for parsing create user statement.
type UserSpec struct {
	User               *auth.UserIdentity
	AuthOpt            *AuthOption
	DualPasswordOption DualPasswordOptionType
	IsRole             bool
	// ExtraAuthFactors are the second and third authentication factors of a
	// multi-factor spec: IDENTIFIED ... AND IDENTIFIED ... [AND IDENTIFIED ...]
	// (MySQL 8.0.27+). AuthOpt is the first factor.
	ExtraAuthFactors []*AuthOption
}

// Restore implements Node interface.
func (n *UserSpec) Restore(ctx *format.RestoreCtx) error {
	if err := n.User.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore UserSpec.User: %w", err)
	}
	if n.AuthOpt != nil {
		ctx.WritePlain(" ")
		if err := n.AuthOpt.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore UserSpec.AuthOpt: %w", err)
		}
	}
	for i, factor := range n.ExtraAuthFactors {
		ctx.WriteKeyWord(" AND ")
		if err := factor.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore UserSpec.ExtraAuthFactors[%d]: %w", i, err)
		}
	}
	if n.DualPasswordOption != 0 {
		ctx.WritePlain(" ")
		if err := n.DualPasswordOption.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore UserSpec.DualPasswordOption: %w", err)
		}
	}
	return nil
}

// SecurityString formats the UserSpec without password information. The
// dual-password clause (RETAIN CURRENT PASSWORD / DISCARD OLD PASSWORD) is
// non-secret and is surfaced verbatim so the redacted output preserves the
// fact that the statement targets the secondary-password slot.
func (n *UserSpec) SecurityString() string {
	withPassword := false
	if opt := n.AuthOpt; opt != nil {
		if len(opt.AuthString) > 0 || len(opt.HashString) > 0 {
			withPassword = true
		}
	}
	dualClause := ""
	switch n.DualPasswordOption {
	case DualPasswordRetainCurrent:
		dualClause = " RETAIN CURRENT PASSWORD"
	case DualPasswordDiscardOld:
		dualClause = " DISCARD OLD PASSWORD"
	}
	if withPassword {
		return fmt.Sprintf("{%s password = ***%s}", n.User, dualClause)
	}
	if dualClause != "" {
		return fmt.Sprintf("{%s%s}", n.User, dualClause)
	}
	return n.User.String()
}

// DualPasswordOptionType identifies the per-UserSpec MySQL 8.0 dual-password
// clause (RETAIN CURRENT PASSWORD or DISCARD OLD PASSWORD). The grammar
// attaches it to the UserSpec rather than to AlterUserStmt because MySQL
// allows different dual-password actions per spec inside a multi-user ALTER
// USER statement. The zero value means "no dual-password clause".
type DualPasswordOptionType int

const (
	// DualPasswordRetainCurrent corresponds to RETAIN CURRENT PASSWORD.
	DualPasswordRetainCurrent DualPasswordOptionType = iota + 1
	// DualPasswordDiscardOld corresponds to DISCARD OLD PASSWORD.
	DualPasswordDiscardOld
)

// Restore implements Node interface.
func (t DualPasswordOptionType) Restore(ctx *format.RestoreCtx) error {
	switch t {
	case DualPasswordRetainCurrent:
		ctx.WriteKeyWord("RETAIN CURRENT PASSWORD")
	case DualPasswordDiscardOld:
		ctx.WriteKeyWord("DISCARD OLD PASSWORD")
	default:
		return fmt.Errorf("unsupported DualPasswordOptionType %d", t)
	}
	return nil
}

type AuthTokenOrTLSOption struct {
	Type  AuthTokenOrTLSOptionType
	Value string
}

func (t *AuthTokenOrTLSOption) Restore(ctx *format.RestoreCtx) error {
	switch t.Type {
	case TlsNone:
		ctx.WriteKeyWord("NONE")
	case Ssl:
		ctx.WriteKeyWord("SSL")
	case X509:
		ctx.WriteKeyWord("X509")
	case Cipher:
		ctx.WriteKeyWord("CIPHER ")
		ctx.WriteString(t.Value)
	case Issuer:
		ctx.WriteKeyWord("ISSUER ")
		ctx.WriteString(t.Value)
	case Subject:
		ctx.WriteKeyWord("SUBJECT ")
		ctx.WriteString(t.Value)
	case SAN:
		ctx.WriteKeyWord("SAN ")
		ctx.WriteString(t.Value)
	case TokenIssuer:
		ctx.WriteKeyWord("TOKEN_ISSUER ")
		ctx.WriteString(t.Value)
	default:
		return fmt.Errorf("unsupported AuthTokenOrTLSOption.Type %d", t.Type)
	}
	return nil
}

type AuthTokenOrTLSOptionType int

const (
	TlsNone AuthTokenOrTLSOptionType = iota
	Ssl
	X509
	Cipher
	Issuer
	Subject
	SAN
	TokenIssuer
)

func (t AuthTokenOrTLSOptionType) String() string {
	switch t {
	case TlsNone:
		return "NONE"
	case Ssl:
		return "SSL"
	case X509:
		return "X509"
	case Cipher:
		return "CIPHER"
	case Issuer:
		return "ISSUER"
	case Subject:
		return "SUBJECT"
	case SAN:
		return "SAN"
	case TokenIssuer:
		return "TOKEN_ISSUER"
	default:
		return "UNKNOWN"
	}
}

const (
	MaxQueriesPerHour = iota + 1
	MaxUpdatesPerHour
	MaxConnectionsPerHour
	MaxUserConnections
)

type ResourceOption struct {
	Type  int
	Count int64
}

func (r *ResourceOption) Restore(ctx *format.RestoreCtx) error {
	switch r.Type {
	case MaxQueriesPerHour:
		ctx.WriteKeyWord("MAX_QUERIES_PER_HOUR ")
	case MaxUpdatesPerHour:
		ctx.WriteKeyWord("MAX_UPDATES_PER_HOUR ")
	case MaxConnectionsPerHour:
		ctx.WriteKeyWord("MAX_CONNECTIONS_PER_HOUR ")
	case MaxUserConnections:
		ctx.WriteKeyWord("MAX_USER_CONNECTIONS ")
	default:
		return fmt.Errorf("unsupported ResourceOption.Type %d", r.Type)
	}
	ctx.WritePlainf("%d", r.Count)
	return nil
}

const (
	PasswordExpire = iota + 1
	PasswordExpireDefault
	PasswordExpireNever
	PasswordExpireInterval
	PasswordHistory
	PasswordHistoryDefault
	PasswordReuseInterval
	PasswordReuseDefault
	Lock
	Unlock
	FailedLoginAttempts
	PasswordLockTime
	PasswordLockTimeUnbounded
	UserCommentType
	UserAttributeType
	PasswordRequireCurrentDefault
	PasswordRequireCurrent
	PasswordRequireCurrentOptional

	UserResourceGroupName
)

type PasswordOrLockOption struct {
	Type  int
	Count int64
}

func (p *PasswordOrLockOption) Restore(ctx *format.RestoreCtx) error {
	switch p.Type {
	case PasswordExpire:
		ctx.WriteKeyWord("PASSWORD EXPIRE")
	case PasswordExpireDefault:
		ctx.WriteKeyWord("PASSWORD EXPIRE DEFAULT")
	case PasswordExpireNever:
		ctx.WriteKeyWord("PASSWORD EXPIRE NEVER")
	case PasswordExpireInterval:
		ctx.WriteKeyWord("PASSWORD EXPIRE INTERVAL")
		ctx.WritePlainf(" %d", p.Count)
		ctx.WriteKeyWord(" DAY")
	case Lock:
		ctx.WriteKeyWord("ACCOUNT LOCK")
	case Unlock:
		ctx.WriteKeyWord("ACCOUNT UNLOCK")
	case FailedLoginAttempts:
		ctx.WriteKeyWord("FAILED_LOGIN_ATTEMPTS")
		ctx.WritePlainf(" %d", p.Count)
	case PasswordLockTime:
		ctx.WriteKeyWord("PASSWORD_LOCK_TIME")
		ctx.WritePlainf(" %d", p.Count)
	case PasswordLockTimeUnbounded:
		ctx.WriteKeyWord("PASSWORD_LOCK_TIME UNBOUNDED")
	case PasswordHistory:
		ctx.WriteKeyWord("PASSWORD HISTORY")
		ctx.WritePlainf(" %d", p.Count)
	case PasswordHistoryDefault:
		ctx.WriteKeyWord("PASSWORD HISTORY DEFAULT")
	case PasswordReuseInterval:
		ctx.WriteKeyWord("PASSWORD REUSE INTERVAL")
		ctx.WritePlainf(" %d", p.Count)
		ctx.WriteKeyWord(" DAY")
	case PasswordReuseDefault:
		ctx.WriteKeyWord("PASSWORD REUSE INTERVAL DEFAULT")
	case PasswordRequireCurrentDefault:
		ctx.WriteKeyWord("PASSWORD REQUIRE CURRENT DEFAULT")
	case PasswordRequireCurrent:
		ctx.WriteKeyWord("PASSWORD REQUIRE CURRENT")
	case PasswordRequireCurrentOptional:
		ctx.WriteKeyWord("PASSWORD REQUIRE CURRENT OPTIONAL")
	default:
		return fmt.Errorf("unsupported PasswordOrLockOption.Type %d", p.Type)
	}
	return nil
}

type CommentOrAttributeOption struct {
	Type  int
	Value string
}

func (c *CommentOrAttributeOption) Restore(ctx *format.RestoreCtx) error {
	switch c.Type {
	case UserCommentType:
		ctx.WriteKeyWord(" COMMENT ")
		ctx.WriteString(c.Value)
	case UserAttributeType:
		ctx.WriteKeyWord(" ATTRIBUTE ")
		ctx.WriteString(c.Value)
	}
	return nil
}

type ResourceGroupNameOption struct {
	Value string
}

func (c *ResourceGroupNameOption) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord(" RESOURCE GROUP ")
	ctx.WriteName(c.Value)
	return nil
}

// CreateUserStmt creates user account.
// See https://dev.mysql.com/doc/refman/8.0/en/create-user.html
type CreateUserStmt struct {
	stmtNode

	IsCreateRole             bool
	IfNotExists              bool
	Specs                    []*UserSpec
	DefaultRoles             []*auth.RoleIdentity
	AuthTokenOrTLSOptions    []*AuthTokenOrTLSOption
	ResourceOptions          []*ResourceOption
	PasswordOrLockOptions    []*PasswordOrLockOption
	CommentOrAttributeOption *CommentOrAttributeOption
	ResourceGroupNameOption  *ResourceGroupNameOption
}

// Restore implements Node interface.
func (n *CreateUserStmt) Restore(ctx *format.RestoreCtx) error {
	if n.IsCreateRole {
		ctx.WriteKeyWord("CREATE ROLE ")
	} else {
		ctx.WriteKeyWord("CREATE USER ")
	}
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}
	for i, v := range n.Specs {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore CreateUserStmt.Specs[%d]: %w", i, err)
		}
	}

	if len(n.DefaultRoles) != 0 {
		ctx.WriteKeyWord(" DEFAULT ROLE ")
		for i, role := range n.DefaultRoles {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := role.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore CreateUserStmt.DefaultRoles[%d]: %w", i, err)
			}
		}
	}

	if len(n.AuthTokenOrTLSOptions) != 0 {
		ctx.WriteKeyWord(" REQUIRE ")
	}

	for i, option := range n.AuthTokenOrTLSOptions {
		if i != 0 {
			ctx.WriteKeyWord(" AND ")
		}
		if err := option.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore CreateUserStmt.AuthTokenOrTLSOptions[%d]: %w", i, err)
		}
	}

	if len(n.ResourceOptions) != 0 {
		ctx.WriteKeyWord(" WITH")
	}

	for i, v := range n.ResourceOptions {
		ctx.WritePlain(" ")
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore CreateUserStmt.ResourceOptions[%d]: %w", i, err)
		}
	}

	for i, v := range n.PasswordOrLockOptions {
		ctx.WritePlain(" ")
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore CreateUserStmt.PasswordOrLockOptions[%d]: %w", i, err)
		}
	}

	if n.CommentOrAttributeOption != nil {
		if err := n.CommentOrAttributeOption.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore CreateUserStmt.CommentOrAttributeOption: %w", err)
		}
	}

	if n.ResourceGroupNameOption != nil {
		if err := n.ResourceGroupNameOption.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore CreateUserStmt.ResourceGroupNameOption: %w", err)
		}
	}

	return nil
}

// Accept implements Node Accept interface.
func (n *CreateUserStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateUserStmt)
	return v.Leave(n)
}

// AlterUserStmt modifies user account.
// See https://dev.mysql.com/doc/refman/8.0/en/alter-user.html
type AlterUserStmt struct {
	stmtNode

	IfExists    bool
	CurrentAuth *AuthOption
	// CurrentDualPasswordOption carries the dual-password clause attached to
	// the `ALTER USER USER() ...` (current-user) form. The named-user form
	// stores its dual-password clause on the per-UserSpec DualPasswordOption.
	CurrentDualPasswordOption DualPasswordOptionType
	Specs                     []*UserSpec
	AuthTokenOrTLSOptions     []*AuthTokenOrTLSOption
	ResourceOptions           []*ResourceOption
	PasswordOrLockOptions     []*PasswordOrLockOption
	CommentOrAttributeOption  *CommentOrAttributeOption
	ResourceGroupNameOption   *ResourceGroupNameOption
}

// Restore implements Node interface.
func (n *AlterUserStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER USER ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	if n.CurrentAuth != nil {
		ctx.WriteKeyWord("USER")
		ctx.WritePlain("() ")
		if err := n.CurrentAuth.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterUserStmt.CurrentAuth: %w", err)
		}
		if n.CurrentDualPasswordOption != 0 {
			ctx.WritePlain(" ")
			if err := n.CurrentDualPasswordOption.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore AlterUserStmt.CurrentDualPasswordOption: %w", err)
			}
		}
	} else if n.CurrentDualPasswordOption != 0 {
		// Standalone DISCARD OLD PASSWORD on the current-user form (no IDENTIFIED BY).
		ctx.WriteKeyWord("USER")
		ctx.WritePlain("() ")
		if err := n.CurrentDualPasswordOption.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterUserStmt.CurrentDualPasswordOption: %w", err)
		}
	}
	for i, v := range n.Specs {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterUserStmt.Specs[%d]: %w", i, err)
		}
	}

	if len(n.AuthTokenOrTLSOptions) != 0 {
		ctx.WriteKeyWord(" REQUIRE ")
	}

	for i, option := range n.AuthTokenOrTLSOptions {
		if i != 0 {
			ctx.WriteKeyWord(" AND ")
		}
		if err := option.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterUserStmt.AuthTokenOrTLSOptions[%d]: %w", i, err)
		}
	}

	if len(n.ResourceOptions) != 0 {
		ctx.WriteKeyWord(" WITH")
	}

	for i, v := range n.ResourceOptions {
		ctx.WritePlain(" ")
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterUserStmt.ResourceOptions[%d]: %w", i, err)
		}
	}

	for i, v := range n.PasswordOrLockOptions {
		ctx.WritePlain(" ")
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterUserStmt.PasswordOrLockOptions[%d]: %w", i, err)
		}
	}

	if n.CommentOrAttributeOption != nil {
		if err := n.CommentOrAttributeOption.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterUserStmt.CommentOrAttributeOption: %w", err)
		}
	}

	if n.ResourceGroupNameOption != nil {
		if err := n.ResourceGroupNameOption.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterUserStmt.ResourceGroupNameOption: %w", err)
		}
	}

	return nil
}

// Accept implements Node Accept interface.
func (n *AlterUserStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterUserStmt)
	return v.Leave(n)
}

// RedoLogAction is the {ENABLE | DISABLE} INNODB REDO_LOG action of ALTER INSTANCE.
type RedoLogAction int

// RedoLogAction values.
const (
	RedoLogActionNone RedoLogAction = iota
	RedoLogActionEnable
	RedoLogActionDisable
)

// AlterInstanceStmt modifies instance.
// See https://dev.mysql.com/doc/refman/8.4/en/alter-instance.html
type AlterInstanceStmt struct {
	stmtNode

	ReloadTLS         bool
	NoRollbackOnError bool
	// TLSChannel is the optional FOR CHANNEL of RELOAD TLS.
	TLSChannel string
	// RotateMasterKey is "INNODB" or "BINLOG" for ROTATE ... MASTER KEY.
	RotateMasterKey string
	// RedoLog is set for {ENABLE | DISABLE} INNODB REDO_LOG.
	RedoLog       RedoLogAction
	ReloadKeyring bool
}

// Restore implements Node interface.
func (n *AlterInstanceStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER INSTANCE")
	switch {
	case n.ReloadTLS:
		ctx.WriteKeyWord(" RELOAD TLS")
		if n.TLSChannel != "" {
			ctx.WriteKeyWord(" FOR CHANNEL ")
			ctx.WriteName(n.TLSChannel)
		}
		if n.NoRollbackOnError {
			ctx.WriteKeyWord(" NO ROLLBACK ON ERROR")
		}
	case n.RotateMasterKey != "":
		ctx.WriteKeyWord(" ROTATE ")
		ctx.WriteKeyWord(n.RotateMasterKey)
		ctx.WriteKeyWord(" MASTER KEY")
	case n.RedoLog == RedoLogActionEnable:
		ctx.WriteKeyWord(" ENABLE INNODB REDO_LOG")
	case n.RedoLog == RedoLogActionDisable:
		ctx.WriteKeyWord(" DISABLE INNODB REDO_LOG")
	case n.ReloadKeyring:
		ctx.WriteKeyWord(" RELOAD KEYRING")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *AlterInstanceStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterInstanceStmt)
	return v.Leave(n)
}

// DropUserStmt creates user account.
// See http://dev.mysql.com/doc/refman/5.7/en/drop-user.html
type DropUserStmt struct {
	stmtNode

	IfExists   bool
	IsDropRole bool
	UserList   []*auth.UserIdentity
}

// Restore implements Node interface.
func (n *DropUserStmt) Restore(ctx *format.RestoreCtx) error {
	if n.IsDropRole {
		ctx.WriteKeyWord("DROP ROLE ")
	} else {
		ctx.WriteKeyWord("DROP USER ")
	}
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	for i, v := range n.UserList {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore DropUserStmt.UserList[%d]: %w", i, err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *DropUserStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropUserStmt)
	return v.Leave(n)
}

// DoStmt is the struct for DO statement.
type DoStmt struct {
	stmtNode

	Exprs []ExprNode
}

// Restore implements Node interface.
func (n *DoStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DO ")
	for i, v := range n.Exprs {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore DoStmt.Exprs[%d]: %w", i, err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *DoStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DoStmt)
	for i, val := range n.Exprs {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Exprs[i] = node.(ExprNode)
	}
	return v.Leave(n)
}

// RoleOrPriv is a temporary structure to be further processed into auth.RoleIdentity or PrivElem
type RoleOrPriv struct {
	Symbols string // hold undecided symbols
	Node    any    // hold auth.RoleIdentity or PrivElem that can be sure when parsing
}

func (n *RoleOrPriv) ToRole() (*auth.RoleIdentity, error) {
	if n.Node != nil {
		if r, ok := n.Node.(*auth.RoleIdentity); ok {
			return r, nil
		}
		return nil, fmt.Errorf("can't convert to RoleIdentity, type %T", n.Node)
	}
	return &auth.RoleIdentity{Username: n.Symbols, Hostname: "%"}, nil
}

func (n *RoleOrPriv) ToPriv() (*PrivElem, error) {
	if n.Node != nil {
		if p, ok := n.Node.(*PrivElem); ok {
			return p, nil
		}
		if r, ok := n.Node.(*auth.RoleIdentity); ok && r.Hostname == "%" && r.Username != "" {
			// A quoted dynamic privilege, e.g. GRANT 'SYSTEM_VARIABLES_ADMIN'
			// ON *.*: the grammar reads the string as a role name, and the
			// privilege position (an ON clause is present) resolves it here.
			return &PrivElem{Priv: mysql.ExtendedPriv, Name: r.Username}, nil
		}
		return nil, fmt.Errorf("can't convert to PrivElem, type %T", n.Node)
	}
	if len(n.Symbols) == 0 {
		return nil, errors.New("symbols should not be length 0")
	}
	return &PrivElem{Priv: mysql.ExtendedPriv, Name: n.Symbols}, nil
}

// PrivElem is the privilege type and optional column list.
type PrivElem struct {
	node

	Priv mysql.PrivilegeType
	Cols []*ColumnName
	Name string
}

// Restore implements Node interface.
func (n *PrivElem) Restore(ctx *format.RestoreCtx) error {
	switch n.Priv { //nolint:exhaustive
	case mysql.AllPriv:
		ctx.WriteKeyWord("ALL")
	case mysql.ExtendedPriv:
		ctx.WriteKeyWord(n.Name)
	default:
		str, ok := mysql.Priv2Str[n.Priv]
		if !ok {
			return errors.New("undefined privilege type")
		}
		ctx.WriteKeyWord(str)
	}
	if n.Cols != nil {
		ctx.WritePlain(" (")
		for i, v := range n.Cols {
			if i != 0 {
				ctx.WritePlain(",")
			}
			if err := v.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore PrivElem.Cols[%d]: %w", i, err)
			}
		}
		ctx.WritePlain(")")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *PrivElem) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*PrivElem)
	for i, val := range n.Cols {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Cols[i] = node.(*ColumnName)
	}
	return v.Leave(n)
}

// ObjectTypeType is the type for object type.
type ObjectTypeType int

const (
	// ObjectTypeNone is for empty object type.
	ObjectTypeNone ObjectTypeType = iota + 1
	// ObjectTypeTable means the following object is a table.
	ObjectTypeTable
	// ObjectTypeFunction means the following object is a stored function.
	ObjectTypeFunction
	// ObjectTypeProcedure means the following object is a stored procedure.
	ObjectTypeProcedure
	// ObjectTypeLibrary means the following object is a library (MySQL 9.x).
	ObjectTypeLibrary
)

// Restore implements Node interface.
func (n ObjectTypeType) Restore(ctx *format.RestoreCtx) error {
	switch n {
	case ObjectTypeNone:
		// do nothing
	case ObjectTypeTable:
		ctx.WriteKeyWord("TABLE")
	case ObjectTypeFunction:
		ctx.WriteKeyWord("FUNCTION")
	case ObjectTypeProcedure:
		ctx.WriteKeyWord("PROCEDURE")
	case ObjectTypeLibrary:
		ctx.WriteKeyWord("LIBRARY")
	default:
		return errors.New("unsupported object type")
	}
	return nil
}

// GrantLevelType is the type for grant level.
type GrantLevelType int

const (
	// GrantLevelNone is the dummy const for default value.
	GrantLevelNone GrantLevelType = iota + 1
	// GrantLevelGlobal means the privileges are administrative or apply to all databases on a given server.
	GrantLevelGlobal
	// GrantLevelDB means the privileges apply to all objects in a given database.
	GrantLevelDB
	// GrantLevelTable means the privileges apply to all columns in a given table.
	GrantLevelTable
)

// GrantLevel is used for store the privilege scope.
type GrantLevel struct {
	Level     GrantLevelType
	DBName    string
	TableName string
}

// Restore implements Node interface.
func (n *GrantLevel) Restore(ctx *format.RestoreCtx) error {
	switch n.Level { //nolint:exhaustive
	case GrantLevelDB:
		if n.DBName == "" {
			ctx.WritePlain("*")
		} else {
			ctx.WriteName(n.DBName)
			ctx.WritePlain(".*")
		}
	case GrantLevelGlobal:
		ctx.WritePlain("*.*")
	case GrantLevelTable:
		if n.DBName != "" {
			ctx.WriteName(n.DBName)
			ctx.WritePlain(".")
		}
		ctx.WriteName(n.TableName)
	}
	return nil
}

// RevokeStmt is the struct for REVOKE statement.
type RevokeStmt struct {
	stmtNode

	IfExists   bool
	Privs      []*PrivElem
	ObjectType ObjectTypeType
	Level      *GrantLevel
	Users      []*UserSpec
	// IgnoreUnknownUser is the trailing IGNORE UNKNOWN USER clause (MySQL 8.0.30+).
	IgnoreUnknownUser bool
}

// Restore implements Node interface.
func (n *RevokeStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("REVOKE ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	for i, v := range n.Privs {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore RevokeStmt.Privs[%d]: %w", i, err)
		}
	}
	ctx.WriteKeyWord(" ON ")
	if n.ObjectType != ObjectTypeNone {
		if err := n.ObjectType.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore RevokeStmt.ObjectType: %w", err)
		}
		ctx.WritePlain(" ")
	}
	if err := n.Level.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore RevokeStmt.Level: %w", err)
	}
	ctx.WriteKeyWord(" FROM ")
	for i, v := range n.Users {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore RevokeStmt.Users[%d]: %w", i, err)
		}
	}
	if n.IgnoreUnknownUser {
		ctx.WriteKeyWord(" IGNORE UNKNOWN USER")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *RevokeStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RevokeStmt)
	for i, val := range n.Privs {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Privs[i] = node.(*PrivElem)
	}
	return v.Leave(n)
}

// RevokeRoleStmt is the struct for REVOKE <roles> FROM <users>.
type RevokeRoleStmt struct {
	stmtNode

	IfExists bool
	Roles    []*auth.RoleIdentity
	Users    []*auth.UserIdentity
	// IgnoreUnknownUser is the trailing IGNORE UNKNOWN USER clause (MySQL 8.0.30+).
	IgnoreUnknownUser bool
}

// Restore implements Node interface.
func (n *RevokeRoleStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("REVOKE ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	for i, role := range n.Roles {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := role.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore RevokeRoleStmt.Roles[%d]: %w", i, err)
		}
	}
	ctx.WriteKeyWord(" FROM ")
	for i, v := range n.Users {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore RevokeRoleStmt.Users[%d]: %w", i, err)
		}
	}
	if n.IgnoreUnknownUser {
		ctx.WriteKeyWord(" IGNORE UNKNOWN USER")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *RevokeRoleStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RevokeRoleStmt)
	return v.Leave(n)
}

// GrantAsClause is the trailing AS user [WITH ROLE ...] clause of GRANT
// (MySQL 8.0.16+): the grant is evaluated as if executed by the given user
// with the given roles active.
type GrantAsClause struct {
	User *auth.UserIdentity
	// WithRole reports whether a WITH ROLE clause was present; SetRoleOpt's
	// zero value (SetRoleDefault) is a valid WITH ROLE DEFAULT otherwise.
	WithRole   bool
	SetRoleOpt SetRoleStmtType
	RoleList   []*auth.RoleIdentity
}

// Restore implements Node interface.
func (n *GrantAsClause) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("AS ")
	if err := n.User.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore GrantAsClause.User: %w", err)
	}
	if n.WithRole {
		ctx.WriteKeyWord(" WITH ROLE")
		switch n.SetRoleOpt { //nolint:exhaustive
		case SetRoleDefault:
			ctx.WriteKeyWord(" DEFAULT")
		case SetRoleNone:
			ctx.WriteKeyWord(" NONE")
		case SetRoleAll:
			ctx.WriteKeyWord(" ALL")
		case SetRoleAllExcept:
			ctx.WriteKeyWord(" ALL EXCEPT")
		}
		for i, role := range n.RoleList {
			ctx.WritePlain(" ")
			if err := role.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore GrantAsClause.RoleList[%d]: %w", i, err)
			}
			if i != len(n.RoleList)-1 {
				ctx.WritePlain(",")
			}
		}
	}
	return nil
}

// GrantStmt is the struct for GRANT statement.
type GrantStmt struct {
	stmtNode

	Privs                 []*PrivElem
	ObjectType            ObjectTypeType
	Level                 *GrantLevel
	Users                 []*UserSpec
	AuthTokenOrTLSOptions []*AuthTokenOrTLSOption
	WithGrant             bool
	As                    *GrantAsClause
}

// Restore implements Node interface.
func (n *GrantStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("GRANT ")
	for i, v := range n.Privs {
		if i != 0 && v.Priv != 0 {
			ctx.WritePlain(", ")
		} else if v.Priv == 0 {
			ctx.WritePlain(" ")
		}
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore GrantStmt.Privs[%d]: %w", i, err)
		}
	}
	ctx.WriteKeyWord(" ON ")
	if n.ObjectType != ObjectTypeNone {
		if err := n.ObjectType.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore GrantStmt.ObjectType: %w", err)
		}
		ctx.WritePlain(" ")
	}
	if err := n.Level.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore GrantStmt.Level: %w", err)
	}
	ctx.WriteKeyWord(" TO ")
	for i, v := range n.Users {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore GrantStmt.Users[%d]: %w", i, err)
		}
	}
	if n.AuthTokenOrTLSOptions != nil {
		if len(n.AuthTokenOrTLSOptions) != 0 {
			ctx.WriteKeyWord(" REQUIRE ")
		}
		for i, option := range n.AuthTokenOrTLSOptions {
			if i != 0 {
				ctx.WriteKeyWord(" AND ")
			}
			if err := option.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore GrantStmt.AuthTokenOrTLSOptions[%d]: %w", i, err)
			}
		}
	}
	if n.WithGrant {
		ctx.WriteKeyWord(" WITH GRANT OPTION")
	}
	if n.As != nil {
		ctx.WritePlain(" ")
		if err := n.As.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore GrantStmt.As: %w", err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *GrantStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*GrantStmt)
	for i, val := range n.Privs {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Privs[i] = node.(*PrivElem)
	}
	return v.Leave(n)
}

// GrantProxyStmt is the struct for GRANT PROXY statement.
type GrantProxyStmt struct {
	stmtNode

	LocalUser     *auth.UserIdentity
	ExternalUsers []*auth.UserIdentity
	WithGrant     bool
}

// Accept implements Node Accept interface.
func (n *GrantProxyStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*GrantProxyStmt)
	return v.Leave(n)
}

// Restore implements Node interface.
func (n *GrantProxyStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("GRANT PROXY ON ")
	if err := n.LocalUser.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore GrantProxyStmt.LocalUser: %w", err)
	}
	ctx.WriteKeyWord(" TO ")
	for i, v := range n.ExternalUsers {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore GrantProxyStmt.ExternalUsers[%d]: %w", i, err)
		}
	}
	if n.WithGrant {
		ctx.WriteKeyWord(" WITH GRANT OPTION")
	}
	return nil
}

// GrantRoleStmt is the struct for GRANT TO statement.
type GrantRoleStmt struct {
	stmtNode

	Roles []*auth.RoleIdentity
	Users []*auth.UserIdentity
	// WithAdminOption is the trailing WITH ADMIN OPTION clause.
	WithAdminOption bool
}

// Accept implements Node Accept interface.
func (n *GrantRoleStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*GrantRoleStmt)
	return v.Leave(n)
}

// Restore implements Node interface.
func (n *GrantRoleStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("GRANT ")
	if len(n.Roles) > 0 {
		for i, role := range n.Roles {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := role.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore GrantRoleStmt.Roles[%d]: %w", i, err)
			}
		}
	}
	ctx.WriteKeyWord(" TO ")
	for i, v := range n.Users {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore GrantStmt.Users[%d]: %w", i, err)
		}
	}
	if n.WithAdminOption {
		ctx.WriteKeyWord(" WITH ADMIN OPTION")
	}
	return nil
}

// RevokeProxyStmt is the struct for REVOKE [IF EXISTS] PROXY ON user FROM users.
type RevokeProxyStmt struct {
	stmtNode

	IfExists      bool
	LocalUser     *auth.UserIdentity
	ExternalUsers []*auth.UserIdentity
	// IgnoreUnknownUser is the trailing IGNORE UNKNOWN USER clause (MySQL 8.0.30+).
	IgnoreUnknownUser bool
}

// Restore implements Node interface.
func (n *RevokeProxyStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("REVOKE ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	ctx.WriteKeyWord("PROXY ON ")
	if err := n.LocalUser.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore RevokeProxyStmt.LocalUser: %w", err)
	}
	ctx.WriteKeyWord(" FROM ")
	for i, v := range n.ExternalUsers {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := v.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore RevokeProxyStmt.ExternalUsers[%d]: %w", i, err)
		}
	}
	if n.IgnoreUnknownUser {
		ctx.WriteKeyWord(" IGNORE UNKNOWN USER")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *RevokeProxyStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RevokeProxyStmt)
	return v.Leave(n)
}

// AlterUserDefaultRoleStmt is ALTER USER [IF EXISTS] user DEFAULT ROLE
// {NONE | ALL | role [, role]...}.
type AlterUserDefaultRoleStmt struct {
	stmtNode

	IfExists   bool
	User       *auth.UserIdentity
	SetRoleOpt SetRoleStmtType
	RoleList   []*auth.RoleIdentity
}

// Restore implements Node interface.
func (n *AlterUserDefaultRoleStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER USER ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	if err := n.User.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore AlterUserDefaultRoleStmt.User: %w", err)
	}
	ctx.WriteKeyWord(" DEFAULT ROLE")
	switch n.SetRoleOpt { //nolint:exhaustive // the grammar only produces NONE, ALL, or a role list
	case SetRoleNone:
		ctx.WriteKeyWord(" NONE")
	case SetRoleAll:
		ctx.WriteKeyWord(" ALL")
	}
	for i, role := range n.RoleList {
		ctx.WritePlain(" ")
		if err := role.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterUserDefaultRoleStmt.RoleList[%d]: %w", i, err)
		}
		if i != len(n.RoleList)-1 {
			ctx.WritePlain(",")
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *AlterUserDefaultRoleStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterUserDefaultRoleStmt)
	return v.Leave(n)
}

// FactorOp is the multi-factor authentication operation of an
// AlterUserFactorStmt.
type FactorOp int

// FactorOp values.
const (
	FactorOpAdd FactorOp = iota + 1
	FactorOpModify
	FactorOpDrop
	FactorOpInitiateRegistration
	FactorOpFinishRegistration
	FactorOpUnregister
)

// AlterUserFactorStmt covers the ALTER USER multi-factor authentication
// forms (MySQL 8.0.27+):
//
//	ALTER USER [IF EXISTS] user ADD | MODIFY factor FACTOR identification
//	ALTER USER [IF EXISTS] user DROP factor FACTOR
//	ALTER USER [IF EXISTS] user factor FACTOR INITIATE REGISTRATION
//	ALTER USER [IF EXISTS] user factor FACTOR FINISH REGISTRATION
//	    SET CHALLENGE_RESPONSE AS 'auth_string'
//	ALTER USER [IF EXISTS] user factor FACTOR UNREGISTER
type AlterUserFactorStmt struct {
	stmtNode

	IfExists             bool
	User                 *auth.UserIdentity
	Op                   FactorOp
	Factor               uint64
	AuthOpt              *AuthOption // ADD / MODIFY
	HasChallengeResponse bool
	ChallengeResponse    string // FINISH REGISTRATION
}

// Restore implements Node interface.
func (n *AlterUserFactorStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER USER ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	if err := n.User.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore AlterUserFactorStmt.User: %w", err)
	}
	switch n.Op {
	case FactorOpAdd, FactorOpModify:
		if n.Op == FactorOpAdd {
			ctx.WriteKeyWord(" ADD")
		} else {
			ctx.WriteKeyWord(" MODIFY")
		}
		ctx.WritePlainf(" %d ", n.Factor)
		ctx.WriteKeyWord("FACTOR ")
		if err := n.AuthOpt.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AlterUserFactorStmt.AuthOpt: %w", err)
		}
	case FactorOpDrop:
		ctx.WriteKeyWord(" DROP")
		ctx.WritePlainf(" %d ", n.Factor)
		ctx.WriteKeyWord("FACTOR")
	case FactorOpInitiateRegistration:
		ctx.WritePlainf(" %d ", n.Factor)
		ctx.WriteKeyWord("FACTOR INITIATE REGISTRATION")
	case FactorOpFinishRegistration:
		ctx.WritePlainf(" %d ", n.Factor)
		ctx.WriteKeyWord("FACTOR FINISH REGISTRATION")
		if n.HasChallengeResponse {
			ctx.WriteKeyWord(" SET CHALLENGE_RESPONSE AS ")
			ctx.WriteString(n.ChallengeResponse)
		}
	case FactorOpUnregister:
		ctx.WritePlainf(" %d ", n.Factor)
		ctx.WriteKeyWord("FACTOR UNREGISTER")
	default:
		return fmt.Errorf("unsupported AlterUserFactorStmt.Op %d", n.Op)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *AlterUserFactorStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterUserFactorStmt)
	return v.Leave(n)
}

// ShutdownStmt is a statement to stop the MySQL server.
// See https://dev.mysql.com/doc/refman/8.0/en/shutdown.html
type ShutdownStmt struct {
	stmtNode
}

// Restore implements Node interface.
func (n *ShutdownStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SHUTDOWN")
	return nil
}

// Accept implements Node Accept interface.
func (n *ShutdownStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ShutdownStmt)
	return v.Leave(n)
}

// RestartStmt is a statement to restart the server (MySQL 8.0 RESTART).
// See https://dev.mysql.com/doc/refman/8.0/en/restart.html
type RestartStmt struct {
	stmtNode
}

// Restore implements Node interface.
func (n *RestartStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("RESTART")
	return nil
}

// Accept implements Node Accept interface.
func (n *RestartStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RestartStmt)
	return v.Leave(n)
}

// RenameUserStmt is a statement to rename a user.
// See http://dev.mysql.com/doc/refman/5.7/en/rename-user.html
type RenameUserStmt struct {
	stmtNode

	UserToUsers []*UserToUser
}

// Restore implements Node interface.
func (n *RenameUserStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("RENAME USER ")
	for index, user2user := range n.UserToUsers {
		if index != 0 {
			ctx.WritePlain(", ")
		}
		if err := user2user.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore RenameUserStmt.UserToUsers: %w", err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *RenameUserStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*RenameUserStmt)

	for i, t := range n.UserToUsers {
		node, ok := t.Accept(v)
		if !ok {
			return n, false
		}
		n.UserToUsers[i] = node.(*UserToUser)
	}
	return v.Leave(n)
}

// UserToUser represents renaming old user to new user used in RenameUserStmt.
type UserToUser struct {
	node
	OldUser *auth.UserIdentity
	NewUser *auth.UserIdentity
}

// Restore implements Node interface.
func (n *UserToUser) Restore(ctx *format.RestoreCtx) error {
	if err := n.OldUser.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore UserToUser.OldUser: %w", err)
	}
	ctx.WriteKeyWord(" TO ")
	if err := n.NewUser.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore UserToUser.NewUser: %w", err)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *UserToUser) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*UserToUser)
	return v.Leave(n)
}

// SelectStmtOpts wrap around select hints and switches
type SelectStmtOpts struct {
	Distinct        bool
	SQLBigResult    bool
	SQLBufferResult bool
	SQLCache        bool
	SQLSmallResult  bool
	CalcFoundRows   bool
	StraightJoin    bool
	Priority        mysql.PriorityEnum
	TableHints      []*TableOptimizerHint
	ExplicitAll     bool
}

// TableOptimizerHint is Table level optimizer hint
type TableOptimizerHint struct {
	node
	// HintName is the name or alias of the table(s) which the hint will affect.
	// Table hints has no schema info
	// It allows only table name or alias (if table has an alias)
	HintName CIStr
	// HintData is the payload of the hint. The actual type of this field
	// depends on `HintName`:
	//
	// - MAX_EXECUTION_TIME  => uint64
	// - RESOURCE_GROUP      => string
	// - SET_VAR             => HintSetVar
	HintData any
	// QBName is the default effective query block of this hint.
	QBName  CIStr
	Tables  []HintTable
	Indexes []CIStr
}

// HintSetVar is the payload of `SET_VAR` hint
type HintSetVar struct {
	VarName string
	Value   string
}

// HintTable is table in the hint. It may have query block info.
type HintTable struct {
	DBName    CIStr
	TableName CIStr
	QBName    CIStr
}

func (ht *HintTable) Restore(ctx *format.RestoreCtx) {
	if !ctx.Flags.HasWithoutSchemaNameFlag() {
		if ht.DBName.L != "" {
			ctx.WriteName(ht.DBName.String())
			ctx.WriteKeyWord(".")
		}
	}
	ctx.WriteName(ht.TableName.String())
	if ht.QBName.L != "" {
		ctx.WriteKeyWord("@")
		ctx.WriteName(ht.QBName.String())
	}
}

// Restore implements Node interface.
func (n *TableOptimizerHint) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord(n.HintName.String())
	ctx.WritePlain("(")
	if n.QBName.L != "" {
		if n.HintName.L != "qb_name" {
			ctx.WriteKeyWord("@")
		}
		ctx.WriteName(n.QBName.String())
	}
	if n.HintName.L == "qb_name" && len(n.Tables) == 0 {
		ctx.WritePlain(")")
		return nil
	}
	// Hints without args except query block.
	switch n.HintName.L {
	case "no_index_merge", "merge":
		ctx.WritePlain(")")
		return nil
	}
	if n.QBName.L != "" {
		ctx.WritePlain(" ")
	}
	// Hints with args except query block.
	switch n.HintName.L {
	case "max_execution_time":
		ctx.WritePlainf("%d", n.HintData.(uint64))
	case "resource_group":
		ctx.WriteName(n.HintData.(string))
	case "hash_join", "no_hash_join":
		for i, table := range n.Tables {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			table.Restore(ctx)
		}
	case "order_index", "no_order_index":
		n.Tables[0].Restore(ctx)
		ctx.WritePlain(" ")
		for i, index := range n.Indexes {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			ctx.WriteName(index.String())
		}
	case "qb_name":
		if len(n.Tables) > 0 {
			ctx.WritePlain(", ")
			for i, table := range n.Tables {
				if i != 0 {
					ctx.WritePlain(". ")
				}
				table.Restore(ctx)
			}
		}
	case "set_var":
		hintData := n.HintData.(HintSetVar)
		ctx.WritePlain(hintData.VarName)
		ctx.WritePlain(" = ")
		ctx.WriteString(hintData.Value)
	}
	ctx.WritePlain(")")
	return nil
}

// Accept implements Node Accept interface.
func (n *TableOptimizerHint) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*TableOptimizerHint)
	return v.Leave(n)
}

// TextString represent a string, it can be a binary literal.
type TextString struct {
	Value           string
	IsBinaryLiteral bool
}

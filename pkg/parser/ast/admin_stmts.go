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

// This file holds the administrative statement classes MySQL accepts but
// TiDB never implemented: table maintenance (CHECK/CHECKSUM), the HANDLER
// interface, binary log purging, SDI import, plugin/component management,
// federated SERVER objects, resource groups, key caches, CLONE, and
// instance backup locks. spirit only needs to parse and restore them.

package ast

import (
	"fmt"
	"strconv"

	"github.com/block/spirit/pkg/parser/auth"
	"github.com/block/spirit/pkg/parser/format"
)

// CheckTableCheckOption is one option of a CHECK TABLE statement.
type CheckTableCheckOption int

// CheckTableCheckOption values, in MySQL's own listing order.
const (
	CheckOptionForUpgrade CheckTableCheckOption = iota
	CheckOptionQuick
	CheckOptionFast
	CheckOptionMedium
	CheckOptionExtended
	CheckOptionChanged
)

// String implements fmt.Stringer interface.
func (o CheckTableCheckOption) String() string {
	switch o {
	case CheckOptionForUpgrade:
		return "FOR UPGRADE"
	case CheckOptionQuick:
		return "QUICK"
	case CheckOptionFast:
		return "FAST"
	case CheckOptionMedium:
		return "MEDIUM"
	case CheckOptionExtended:
		return "EXTENDED"
	case CheckOptionChanged:
		return "CHANGED"
	}
	return ""
}

// CheckTableStmt is a CHECK TABLE statement.
// See https://dev.mysql.com/doc/refman/8.0/en/check-table.html
type CheckTableStmt struct {
	stmtNode

	Tables  []*TableName
	Options []CheckTableCheckOption
}

// Restore implements Node interface.
func (n *CheckTableStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CHECK TABLE ")
	for i, table := range n.Tables {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := table.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore CheckTableStmt.Tables: %w", err)
		}
	}
	for _, opt := range n.Options {
		ctx.WritePlain(" ")
		ctx.WriteKeyWord(opt.String())
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *CheckTableStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CheckTableStmt)
	for i, table := range n.Tables {
		node, ok := table.Accept(v)
		if !ok {
			return n, false
		}
		n.Tables[i] = node.(*TableName)
	}
	return v.Leave(n)
}

// ChecksumType is the QUICK/EXTENDED modifier of a CHECKSUM TABLE statement.
type ChecksumType int

// ChecksumType values.
const (
	ChecksumTypeDefault ChecksumType = iota
	ChecksumTypeQuick
	ChecksumTypeExtended
)

// ChecksumTableStmt is a CHECKSUM TABLE statement.
// See https://dev.mysql.com/doc/refman/8.0/en/checksum-table.html
type ChecksumTableStmt struct {
	stmtNode

	Tables []*TableName
	Type   ChecksumType
}

// Restore implements Node interface.
func (n *ChecksumTableStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CHECKSUM TABLE ")
	for i, table := range n.Tables {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := table.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ChecksumTableStmt.Tables: %w", err)
		}
	}
	switch n.Type {
	case ChecksumTypeQuick:
		ctx.WriteKeyWord(" QUICK")
	case ChecksumTypeExtended:
		ctx.WriteKeyWord(" EXTENDED")
	case ChecksumTypeDefault:
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ChecksumTableStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ChecksumTableStmt)
	for i, table := range n.Tables {
		node, ok := table.Accept(v)
		if !ok {
			return n, false
		}
		n.Tables[i] = node.(*TableName)
	}
	return v.Leave(n)
}

// HandlerOpenStmt is a HANDLER ... OPEN statement.
// See https://dev.mysql.com/doc/refman/8.0/en/handler.html
type HandlerOpenStmt struct {
	stmtNode

	Table  *TableName
	AsName CIStr
}

// Restore implements Node interface.
func (n *HandlerOpenStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("HANDLER ")
	if err := n.Table.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore HandlerOpenStmt.Table: %w", err)
	}
	ctx.WriteKeyWord(" OPEN")
	if n.AsName.O != "" {
		ctx.WriteKeyWord(" AS ")
		ctx.WriteName(n.AsName.O)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *HandlerOpenStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*HandlerOpenStmt)
	node, ok := n.Table.Accept(v)
	if !ok {
		return n, false
	}
	n.Table = node.(*TableName)
	return v.Leave(n)
}

// HandlerCloseStmt is a HANDLER ... CLOSE statement.
type HandlerCloseStmt struct {
	stmtNode

	Handler CIStr
}

// Restore implements Node interface.
func (n *HandlerCloseStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("HANDLER ")
	ctx.WriteName(n.Handler.O)
	ctx.WriteKeyWord(" CLOSE")
	return nil
}

// Accept implements Node Accept interface.
func (n *HandlerCloseStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

// HandlerReadStmt is a HANDLER ... READ statement, in any of its three
// shapes: index-value lookup (Index + CompareOp + Values), index scan
// (Index + Direction), or natural-order table scan (Direction only, and
// only FIRST or NEXT).
type HandlerReadStmt struct {
	stmtNode

	Handler CIStr
	Index   CIStr
	// Direction is FIRST, NEXT, PREV or LAST; empty for value lookups.
	Direction string
	// CompareOp is =, <, >, <= or >= for value lookups.
	CompareOp string
	Values    []ExprNode
	Where     ExprNode
	Limit     *Limit
}

// Restore implements Node interface.
func (n *HandlerReadStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("HANDLER ")
	ctx.WriteName(n.Handler.O)
	ctx.WriteKeyWord(" READ")
	if n.Index.O != "" {
		ctx.WritePlain(" ")
		ctx.WriteName(n.Index.O)
	}
	if n.CompareOp != "" {
		ctx.WritePlain(" " + n.CompareOp + " (")
		for i, val := range n.Values {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := val.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore HandlerReadStmt.Values: %w", err)
			}
		}
		ctx.WritePlain(")")
	} else {
		ctx.WriteKeyWord(" " + n.Direction)
	}
	if n.Where != nil {
		ctx.WriteKeyWord(" WHERE ")
		if err := n.Where.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore HandlerReadStmt.Where: %w", err)
		}
	}
	if n.Limit != nil {
		ctx.WritePlain(" ")
		if err := n.Limit.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore HandlerReadStmt.Limit: %w", err)
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *HandlerReadStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*HandlerReadStmt)
	for i, val := range n.Values {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Values[i] = node.(ExprNode)
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
	return v.Leave(n)
}

// PurgeLogsStmt is a PURGE BINARY LOGS statement. The deprecated MASTER
// spelling parses too and restores as BINARY.
// See https://dev.mysql.com/doc/refman/8.0/en/purge-binary-logs.html
type PurgeLogsStmt struct {
	stmtNode

	// To is the target log file name; empty when Before is set.
	To string
	// Before is the cutoff datetime expression; nil when To is set.
	Before ExprNode
}

// Restore implements Node interface.
func (n *PurgeLogsStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("PURGE BINARY LOGS ")
	if n.Before != nil {
		ctx.WriteKeyWord("BEFORE ")
		if err := n.Before.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore PurgeLogsStmt.Before: %w", err)
		}
		return nil
	}
	ctx.WriteKeyWord("TO ")
	ctx.WriteString(n.To)
	return nil
}

// Accept implements Node Accept interface.
func (n *PurgeLogsStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*PurgeLogsStmt)
	if n.Before != nil {
		node, ok := n.Before.Accept(v)
		if !ok {
			return n, false
		}
		n.Before = node.(ExprNode)
	}
	return v.Leave(n)
}

// ImportTableStmt is an IMPORT TABLE FROM statement (SDI import).
// See https://dev.mysql.com/doc/refman/8.0/en/import-table.html
type ImportTableStmt struct {
	stmtNode

	Files []string
}

// Restore implements Node interface.
func (n *ImportTableStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("IMPORT TABLE FROM ")
	for i, f := range n.Files {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		ctx.WriteString(f)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ImportTableStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

// CacheTableIndex is one table entry of a CACHE INDEX or
// LOAD INDEX INTO CACHE statement.
type CacheTableIndex struct {
	Table *TableName
	// AllPartitions is PARTITION (ALL); PartitionNames is the explicit list.
	AllPartitions  bool
	PartitionNames []CIStr
	IndexNames     []CIStr
	// IgnoreLeaves is only meaningful under LOAD INDEX INTO CACHE.
	IgnoreLeaves bool
}

func (ti *CacheTableIndex) restore(ctx *format.RestoreCtx) error {
	if err := ti.Table.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore CacheTableIndex.Table: %w", err)
	}
	if ti.AllPartitions || len(ti.PartitionNames) > 0 {
		ctx.WriteKeyWord(" PARTITION ")
		ctx.WritePlain("(")
		if ti.AllPartitions {
			ctx.WriteKeyWord("ALL")
		}
		for i, p := range ti.PartitionNames {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			ctx.WriteName(p.O)
		}
		ctx.WritePlain(")")
	}
	if len(ti.IndexNames) > 0 {
		ctx.WriteKeyWord(" INDEX ")
		ctx.WritePlain("(")
		for i, idx := range ti.IndexNames {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			ctx.WriteName(idx.O)
		}
		ctx.WritePlain(")")
	}
	if ti.IgnoreLeaves {
		ctx.WriteKeyWord(" IGNORE LEAVES")
	}
	return nil
}

// CacheIndexStmt is a CACHE INDEX ... IN statement (MyISAM key caches).
// See https://dev.mysql.com/doc/refman/8.0/en/cache-index.html
type CacheIndexStmt struct {
	stmtNode

	TableIndexes []*CacheTableIndex
	CacheName    CIStr
}

// Restore implements Node interface.
func (n *CacheIndexStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CACHE INDEX ")
	for i, ti := range n.TableIndexes {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := ti.restore(ctx); err != nil {
			return err
		}
	}
	ctx.WriteKeyWord(" IN ")
	ctx.WriteName(n.CacheName.O)
	return nil
}

// Accept implements Node Accept interface.
func (n *CacheIndexStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CacheIndexStmt)
	for _, ti := range n.TableIndexes {
		node, ok := ti.Table.Accept(v)
		if !ok {
			return n, false
		}
		ti.Table = node.(*TableName)
	}
	return v.Leave(n)
}

// LoadIndexStmt is a LOAD INDEX INTO CACHE statement.
// See https://dev.mysql.com/doc/refman/8.0/en/load-index.html
type LoadIndexStmt struct {
	stmtNode

	TableIndexes []*CacheTableIndex
}

// Restore implements Node interface.
func (n *LoadIndexStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("LOAD INDEX INTO CACHE ")
	for i, ti := range n.TableIndexes {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		if err := ti.restore(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *LoadIndexStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*LoadIndexStmt)
	for _, ti := range n.TableIndexes {
		node, ok := ti.Table.Accept(v)
		if !ok {
			return n, false
		}
		ti.Table = node.(*TableName)
	}
	return v.Leave(n)
}

// InstallPluginStmt is an INSTALL PLUGIN statement.
type InstallPluginStmt struct {
	stmtNode

	Name   CIStr
	SoName string
}

// Restore implements Node interface.
func (n *InstallPluginStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("INSTALL PLUGIN ")
	ctx.WriteName(n.Name.O)
	ctx.WriteKeyWord(" SONAME ")
	ctx.WriteString(n.SoName)
	return nil
}

// Accept implements Node Accept interface.
func (n *InstallPluginStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

// UninstallPluginStmt is an UNINSTALL PLUGIN statement.
type UninstallPluginStmt struct {
	stmtNode

	Name CIStr
}

// Restore implements Node interface.
func (n *UninstallPluginStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("UNINSTALL PLUGIN ")
	ctx.WriteName(n.Name.O)
	return nil
}

// Accept implements Node Accept interface.
func (n *UninstallPluginStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

// InstallComponentStmt is an INSTALL COMPONENT statement, with the
// optional 8.0.33+ SET clause for component system variables.
type InstallComponentStmt struct {
	stmtNode

	Components []string
	SetVars    []*VariableAssignment
}

// Restore implements Node interface.
func (n *InstallComponentStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("INSTALL COMPONENT ")
	for i, c := range n.Components {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		ctx.WriteString(c)
	}
	if len(n.SetVars) > 0 {
		ctx.WriteKeyWord(" SET ")
		for i, assign := range n.SetVars {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := assign.Restore(ctx); err != nil {
				return fmt.Errorf("an error occurred while restore InstallComponentStmt.SetVars: %w", err)
			}
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *InstallComponentStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*InstallComponentStmt)
	for i, assign := range n.SetVars {
		node, ok := assign.Accept(v)
		if !ok {
			return n, false
		}
		n.SetVars[i] = node.(*VariableAssignment)
	}
	return v.Leave(n)
}

// UninstallComponentStmt is an UNINSTALL COMPONENT statement.
type UninstallComponentStmt struct {
	stmtNode

	Components []string
}

// Restore implements Node interface.
func (n *UninstallComponentStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("UNINSTALL COMPONENT ")
	for i, c := range n.Components {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		ctx.WriteString(c)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *UninstallComponentStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

// ServerOption is one name/value pair in a CREATE/ALTER SERVER OPTIONS
// list. MySQL fixes the option names (HOST, DATABASE, USER, PASSWORD,
// SOCKET, OWNER, PORT); the parser accepts any identifier and preserves
// it, leaving validation to the server.
type ServerOption struct {
	Name string
	// StrValue is the quoted value; PORT takes a number instead.
	StrValue string
	NumValue uint64
	IsNum    bool
}

func (opt *ServerOption) restore(ctx *format.RestoreCtx) {
	ctx.WriteKeyWord(opt.Name)
	ctx.WritePlain(" ")
	if opt.IsNum {
		ctx.WritePlain(strconv.FormatUint(opt.NumValue, 10))
	} else {
		ctx.WriteString(opt.StrValue)
	}
}

// CreateServerStmt is a CREATE SERVER statement (FEDERATED tables).
// See https://dev.mysql.com/doc/refman/8.0/en/create-server.html
type CreateServerStmt struct {
	stmtNode

	Name CIStr
	// Wrapper is the FOREIGN DATA WRAPPER name, an identifier or a
	// string literal depending on WrapperString.
	Wrapper       string
	WrapperString bool
	Options       []*ServerOption
}

// Restore implements Node interface.
func (n *CreateServerStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE SERVER ")
	ctx.WriteName(n.Name.O)
	ctx.WriteKeyWord(" FOREIGN DATA WRAPPER ")
	if n.WrapperString {
		ctx.WriteString(n.Wrapper)
	} else {
		ctx.WriteName(n.Wrapper)
	}
	ctx.WriteKeyWord(" OPTIONS ")
	ctx.WritePlain("(")
	for i, opt := range n.Options {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		opt.restore(ctx)
	}
	ctx.WritePlain(")")
	return nil
}

// Accept implements Node Accept interface.
func (n *CreateServerStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

// AlterServerStmt is an ALTER SERVER statement.
type AlterServerStmt struct {
	stmtNode

	Name    CIStr
	Options []*ServerOption
}

// Restore implements Node interface.
func (n *AlterServerStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER SERVER ")
	ctx.WriteName(n.Name.O)
	ctx.WriteKeyWord(" OPTIONS ")
	ctx.WritePlain("(")
	for i, opt := range n.Options {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		opt.restore(ctx)
	}
	ctx.WritePlain(")")
	return nil
}

// Accept implements Node Accept interface.
func (n *AlterServerStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

// DropServerStmt is a DROP SERVER statement.
type DropServerStmt struct {
	stmtNode

	IfExists bool
	Name     CIStr
}

// Restore implements Node interface.
func (n *DropServerStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP SERVER ")
	if n.IfExists {
		ctx.WriteKeyWord("IF EXISTS ")
	}
	ctx.WriteName(n.Name.O)
	return nil
}

// Accept implements Node Accept interface.
func (n *DropServerStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

// VcpuRange is one CPU or CPU range in a resource group VCPU list.
type VcpuRange struct {
	Start uint64
	End   uint64
	// IsRange distinguishes "0-3" from a bare "0".
	IsRange bool
}

func (r VcpuRange) restore(ctx *format.RestoreCtx) {
	ctx.WritePlain(strconv.FormatUint(r.Start, 10))
	if r.IsRange {
		ctx.WritePlain("-" + strconv.FormatUint(r.End, 10))
	}
}

// resourceGroupClauses restores the common tail of CREATE/ALTER RESOURCE
// GROUP: VCPU, THREAD_PRIORITY and ENABLE/DISABLE.
func resourceGroupClauses(ctx *format.RestoreCtx, vcpus []VcpuRange, priority *int64, enable *bool) {
	if len(vcpus) > 0 {
		ctx.WriteKeyWord(" VCPU = ")
		for i, r := range vcpus {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			r.restore(ctx)
		}
	}
	if priority != nil {
		ctx.WriteKeyWord(" THREAD_PRIORITY = ")
		ctx.WritePlain(strconv.FormatInt(*priority, 10))
	}
	if enable != nil {
		if *enable {
			ctx.WriteKeyWord(" ENABLE")
		} else {
			ctx.WriteKeyWord(" DISABLE")
		}
	}
}

// CreateResourceGroupStmt is a CREATE RESOURCE GROUP statement.
// See https://dev.mysql.com/doc/refman/8.0/en/create-resource-group.html
type CreateResourceGroupStmt struct {
	stmtNode

	Name CIStr
	// System is TYPE = SYSTEM; otherwise TYPE = USER.
	System         bool
	Vcpus          []VcpuRange
	ThreadPriority *int64
	Enable         *bool
}

// Restore implements Node interface.
func (n *CreateResourceGroupStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE RESOURCE GROUP ")
	ctx.WriteName(n.Name.O)
	ctx.WriteKeyWord(" TYPE = ")
	if n.System {
		ctx.WriteKeyWord("SYSTEM")
	} else {
		ctx.WriteKeyWord("USER")
	}
	resourceGroupClauses(ctx, n.Vcpus, n.ThreadPriority, n.Enable)
	return nil
}

// Accept implements Node Accept interface.
func (n *CreateResourceGroupStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

// AlterResourceGroupStmt is an ALTER RESOURCE GROUP statement.
type AlterResourceGroupStmt struct {
	stmtNode

	Name           CIStr
	Vcpus          []VcpuRange
	ThreadPriority *int64
	Enable         *bool
	Force          bool
}

// Restore implements Node interface.
func (n *AlterResourceGroupStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER RESOURCE GROUP ")
	ctx.WriteName(n.Name.O)
	resourceGroupClauses(ctx, n.Vcpus, n.ThreadPriority, n.Enable)
	if n.Force {
		ctx.WriteKeyWord(" FORCE")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *AlterResourceGroupStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

// DropResourceGroupStmt is a DROP RESOURCE GROUP statement.
type DropResourceGroupStmt struct {
	stmtNode

	Name  CIStr
	Force bool
}

// Restore implements Node interface.
func (n *DropResourceGroupStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP RESOURCE GROUP ")
	ctx.WriteName(n.Name.O)
	if n.Force {
		ctx.WriteKeyWord(" FORCE")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *DropResourceGroupStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

// SetResourceGroupStmt is a SET RESOURCE GROUP statement.
type SetResourceGroupStmt struct {
	stmtNode

	Name    CIStr
	Threads []uint64
}

// Restore implements Node interface.
func (n *SetResourceGroupStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("SET RESOURCE GROUP ")
	ctx.WriteName(n.Name.O)
	if len(n.Threads) > 0 {
		ctx.WriteKeyWord(" FOR ")
		for i, id := range n.Threads {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			ctx.WritePlain(strconv.FormatUint(id, 10))
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *SetResourceGroupStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

// CloneStmt is a CLONE LOCAL or CLONE INSTANCE statement.
// See https://dev.mysql.com/doc/refman/8.0/en/clone.html
type CloneStmt struct {
	stmtNode

	// Local is CLONE LOCAL DATA DIRECTORY; otherwise CLONE INSTANCE.
	Local bool
	// DataDirectory is required for LOCAL, optional for INSTANCE.
	HasDataDirectory bool
	DataDirectory    string

	User     *auth.UserIdentity
	Port     uint64
	Password string
	// RequireSSL is nil when no REQUIRE clause was given.
	RequireSSL *bool
}

// Restore implements Node interface.
func (n *CloneStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CLONE ")
	if n.Local {
		ctx.WriteKeyWord("LOCAL DATA DIRECTORY = ")
		ctx.WriteString(n.DataDirectory)
		return nil
	}
	ctx.WriteKeyWord("INSTANCE FROM ")
	if err := n.User.Restore(ctx); err != nil {
		return fmt.Errorf("an error occurred while restore CloneStmt.User: %w", err)
	}
	ctx.WritePlain(":" + strconv.FormatUint(n.Port, 10))
	ctx.WriteKeyWord(" IDENTIFIED BY ")
	ctx.WriteString(n.Password)
	if n.HasDataDirectory {
		ctx.WriteKeyWord(" DATA DIRECTORY = ")
		ctx.WriteString(n.DataDirectory)
	}
	if n.RequireSSL != nil {
		if *n.RequireSSL {
			ctx.WriteKeyWord(" REQUIRE SSL")
		} else {
			ctx.WriteKeyWord(" REQUIRE NO SSL")
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *CloneStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

// LockInstanceStmt is a LOCK INSTANCE FOR BACKUP statement.
type LockInstanceStmt struct {
	stmtNode
}

// Restore implements Node interface.
func (n *LockInstanceStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("LOCK INSTANCE FOR BACKUP")
	return nil
}

// Accept implements Node Accept interface.
func (n *LockInstanceStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

// UnlockInstanceStmt is an UNLOCK INSTANCE statement.
type UnlockInstanceStmt struct {
	stmtNode
}

// Restore implements Node interface.
func (n *UnlockInstanceStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("UNLOCK INSTANCE")
	return nil
}

// Accept implements Node Accept interface.
func (n *UnlockInstanceStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

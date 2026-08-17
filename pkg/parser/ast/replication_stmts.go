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

// This file holds the replication administration statements: CHANGE
// REPLICATION SOURCE/FILTER, START/STOP REPLICA and GROUP_REPLICATION,
// and the RESET family. Option lists parse generically as name/value
// pairs; option-name validation is left to the server. The deprecated
// MASTER/SLAVE spellings parse to the same nodes and restore in the
// REPLICA spelling.

package ast

import (
	"fmt"
	"strconv"

	"github.com/block/spirit/pkg/parser/auth"
	"github.com/block/spirit/pkg/parser/format"
)

// ReplicationListItem is one entry of a parenthesized replication option
// value, e.g. a database in REPLICATE_DO_DB, a table in
// REPLICATE_DO_TABLE, a server id in IGNORE_SERVER_IDS, or a rename pair
// in REPLICATE_REWRITE_DB. Exactly one field group is set.
type ReplicationListItem struct {
	Table *TableName
	Str   string
	IsStr bool
	Num   uint64
	IsNum bool
	// PairFrom/PairTo hold a REPLICATE_REWRITE_DB ((from, to)) pair.
	PairFrom CIStr
	PairTo   CIStr
	IsPair   bool
}

func (item *ReplicationListItem) restore(ctx *format.RestoreCtx) error {
	switch {
	case item.Table != nil:
		if err := item.Table.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ReplicationListItem.Table: %w", err)
		}
	case item.IsStr:
		ctx.WriteString(item.Str)
	case item.IsNum:
		ctx.WritePlain(strconv.FormatUint(item.Num, 10))
	case item.IsPair:
		ctx.WritePlain("(")
		ctx.WriteName(item.PairFrom.O)
		ctx.WritePlain(", ")
		ctx.WriteName(item.PairTo.O)
		ctx.WritePlain(")")
	}
	return nil
}

// ReplicationOption is one name/value pair of a replication statement
// option list. At most one value field is set; none for bare options
// such as SQL_AFTER_MTS_GAPS.
type ReplicationOption struct {
	Name string
	// Value is a literal value (string, number or NULL).
	Value ExprNode
	// IdentValue is a bare word value such as LOCAL or OFF.
	IdentValue CIStr
	// UserValue is a user@host value (PRIVILEGE_CHECKS_USER).
	UserValue *auth.UserIdentity
	// List is a parenthesized value list; HasList distinguishes an
	// empty list from no value.
	List    []*ReplicationListItem
	HasList bool
}

func (opt *ReplicationOption) restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord(opt.Name)
	switch {
	case opt.Value != nil:
		ctx.WritePlain(" = ")
		if err := opt.Value.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ReplicationOption.Value: %w", err)
		}
	case opt.IdentValue.O != "":
		ctx.WritePlain(" = ")
		ctx.WriteKeyWord(opt.IdentValue.O)
	case opt.UserValue != nil:
		ctx.WritePlain(" = ")
		if err := opt.UserValue.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore ReplicationOption.UserValue: %w", err)
		}
	case opt.HasList:
		ctx.WritePlain(" = (")
		for i, item := range opt.List {
			if i != 0 {
				ctx.WritePlain(", ")
			}
			if err := item.restore(ctx); err != nil {
				return err
			}
		}
		ctx.WritePlain(")")
	}
	return nil
}

func restoreReplicationOptions(ctx *format.RestoreCtx, options []*ReplicationOption, sep string) error {
	for i, opt := range options {
		if i != 0 {
			ctx.WritePlain(sep)
		}
		if err := opt.restore(ctx); err != nil {
			return err
		}
	}
	return nil
}

func acceptReplicationOptions(v Visitor, options []*ReplicationOption) bool {
	for _, opt := range options {
		if opt.Value == nil {
			continue
		}
		node, ok := opt.Value.Accept(v)
		if !ok {
			return false
		}
		opt.Value = node.(ExprNode)
	}
	return true
}

func restoreForChannel(ctx *format.RestoreCtx, hasChannel bool, channel string) {
	if hasChannel {
		ctx.WriteKeyWord(" FOR CHANNEL ")
		ctx.WriteString(channel)
	}
}

// ChangeReplicationSourceStmt is a CHANGE REPLICATION SOURCE TO
// statement. The deprecated CHANGE MASTER TO spelling parses to the same
// node, keeping its MASTER_* option names.
// See https://dev.mysql.com/doc/refman/8.0/en/change-replication-source-to.html
type ChangeReplicationSourceStmt struct {
	stmtNode

	Options    []*ReplicationOption
	Channel    string
	HasChannel bool
}

// Restore implements Node interface.
func (n *ChangeReplicationSourceStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CHANGE REPLICATION SOURCE TO ")
	if err := restoreReplicationOptions(ctx, n.Options, ", "); err != nil {
		return err
	}
	restoreForChannel(ctx, n.HasChannel, n.Channel)
	return nil
}

// Accept implements Node Accept interface.
func (n *ChangeReplicationSourceStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ChangeReplicationSourceStmt)
	if !acceptReplicationOptions(v, n.Options) {
		return n, false
	}
	return v.Leave(n)
}

// ChangeReplicationFilterStmt is a CHANGE REPLICATION FILTER statement.
// See https://dev.mysql.com/doc/refman/8.0/en/change-replication-filter.html
type ChangeReplicationFilterStmt struct {
	stmtNode

	Filters    []*ReplicationOption
	Channel    string
	HasChannel bool
}

// Restore implements Node interface.
func (n *ChangeReplicationFilterStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CHANGE REPLICATION FILTER ")
	if err := restoreReplicationOptions(ctx, n.Filters, ", "); err != nil {
		return err
	}
	restoreForChannel(ctx, n.HasChannel, n.Channel)
	return nil
}

// Accept implements Node Accept interface.
func (n *ChangeReplicationFilterStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ChangeReplicationFilterStmt)
	if !acceptReplicationOptions(v, n.Filters) {
		return n, false
	}
	return v.Leave(n)
}

// StartReplicaStmt is a START REPLICA (or deprecated START SLAVE)
// statement.
// See https://dev.mysql.com/doc/refman/8.0/en/start-replica.html
type StartReplicaStmt struct {
	stmtNode

	// Threads holds IO_THREAD/SQL_THREAD in statement order.
	Threads []string
	Until   []*ReplicationOption
	// Options are the space-separated connection options (USER,
	// PASSWORD, DEFAULT_AUTH, PLUGIN_DIR).
	Options    []*ReplicationOption
	Channel    string
	HasChannel bool
}

// Restore implements Node interface.
func (n *StartReplicaStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("START REPLICA")
	for i, t := range n.Threads {
		if i != 0 {
			ctx.WritePlain(",")
		}
		ctx.WriteKeyWord(" " + t)
	}
	if len(n.Until) > 0 {
		ctx.WriteKeyWord(" UNTIL ")
		if err := restoreReplicationOptions(ctx, n.Until, ", "); err != nil {
			return err
		}
	}
	if len(n.Options) > 0 {
		ctx.WritePlain(" ")
		if err := restoreReplicationOptions(ctx, n.Options, " "); err != nil {
			return err
		}
	}
	restoreForChannel(ctx, n.HasChannel, n.Channel)
	return nil
}

// Accept implements Node Accept interface.
func (n *StartReplicaStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*StartReplicaStmt)
	if !acceptReplicationOptions(v, n.Until) {
		return n, false
	}
	if !acceptReplicationOptions(v, n.Options) {
		return n, false
	}
	return v.Leave(n)
}

// StopReplicaStmt is a STOP REPLICA (or deprecated STOP SLAVE) statement.
type StopReplicaStmt struct {
	stmtNode

	Threads    []string
	Channel    string
	HasChannel bool
}

// Restore implements Node interface.
func (n *StopReplicaStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("STOP REPLICA")
	for i, t := range n.Threads {
		if i != 0 {
			ctx.WritePlain(",")
		}
		ctx.WriteKeyWord(" " + t)
	}
	restoreForChannel(ctx, n.HasChannel, n.Channel)
	return nil
}

// Accept implements Node Accept interface.
func (n *StopReplicaStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

// StartGroupReplicationStmt is a START GROUP_REPLICATION statement.
type StartGroupReplicationStmt struct {
	stmtNode

	Options []*ReplicationOption
}

// Restore implements Node interface.
func (n *StartGroupReplicationStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("START GROUP_REPLICATION")
	if len(n.Options) > 0 {
		ctx.WritePlain(" ")
		if err := restoreReplicationOptions(ctx, n.Options, ", "); err != nil {
			return err
		}
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *StartGroupReplicationStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*StartGroupReplicationStmt)
	if !acceptReplicationOptions(v, n.Options) {
		return n, false
	}
	return v.Leave(n)
}

// StopGroupReplicationStmt is a STOP GROUP_REPLICATION statement.
type StopGroupReplicationStmt struct {
	stmtNode
}

// Restore implements Node interface.
func (n *StopGroupReplicationStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("STOP GROUP_REPLICATION")
	return nil
}

// Accept implements Node Accept interface.
func (n *StopGroupReplicationStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

// ResetReplicaStmt is a RESET REPLICA (or deprecated RESET SLAVE)
// statement.
type ResetReplicaStmt struct {
	stmtNode

	All        bool
	Channel    string
	HasChannel bool
}

// Restore implements Node interface.
func (n *ResetReplicaStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("RESET REPLICA")
	if n.All {
		ctx.WriteKeyWord(" ALL")
	}
	restoreForChannel(ctx, n.HasChannel, n.Channel)
	return nil
}

// Accept implements Node Accept interface.
func (n *ResetReplicaStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

// ResetBinaryLogsStmt is a RESET BINARY LOGS AND GTIDS statement. The
// deprecated RESET MASTER spelling parses to the same node.
type ResetBinaryLogsStmt struct {
	stmtNode

	// To is the optional binary log index number to start from.
	To    uint64
	HasTo bool
}

// Restore implements Node interface.
func (n *ResetBinaryLogsStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("RESET BINARY LOGS AND GTIDS")
	if n.HasTo {
		ctx.WriteKeyWord(" TO ")
		ctx.WritePlain(strconv.FormatUint(n.To, 10))
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ResetBinaryLogsStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

// ResetPersistStmt is a RESET PERSIST statement.
type ResetPersistStmt struct {
	stmtNode

	IfExists bool
	// Variable is empty when resetting all persisted variables.
	Variable string
}

// Restore implements Node interface.
func (n *ResetPersistStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("RESET PERSIST")
	if n.IfExists {
		ctx.WriteKeyWord(" IF EXISTS")
	}
	if n.Variable != "" {
		ctx.WritePlain(" ")
		ctx.WriteName(n.Variable)
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *ResetPersistStmt) Accept(v Visitor) (Node, bool) {
	newNode, _ := v.Enter(n)
	return v.Leave(newNode)
}

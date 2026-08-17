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

package ast

import (
	"fmt"

	"github.com/block/spirit/pkg/parser/format"
)

var (
	_ DDLNode = &CreateTablespaceStmt{}
	_ DDLNode = &AlterTablespaceStmt{}
	_ DDLNode = &DropTablespaceStmt{}
	_ DDLNode = &CreateLogfileGroupStmt{}
	_ DDLNode = &AlterLogfileGroupStmt{}
	_ DDLNode = &DropLogfileGroupStmt{}
)

// TablespaceOptionType is the type of a tablespace / logfile group option.
type TablespaceOptionType int

// Tablespace option types.
const (
	TablespaceOptNone TablespaceOptionType = iota
	TablespaceOptInitialSize
	TablespaceOptMaxSize
	TablespaceOptExtentSize
	TablespaceOptAutoextendSize
	TablespaceOptFileBlockSize
	TablespaceOptUndoBufferSize
	TablespaceOptRedoBufferSize
	TablespaceOptNodegroup
	TablespaceOptWait
	TablespaceOptNoWait
	TablespaceOptComment
	TablespaceOptEncryption
	TablespaceOptEngine
	TablespaceOptEngineAttribute
)

// TablespaceOption is a single tablespace / logfile group option. Size
// options keep either a raw numeric value (UintValue) or the identifier
// spelling like "10M" (StrValue), matching how they were written.
type TablespaceOption struct {
	Tp        TablespaceOptionType
	StrValue  string
	UintValue uint64
}

// Restore implements Node interface.
func (n *TablespaceOption) Restore(ctx *format.RestoreCtx) error {
	switch n.Tp { //nolint:exhaustive // TablespaceOptNone is invalid and hits the default error.
	case TablespaceOptInitialSize:
		ctx.WriteKeyWord("INITIAL_SIZE")
	case TablespaceOptMaxSize:
		ctx.WriteKeyWord("MAX_SIZE")
	case TablespaceOptExtentSize:
		ctx.WriteKeyWord("EXTENT_SIZE")
	case TablespaceOptAutoextendSize:
		ctx.WriteKeyWord("AUTOEXTEND_SIZE")
	case TablespaceOptFileBlockSize:
		ctx.WriteKeyWord("FILE_BLOCK_SIZE")
	case TablespaceOptUndoBufferSize:
		ctx.WriteKeyWord("UNDO_BUFFER_SIZE")
	case TablespaceOptRedoBufferSize:
		ctx.WriteKeyWord("REDO_BUFFER_SIZE")
	case TablespaceOptNodegroup:
		ctx.WriteKeyWord("NODEGROUP")
	case TablespaceOptWait:
		ctx.WriteKeyWord("WAIT")
		return nil
	case TablespaceOptNoWait:
		ctx.WriteKeyWord("NO_WAIT")
		return nil
	case TablespaceOptComment:
		ctx.WriteKeyWord("COMMENT")
	case TablespaceOptEncryption:
		ctx.WriteKeyWord("ENCRYPTION")
	case TablespaceOptEngine:
		ctx.WriteKeyWord("ENGINE")
	case TablespaceOptEngineAttribute:
		ctx.WriteKeyWord("ENGINE_ATTRIBUTE")
	default:
		return fmt.Errorf("invalid TablespaceOptionType: %d", n.Tp)
	}
	ctx.WritePlain(" = ")
	switch n.Tp { //nolint:exhaustive // remaining options are numeric sizes handled by default
	case TablespaceOptComment, TablespaceOptEncryption, TablespaceOptEngineAttribute:
		ctx.WriteString(n.StrValue)
	case TablespaceOptEngine:
		ctx.WriteName(n.StrValue)
	default:
		if n.StrValue != "" {
			ctx.WritePlain(n.StrValue) // e.g. 10M
		} else {
			ctx.WritePlainf("%d", n.UintValue)
		}
	}
	return nil
}

func restoreTablespaceOptions(ctx *format.RestoreCtx, options []*TablespaceOption) error {
	for i, opt := range options {
		ctx.WritePlain(" ")
		if err := opt.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore TablespaceOption[%d]: %w", i, err)
		}
	}
	return nil
}

// CreateTablespaceStmt is a statement to create a tablespace.
// See https://dev.mysql.com/doc/refman/8.4/en/create-tablespace.html
type CreateTablespaceStmt struct {
	ddlNode

	Undo         bool
	Name         CIStr
	DataFile     string
	HasDataFile  bool
	LogfileGroup CIStr // USE LOGFILE GROUP
	Options      []*TablespaceOption
}

// Restore implements Node interface.
func (n *CreateTablespaceStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE ")
	if n.Undo {
		ctx.WriteKeyWord("UNDO ")
	}
	ctx.WriteKeyWord("TABLESPACE ")
	ctx.WriteName(n.Name.O)
	if n.HasDataFile {
		ctx.WriteKeyWord(" ADD DATAFILE ")
		ctx.WriteString(n.DataFile)
	}
	if n.LogfileGroup.O != "" {
		ctx.WriteKeyWord(" USE LOGFILE GROUP ")
		ctx.WriteName(n.LogfileGroup.O)
	}
	return restoreTablespaceOptions(ctx, n.Options)
}

// Accept implements Node Accept interface.
func (n *CreateTablespaceStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateTablespaceStmt)
	return v.Leave(n)
}

// AlterTablespaceActionType is the action of an ALTER [UNDO] TABLESPACE.
type AlterTablespaceActionType int

// Alter tablespace actions.
const (
	AlterTablespaceOptionsOnly AlterTablespaceActionType = iota
	AlterTablespaceAddDataFile
	AlterTablespaceDropDataFile
	AlterTablespaceRenameTo
	AlterTablespaceSetActive
	AlterTablespaceSetInactive
)

// AlterTablespaceStmt is a statement to modify a tablespace.
// See https://dev.mysql.com/doc/refman/8.4/en/alter-tablespace.html
type AlterTablespaceStmt struct {
	ddlNode

	Undo     bool
	Name     CIStr
	Action   AlterTablespaceActionType
	DataFile string
	NewName  CIStr
	Options  []*TablespaceOption
}

// Restore implements Node interface.
func (n *AlterTablespaceStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER ")
	if n.Undo {
		ctx.WriteKeyWord("UNDO ")
	}
	ctx.WriteKeyWord("TABLESPACE ")
	ctx.WriteName(n.Name.O)
	switch n.Action {
	case AlterTablespaceOptionsOnly:
	case AlterTablespaceAddDataFile:
		ctx.WriteKeyWord(" ADD DATAFILE ")
		ctx.WriteString(n.DataFile)
	case AlterTablespaceDropDataFile:
		ctx.WriteKeyWord(" DROP DATAFILE ")
		ctx.WriteString(n.DataFile)
	case AlterTablespaceRenameTo:
		ctx.WriteKeyWord(" RENAME TO ")
		ctx.WriteName(n.NewName.O)
	case AlterTablespaceSetActive:
		ctx.WriteKeyWord(" SET ACTIVE")
	case AlterTablespaceSetInactive:
		ctx.WriteKeyWord(" SET INACTIVE")
	}
	return restoreTablespaceOptions(ctx, n.Options)
}

// Accept implements Node Accept interface.
func (n *AlterTablespaceStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterTablespaceStmt)
	return v.Leave(n)
}

// DropTablespaceStmt is a statement to drop a tablespace.
// See https://dev.mysql.com/doc/refman/8.4/en/drop-tablespace.html
type DropTablespaceStmt struct {
	ddlNode

	Undo    bool
	Name    CIStr
	Options []*TablespaceOption
}

// Restore implements Node interface.
func (n *DropTablespaceStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP ")
	if n.Undo {
		ctx.WriteKeyWord("UNDO ")
	}
	ctx.WriteKeyWord("TABLESPACE ")
	ctx.WriteName(n.Name.O)
	return restoreTablespaceOptions(ctx, n.Options)
}

// Accept implements Node Accept interface.
func (n *DropTablespaceStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropTablespaceStmt)
	return v.Leave(n)
}

// CreateLogfileGroupStmt is a statement to create a logfile group (NDB).
// See https://dev.mysql.com/doc/refman/8.4/en/create-logfile-group.html
type CreateLogfileGroupStmt struct {
	ddlNode

	Name     CIStr
	UndoFile string
	Options  []*TablespaceOption
}

// Restore implements Node interface.
func (n *CreateLogfileGroupStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("CREATE LOGFILE GROUP ")
	ctx.WriteName(n.Name.O)
	ctx.WriteKeyWord(" ADD UNDOFILE ")
	ctx.WriteString(n.UndoFile)
	return restoreTablespaceOptions(ctx, n.Options)
}

// Accept implements Node Accept interface.
func (n *CreateLogfileGroupStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*CreateLogfileGroupStmt)
	return v.Leave(n)
}

// AlterLogfileGroupStmt is a statement to add an undofile to a logfile group.
// See https://dev.mysql.com/doc/refman/8.4/en/alter-logfile-group.html
type AlterLogfileGroupStmt struct {
	ddlNode

	Name     CIStr
	UndoFile string
	Options  []*TablespaceOption
}

// Restore implements Node interface.
func (n *AlterLogfileGroupStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ALTER LOGFILE GROUP ")
	ctx.WriteName(n.Name.O)
	ctx.WriteKeyWord(" ADD UNDOFILE ")
	ctx.WriteString(n.UndoFile)
	return restoreTablespaceOptions(ctx, n.Options)
}

// Accept implements Node Accept interface.
func (n *AlterLogfileGroupStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AlterLogfileGroupStmt)
	return v.Leave(n)
}

// DropLogfileGroupStmt is a statement to drop a logfile group (NDB).
// See https://dev.mysql.com/doc/refman/8.4/en/drop-logfile-group.html
type DropLogfileGroupStmt struct {
	ddlNode

	Name    CIStr
	Options []*TablespaceOption
}

// Restore implements Node interface.
func (n *DropLogfileGroupStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("DROP LOGFILE GROUP ")
	ctx.WriteName(n.Name.O)
	return restoreTablespaceOptions(ctx, n.Options)
}

// Accept implements Node Accept interface.
func (n *DropLogfileGroupStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*DropLogfileGroupStmt)
	return v.Leave(n)
}

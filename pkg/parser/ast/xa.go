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
	"github.com/block/spirit/pkg/parser/format"
)

var _ StmtNode = &XAStmt{}

// XAOpType is the operation of an XA statement.
type XAOpType int

// XA operations.
const (
	XAOpStart XAOpType = iota // XA {START|BEGIN}
	XAOpEnd
	XAOpPrepare
	XAOpCommit
	XAOpRollback
	XAOpRecover
)

// XAXid is the xid of an XA statement: gtrid [, bqual [, formatID]].
// The gtrid and bqual are byte strings; NParts records how many parts were
// written so restore preserves the original shape.
type XAXid struct {
	GTRID    string
	BQual    string
	FormatID uint64
	NParts   int
}

// Restore implements Node interface.
func (x *XAXid) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteString(x.GTRID)
	if x.NParts >= 2 {
		ctx.WritePlain(", ")
		ctx.WriteString(x.BQual)
	}
	if x.NParts >= 3 {
		ctx.WritePlainf(", %d", x.FormatID)
	}
	return nil
}

// XAStmt is an XA transaction control statement. spirit does not support XA
// workloads (they binlog in ways the replication client cannot apply
// consistently), but the parser must recognize them so pkg/change can refuse
// them cleanly instead of failing to parse.
// See https://dev.mysql.com/doc/refman/8.4/en/xa-statements.html
type XAStmt struct {
	stmtNode

	Op  XAOpType
	Xid *XAXid // nil for XA RECOVER

	Join       bool // XA START ... JOIN
	Resume     bool // XA START ... RESUME
	Suspend    bool // XA END ... SUSPEND
	ForMigrate bool // XA END ... SUSPEND FOR MIGRATE
	OnePhase   bool // XA COMMIT ... ONE PHASE
	ConvertXid bool // XA RECOVER CONVERT XID
}

// Restore implements Node interface.
func (n *XAStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("XA ")
	switch n.Op {
	case XAOpStart:
		ctx.WriteKeyWord("START ")
	case XAOpEnd:
		ctx.WriteKeyWord("END ")
	case XAOpPrepare:
		ctx.WriteKeyWord("PREPARE ")
	case XAOpCommit:
		ctx.WriteKeyWord("COMMIT ")
	case XAOpRollback:
		ctx.WriteKeyWord("ROLLBACK ")
	case XAOpRecover:
		ctx.WriteKeyWord("RECOVER")
		if n.ConvertXid {
			ctx.WriteKeyWord(" CONVERT XID")
		}
		return nil
	}
	if err := n.Xid.Restore(ctx); err != nil {
		return err
	}
	if n.Join {
		ctx.WriteKeyWord(" JOIN")
	}
	if n.Resume {
		ctx.WriteKeyWord(" RESUME")
	}
	if n.Suspend {
		ctx.WriteKeyWord(" SUSPEND")
		if n.ForMigrate {
			ctx.WriteKeyWord(" FOR MIGRATE")
		}
	}
	if n.OnePhase {
		ctx.WriteKeyWord(" ONE PHASE")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *XAStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*XAStmt)
	return v.Leave(n)
}

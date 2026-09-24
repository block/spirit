// Copyright 2017 PingCAP, Inc.
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
	_ StmtNode = &AnalyzeTableStmt{}
)

// AnalyzeTableStmt is the MySQL ANALYZE TABLE statement, including the
// histogram forms: ANALYZE TABLE t UPDATE HISTOGRAM ON c [WITH n BUCKETS]
// and ANALYZE TABLE t DROP HISTOGRAM ON c.
type AnalyzeTableStmt struct {
	stmtNode

	TableNames     []*TableName
	PartitionNames []CIStr
	AnalyzeOpts    []AnalyzeOpt

	NoWriteToBinLog bool
	// HistogramOperation is set in "ANALYZE TABLE ... UPDATE/DROP HISTOGRAM ..." statement.
	HistogramOperation HistogramOperationType
	// ColumnNames indicate the columns whose histograms are updated or dropped.
	ColumnNames []CIStr
	// HistogramUpdate is the MANUAL/AUTO UPDATE suffix of UPDATE HISTOGRAM (MySQL 8.4+).
	HistogramUpdate HistogramUpdateType
	// UsingData is set for UPDATE HISTOGRAM ... USING DATA 'json'; HistogramData
	// holds the JSON payload.
	UsingData     bool
	HistogramData string
}

// AnalyzeOptionType is the type for analyze options.
type AnalyzeOptionType int

// Analyze option types.
const (
	AnalyzeOptNumBuckets AnalyzeOptionType = iota
)

// AnalyzeOptionString stores the string form of analyze options.
var AnalyzeOptionString = map[AnalyzeOptionType]string{
	AnalyzeOptNumBuckets: "BUCKETS",
}

// HistogramUpdateType is the MANUAL/AUTO UPDATE suffix of UPDATE HISTOGRAM.
type HistogramUpdateType int

// Histogram update types.
const (
	HistogramUpdateNop HistogramUpdateType = iota
	HistogramUpdateManual
	HistogramUpdateAuto
)

// HistogramOperationType is the type for histogram operation.
type HistogramOperationType int

// Histogram operation types.
const (
	// HistogramOperationNop shows no operation in histogram. Default value.
	HistogramOperationNop HistogramOperationType = iota
	HistogramOperationUpdate
	HistogramOperationDrop
)

// String implements fmt.Stringer for HistogramOperationType.
func (hot HistogramOperationType) String() string {
	switch hot { //nolint:exhaustive
	case HistogramOperationUpdate:
		return "UPDATE HISTOGRAM"
	case HistogramOperationDrop:
		return "DROP HISTOGRAM"
	}
	return ""
}

// AnalyzeOpt stores the analyze option type and value.
type AnalyzeOpt struct {
	Type  AnalyzeOptionType
	Value *ValueExpr
}

// Restore implements Node interface.
func (n *AnalyzeTableStmt) Restore(ctx *format.RestoreCtx) error {
	ctx.WriteKeyWord("ANALYZE ")
	if n.NoWriteToBinLog {
		ctx.WriteKeyWord("NO_WRITE_TO_BINLOG ")
	}
	ctx.WriteKeyWord("TABLE ")
	for i, table := range n.TableNames {
		if i != 0 {
			ctx.WritePlain(",")
		}
		if err := table.Restore(ctx); err != nil {
			return fmt.Errorf("an error occurred while restore AnalyzeTableStmt.TableNames[%d]: %w", i, err)
		}
	}
	if len(n.PartitionNames) != 0 {
		ctx.WriteKeyWord(" PARTITION ")
	}
	for i, partition := range n.PartitionNames {
		if i != 0 {
			ctx.WritePlain(",")
		}
		ctx.WriteName(partition.O)
	}
	if n.HistogramOperation != HistogramOperationNop {
		ctx.WritePlain(" ")
		ctx.WriteKeyWord(n.HistogramOperation.String())
		ctx.WritePlain(" ")
		if len(n.ColumnNames) > 0 {
			ctx.WriteKeyWord("ON ")
			for i, columnName := range n.ColumnNames {
				if i != 0 {
					ctx.WritePlain(",")
				}
				ctx.WriteName(columnName.O)
			}
		}
		if n.UsingData {
			ctx.WriteKeyWord(" USING DATA ")
			ctx.WriteString(n.HistogramData)
		}
	}
	if len(n.AnalyzeOpts) != 0 {
		ctx.WriteKeyWord(" WITH")
		for i, opt := range n.AnalyzeOpts {
			if i != 0 {
				ctx.WritePlain(",")
			}
			ctx.WritePlainf(" %v ", opt.Value.GetValue())
			ctx.WritePlain(AnalyzeOptionString[opt.Type])
		}
	}
	switch n.HistogramUpdate {
	case HistogramUpdateNop:
	case HistogramUpdateManual:
		ctx.WriteKeyWord(" MANUAL UPDATE")
	case HistogramUpdateAuto:
		ctx.WriteKeyWord(" AUTO UPDATE")
	}
	return nil
}

// Accept implements Node Accept interface.
func (n *AnalyzeTableStmt) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AnalyzeTableStmt)
	for i, val := range n.TableNames {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.TableNames[i] = node.(*TableName)
	}
	return v.Leave(n)
}

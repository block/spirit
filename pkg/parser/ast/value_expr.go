// Copyright 2019 PingCAP, Inc.
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
	"strconv"

	"github.com/block/spirit/pkg/parser/charset"
	"github.com/block/spirit/pkg/parser/format"
	"github.com/block/spirit/pkg/parser/mysql"
)

var _ ExprNode = &ValueExpr{}

// NewDecimal parses a string into a MyDecimal value.
func NewDecimal(str string) (*MyDecimal, error) {
	dec := new(MyDecimal)
	err := dec.FromString([]byte(str))
	return dec, err
}

// ValueExpr is the simple value expression.
type ValueExpr struct {
	exprNode
	Datum
	projectionOffset int
}

// Restore implements Node interface.
func (n *ValueExpr) Restore(ctx *format.RestoreCtx) error {
	switch n.Kind() {
	case KindNull:
		ctx.WriteKeyWord("NULL")
	case KindInt64:
		if n.Type.GetFlag()&mysql.IsBooleanFlag != 0 {
			if n.GetInt64() > 0 {
				ctx.WriteKeyWord("TRUE")
			} else {
				ctx.WriteKeyWord("FALSE")
			}
		} else {
			ctx.WritePlain(strconv.FormatInt(n.GetInt64(), 10))
		}
	case KindUint64:
		ctx.WritePlain(strconv.FormatUint(n.GetUint64(), 10))
	case KindFloat32:
		ctx.WritePlain(strconv.FormatFloat(n.GetFloat64(), 'e', -1, 32))
	case KindFloat64:
		ctx.WritePlain(strconv.FormatFloat(n.GetFloat64(), 'e', -1, 64))
	case KindString:
		if n.Type.GetCharset() != "" &&
			!ctx.Flags.HasStringWithoutCharset() &&
			(!ctx.Flags.HasStringWithoutDefaultCharset() || n.Type.GetCharset() != mysql.DefaultCharset) {
			ctx.WritePlain("_")
			ctx.WriteKeyWord(n.Type.GetCharset())
		}
		ctx.WriteString(n.GetString())
	case KindBytes:
		ctx.WriteString(n.GetString())
	case KindMysqlDecimal:
		ctx.WritePlain(n.GetMysqlDecimal().String())
	case KindBinaryLiteral:
		if n.Type.GetCharset() != "" && n.Type.GetCharset() != mysql.DefaultCharset &&
			!ctx.Flags.HasStringWithoutCharset() &&
			n.Type.GetCharset() != charset.CharsetBin {
			ctx.WritePlain("_")
			ctx.WriteKeyWord(n.Type.GetCharset() + " ")
		}
		if n.Type.GetFlag()&mysql.UnsignedFlag != 0 {
			ctx.WritePlainf("x'%x'", n.GetBytes())
		} else {
			ctx.WritePlain(n.GetBinaryLiteral().ToBitLiteralString(true))
		}
	case KindMysqlDuration, KindMysqlEnum,
		KindMysqlBit, KindMysqlSet, KindMysqlTime,
		KindInterface, KindMinNotNull, KindMaxValue,
		KindRaw, KindMysqlJSON:
		// TODO implement Restore function
		return fmt.Errorf("not implemented")
	default:
		return fmt.Errorf("can't format to string")
	}
	return nil
}

// GetDatumString returns the string value of the datum.
func (n *ValueExpr) GetDatumString() string {
	return n.GetString()
}

// NewValueExpr creates a ValueExpr with value, and sets default field type.
func NewValueExpr(value any, charset string, collate string) *ValueExpr {
	if ve, ok := value.(*ValueExpr); ok {
		return ve
	}
	ve := &ValueExpr{}
	ve.SetValue(value)
	DefaultTypeForValue(value, &ve.Type, charset, collate)
	ve.projectionOffset = -1
	return ve
}

// SetProjectionOffset sets ValueExpr.projectionOffset for logical plan builder.
func (n *ValueExpr) SetProjectionOffset(offset int) {
	n.projectionOffset = offset
}

// GetProjectionOffset returns ValueExpr.projectionOffset.
func (n *ValueExpr) GetProjectionOffset() int {
	return n.projectionOffset
}

// Accept implements Node interface.
func (n *ValueExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ValueExpr)
	return v.Leave(n)
}

// ParamMarkerExpr expression holds a place for another expression.
// Used in parsing prepare statement.
type ParamMarkerExpr struct {
	ValueExpr
	Offset    int
	Order     int
	InExecute bool
}

// Restore implements Node interface.
func (n *ParamMarkerExpr) Restore(ctx *format.RestoreCtx) error {
	ctx.WritePlain("?")
	return nil
}

// NewParamMarkerExpr creates a ParamMarkerExpr.
func NewParamMarkerExpr(offset int) *ParamMarkerExpr {
	return &ParamMarkerExpr{
		Offset: offset,
	}
}

// Accept implements Node Accept interface.
func (n *ParamMarkerExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*ParamMarkerExpr)
	return v.Leave(n)
}

// SetOrder sets the order of the parameter marker.
func (n *ParamMarkerExpr) SetOrder(order int) {
	n.Order = order
}

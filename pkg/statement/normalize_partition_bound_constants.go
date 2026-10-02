package statement

import (
	"math"
	"math/big"
	"time"

	"github.com/block/spirit/pkg/parser"
	"github.com/block/spirit/pkg/parser/ast"
	"github.com/block/spirit/pkg/parser/opcode"
)

func init() { registerNormalizer(partitionBoundConstantNormalizer{}) }

// partitionBoundConstantNormalizer folds a constant expression in a partition
// value into the integer MySQL stores for it. MySQL evaluates a VALUES LESS
// THAN / VALUES IN expression when it creates the partition and keeps only the
// result, so SHOW CREATE TABLE reports VALUES LESS THAN (10+10) as
// VALUES LESS THAN (20) and TO_DAYS('2030-01-01') as 741443. Without folding,
// a desired schema that spells a bound as an expression differs from the live
// table forever, and every diff re-emits a partition change.
//
// It folds what MySQL computes the same way on every server: integer
// literals, unary + and -, and the integer operators +, -, *, DIV and MOD
// (also written %), plus TO_DAYS, TO_SECONDS and YEAR of a 'YYYY-MM-DD' or
// 'YYYY-MM-DD HH:MM:SS' literal. Everything else is left as an expression,
// which still emits valid SQL but does not converge. UNIX_TIMESTAMP is left
// on purpose: its result depends on the session time zone.
//
// An intermediate result out of range for its type (BIGINT, or BIGINT
// UNSIGNED once an operand is unsigned) is not folded, because MySQL rejects
// it (error 1690); neither is division by zero, which MySQL evaluates to NULL
// and rejects (error 1566).
type partitionBoundConstantNormalizer struct{}

func (partitionBoundConstantNormalizer) Name() string { return "partition-bound-constants" }

func (partitionBoundConstantNormalizer) Normalize(ct *CreateTable) *CreateTable {
	if ct.Partition == nil {
		return ct
	}
	p := parser.New()
	for i := range ct.Partition.Definitions {
		values := ct.Partition.Definitions[i].Values
		if values == nil {
			continue
		}
		for j, v := range values.Values {
			values.Values[j] = foldPartitionValue(p, v)
		}
	}
	return ct
}

// foldPartitionValue returns v with a foldable partitionExprValue (also inside
// a multi-column tuple) replaced by its integer text.
func foldPartitionValue(p *parser.Parser, v any) any {
	switch val := v.(type) {
	case partitionValueTuple:
		for i, elem := range val {
			val[i] = foldPartitionValue(p, elem)
		}
		return val
	case partitionExprValue:
		expr, ok := parseExpressionText(p, string(val))
		if !ok {
			return val
		}
		n, ok := foldPartitionConstant(expr)
		if !ok {
			return val
		}
		return n.String()
	default:
		return v
	}
}

var (
	minBigint         = big.NewInt(math.MinInt64)
	maxBigint         = big.NewInt(math.MaxInt64)
	maxUnsignedBigint = new(big.Int).SetUint64(math.MaxUint64)
)

// mysqlDayNumberOfUnixEpoch is TO_DAYS('1970-01-01').
const mysqlDayNumberOfUnixEpoch = 719528

// partitionConstant is an integer with the type MySQL gives it: BIGINT, or
// BIGINT UNSIGNED for a literal above the BIGINT range and for any arithmetic
// with an unsigned operand.
type partitionConstant struct {
	n        *big.Int
	unsigned bool
}

// foldPartitionConstant evaluates expr as MySQL would for a partition value,
// returning false when expr is not one of the forms
// partitionBoundConstantNormalizer folds, or when any intermediate result is
// out of range for its type.
func foldPartitionConstant(expr ast.ExprNode) (*big.Int, bool) {
	c, ok := evalPartitionConstant(expr)
	if !ok {
		return nil, false
	}
	return c.n, true
}

func evalPartitionConstant(expr ast.ExprNode) (partitionConstant, bool) {
	c, ok := evalPartitionConstantUnchecked(expr)
	if !ok {
		return partitionConstant{}, false
	}
	if c.unsigned {
		ok = c.n.Sign() >= 0 && c.n.Cmp(maxUnsignedBigint) <= 0
	} else {
		ok = c.n.Cmp(minBigint) >= 0 && c.n.Cmp(maxBigint) <= 0
	}
	return c, ok
}

func evalPartitionConstantUnchecked(expr ast.ExprNode) (partitionConstant, bool) {
	switch e := expr.(type) {
	case *ast.ParenthesesExpr:
		return evalPartitionConstant(e.Expr)
	case *ast.ValueExpr:
		switch e.Kind() {
		case ast.KindInt64:
			return partitionConstant{n: big.NewInt(e.GetInt64())}, true
		case ast.KindUint64:
			v := e.GetUint64()
			return partitionConstant{n: new(big.Int).SetUint64(v), unsigned: v > math.MaxInt64}, true
		}
	case *ast.UnaryOperationExpr:
		v, ok := evalPartitionConstant(e.V)
		if !ok {
			return partitionConstant{}, false
		}
		switch e.Op { //nolint:exhaustive // every other operator is left unfolded
		case opcode.Plus:
			return v, true
		case opcode.Minus:
			return partitionConstant{n: new(big.Int).Neg(v.n)}, true
		}
	case *ast.BinaryOperationExpr:
		l, ok := evalPartitionConstant(e.L)
		if !ok {
			return partitionConstant{}, false
		}
		r, ok := evalPartitionConstant(e.R)
		if !ok {
			return partitionConstant{}, false
		}
		result := partitionConstant{n: new(big.Int), unsigned: l.unsigned || r.unsigned}
		switch e.Op { //nolint:exhaustive // every other operator is left unfolded
		case opcode.Plus:
			result.n.Add(l.n, r.n)
		case opcode.Minus:
			result.n.Sub(l.n, r.n)
		case opcode.Mul:
			result.n.Mul(l.n, r.n)
		case opcode.IntDiv:
			if r.n.Sign() == 0 {
				return partitionConstant{}, false
			}
			// MySQL's DIV truncates toward zero, as Quo does.
			result.n.Quo(l.n, r.n)
		case opcode.Mod:
			if r.n.Sign() == 0 {
				return partitionConstant{}, false
			}
			// MySQL's MOD takes the sign of the dividend, as Rem does.
			result.n.Rem(l.n, r.n)
		default:
			return partitionConstant{}, false
		}
		return result, true
	case *ast.FuncCallExpr:
		n, ok := evalPartitionDateFunc(e)
		return partitionConstant{n: n}, ok
	}
	return partitionConstant{}, false
}

// evalPartitionDateFunc evaluates TO_DAYS, TO_SECONDS or YEAR of a date or
// datetime string literal.
func evalPartitionDateFunc(call *ast.FuncCallExpr) (*big.Int, bool) {
	if call.Schema.L != "" || len(call.Args) != 1 {
		return nil, false
	}
	literal, ok := stringLiteralValue(unwrapParenExpr(call.Args[0]))
	if !ok {
		return nil, false
	}
	var t time.Time
	var err error
	if len(literal) == len(time.DateOnly) {
		t, err = time.Parse(time.DateOnly, literal)
	} else {
		t, err = time.Parse(time.DateTime, literal)
	}
	if err != nil || t.Year() < 1 {
		return nil, false
	}
	days := mysqlDayNumberOfUnixEpoch + t.Unix()/86400
	if t.Unix() < 0 && t.Unix()%86400 != 0 {
		days-- // floor, not truncation, for dates before 1970
	}
	switch call.FnName.L {
	case "to_days":
		return big.NewInt(days), true
	case "to_seconds":
		secondOfDay := int64(t.Hour()*3600 + t.Minute()*60 + t.Second())
		return big.NewInt(days*86400 + secondOfDay), true
	case "year":
		return big.NewInt(int64(t.Year())), true
	}
	return nil, false
}

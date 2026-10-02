package statement

import (
	"cmp"
	"math/big"
	"slices"

	"github.com/block/spirit/pkg/parser"
)

func init() { registerNormalizer(partitionListValueOrderNormalizer{}) }

// partitionListValueOrderNormalizer sorts the values of each VALUES IN list.
// The order of a VALUES IN list has no meaning, but MySQL stores it as
// written (LIST (expr) moves NULL first), so without this rule a desired
// schema that lists the values in another order than the live table never
// converges, and every diff re-emits the same REORGANIZE.
//
// The order is NULL, then numbers by value, then string literals, then
// expressions spirit could not fold, by text. A LIST COLUMNS tuple sorts by
// its elements in turn. A foldable expression sorts by the value
// partitionBoundConstantNormalizer folds it to, so the result does not
// depend on which rule runs first.
type partitionListValueOrderNormalizer struct{}

func (partitionListValueOrderNormalizer) Name() string { return "partition-list-value-order" }

func (partitionListValueOrderNormalizer) Normalize(ct *CreateTable) *CreateTable {
	if ct.Partition == nil || ct.Partition.Type != "LIST" {
		return ct
	}
	p := parser.New()
	for i := range ct.Partition.Definitions {
		values := ct.Partition.Definitions[i].Values
		if values == nil || values.Type != "IN" {
			continue
		}
		keys := make([]listSortKey, len(values.Values))
		order := make([]int, len(values.Values))
		for j, v := range values.Values {
			order[j] = j
			keys[j] = newListSortKey(p, v)
		}
		slices.SortStableFunc(order, func(a, b int) int { return keys[a].compare(keys[b]) })
		sorted := make([]any, len(values.Values))
		for j, k := range order {
			sorted[j] = values.Values[k]
		}
		values.Values = sorted
	}
	return ct
}

// listSortKey is the sort position of one VALUES IN value.
type listSortKey struct {
	rank  int      // listRank*
	n     *big.Int // for listRankNumber
	text  string   // for listRankString and listRankExpression
	tuple []listSortKey
}

const (
	listRankNull = iota
	listRankNumber
	listRankString
	listRankExpression
	listRankTuple
)

func newListSortKey(p *parser.Parser, v any) listSortKey {
	switch val := foldPartitionValue(p, clonePartitionValue(v)).(type) {
	case partitionNullValue:
		return listSortKey{rank: listRankNull}
	case partitionStringLiteral:
		return listSortKey{rank: listRankString, text: string(val)}
	case partitionExprValue:
		return listSortKey{rank: listRankExpression, text: string(val)}
	case partitionValueTuple:
		key := listSortKey{rank: listRankTuple, tuple: make([]listSortKey, len(val))}
		for i, elem := range val {
			key.tuple[i] = newListSortKey(p, elem)
		}
		return key
	case string:
		if n, ok := new(big.Int).SetString(val, 10); ok {
			return listSortKey{rank: listRankNumber, n: n}
		}
		return listSortKey{rank: listRankExpression, text: val}
	default:
		return listSortKey{rank: listRankExpression, text: formatPartitionValue(val)}
	}
}

func (k listSortKey) compare(o listSortKey) int {
	if c := cmp.Compare(k.rank, o.rank); c != 0 {
		return c
	}
	switch k.rank {
	case listRankNumber:
		return k.n.Cmp(o.n)
	case listRankTuple:
		for i := range min(len(k.tuple), len(o.tuple)) {
			if c := k.tuple[i].compare(o.tuple[i]); c != 0 {
				return c
			}
		}
		return cmp.Compare(len(k.tuple), len(o.tuple))
	default:
		return cmp.Compare(k.text, o.text)
	}
}

// clonePartitionValue copies a tuple, which foldPartitionValue folds in place.
func clonePartitionValue(v any) any {
	if tuple, ok := v.(partitionValueTuple); ok {
		return slices.Clone(tuple)
	}
	return v
}

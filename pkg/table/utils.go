package table

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

// lazyFindP90 finds the second to last value in a slice.
// This is the same as a p90 if there are 10 values, but if
// there were 100 values it would technically be a p99 etc.
func lazyFindP90(a []time.Duration) time.Duration {
	sort.Slice(a, func(i, j int) bool {
		return a[i] > a[j]
	})
	return a[len(a)/10]
}

// castableTp returns an approximate type that tp can be casted to.
// This is because in the context of CAST()/CONVERT() MySQL will
// not allow all of the built-in types, but instead follows SQL standard
// types, which we need to map to.
func castableTp(tp string) string {
	newTp := removeWidth(tp)
	newTp = removeEnumSetOpts(newTp)
	newTp = removeZerofill(newTp)
	newTp = removeDecimalWidth(newTp)
	switch newTp {
	case "tinyint", "smallint", "mediumint", "int", "bigint":
		return "signed"
	case "tinyint unsigned", "smallint unsigned", "mediumint unsigned", "int unsigned", "bigint unsigned":
		return "unsigned"
	case "timestamp", "datetime":
		return "datetime"
	case "varchar", "enum", "set", "text", "mediumtext", "longtext":
		return "char"
	case "tinyblob", "blob", "mediumblob", "longblob", "varbinary", "binary":
		return "binary"
	case "float", "double": // required for MySQL 5.7
		return "char"
	case "json":
		return "json"
	case "decimal":
		return tp
	default:
		return "char" // default to char because it ends up being a char string in the concat anyway.
	}
}

func removeWidth(s string) string {
	regex := regexp.MustCompile(`\([0-9]+\)`)
	s = regex.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

func removeDecimalWidth(s string) string {
	regex := regexp.MustCompile(`\([0-9]+,[0-9]+\)`)
	s = regex.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

func removeEnumSetOpts(s string) string {
	if len(s) > 4 && strings.EqualFold(s[:4], "enum") {
		return "enum"
	}
	if len(s) > 3 && strings.EqualFold(s[:3], "set") {
		return "set"
	}
	return s
}

func removeZerofill(s string) string {
	return strings.Replace(s, " zerofill", "", -1)
}

func QuoteColumns(cols []string) string {
	q := make([]string, len(cols))
	for i, col := range cols {
		q[i] = "`" + col + "`"
	}
	return strings.Join(q, ", ")
}

// expandRowConstructorComparison is a workaround for MySQL
// not always optimizing conditions such as (a,b,c) > (1,2,3).
// This limitation is still current in 8.0, and was not fixed
// by the work in https://dev.mysql.com/worklog/task/?id=7019
func expandRowConstructorComparison(cols []string, operator Operator, vals []Datum) string {
	if len(cols) != len(vals) {
		panic("cols should be same size as values")
	}
	if len(cols) == 1 {
		return fmt.Sprintf("`%s` %s %s", cols[0], operator, vals[0].String())
	}
	// Unless we are in the "final" position
	// we need to use a different intermediate operator
	// for comparison. i.e. >= becomes >
	intermediateOperator := operator
	switch operator { //nolint: exhaustive
	case OpGreaterEqual:
		intermediateOperator = OpGreaterThan
	case OpLessEqual:
		intermediateOperator = OpLessThan
	}
	conds := []string{}
	buffer := []string{}
	for i, col := range cols {
		if i == 0 {
			conds = append(conds, fmt.Sprintf("(`%s` %s %s)", col, intermediateOperator, vals[i].String()))
			buffer = append(buffer, fmt.Sprintf("`%s` %s %s", col, "=", vals[i].String()))
			continue
		}
		// If we are in the final position we can
		// overwrite the intermediate operator with
		// the original operator.
		if i == len(cols)-1 {
			intermediateOperator = operator
		}
		conds = append(conds, fmt.Sprintf("(%s AND `%s` %s %s)", strings.Join(buffer, " AND "), col, intermediateOperator, vals[i].String()))
		buffer = append(buffer, fmt.Sprintf("`%s` %s %s", col, "=", vals[i].String()))
	}
	return "(" + strings.Join(conds, "\n OR ") + ")"
}

// Package utils contains some common utilities used by all other packages.
package utils

import (
	"fmt"
	"strings"

	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/table"
)

const (
	PrimaryKeySeparator = "-#-" // used to hash a composite primary key
)

// HashKey is used to convert a composite key into a string
// so that it can be placed in a map.
func HashKey(key []any) string {
	var pk []string
	for _, v := range key {
		pk = append(pk, fmt.Sprintf("%v", v))
	}
	return strings.Join(pk, PrimaryKeySeparator)
}

// IntersectNonGeneratedColumns returns a string of columns that are in both tables
func IntersectNonGeneratedColumns(t1, t2 *table.TableInfo) string {
	var intersection []string
	for _, col := range t1.NonGeneratedColumns {
		for _, col2 := range t2.NonGeneratedColumns {
			if col == col2 {
				intersection = append(intersection, "`"+col+"`")
			}
		}
	}
	return strings.Join(intersection, ", ")
}

// UnhashKey converts a hashed key to a string that can be used in a query.
func UnhashKey(key string) string {
	str := strings.Split(key, PrimaryKeySeparator)
	if len(str) == 1 {
		return "'" + sqlescape.EscapeString(str[0]) + "'"
	}
	for i, v := range str {
		str[i] = "'" + sqlescape.EscapeString(v) + "'"
	}
	return "(" + strings.Join(str, ",") + ")"
}

// ErrInErr is a wrapper func to not nest too deeply in an error being handled
// inside of an already error path. Not catching the error makes linters unhappy,
// but because it's already in an error path, there's not much to do.
func ErrInErr(_ error) {
}

func StripPort(hostname string) string {
	if strings.Contains(hostname, ":") {
		return strings.Split(hostname, ":")[0]
	}
	return hostname
}

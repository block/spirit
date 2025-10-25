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
// The column names are in backticks and comma separated.
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

// IntersectNonGeneratedColumnsAsSlice returns a slice of column names that are in both tables
func IntersectNonGeneratedColumnsAsSlice(t1, t2 *table.TableInfo) []string {
	var intersection []string
	for _, col := range t1.NonGeneratedColumns {
		for _, col2 := range t2.NonGeneratedColumns {
			if col == col2 {
				intersection = append(intersection, col)
			}
		}
	}
	return intersection
}

// UnhashKey converts a hashed key to a []string
func UnhashKey(key string) []string {
	return strings.Split(key, PrimaryKeySeparator)
}

// UnhashKeyToString converts a hashed key to a string that can be used in a query.
func UnhashKeyToString(key string) string {
	str := strings.Split(key, PrimaryKeySeparator)
	if len(str) == 1 {
		return "'" + sqlescape.EscapeString(str[0]) + "'"
	}
	for i, v := range str {
		str[i] = "'" + sqlescape.EscapeString(v) + "'"
	}
	return "(" + strings.Join(str, ",") + ")"
}

func StripPort(hostname string) string {
	if strings.Contains(hostname, ":") {
		return strings.Split(hostname, ":")[0]
	}
	return hostname
}

// EscapeMySQLType formats a Go value for use in SQL VALUES clause using proper SQL escaping
// It takes into account the MySQL column type to handle escaping appropriately
func EscapeMySQLType(columnType string, value any) string {
	if value == nil {
		return "NULL"
	}

	// Normalize the MySQL type to uppercase and extract the base type
	mysqlType := strings.ToUpper(columnType)
	baseType := mysqlType
	if idx := strings.Index(mysqlType, "("); idx != -1 {
		baseType = mysqlType[:idx]
	}

	// Handle based on MySQL column type
	switch baseType {
	case "JSON":
		// For JSON, treat as string even if it's []byte from the driver
		// Use CONVERT to ensure proper character set handling without changing JSON format
		if b, ok := value.([]byte); ok {
			return "CONVERT('" + sqlescape.EscapeString(string(b)) + "' USING utf8mb4)"
		}
		return "CONVERT('" + sqlescape.EscapeString(fmt.Sprintf("%v", value)) + "' USING utf8mb4)"

	case "VARBINARY", "BINARY", "BLOB", "LONGBLOB", "MEDIUMBLOB", "TINYBLOB":
		// These should use _binary prefix for proper binary handling
		if b, ok := value.([]byte); ok {
			// Use the default escaping which will add _binary prefix for []byte
			return sqlescape.MustEscapeSQL("%?", b)
		}
		// If not []byte, convert to string and then to binary
		str := fmt.Sprintf("%v", value)
		return sqlescape.MustEscapeSQL("%?", []byte(str))

	case "VARCHAR", "CHAR", "TEXT", "LONGTEXT", "MEDIUMTEXT", "TINYTEXT":
		// Text types should be quoted strings, not binary
		if b, ok := value.([]byte); ok {
			return "'" + sqlescape.EscapeString(string(b)) + "'"
		}
		return "'" + sqlescape.EscapeString(fmt.Sprintf("%v", value)) + "'"

	case "DATETIME", "TIMESTAMP", "DATE", "TIME":
		// Time types - handle []byte from driver
		if b, ok := value.([]byte); ok {
			return "'" + sqlescape.EscapeString(string(b)) + "'"
		}
		// Use default escaping for time.Time values
		return sqlescape.MustEscapeSQL("%?", value)
	case "INT", "BIGINT", "TINYINT", "SMALLINT", "MEDIUMINT", "FLOAT", "DOUBLE", "DECIMAL":
		// Numeric types - convert []byte to string without quotes
		if b, ok := value.([]byte); ok {
			return string(b)
		}
		return fmt.Sprintf("%v", value)

	default:
		// For other types, convert []byte to string and use default escaping
		if b, ok := value.([]byte); ok {
			// Convert to string first, then escape
			return "'" + sqlescape.EscapeString(string(b)) + "'"
		}
		return sqlescape.MustEscapeSQL("%?", value)
	}
}

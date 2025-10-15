package utils

import (
	"os"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
	os.Exit(m.Run())
}

func TestIntersectColumns(t *testing.T) {
	t1 := table.NewTableInfo(nil, "test", "t1")
	t1new := table.NewTableInfo(nil, "test", "t1_new")
	t1.NonGeneratedColumns = []string{"a", "b", "c"}
	t1new.NonGeneratedColumns = []string{"a", "b", "c"}
	str := IntersectNonGeneratedColumns(t1, t1new)
	assert.Equal(t, "`a`, `b`, `c`", str)

	t1new.NonGeneratedColumns = []string{"a", "c"}
	str = IntersectNonGeneratedColumns(t1, t1new)
	assert.Equal(t, "`a`, `c`", str)

	t1new.NonGeneratedColumns = []string{"a", "c", "d"}
	str = IntersectNonGeneratedColumns(t1, t1new)
	assert.Equal(t, "`a`, `c`", str)
}

func TestGetIntersectingColumns(t *testing.T) {
	t1 := table.NewTableInfo(nil, "test", "t1")
	t1new := table.NewTableInfo(nil, "test", "t1_new")
	t1.NonGeneratedColumns = []string{"a", "b", "c"}
	t1new.NonGeneratedColumns = []string{"a", "b", "c"}
	cols := IntersectNonGeneratedColumnsAsSlice(t1, t1new)
	assert.Equal(t, []string{"a", "b", "c"}, cols)

	t1new.NonGeneratedColumns = []string{"a", "c"}
	cols = IntersectNonGeneratedColumnsAsSlice(t1, t1new)
	assert.Equal(t, []string{"a", "c"}, cols)

	t1new.NonGeneratedColumns = []string{"a", "c", "d"}
	cols = IntersectNonGeneratedColumnsAsSlice(t1, t1new)
	assert.Equal(t, []string{"a", "c"}, cols)
}

func TestHashAndUnhashKey(t *testing.T) {
	// This func helps put composite keys in a map.
	key := []any{"1234", "ACDC", "12"}
	hashed := HashKey(key)
	assert.Equal(t, "1234-#-ACDC-#-12", hashed)
	unhashed := UnhashKey(hashed)
	// unhashed returns as a string, not the original any
	assert.Equal(t, "('1234','ACDC','12')", unhashed)

	// This also works on single keys.
	key = []any{"1234"}
	hashed = HashKey(key)
	assert.Equal(t, "1234", hashed)
	unhashed = UnhashKey(hashed)
	assert.Equal(t, "'1234'", unhashed)
}

func TestStripPort(t *testing.T) {
	assert.Equal(t, "hostname.com", StripPort("hostname.com"))
	assert.Equal(t, "hostname.com", StripPort("hostname.com:3306"))
	assert.Equal(t, "127.0.0.1", StripPort("127.0.0.1:3306"))
}

func TestEscapeMySQLType(t *testing.T) {
	// Test NULL values
	assert.Equal(t, "NULL", EscapeMySQLType("INT", nil))
	assert.Equal(t, "NULL", EscapeMySQLType("VARCHAR(255)", nil))
	assert.Equal(t, "NULL", EscapeMySQLType("JSON", nil))

	// Test JSON types
	jsonBytes := []byte(`[1, 2, 3]`)
	assert.Equal(t, "CONVERT('[1, 2, 3]' USING utf8mb4)", EscapeMySQLType("JSON", jsonBytes))
	assert.Equal(t, "CONVERT('test' USING utf8mb4)", EscapeMySQLType("json", "test"))

	// Test VARBINARY types
	binaryData := []byte{0x01, 0x02, 0x03}
	result := EscapeMySQLType("VARBINARY(255)", binaryData)
	assert.Contains(t, result, "_binary")
	// The exact escaping format depends on the sqlescape implementation
	// Just verify it contains _binary and the data is escaped
	assert.True(t, strings.HasPrefix(result, "_binary'"))

	// Test VARCHAR/TEXT types
	textBytes := []byte("hello world")
	assert.Equal(t, "'hello world'", EscapeMySQLType("VARCHAR(255)", textBytes))
	assert.Equal(t, "'hello world'", EscapeMySQLType("TEXT", "hello world"))
	assert.Equal(t, "'hello world'", EscapeMySQLType("CHAR(50)", textBytes))

	// Test with quotes that need escaping
	textWithQuotes := []byte("hello 'world'")
	assert.Equal(t, "'hello \\'world\\''", EscapeMySQLType("VARCHAR(255)", textWithQuotes))

	// Test DATETIME/TIMESTAMP types
	timeBytes := []byte("2023-01-01 12:00:00")
	assert.Equal(t, "'2023-01-01 12:00:00'", EscapeMySQLType("DATETIME", timeBytes))
	assert.Equal(t, "'2023-01-01 12:00:00'", EscapeMySQLType("TIMESTAMP", timeBytes))

	// Test numeric types
	intBytes := []byte("123")
	assert.Equal(t, "123", EscapeMySQLType("INT", intBytes))
	assert.Equal(t, "123", EscapeMySQLType("BIGINT", intBytes))
	assert.Equal(t, "456", EscapeMySQLType("INT", 456))

	floatBytes := []byte("123.45")
	assert.Equal(t, "123.45", EscapeMySQLType("FLOAT", floatBytes))
	assert.Equal(t, "123.45", EscapeMySQLType("DOUBLE", floatBytes))

	// Test types with size specifications
	assert.Equal(t, "123", EscapeMySQLType("INT(11)", intBytes))
	assert.Equal(t, "'hello'", EscapeMySQLType("VARCHAR(255)", []byte("hello")))

	// Test unknown/default types
	unknownBytes := []byte("unknown data")
	assert.Equal(t, "'unknown data'", EscapeMySQLType("UNKNOWN_TYPE", unknownBytes))
	assert.Equal(t, "'test'", EscapeMySQLType("CUSTOM_TYPE", "test"))

	// Test BLOB types
	blobData := []byte{0xFF, 0xFE, 0xFD}
	result = EscapeMySQLType("BLOB", blobData)
	assert.Contains(t, result, "_binary")

	// Test case insensitivity
	assert.Equal(t, "123", EscapeMySQLType("int", intBytes))
	assert.Equal(t, "123", EscapeMySQLType("Int", intBytes))
	assert.Equal(t, "'hello'", EscapeMySQLType("varchar(100)", []byte("hello")))
}

package change

import (
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
)

// TestDescribeTableCopies pins that a descriptor does not alias the
// TableInfo's slices: a reload of the table must not rewrite what a
// subscription already told its source.
func TestDescribeTableCopies(t *testing.T) {
	ti := &table.TableInfo{SchemaName: "s", TableName: "t", Columns: []string{"a", "b"}, KeyColumns: []string{"a"}}
	d := DescribeTable(ti)
	ti.Columns[0] = "x"
	ti.KeyColumns[0] = "x"
	assert.Equal(t, TableDescriptor{SchemaName: "s", TableName: "t", Columns: []string{"a", "b"}, KeyColumns: []string{"a"}}, d)
}

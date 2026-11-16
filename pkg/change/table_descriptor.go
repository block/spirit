package change

import (
	"slices"

	"github.com/block/spirit/pkg/table"
)

// TableDescriptor names a table a Subscription is for and gives its shape. It
// is what a Source needs to route row changes to a subscription and to
// recognize DDL against its tables, and nothing engine-specific: how a source
// decodes its own row images is the source's business, and how changes are
// written to the target is the subscription's.
type TableDescriptor struct {
	SchemaName string
	TableName  string
	// Columns is every column, in the order a row image carries them.
	Columns []string
	// KeyColumns is the primary key's columns, in key order.
	KeyColumns []string
}

// DescribeTable returns t's descriptor. The slices are copies, so the
// descriptor does not change if t is reloaded.
func DescribeTable(t *table.TableInfo) TableDescriptor {
	return TableDescriptor{
		SchemaName: t.SchemaName,
		TableName:  t.TableName,
		Columns:    slices.Clone(t.Columns),
		KeyColumns: slices.Clone(t.KeyColumns),
	}
}

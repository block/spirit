package contract

// TableID names a table: its schema (database) and its table name, both
// unquoted.
type TableID struct {
	Schema string
	Name   string
}

// Table describes a table that a change stream carries rows for: which table
// it is, and the layout of its row images.
//
// A row image for the table holds the value of each ColumnNames entry at that
// entry's position. For a subscription's current (source) table, that is the
// image the Subscription receives through HasChanged and the image a
// LogicalRow carries. A key holds one value per KeyColumnNames entry, in that
// order.
//
// The returned slices belong to the implementation; callers must not modify
// them.
type Table interface {
	// ID returns the table's schema and name.
	ID() TableID
	// ColumnNames returns every column of the table, in row-image order.
	ColumnNames() []string
	// KeyColumnNames returns the primary-key columns, in key order. Each
	// is also in ColumnNames.
	KeyColumnNames() []string
}

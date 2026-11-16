package contract

// ColumnMapping says how the columns of a source table map onto the columns
// of a target table: which columns a write carries, and what each is called
// on either side. The target can be the source table itself, or a table of
// the same shape on another server.
type ColumnMapping interface {
	// Source returns the table the row images come from.
	Source() Table

	// Target returns the table the rows are written to.
	Target() Table

	// ColumnPairs returns the columns a write carries, as two slices of the
	// same length: source[i] is a column of Source, and target[i] is the
	// name of the Target column it is written to. Pairs are in Source's
	// column order. A Source column with no writable Target column is not
	// in either slice. The slices belong to the implementation; callers
	// must not modify them.
	ColumnPairs() (source, target []string)
}

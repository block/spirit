package contract

import "context"

// StreamedChangesWriter writes streamed row changes to the target(s)
// synchronously: the changes a change stream's subscription has buffered, as
// deletes by key and upserts of full row images. A subscription's flush calls
// it; each call returns only once the change has been written.
//
// Locks: an empty locks slice means the write is not under a table lock and
// runs on the writer's regular connection(s). A non-empty slice carries the
// table locks the caller holds, one per target server. Each target's
// statements must run under that target's own lock, matched by the lock's
// DB(). A target with no lock, or a lock that matches no target, is an error,
// returned before anything is written.
type StreamedChangesWriter interface {
	// DeleteKeys deletes the target rows with the given keys. Each entry in
	// keys is one key tuple of the source row's values, in
	// sourceTable.KeyColumnNames() order. targetTable may be nil, meaning a
	// table with the same name as sourceTable on the target. It returns the
	// number of rows deleted.
	DeleteKeys(ctx context.Context, sourceTable, targetTable Table, keys [][]any, locks []TableLock) (int64, error)

	// UpsertRows writes each row so that the target row with its key ends
	// up equal to its image, inserting it or replacing the existing row.
	// Each row's RowImage is a row image of mapping.Source(); the writer
	// writes the columns mapping.ColumnPairs() lists, under their target
	// names. It returns the number of rows affected, as the target counts
	// them.
	UpsertRows(ctx context.Context, mapping ColumnMapping, rows []LogicalRow, locks []TableLock) (int64, error)
}

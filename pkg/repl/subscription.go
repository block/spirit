package repl

import (
	"context"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
)

// Subscription is now an interface! It defines how the replication changes are tracked.
// We have two strategies:
// 1. Delta Map: The OG, works only with memory comparable primary keys. Tracks the PK and if the last operation was a delete or not.
// 2. Delta Queue: A queue of changes, works with any primary key type. Does not effectively de-deplucate intermediate changes and must be replayed in order.
// 3. Buffered: A version of delta map that doesn't use REPLACE INTO .. SELECT.

type Subscription interface {
	HasChanged(key, row []any, deleted bool)
	Length() int
	Flush(ctx context.Context, underLock bool, lock *dbconn.TableLock) (allChangesFlushed bool, err error)
	Tables() []*table.TableInfo            // returns the tables related to the subscription in currentTable, newTable order
	SetWatermarkOptimization(enabled bool) // Controls both high and low watermark optimizations
}

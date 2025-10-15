package repl

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
)

type queuedChange struct {
	key      string
	isDelete bool
}

type deltaQueue struct {
	sync.Mutex // protects the subscription from changes.

	c *Client // reference back to the client.

	table    *table.TableInfo
	newTable *table.TableInfo

	changes []queuedChange

	enableKeyAboveWatermark bool
	keyAboveCopierCallback  func(any) bool
}

// Assert that deltaQueue implements subscription
var _ Subscription = (*deltaQueue)(nil)

func (s *deltaQueue) Length() int {
	s.Lock()
	defer s.Unlock()

	return len(s.changes)
}

func (s *deltaQueue) Tables() []*table.TableInfo {
	return []*table.TableInfo{s.table, s.newTable}
}

func (s *deltaQueue) HasChanged(key, _ []any, deleted bool) {
	s.Lock()
	defer s.Unlock()

	// The KeyAboveWatermark optimization has to be enabled
	// We enable it once all the setup has been done (since we create a repl client
	// earlier in setup to ensure binary logs are available).
	// We then disable the optimization after the copier phase has finished.
	if s.keyAboveWatermarkEnabled() && s.keyAboveCopierCallback(key[0]) {
		s.c.logger.Debugf("key above watermark: %v", key[0])
		return
	}
	s.changes = append(s.changes, queuedChange{key: utils.HashKey(key), isDelete: deleted})
}

func (s *deltaQueue) createDeleteStmt(deleteKeys []string) statement {
	var deleteStmt string
	if len(deleteKeys) > 0 {
		deleteStmt = fmt.Sprintf("DELETE FROM %s WHERE (%s) IN (%s)",
			s.newTable.QuotedName,
			table.QuoteColumns(s.table.KeyColumns),
			pksToRowValueConstructor(deleteKeys),
		)
	}
	return statement{
		numKeys: len(deleteKeys),
		stmt:    deleteStmt,
	}
}

func (s *deltaQueue) createReplaceStmt(replaceKeys []string) statement {
	var replaceStmt string
	if len(replaceKeys) > 0 {
		replaceStmt = fmt.Sprintf("REPLACE INTO %s (%s) SELECT %s FROM %s FORCE INDEX (PRIMARY) WHERE (%s) IN (%s)",
			s.newTable.QuotedName,
			utils.IntersectNonGeneratedColumns(s.table, s.newTable),
			utils.IntersectNonGeneratedColumns(s.table, s.newTable),
			s.table.QuotedName,
			table.QuoteColumns(s.table.KeyColumns),
			pksToRowValueConstructor(replaceKeys),
		)
	}
	return statement{
		numKeys: len(replaceKeys),
		stmt:    replaceStmt,
	}
}

// Flush flushes the FIFO queue that is used when the PRIMARY KEY
// is not memory comparable. It needs to be single threaded,
// so it might not scale as well as the Delta Map, but offering
// it at least helps improve compatibility.
//
// The only optimization we do is we try to MERGE statements together, such
// that if there are operations: REPLACE<1>, REPLACE<2>, DELETE<3>, REPLACE<4>
// we merge it to REPLACE<1,2>, DELETE<3>, REPLACE<4>.
func (s *deltaQueue) Flush(ctx context.Context, underLock bool, lock *dbconn.TableLock) error {
	s.Lock()
	defer s.Unlock()

	// Early return if there is nothing to flush.
	if len(s.changes) == 0 {
		return nil
	}
	// Otherwise, flush the changes.
	var stmts []statement
	var buffer []string
	prevKey := s.changes[0] // for initialization
	target := int(atomic.LoadInt64(&s.c.targetBatchSize))
	for _, change := range s.changes {
		// We are changing from DELETE to REPLACE
		// or vice versa, *or* the buffer is getting very large.
		if change.isDelete != prevKey.isDelete || len(buffer) > target {
			if prevKey.isDelete {
				stmts = append(stmts, s.createDeleteStmt(buffer))
			} else {
				stmts = append(stmts, s.createReplaceStmt(buffer))
			}
			buffer = nil // reset
		}
		buffer = append(buffer, change.key)
		prevKey.isDelete = change.isDelete
	}
	// Flush the buffer once more.
	if prevKey.isDelete {
		stmts = append(stmts, s.createDeleteStmt(buffer))
	} else {
		stmts = append(stmts, s.createReplaceStmt(buffer))
	}
	if underLock {
		// Execute under lock means it is a final flush
		// We need to use the lock connection to do this
		// so there is no parallelism.
		if err := lock.ExecUnderLock(ctx, extractStmt(stmts)...); err != nil {
			return err
		}
	} else {
		// Execute the statements in a transaction.
		// They still need to be single threaded.
		if _, err := dbconn.RetryableTransaction(ctx, s.c.db, true, s.c.dbConfig, extractStmt(stmts)...); err != nil {
			return err
		}
	}
	// If it's successful, we can clear the queue
	// and return to release the mutex for new changes
	// to start accumulating again.
	s.changes = nil
	return nil
}

// keyAboveWatermarkEnabled returns true if the KeyAboveWatermark optimization
// is enabled. This is already called under a mutex.
func (s *deltaQueue) keyAboveWatermarkEnabled() bool {
	return s.enableKeyAboveWatermark && s.keyAboveCopierCallback != nil
}

func (s *deltaQueue) SetKeyAboveWatermarkOptimization(enabled bool) {
	s.Lock()
	defer s.Unlock()
	s.enableKeyAboveWatermark = enabled
}

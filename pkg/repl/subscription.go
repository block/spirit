package repl

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cashapp/spirit/pkg/dbconn"
	"github.com/cashapp/spirit/pkg/table"
	"github.com/cashapp/spirit/pkg/utils"
	"golang.org/x/sync/errgroup"
)

type queuedChange struct {
	key      string
	isDelete bool
}

type subscription struct {
	sync.Mutex // protects the subscription from changes.

	c *Client // reference back to the client.

	table    *table.TableInfo
	newTable *table.TableInfo

	disableDeltaMap bool            // use queue instead
	deltaMap        map[string]bool // delta map, for memory comparable PKs
	deltaQueue      []queuedChange  // used when disableDeltaMap is true

	enableKeyAboveWatermark bool
	keyAboveCopierCallback  func(any) bool
}

func (s *subscription) getDeltaLen() int {
	s.Lock()
	defer s.Unlock()

	if s.disableDeltaMap {
		return len(s.deltaQueue)
	}
	return len(s.deltaMap)
}

func (s *subscription) keyHasChanged(key []any, deleted bool) {
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
	if s.disableDeltaMap {
		s.deltaQueue = append(s.deltaQueue, queuedChange{key: utils.HashKey(key), isDelete: deleted})
		return
	}
	s.deltaMap[utils.HashKey(key)] = deleted
}

func (s *subscription) createDeleteStmt(deleteKeys []string) statement {
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

func (s *subscription) createReplaceStmt(replaceKeys []string) statement {
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

func (s *subscription) flush(ctx context.Context, underLock bool, lock *dbconn.TableLock) error {
	if s.disableDeltaMap {
		return s.flushDeltaQueue(ctx, underLock, lock)
	}
	return s.flushDeltaMap(ctx, underLock, lock)
}

// flushDeltaQueue flushes the FIFO queue that is used when the PRIMARY KEY
// is not memory comparable. It needs to be single threaded,
// so it might not scale as well as the Delta Map, but offering
// it at least helps improve compatibility.
//
// The only optimization we do is we try to MERGE statements together, such
// that if there are operations: REPLACE<1>, REPLACE<2>, DELETE<3>, REPLACE<4>
// we merge it to REPLACE<1,2>, DELETE<3>, REPLACE<4>.
func (s *subscription) flushDeltaQueue(ctx context.Context, underLock bool, lock *dbconn.TableLock) error {
	s.Lock()
	defer s.Unlock()

	// Early return if there is nothing to flush.
	if len(s.deltaQueue) == 0 {
		return nil
	}
	// Otherwise, flush the changes.
	var stmts []statement
	var buffer []string
	prevKey := s.deltaQueue[0] // for initialization
	target := int(atomic.LoadInt64(&s.c.targetBatchSize))
	for _, change := range s.deltaQueue {
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
	s.deltaQueue = nil
	return nil
}

// flushMap is the internal version of Flush() for the delta map.
// it is used by default unless the PRIMARY KEY is non memory comparable.
func (s *subscription) flushDeltaMap(ctx context.Context, underLock bool, lock *dbconn.TableLock) error {
	s.Lock()
	defer s.Unlock()

	// We must now apply the changeset s.deltaMap to the new table.
	var deleteKeys []string
	var replaceKeys []string
	var stmts []statement
	var i int64
	target := atomic.LoadInt64(&s.c.targetBatchSize)
	for key, isDelete := range s.deltaMap {
		i++
		if isDelete {
			deleteKeys = append(deleteKeys, key)
		} else {
			replaceKeys = append(replaceKeys, key)
		}
		if (i % target) == 0 {
			stmts = append(stmts, s.createDeleteStmt(deleteKeys))
			stmts = append(stmts, s.createReplaceStmt(replaceKeys))
			deleteKeys = []string{}
			replaceKeys = []string{}
		}
	}
	stmts = append(stmts, s.createDeleteStmt(deleteKeys))
	stmts = append(stmts, s.createReplaceStmt(replaceKeys))

	if underLock {
		// Execute under lock means it is a final flush
		// We need to use the lock connection to do this
		// so there is no parallelism.
		if err := lock.ExecUnderLock(ctx, extractStmt(stmts)...); err != nil {
			return err
		}
	} else {
		// Execute the statements in parallel
		// They should not conflict and order should not matter
		// because they come from a consistent view of a map,
		// which is distinct keys.
		g, errGrpCtx := errgroup.WithContext(ctx)
		g.SetLimit(s.c.concurrency)
		for _, stmt := range stmts {
			st := stmt
			g.Go(func() error {
				startTime := time.Now()
				_, err := dbconn.RetryableTransaction(errGrpCtx, s.c.db, false, dbconn.NewDBConfig(), st.stmt)
				s.c.feedback(st.numKeys, time.Since(startTime))
				return err
			})
		}
		// wait for all work to finish
		if err := g.Wait(); err != nil {
			return err
		}
	}
	// If it's successful, we can clear the map
	// and return to release the mutex for new changes
	// to start accumulating again.
	s.deltaMap = make(map[string]bool)
	return nil
}

// keyAboveWatermarkEnabled returns true if the KeyAboveWatermark optimization
// is enabled. This is already called under a mutex.
func (s *subscription) keyAboveWatermarkEnabled() bool {
	return s.enableKeyAboveWatermark && s.keyAboveCopierCallback != nil
}

func (s *subscription) setKeyAboveWatermarkOptimization(enabled bool) {
	s.Lock()
	defer s.Unlock()
	s.enableKeyAboveWatermark = enabled
}

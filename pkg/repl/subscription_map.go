package repl

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	"golang.org/x/sync/errgroup"
)

type deltaMap struct {
	sync.Mutex // protects the subscription from changes.

	c *Client // reference back to the client.

	table    *table.TableInfo
	newTable *table.TableInfo

	changes map[string]bool // delta map, for memory comparable PKs

	watermarkOptimization bool
	chunker               table.Chunker
}

// Assert that deltaMap implements subscription
var _ Subscription = (*deltaMap)(nil)

func (s *deltaMap) Length() int {
	s.Lock()
	defer s.Unlock()

	return len(s.changes)
}

func (s *deltaMap) Tables() []*table.TableInfo {
	return []*table.TableInfo{s.table, s.newTable}
}

func (s *deltaMap) HasChanged(key, _ []any, deleted bool) {
	s.Lock()
	defer s.Unlock()

	// The KeyAboveWatermark optimization has to be enabled
	// We enable it once all the setup has been done (since we create a repl client
	// earlier in setup to ensure binary logs are available).
	// We then disable the optimization after the copier phase has finished.
	if s.watermarkOptimizationEnabled() && s.chunker.KeyAboveHighWatermark(key[0]) {
		s.c.logger.Debug("key above watermark", "key", key[0])
		return
	}
	s.changes[utils.HashKey(key)] = deleted
}

func (s *deltaMap) createDeleteStmt(deleteKeys []string) statement {
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

func (s *deltaMap) createReplaceStmt(replaceKeys []string) statement {
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

// Flush writes the pending changes to the new table.
// We do this under a mutex, which means that unfortunately pending changes
// are blocked from being collected while we do this. In future we may
// come up with a more sophisticated approach to allow concurrent
// collection of changes while we flush.
func (s *deltaMap) Flush(ctx context.Context, underLock bool, lock *dbconn.TableLock) (allChangesFlushed bool, err error) {
	s.Lock()
	defer s.Unlock()

	var deleteKeys []string
	var replaceKeys []string
	var stmts []statement
	var keysFlushed []string // keys we need to remove from the map at the end.
	var i int64
	allChangesFlushed = true // assume all changes are flushed unless we find some that are not.
	target := atomic.LoadInt64(&s.c.targetBatchSize)
	for key, isDelete := range s.changes {
		unhashedKey := utils.UnhashKey(key)
		// Check low watermark only if the optimization is enabled
		if s.watermarkOptimizationEnabled() && !s.chunker.KeyBelowLowWatermark(unhashedKey[0]) {
			s.c.logger.Debug("key not below watermark", "key", unhashedKey[0])
			allChangesFlushed = false
			continue
		}
		i++
		keysFlushed = append(keysFlushed, key) // we are going to flush this key either way.
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
			return false, err
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
			return false, err
		}
	}
	// The statements have been executed successfully.
	// We can now remove the flushed keys from the map.
	for _, key := range keysFlushed {
		delete(s.changes, key)
	}
	return allChangesFlushed, nil
}

// watermarkOptimizationEnabled returns true if the watermark optimization
// is enabled. This is already called under a mutex.
func (s *deltaMap) watermarkOptimizationEnabled() bool {
	return s.watermarkOptimization && s.chunker != nil
}

func (s *deltaMap) SetWatermarkOptimization(enabled bool) {
	s.Lock()
	defer s.Unlock()
	s.watermarkOptimization = enabled
}

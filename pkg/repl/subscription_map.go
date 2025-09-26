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

	enableKeyAboveWatermark bool
	keyAboveCopierCallback  func(any) bool
}

// Assert that deltaMap implements subscription
var _ Subscription = (*deltaMap)(nil)

func (s *deltaMap) Length() int {
	s.Lock()
	defer s.Unlock()

	return len(s.changes)
}

func (s *deltaMap) Table() *table.TableInfo {
	return s.table
}

func (s *deltaMap) HasChanged(key, _ []any, deleted bool) {
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

func (s *deltaMap) Flush(ctx context.Context, underLock bool, lock *dbconn.TableLock) error {
	// Pop the changes into changesToFlush
	// and then reset the delta map. This allows concurrent
	// inserts back into the map to increase parallelism.
	s.Lock()
	changesToFlush := s.changes
	s.changes = make(map[string]bool)
	s.Unlock()

	// We must now apply the changeset setToFlush to the new table.
	var deleteKeys []string
	var replaceKeys []string
	var stmts []statement
	var i int64
	target := atomic.LoadInt64(&s.c.targetBatchSize)
	for key, isDelete := range changesToFlush {
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
	return nil
}

// keyAboveWatermarkEnabled returns true if the KeyAboveWatermark optimization
// is enabled. This is already called under a mutex.
func (s *deltaMap) keyAboveWatermarkEnabled() bool {
	return s.enableKeyAboveWatermark && s.keyAboveCopierCallback != nil
}

func (s *deltaMap) SetKeyAboveWatermarkOptimization(enabled bool) {
	s.Lock()
	defer s.Unlock()
	s.enableKeyAboveWatermark = enabled
}

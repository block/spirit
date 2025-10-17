package repl

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	"golang.org/x/sync/errgroup"
)

// The bufferedMap is an experiment to see if we can avoid using REPLACE INTO .. SELECT.
// See: https://github.com/block/spirit/issues/451
// This has the advantage that we can use spirit for MoveTable operations
// across different MySQL servers. In combination with Atomic DDL,
// we have all the components needed for cloning sets of tables between servers.

type bufferedMap struct {
	sync.Mutex // protects the subscription from changes.

	c *Client // reference back to the client.

	table    *table.TableInfo
	newTable *table.TableInfo

	changes map[string]logicalRow

	enableKeyAboveWatermark bool
	keyAboveCopierCallback  func(any) bool
}

// logicalRow represents the current state of a row in the subscription buffer.
// This could be that it is deleted, or that it has rowImage that describes it.
// If there is a rowImage, then it needs to be converted into the rowImage of the
// newTable.
type logicalRow struct {
	isDeleted bool
	rowImage  []any
}

// Assert that bufferedMap implements subscription
var _ Subscription = (*bufferedMap)(nil)

func (s *bufferedMap) Length() int {
	s.Lock()
	defer s.Unlock()

	return len(s.changes)
}

func (s *bufferedMap) Tables() []*table.TableInfo {
	return []*table.TableInfo{s.table, s.newTable}
}

func (s *bufferedMap) HasChanged(key, row []any, deleted bool) {
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

	hashedKey := utils.HashKey(key)

	if deleted {
		s.changes[hashedKey] = logicalRow{isDeleted: true}
		return
	}

	// Set the logical row to be the new row
	s.changes[hashedKey] = logicalRow{rowImage: row}
}

func (s *bufferedMap) createDeleteStmt(deleteKeys []string) (statement, error) {
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
	}, nil
}

// createUpsertStmt creates an Upsert (aka INSERT.. ON DUPLICATE KEY UPDATE).
// to insert each of the logicalRows in this buffer.
func (s *bufferedMap) createUpsertStmt(insertRows []logicalRow) (statement, error) {
	var insertStmt string
	if len(insertRows) > 0 {
		// Get the columns that exist in both source and destination tables
		columnList := utils.IntersectNonGeneratedColumns(s.table, s.newTable)
		columnNames := utils.IntersectNonGeneratedColumnsAsSlice(s.table, s.newTable)

		// Build the VALUES clause from the row images
		var valuesClauses []string
		for _, logicalRow := range insertRows {
			if logicalRow.isDeleted {
				continue // They should already be skipped, but skip anyway
			}

			// Convert the row image to a VALUES clause, but only for intersected columns
			// The row image may contain more columns than we want to copy
			var values []string
			intersectedColumns := s.getIntersectedColumns()

			for i, colIndex := range intersectedColumns {
				if colIndex >= len(logicalRow.rowImage) {
					return statement{}, fmt.Errorf("column index %d exceeds row image length %d", colIndex, len(logicalRow.rowImage))
				}
				value := logicalRow.rowImage[colIndex]
				if value == nil {
					values = append(values, "NULL")
				} else {
					// Get the column type for proper escaping
					if i >= len(columnNames) {
						return statement{}, fmt.Errorf("column index %d exceeds columnNames length %d", i, len(columnNames))
					}
					columnType, ok := s.table.GetColumnMySQLType(columnNames[i])
					if !ok {
						return statement{}, fmt.Errorf("column %s not found in table info", columnNames[i])
					}
					values = append(values, utils.EscapeMySQLType(columnType, value))
				}
			}
			valuesClauses = append(valuesClauses, fmt.Sprintf("(%s)", strings.Join(values, ", ")))
		}

		if len(valuesClauses) > 0 {
			// Build the ON DUPLICATE KEY UPDATE clause using MySQL 8.0+ syntax
			// Update all non-primary-key columns with NEW.column_name
			var updateClauses []string
			for _, col := range s.newTable.NonGeneratedColumns {
				// Skip primary key columns in the UPDATE clause
				isPrimaryKey := false
				for _, pkCol := range s.newTable.KeyColumns {
					if col == pkCol {
						isPrimaryKey = true
						break
					}
				}
				if !isPrimaryKey {
					// Check if this column exists in both tables (intersected columns)
					if s.columnExistsInBothTables(col) {
						updateClauses = append(updateClauses, fmt.Sprintf("`%s` = new.`%s`", col, col))
					}
				}
			}

			insertStmt = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s AS new ON DUPLICATE KEY UPDATE %s",
				s.newTable.QuotedName,
				columnList,
				strings.Join(valuesClauses, ", "),
				strings.Join(updateClauses, ", "),
			)
		}
	}
	return statement{
		numKeys: len(insertRows),
		stmt:    insertStmt,
	}, nil
}

// Flush writes changes to the new table.
// If underLock is true, then it uses the provided lock to execute
// the statements under a table lock. This is used for the final flush
// to ensure no changes are missed.
func (s *bufferedMap) Flush(ctx context.Context, underLock bool, lock *dbconn.TableLock) error {
	s.Lock()
	defer s.Unlock()

	// We must now apply the changeset setToFlush to the new table.
	var deleteKeys []string
	var upsertRows []logicalRow
	var stmts []statement
	var i int64
	target := atomic.LoadInt64(&s.c.targetBatchSize)
	for key, logicalRow := range s.changes {
		i++
		if logicalRow.isDeleted {
			deleteKeys = append(deleteKeys, key)
		} else {
			upsertRows = append(upsertRows, logicalRow)
		}
		if (i % target) == 0 {
			deleteStmts, err := s.createDeleteStmt(deleteKeys)
			if err != nil {
				return err
			}
			upsertStmts, err := s.createUpsertStmt(upsertRows)
			if err != nil {
				return err
			}
			stmts = append(stmts, deleteStmts)
			stmts = append(stmts, upsertStmts)
			deleteKeys = nil
			upsertRows = nil
		}
	}
	deleteStmts, err := s.createDeleteStmt(deleteKeys)
	if err != nil {
		return err
	}
	upsertStmts, err := s.createUpsertStmt(upsertRows)
	if err != nil {
		return err
	}
	stmts = append(stmts, deleteStmts)
	stmts = append(stmts, upsertStmts)

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
				_, err := dbconn.RetryableTransaction(errGrpCtx, s.c.writeDB, false, dbconn.NewDBConfig(), st.stmt)
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
	s.changes = make(map[string]logicalRow)
	return nil
}

// keyAboveWatermarkEnabled returns true if the KeyAboveWatermark optimization
// is enabled. This is already called under a mutex.
func (s *bufferedMap) keyAboveWatermarkEnabled() bool {
	return s.enableKeyAboveWatermark && s.keyAboveCopierCallback != nil
}

func (s *bufferedMap) SetKeyAboveWatermarkOptimization(enabled bool) {
	s.Lock()
	defer s.Unlock()
	s.enableKeyAboveWatermark = enabled
}

// getIntersectedColumns returns the column indices from the source table
// that correspond to columns that exist in both source and destination tables
func (s *bufferedMap) getIntersectedColumns() []int {
	var indices []int
	for i, sourceCol := range s.table.NonGeneratedColumns {
		for _, destCol := range s.newTable.NonGeneratedColumns {
			if sourceCol == destCol {
				indices = append(indices, i)
				break
			}
		}
	}
	return indices
}

// columnExistsInBothTables checks if a column exists in both source and destination tables
func (s *bufferedMap) columnExistsInBothTables(columnName string) bool {
	// Check if column exists in source table
	sourceExists := false
	for _, col := range s.table.NonGeneratedColumns {
		if col == columnName {
			sourceExists = true
			break
		}
	}

	// Check if column exists in destination table
	destExists := false
	for _, col := range s.newTable.NonGeneratedColumns {
		if col == columnName {
			destExists = true
			break
		}
	}

	return sourceExists && destExists
}

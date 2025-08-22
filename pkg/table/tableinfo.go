// Package table contains some common utilities for working with tables
// such as a 'Chunker' feature.
package table

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/siddontang/loggers"
)

const (
	lastChunkStatisticsThreshold = 10 * time.Second
)

var (
	ErrTableIsRead       = errors.New("table is read")
	ErrTableNotOpen      = errors.New("please call Open() first")
	ErrUnsupportedPKType = errors.New("unsupported primary key type")
)

type TableInfo struct {
	sync.Mutex
	db                          *sql.DB
	EstimatedRows               uint64 // used by the composite chunker for Max
	SchemaName                  string
	TableName                   string
	QuotedName                  string
	Columns                     []string          // all the column names
	NonGeneratedColumns         []string          // all the non-generated column names
	Indexes                     []string          // all the index names
	columnsMySQLTps             map[string]string // map from column name to MySQL type
	KeyColumns                  []string          // the column names of the primaryKey
	keyColumnsMySQLTp           []string          // the MySQL types of the primaryKey
	KeyIsAutoInc                bool              // if pk[0] is an auto_increment column
	keyDatums                   []datumTp         // the datum type of pk
	minValue                    Datum             // known minValue of pk[0] (using type of PK[0])
	maxValue                    Datum             // known maxValue of pk[0] (using type of PK[0])
	statisticsLastUpdated       time.Time
	statisticsLock              sync.Mutex
	DisableAutoUpdateStatistics atomic.Bool
}

func NewTableInfo(db *sql.DB, schema, table string) *TableInfo {
	return &TableInfo{
		db:         db,
		SchemaName: schema,
		TableName:  table,
		QuotedName: fmt.Sprintf("`%s`.`%s`", schema, table),
	}
}

// PrimaryKeyValues helps extract the PRIMARY KEY from a row image.
// It uses our knowledge of the ordinal position of columns to find the
// position of primary key columns (there might be more than one).
// For minimal row image, you need to send the before image to extract the PK.
// This is because in the after image, the PK might be nil.
func (t *TableInfo) PrimaryKeyValues(row any) ([]any, error) {
	var pkCols []any
	for _, pCol := range t.KeyColumns {
		for i, col := range t.Columns {
			if col == pCol {
				if row.([]any)[i] == nil {
					return nil, errors.New("primary key column is NULL, possibly a bug sending after-image instead of before")
				}
				pkCols = append(pkCols, row.([]any)[i])
			}
		}
	}
	return pkCols, nil
}

// SetInfo reads from MySQL metadata (usually infoschema) and sets the values in TableInfo.
func (t *TableInfo) SetInfo(ctx context.Context) error {
	t.statisticsLock.Lock()
	defer t.statisticsLock.Unlock()
	if err := t.setRowEstimate(ctx); err != nil {
		return err
	}
	if err := t.setColumns(ctx); err != nil {
		return err
	}
	if err := t.setPrimaryKey(ctx); err != nil {
		return err
	}
	if err := t.setIndexes(ctx); err != nil {
		return err
	}
	return t.setMinMax(ctx)
}

// setRowEstimate is a separate function so it can be repeated continuously
// Since if a schema migration takes 14 days, it could change.
func (t *TableInfo) setRowEstimate(ctx context.Context) error {
	_, err := t.db.ExecContext(ctx, "ANALYZE TABLE "+t.QuotedName)
	if err != nil {
		return err
	}
	err = t.db.QueryRowContext(ctx, "SELECT IFNULL(table_rows,0) FROM information_schema.tables WHERE table_schema=? AND table_name=?", t.SchemaName, t.TableName).Scan(&t.EstimatedRows)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("table %s.%s does not exist", t.SchemaName, t.TableName)
		}
		return err
	}
	return nil
}

func (t *TableInfo) setIndexes(ctx context.Context) error {
	rows, err := t.db.QueryContext(ctx, "SELECT DISTINCT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema=? AND table_name=? AND index_name != 'PRIMARY'",
		t.SchemaName,
		t.TableName,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	t.Indexes = []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		t.Indexes = append(t.Indexes, name)
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	return nil
}

func (t *TableInfo) setColumns(ctx context.Context) error {
	rows, err := t.db.QueryContext(ctx, "SELECT column_name, column_type, GENERATION_EXPRESSION FROM information_schema.columns WHERE table_schema=? AND table_name=? ORDER BY ORDINAL_POSITION",
		t.SchemaName,
		t.TableName,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	t.Columns = []string{}
	t.NonGeneratedColumns = []string{}
	t.columnsMySQLTps = make(map[string]string)
	for rows.Next() {
		var col, tp, expression string
		if err := rows.Scan(&col, &tp, &expression); err != nil {
			return err
		}
		t.Columns = append(t.Columns, col)
		t.columnsMySQLTps[col] = tp
		if expression == "" {
			t.NonGeneratedColumns = append(t.NonGeneratedColumns, col)
		}
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	return nil
}

// DescIndex describes the columns in an index.
func (t *TableInfo) DescIndex(keyName string) ([]string, error) {
	cols := []string{}
	rows, err := t.db.Query("SELECT column_name FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND index_name=? ORDER BY seq_in_index",
		t.SchemaName,
		t.TableName,
		keyName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return cols, nil
}

// setPrimaryKey sets the primary key and also the primary key type.
// A primary key can contain multiple columns.
func (t *TableInfo) setPrimaryKey(ctx context.Context) error {
	rows, err := t.db.QueryContext(ctx, "SELECT column_name FROM information_schema.key_column_usage WHERE table_schema=? and table_name=? and constraint_name='PRIMARY' ORDER BY ORDINAL_POSITION",
		t.SchemaName,
		t.TableName,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	t.KeyColumns = []string{}
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return err
		}
		t.KeyColumns = append(t.KeyColumns, col)
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	if len(t.KeyColumns) == 0 {
		return errors.New("no primary key found (not supported)")
	}
	for i, col := range t.KeyColumns {
		// Get primary key type and auto_inc info.
		query := "SELECT column_type, extra FROM information_schema.columns WHERE table_schema=? AND table_name=? and column_name=?"
		var extra, pkType string
		err = t.db.QueryRowContext(ctx, query, t.SchemaName, t.TableName, col).Scan(&pkType, &extra)
		if err != nil {
			return err
		}
		pkType = removeWidth(pkType)
		t.keyColumnsMySQLTp = append(t.keyColumnsMySQLTp, pkType)
		t.keyDatums = append(t.keyDatums, mySQLTypeToDatumTp(pkType))
		if i == 0 {
			t.KeyIsAutoInc = (extra == "auto_increment")
		}
	}
	return nil
}

// PrimaryKeyIsMemoryComparable checks that the PRIMARY KEY type is compatible.
// We no longer need this check for the chunker, since it can
// handle any type of key in the composite chunker.
// But the migration still needs to verify this, because of the
// delta map feature, which requires binary comparable keys.
func (t *TableInfo) PrimaryKeyIsMemoryComparable() error {
	if len(t.KeyColumns) == 0 || len(t.keyDatums) == 0 {
		return errors.New("please call setInfo() first")
	}
	for _, tp := range t.keyDatums {
		if tp == unknownType {
			return ErrUnsupportedPKType
		}
	}
	return nil
}

// setMinMax is a separate function so it can be repeated continuously
// Since if a schema migration takes 14 days, it could change.
// It only really applies to KeyColumns[0], since across composite keys
// there could be inter-dependencies between columns.
func (t *TableInfo) setMinMax(ctx context.Context) error {
	if t.keyDatums[0] == binaryType {
		return nil // we don't min/max binary types for now.
	}
	query := fmt.Sprintf("SELECT IFNULL(min(%s),'0'), IFNULL(max(%s),'0') FROM %s", t.KeyColumns[0], t.KeyColumns[0], t.QuotedName)
	var minimum, maximum string
	err := t.db.QueryRowContext(ctx, query).Scan(&minimum, &maximum)
	if err != nil {
		return err
	}

	t.minValue, err = newDatumFromMySQL(minimum, t.keyColumnsMySQLTp[0])
	if err != nil {
		return err
	}
	t.maxValue, err = newDatumFromMySQL(maximum, t.keyColumnsMySQLTp[0])
	if err != nil {
		return err
	}
	return nil
}

// Close currently does nothing
func (t *TableInfo) Close() error {
	return nil
}

// AutoUpdateStatistics runs a loop that updates the table statistics every interval.
// This will continue until Close() is called on the tableInfo, or t.DisableAutoUpdateStatistics is set to true.
func (t *TableInfo) AutoUpdateStatistics(ctx context.Context, interval time.Duration, logger loggers.Advanced) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if t.DisableAutoUpdateStatistics.Load() {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := t.updateTableStatistics(ctx); err != nil {
				logger.Errorf("error updating table statistics: %v", err)
			}
			logger.Infof("table statistics updated: estimated-rows=%d pk[0].max-value=%v", t.EstimatedRows, t.MaxValue())
		}
	}
}

// statisticsNeedUpdating returns true if the statistics are considered order than a threshold.
// this is useful for the chunker to synchronously check as it approaches the end of the table.
func (t *TableInfo) statisticsNeedUpdating() bool {
	threshold := time.Now().Add(-lastChunkStatisticsThreshold)
	return t.statisticsLastUpdated.Before(threshold)
}

// updateTableStatistics recalculates the min/max and row estimate.
func (t *TableInfo) updateTableStatistics(ctx context.Context) error {
	t.statisticsLock.Lock()
	defer t.statisticsLock.Unlock()
	err := t.setMinMax(ctx)
	if err != nil {
		return err
	}
	err = t.setRowEstimate(ctx)
	if err != nil {
		return err
	}
	t.statisticsLastUpdated = time.Now()
	return nil
}

// MaxValue as a datum
func (t *TableInfo) MaxValue() Datum {
	t.statisticsLock.Lock()
	defer t.statisticsLock.Unlock()
	return t.maxValue
}

func (t *TableInfo) WrapCastType(col string) string {
	tp, ok := t.columnsMySQLTps[col] // the tp keeps the width in this context.
	if !ok {
		panic("column not found")
	}
	return fmt.Sprintf("CAST(`%s` AS %s)", col, castableTp(tp))
}

func (t *TableInfo) datumTp(col string) datumTp {
	tp, ok := t.columnsMySQLTps[col] // the tp keeps the width in this context.
	if !ok {
		panic("column not found, can not determine datumTp")
	}
	return mySQLTypeToDatumTp(tp)
}

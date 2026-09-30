// Package table contains some common utilities for working with tables
// such as a 'Chunker' feature.
package table

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	parsermysql "github.com/block/spirit/pkg/parser/mysql"
	"github.com/block/spirit/pkg/utils"
)

const (
	lastChunkStatisticsThreshold = 10 * time.Second
)

var (
	ErrTableIsRead        = errors.New("table is read")
	ErrTableNotOpen       = errors.New("please call Open() first")
	ErrUnsupportedPKType  = errors.New("unsupported primary key type")
	ErrWatermarkNotReady  = errors.New("watermark not yet ready")
	ErrChunkerNotOpen     = errors.New("chunker is not open, call Open() first")
	ErrChunkerAlreadyOpen = errors.New("table is already open, did you mean to call Reset()?")
)

type TableInfo struct {
	sync.Mutex

	db                          *sql.DB
	EstimatedRows               uint64 // used by the composite chunker for Max
	SchemaName                  string
	TableName                   string
	QuotedTableName             string            // `table` - backtick-quoted table name without schema
	Columns                     []string          // all the column names
	NonGeneratedColumns         []string          // all the non-generated column names
	Indexes                     []string          // all the index names
	columnsMySQLTps             map[string]string // map from column name to MySQL type
	columnCharsets              map[string]string // map from column name to the charset it carries; only present for columns that carry one whose charset is known
	columnCollations            map[string]string // map from column name to the collation it compares under; only present for columns that carry a charset
	unknownCharsets             map[string]bool   // columns that carry a charset the table definition does not determine
	unknownCollations           map[string]bool   // columns that carry a charset whose collation the table definition does not determine
	enumSetElements             map[int][]string  // parsed ENUM/SET element list, keyed by column ordinal; only present for ENUM/SET columns
	misreportedEnumSets         []string          // ENUM/SET columns whose stored members information_schema does not report (see readStoredEnumSetMembers), in ordinal order
	binaryColumnWidths          map[int]int       // declared width of BINARY(N) columns, keyed by column ordinal; only present for fixed-width BINARY columns
	floatColumns                []int             // ordinals of FLOAT columns, whose binlog values DecodeBinlogRow widens to float64
	binlogCharsets              map[string]string // map from column name to the charset of the string bytes a binlog row image carries; only present for string columns whose charset is not utf8mb4/utf8mb3 (see BinlogColumnType)
	KeyColumns                  []string          // the column names of the primaryKey
	keyColumnsMySQLTp           []string          // the MySQL types of the primaryKey
	KeyIsAutoInc                bool              // if pk[0] is an auto_increment column
	keyDatums                   []datumTp         // the datum type of pk
	minValue                    Datum             // known minValue of pk[0] (using type of PK[0])
	maxValue                    Datum             // known maxValue of pk[0] (using type of PK[0])
	statisticsLastUpdated       time.Time
	statisticsLock              sync.Mutex
	DisableAutoUpdateStatistics atomic.Bool

	// DisableAnalyze skips the ANALYZE TABLE that setRowEstimate would
	// otherwise run to refresh the optimizer's row estimate. ANALYZE TABLE
	// writes to the statistics tables, so it requires INSERT on the table
	// and a writable server. Set this when reading from a least-privilege
	// (SELECT-only) or read-only source — e.g. sync's Vitess
	// replica — so the row estimate comes straight from information_schema,
	// which only needs SELECT. Set before calling SetInfo.
	DisableAnalyze bool

	// DefaultCharset and DefaultCollation are the table's default charset and
	// collation, which a column declared without either takes. Each is empty
	// when it is not known. Only a TableInfo built from a table definition
	// carries them: SetInfo does not read them, so they are always empty on a
	// TableInfo it populates.
	DefaultCharset   string
	DefaultCollation string

	// Host is an optional identifier for the MySQL server this table belongs to.
	// It is used by MultiChunker to disambiguate tables with the same SchemaName
	// and TableName on different servers (e.g., in N:M move operations).
	// When empty, the multi-chunker keys by SchemaName.TableName only.
	Host string

	// Sharding configuration (read by MySQLApplier to route rows across several targets)
	// These are set per-table when using multi-table migrations with different sharding keys
	ShardingColumn string   // Column name to extract and hash (e.g., "user_id")
	HashFunc       HashFunc // Hash function: value -> uint64
}

// HashFunc is a hash function that takes a single column value and returns a uint64 hash.
// This matches Vitess vindex behavior where the hash is used to determine shard placement.
// The hash value is then matched against key ranges to find the target shard.
type HashFunc func(value any) (uint64, error)

// QualifiedName returns a stable key for this table suitable for use in
// checkpoint watermarks. The format is "host.schema.table" when Host is set,
// or "schema.table" otherwise. This ensures uniqueness even when multiple
// servers have identically-named schemas and tables (N:M moves).
func (t *TableInfo) QualifiedName() string {
	if t.Host != "" {
		return t.Host + "." + t.SchemaName + "." + t.TableName
	}
	return t.SchemaName + "." + t.TableName
}

// DB returns the database connection associated with this table.
// This is used by components like the copier and checksum that need
// to read from the correct source database when multiple sources are in use.
func (t *TableInfo) DB() *sql.DB {
	return t.db
}

func NewTableInfo(db *sql.DB, schema, table string) *TableInfo {
	return &TableInfo{
		db:              db,
		SchemaName:      schema,
		TableName:       table,
		QuotedTableName: sqlescape.EscapeIdentifier(table),
	}
}

// ColumnMeta describes one column the way a table definition declares it: the
// column name, its information_schema `column_type` text (e.g. "enum('a','b')",
// "varchar(100)", "int unsigned"), whether it is a generated column, and the
// charset and collation it compares under — information_schema's
// `character_set_name` and `collation_name`, both empty for a column that
// carries no charset (numeric, temporal, binary string, ...). A Charset left
// empty beside a Collation is taken from the collation, whose name leads with
// its charset's.
//
// CollationUnknown marks a column that carries a charset when the definition it
// was read from does not determine its collation. A hand-written definition can
// leave it to the schema's or the server's default, which information_schema
// always resolves. Charset still names the column's charset when the definition
// determines that much, and is empty when it does not.
type ColumnMeta struct {
	Name             string
	MySQLType        string
	Generated        bool
	Charset          string
	Collation        string
	CollationUnknown bool
}

// NewTableInfoFromMeta builds a TableInfo from a table's declared column
// metadata instead of from a live server. Columns must be supplied in ordinal
// order, matching the order they appear in the table definition.
//
// The returned TableInfo carries no database connection, so it supports only
// the metadata accessors (GetColumnMySQLType, Columns, KeyColumns, ...);
// SetInfo, chunking, and anything else that queries MySQL will fail on the nil
// connection. It exists for callers that hold a table's DDL but no connection —
// a planning tool classifying an ALTER before an apply — so they can supply
// Resources.Table to the statement-scope checks.
func NewTableInfoFromMeta(schemaName, tableName string, columns []ColumnMeta, keyColumns []string) (*TableInfo, error) {
	t := NewTableInfo(nil, schemaName, tableName)
	t.resetColumns()
	for _, col := range columns {
		if err := t.addColumn(col); err != nil {
			return nil, err
		}
	}
	t.KeyColumns = slices.Clone(keyColumns)
	return t, nil
}

// PrimaryKeyValues helps extract the PRIMARY KEY from a row image.
// It uses our knowledge of the ordinal position of columns to find the
// position of primary key columns (there might be more than one).
// Spirit currently requires binlog_row_image=FULL on the source (MINIMAL events are rejected).
// PrimaryKeyValues therefore expects row images to include one value per table column.
func (t *TableInfo) PrimaryKeyValues(row any) ([]any, error) {
	vals, ok := row.([]any)
	if !ok {
		return nil, fmt.Errorf("PrimaryKeyValues: expected []any row, got %T", row)
	}
	if len(vals) < len(t.Columns) {
		return nil, fmt.Errorf("PrimaryKeyValues: row has %d values, fewer than the %d table columns", len(vals), len(t.Columns))
	}
	var pkCols []any
	for _, pCol := range t.KeyColumns {
		for i, col := range t.Columns {
			if col == pCol {
				if vals[i] == nil {
					return nil, errors.New("primary key column is NULL, possibly a bug sending after-image instead of before")
				}
				pkCols = append(pkCols, vals[i])
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
	if err := t.setStoredEnumSetMembers(ctx); err != nil {
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
	// ANALYZE TABLE refreshes the optimizer's row estimate. It writes to the
	// statistics tables, so it requires INSERT on the table and a writable
	// server; callers reading from a least-privilege (SELECT-only) or
	// read-only source set DisableAnalyze to skip it (see the field doc).
	if !t.DisableAnalyze {
		if _, err := t.db.ExecContext(ctx, "ANALYZE TABLE "+t.QuotedTableName); err != nil {
			return err
		}
	}
	// EstimatedRows is read without statisticsLock by the chunkers' Progress()
	// (chunker_composite.go / chunker_optimistic.go), so it is accessed
	// atomically rather than under the lock. Scan into a local and publish with
	// an atomic store.
	var estimatedRows uint64
	err := t.db.QueryRowContext(ctx, "SELECT IFNULL(table_rows,0) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=?", t.TableName).Scan(&estimatedRows)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("table %s.%s does not exist", t.SchemaName, t.TableName)
		}
		return err
	}
	atomic.StoreUint64(&t.EstimatedRows, estimatedRows)
	return nil
}

func (t *TableInfo) setIndexes(ctx context.Context) error {
	rows, err := t.db.QueryContext(ctx, "SELECT DISTINCT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema=DATABASE() AND table_name=? AND index_name != 'PRIMARY'",
		t.TableName,
	)
	if err != nil {
		return err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("failed to close rows", "error", err)
		}
	}()
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
	rows, err := t.db.QueryContext(ctx, "SELECT column_name, column_type, GENERATION_EXPRESSION, IFNULL(character_set_name, ''), IFNULL(collation_name, '') FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=? ORDER BY ORDINAL_POSITION",
		t.TableName,
	)
	if err != nil {
		return err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("failed to close rows", "error", err)
		}
	}()
	t.resetColumns()
	for rows.Next() {
		var col, tp, expression, charset, collation string
		if err := rows.Scan(&col, &tp, &expression, &charset, &collation); err != nil {
			return err
		}
		if err := t.addColumn(ColumnMeta{Name: col, MySQLType: tp, Generated: expression != "", Charset: charset, Collation: collation}); err != nil {
			return err
		}
	}
	if rows.Err() != nil {
		return rows.Err()
	}
	return nil
}

// setStoredEnumSetMembers replaces the ENUM/SET member lists setColumns
// parsed from information_schema with the members MySQL stores, for each
// column whose reported list contains a '?'. information_schema reports every
// member character outside utf8mb3 as '?', so without this the binlog decoder
// would write '?' in place of a stored member such as '😀': a value the
// column rejects, or a different member when '?' is one too.
//
// A '?' is also an ordinary member character, so the stored members are read
// back (see readStoredEnumSetMembers) rather than the column refused. A column
// with a stored member that has a character outside utf8mb3 is recorded for
// MisreportedEnumSetError. If the members cannot be read — the probe needs the
// CREATE TEMPORARY TABLES privilege — SetInfo fails rather than guessing.
func (t *TableInfo) setStoredEnumSetMembers(ctx context.Context) error {
	for ord, name := range t.Columns {
		reported, ok := t.enumSetElements[ord]
		if !ok || !slices.ContainsFunc(reported, func(m string) bool { return strings.Contains(m, "?") }) {
			continue
		}
		mysqlType := t.columnsMySQLTps[name]
		stored, err := readStoredEnumSetMembers(ctx, t.db, t.TableName, name, utils.IsSetType(mysqlType), len(reported))
		if err != nil {
			hint := ""
			if myErr, ok := errors.AsType[*mysql.MySQLError](err); ok &&
				(myErr.Number == parsermysql.ErrDBaccessDenied || myErr.Number == parsermysql.ErrTableaccessDenied) {
				hint = ", which needs the CREATE TEMPORARY TABLES privilege"
			}
			return fmt.Errorf("column %s.%s.%s is %s, which information_schema reports with a '?' in a member. "+
				"MySQL reports each member character outside utf8mb3 as '?', so spirit reads the members MySQL stores "+
				"through a temporary table%s: %w",
				t.SchemaName, t.TableName, name, mysqlType, hint, err)
		}
		// ParseEnumSetElements decodes the characters information_schema
		// escapes (a backslash is reported as \\), so a stored member differs
		// from the parsed one only where a character outside utf8mb3 was
		// reported as '?'. Only such a member is misreported.
		if !slices.Equal(stored, reported) {
			t.enumSetElements[ord] = stored
		}
		if slices.ContainsFunc(stored, hasCharOutsideUTF8MB3) {
			t.misreportedEnumSets = append(t.misreportedEnumSets, name)
		}
	}
	return nil
}

// resetColumns clears the column metadata so it can be repopulated from
// scratch. Columns must then be added in ordinal order: the ENUM/SET element
// and BINARY width caches are keyed by ordinal position.
func (t *TableInfo) resetColumns() {
	t.Columns = []string{}
	t.NonGeneratedColumns = []string{}
	t.columnsMySQLTps = make(map[string]string)
	t.columnCharsets = make(map[string]string)
	t.columnCollations = make(map[string]string)
	t.unknownCharsets = make(map[string]bool)
	t.unknownCollations = make(map[string]bool)
	t.enumSetElements = nil
	t.misreportedEnumSets = nil
	t.binaryColumnWidths = nil
	t.floatColumns = nil
	t.binlogCharsets = nil
}

// addColumn records one column's metadata, caching the parsed ENUM/SET element
// list and BINARY(N) declared width that the binlog decoder needs. MySQLType is
// the information_schema `column_type` text, e.g. "enum('a','b')" or
// "varbinary(16)". Columns must be added in ordinal order.
func (t *TableInfo) addColumn(col ColumnMeta) error {
	name, mysqlType := col.Name, col.MySQLType
	t.Columns = append(t.Columns, name)
	t.columnsMySQLTps[name] = mysqlType
	collation := canonicalCollationName(col.Collation)
	charset := canonicalCharsetName(col.Charset)
	if charset == "" && collation != "" {
		charset, _, _ = strings.Cut(collation, "_")
	}
	if charset != "" && !isCharsetName(charset) {
		// The charset is spliced into SQL as an introducer or a CONVERT
		// target (see BinlogColumnType), so a name that is not one is
		// refused rather than emitted.
		return fmt.Errorf("column %s.%s.%s has unexpected charset name %q", t.SchemaName, t.TableName, name, charset)
	}
	if collation != "" && !isCollationName(collation) {
		// The collation is spliced into SQL as a COLLATE clause (see the
		// lockless checksum's image values), so it is checked the same way.
		return fmt.Errorf("column %s.%s.%s has unexpected collation name %q", t.SchemaName, t.TableName, name, collation)
	}
	switch {
	case col.CollationUnknown && charset == "":
		t.unknownCharsets[name] = true
		t.unknownCollations[name] = true
	case col.CollationUnknown:
		t.columnCharsets[name] = charset
		t.unknownCollations[name] = true
	case collation != "":
		t.columnCharsets[name] = charset
		t.columnCollations[name] = collation
	}
	if !col.Generated {
		t.NonGeneratedColumns = append(t.NonGeneratedColumns, name)
	}
	ordinal := len(t.Columns) - 1
	if utils.IsEnumOrSetType(mysqlType) {
		elements, err := utils.ParseEnumSetElements(mysqlType)
		if err != nil {
			return fmt.Errorf("parsing ENUM/SET elements for %s.%s.%s: %w", t.SchemaName, t.TableName, name, err)
		}
		if t.enumSetElements == nil {
			t.enumSetElements = make(map[int][]string)
		}
		t.enumSetElements[ordinal] = elements
	}
	if isBinaryColumnType(mysqlType) {
		if width := parseBinaryColumnWidth(mysqlType); width > 0 {
			if t.binaryColumnWidths == nil {
				t.binaryColumnWidths = make(map[int]int)
			}
			t.binaryColumnWidths[ordinal] = width
		}
	}
	if isFloatColumnType(mysqlType) {
		t.floatColumns = append(t.floatColumns, ordinal)
	}
	if cs := t.columnCharsets[name]; needsCharsetIntroducer(cs) && !utils.IsEnumOrSetType(mysqlType) {
		if t.binlogCharsets == nil {
			t.binlogCharsets = make(map[string]string)
		}
		t.binlogCharsets[name] = cs
	}
	return nil
}

// needsCharsetIntroducer reports whether string bytes in charset must be
// labelled with it to reach MySQL unchanged over a utf8mb4 connection.
// utf8mb3 is a subset of utf8mb4 and binary strings are emitted as hex
// literals, so only the other charsets need it.
func needsCharsetIntroducer(charset string) bool {
	switch charset {
	case "", "utf8mb4", "utf8mb3", "binary":
		return false
	}
	return true
}

// isCharsetName reports whether charset is safe to splice into SQL, as an
// introducer (_charset) or in CONVERT(... USING charset). MySQL charset names
// are lower-case letters and digits.
func isCharsetName(charset string) bool {
	for _, r := range charset {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') {
			return false
		}
	}
	return charset != ""
}

// isCollationName reports whether collation is lower-case letters, digits and
// underscores, as every MySQL collation name is once canonicalCollationName
// has lower-cased it.
func isCollationName(collation string) bool {
	for _, r := range collation {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' {
			return false
		}
	}
	return collation != ""
}

// DescIndex describes the columns in an index.
func (t *TableInfo) DescIndex(keyName string) ([]string, error) {
	cols := []string{}
	//nolint: noctx // too much refactoring to add context here
	rows, err := t.db.Query("SELECT column_name FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema=DATABASE() AND TABLE_NAME=? AND index_name=? ORDER BY seq_in_index",
		t.TableName,
		keyName,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("failed to close rows", "error", err)
		}
	}()
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

// UniqueIndex describes one UNIQUE secondary index: its name and its columns
// in key order. The order matters to callers reasoning about adjacency, since
// only the leading column decides where a record sorts relative to records with
// a different leading value.
type UniqueIndex struct {
	Name    string
	Columns []string
}

// UniqueSecondaryIndexes returns the table's UNIQUE secondary indexes, PRIMARY
// excluded. It is a live query rather than part of SetInfo because only the
// change feed's flush partitioning needs it, and it needs it once per
// subscription.
//
// This set is exactly the conflict surface between two concurrent REPLACE
// statements on PK-disjoint rows. A *non-unique* secondary index is keyed
// (indexed columns, PK), so PK-disjoint rows always occupy distinct records
// there and cannot collide however equal their indexed values are. A *unique*
// secondary index is keyed on the indexed columns alone, and InnoDB's duplicate
// detection takes a next-key lock — gap included — so rows with merely
// *adjacent* values collide. The clustered index does not belong here either:
// a REPLACE's conflict there is with the row bearing that exact PK, which under
// READ COMMITTED is a record lock with no gap. See
// TestReplaceContendsOnlyOnUniqueIndexes in pkg/applier, which establishes all
// three against a real server.
//
// Indexes with a NULL COLUMN_NAME are skipped: those are functional indexes,
// whose key is an expression rather than a stored column, so a caller holding a
// row image cannot compute where the row sorts in them.
func (t *TableInfo) UniqueSecondaryIndexes(ctx context.Context) ([]UniqueIndex, error) {
	// A TableInfo built by NewTableInfoFromMeta carries metadata with no server
	// behind it (strata's pkg/vstream does this), so this is an ordinary state to
	// be in rather than a caller error — but it must be an error and not a nil
	// dereference, which is what an unguarded t.db gives.
	if t.db == nil {
		return nil, fmt.Errorf("table %s.%s has no database handle: index metadata is unavailable", t.SchemaName, t.TableName)
	}
	rows, err := t.db.QueryContext(ctx, `SELECT INDEX_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS
		WHERE table_schema=DATABASE() AND TABLE_NAME=? AND NON_UNIQUE=0 AND INDEX_NAME<>'PRIMARY'
		ORDER BY INDEX_NAME, SEQ_IN_INDEX`,
		t.TableName,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("failed to close rows", "error", err)
		}
	}()
	var (
		indexes  []UniqueIndex
		skipping = make(map[string]bool)
	)
	byName := make(map[string]int)
	for rows.Next() {
		var name string
		var col sql.NullString
		if err := rows.Scan(&name, &col); err != nil {
			return nil, err
		}
		if !col.Valid {
			// A functional index. Drop whatever was collected for it: a
			// partially usable key order is worse than none, because a caller
			// would sort by a prefix and believe it had the whole key.
			skipping[name] = true
			continue
		}
		if skipping[name] {
			continue
		}
		pos, ok := byName[name]
		if !ok {
			byName[name] = len(indexes)
			indexes = append(indexes, UniqueIndex{Name: name})
			pos = len(indexes) - 1
		}
		indexes[pos].Columns = append(indexes[pos].Columns, col.String)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	// A functional column may appear after a stored one, so the drop above can
	// leave a half-built entry behind. Filter at the end rather than trying to
	// unwind in the loop.
	return slices.DeleteFunc(indexes, func(idx UniqueIndex) bool {
		return skipping[idx.Name] || len(idx.Columns) == 0
	}), nil
}

// setPrimaryKey sets the primary key and also the primary key type.
// A primary key can contain multiple columns.
func (t *TableInfo) setPrimaryKey(ctx context.Context) error {
	rows, err := t.db.QueryContext(ctx, "SELECT column_name FROM information_schema.key_column_usage WHERE table_schema=DATABASE() and table_name=? and constraint_name='PRIMARY' ORDER BY ORDINAL_POSITION",
		t.TableName,
	)
	if err != nil {
		return err
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("failed to close rows", "error", err)
		}
	}()
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
		query := "SELECT column_type, extra FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=? and column_name=?"
		var extra, pkType string
		err = t.db.QueryRowContext(ctx, query, t.TableName, col).Scan(&pkType, &extra)
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

// FloatPrimaryKeyError returns an error if a primary key column is a FLOAT,
// which Spirit refuses to copy, as gh-ost does.
//
// Spirit finds rows by key with SQL literals: chunk boundaries, and the
// DELETEs that replay the binlog. A FLOAT is compared to a literal as a
// DOUBLE, and a FLOAT such as 0.1 has no text form that equals it as a DOUBLE
// ("0.1" does not). A replayed DELETE then matches nothing, so a row deleted
// during the migration is still in the table after cutover; and a chunk
// boundary on a value shared by many rows never advances.
//
// SetInfo does not call it: a TableInfo describes any table, and refusing one
// is for the caller to decide. The migration and move checks refuse such a
// table with it.
func (t *TableInfo) FloatPrimaryKeyError() error {
	for _, col := range t.KeyColumns {
		if tp, ok := t.GetColumnMySQLType(col); ok && isFloatColumnType(tp) {
			return fmt.Errorf("primary key column %q of table %q is a FLOAT, which is not supported: "+
				"a FLOAT does not compare equal to its text form, so rows cannot be located by key", col, t.TableName)
		}
	}
	return nil
}

// MisreportedEnumSetError returns an error if an ENUM or SET column has a
// member that information_schema and SHOW CREATE TABLE do not report: each
// member character outside utf8mb3, such as a 4-byte UTF-8 emoji, is reported
// as '?' (see setStoredEnumSetMembers). A table created from its reported
// definition does not have the member, and cannot hold the rows that use it:
// the copy fails, or, when no row uses it yet, completes and loses the member.
//
// SetInfo does not call it: a TableInfo describes any table, and refusing one
// is for the caller to decide. Move and sync, which create the target table
// from the source's SHOW CREATE TABLE, refuse such a table with it. A
// migration does not need to: CREATE TABLE .. LIKE copies the stored members.
func (t *TableInfo) MisreportedEnumSetError() error {
	if len(t.misreportedEnumSets) == 0 {
		return nil
	}
	name := t.misreportedEnumSets[0]
	return fmt.Errorf("column %q of table %q is %s, but MySQL stores a member with a character outside utf8mb3 there, which "+
		"information_schema and SHOW CREATE TABLE report as '?', so the table cannot be recreated from its definition", name, t.TableName, t.columnsMySQLTps[name])
}

// BitPrimaryKeyError returns an error if a primary key column is a BIT, which
// Spirit does not support.
//
// Spirit handles a BIT key value as an unsigned integer: binlog rows carry it
// as one, and it is written into chunk predicates and DELETEs as a numeric
// literal. But the chunkers read chunk boundaries and the key range back from
// the table with plain SELECTs, which return a BIT as raw big-endian bytes that
// cannot be parsed as a number, so the copy failed on its first chunk, after
// the migration had already set up its tables.
//
// SetInfo does not call it: a TableInfo describes any table, and refusing one
// is for the caller to decide. The migration and move checks refuse such a
// table with it.
func (t *TableInfo) BitPrimaryKeyError() error {
	for _, col := range t.KeyColumns {
		if tp, ok := t.GetColumnMySQLType(col); ok && isBITType(tp) {
			return fmt.Errorf("primary key column %q of table %q is a BIT, which is not supported", col, t.TableName)
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
	if slices.Contains(t.keyDatums, unknownType) {
		return ErrUnsupportedPKType
	}
	// BIT is classified as unsignedType so the binlog applier emits the
	// value as a numeric literal (see mySQLTypeToDatumTp), but BIT primary
	// keys are not supported end-to-end: setMinMax issues a SELECT that
	// returns BIT as raw big-endian bytes, and the chunker's
	// newDatumFromMySQL path parses those as decimal strings — which
	// fails or produces wrong bounds. Until the min/max read path knows
	// how to decode BIT bytes, reject BIT PKs with the same error they
	// returned before BIT got its own datumTp. The migration and move
	// checks refuse them before any copy (see BitPrimaryKeyError).
	if slices.ContainsFunc(t.keyColumnsMySQLTp, isBITType) {
		return ErrUnsupportedPKType
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
	// BIT is classified as unsignedType so the applier emits the value as
	// a numeric literal, but `SELECT min(bit_col)` returns raw big-endian
	// bytes that newDatumFromMySQL can't parse as decimal. BIT primary
	// keys are rejected by PrimaryKeyIsMemoryComparable, and by the
	// migration and move checks (see BitPrimaryKeyError); skip here so
	// SetInfo can complete and the rejection can fire on a well-formed
	// TableInfo.
	if isBITType(t.keyColumnsMySQLTp[0]) {
		return nil
	}
	quotedKey := sqlescape.EscapeIdentifier(t.KeyColumns[0])
	query := fmt.Sprintf("SELECT IFNULL(min(%s),'0'), IFNULL(max(%s),'0') FROM %s", quotedKey, quotedKey, t.QuotedTableName)
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
func (t *TableInfo) AutoUpdateStatistics(ctx context.Context, interval time.Duration, logger *slog.Logger) {
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
				logger.Error("error updating table statistics", "error", err)
			}
			logger.Info("table statistics updated",
				"estimated-rows", atomic.LoadUint64(&t.EstimatedRows),
				"pk[0].max-value", t.MaxValue())
		}
	}
}

// statisticsNeedUpdating returns true if the statistics are considered older than a threshold.
// this is useful for the chunker to synchronously check as it approaches the end of the table.
// Reads statisticsLastUpdated under statisticsLock since updateTableStatistics
// (driven by the background AutoUpdateStatistics goroutine) writes it.
func (t *TableInfo) statisticsNeedUpdating() bool {
	t.statisticsLock.Lock()
	defer t.statisticsLock.Unlock()
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

// MinValue as a datum
func (t *TableInfo) MinValue() Datum {
	t.statisticsLock.Lock()
	defer t.statisticsLock.Unlock()
	return t.minValue
}

// setBoundsIfUnset populates min/max from a chunker's observed bounds, but only
// for whichever is still unset (an empty or not-yet-analyzed table). Guarded by
// statisticsLock so it is safe against a concurrent AutoUpdateStatistics, which
// writes the same fields via setMinMax. The IsNil checks and assignments must
// happen together under the lock so a stats refresh can't land between them.
func (t *TableInfo) setBoundsIfUnset(minVal, maxVal Datum) {
	t.statisticsLock.Lock()
	defer t.statisticsLock.Unlock()
	if t.minValue.IsNil() {
		t.minValue = minVal
	}
	if t.maxValue.IsNil() {
		t.maxValue = maxVal
	}
}

// columnMySQLTp returns the information_schema column_type of col, which keeps
// the width (e.g. "timestamp(6)", "decimal(12,4) unsigned").
func (t *TableInfo) columnMySQLTp(col string) (string, error) {
	tp, ok := t.columnsMySQLTps[col]
	if !ok {
		return "", fmt.Errorf("column %q not found in table %s", col, t.TableName)
	}
	return tp, nil
}

func (t *TableInfo) datumTp(col string) (datumTp, error) {
	tp, ok := t.columnsMySQLTps[col] // the tp keeps the width in this context.
	if !ok {
		return unknownType, fmt.Errorf("column %q not found in table %s", col, t.TableName)
	}
	return mySQLTypeToDatumTp(tp), nil
}

// GetColumnMySQLType returns the MySQL type for a given column name
func (t *TableInfo) GetColumnMySQLType(col string) (string, bool) {
	tp, ok := t.columnsMySQLTps[col]
	return tp, ok
}

// EnumSetMembers returns the members of the ENUM or SET column col, and false
// if col is not one. Use it rather than parsing GetColumnMySQLType: SetInfo
// replaces a member that information_schema reports as '?' with the member
// MySQL stores (see setStoredEnumSetMembers).
func (t *TableInfo) EnumSetMembers(col string) ([]string, bool) {
	ord := slices.Index(t.Columns, col)
	if ord < 0 {
		return nil, false
	}
	members, ok := t.enumSetElements[ord]
	return slices.Clone(members), ok
}

// BinlogColumnType resolves col for NewDatumFromValueWithType when the
// values come from a binlog row image rather than from a query.
//
// The two differ for a string column whose charset is not utf8mb4 or
// utf8mb3 (latin1, gbk, utf16, ...). A query returns the value converted to
// the connection charset, utf8mb4. A binlog row image carries the column's
// own bytes, which must be emitted with the column's charset introducer:
// quoted, MySQL would read them as utf8mb4 and convert them into a different
// value. For every other column the result is NewColumnType's.
func (t *TableInfo) BinlogColumnType(col string) (ColumnType, error) {
	tp, ok := t.columnsMySQLTps[col]
	if !ok {
		return ColumnType{}, fmt.Errorf("column %q not found in table %s", col, t.TableName)
	}
	ct := NewColumnType(tp)
	ct.charset = t.binlogCharsets[col]
	return ct, nil
}

// BinlogCharset returns the charset of the string bytes a binlog row image
// carries for col, when they must be labelled with it to be read correctly
// over a utf8mb4 connection (see BinlogColumnType). It is empty for any
// other column: one that is not a string, an ENUM or SET (decoded to its
// element text by DecodeBinlogRow), a utf8mb4 or utf8mb3 column, or one whose
// charset the table definition does not determine.
func (t *TableInfo) BinlogCharset(col string) string {
	return t.binlogCharsets[col]
}

// GetColumnCollation returns the collation a column compares under, lower
// cased and with the legacy utf8_ prefix spelled utf8mb3_, as MySQL 8.0 names
// it. It is empty for a column that carries no charset. ok is false when the
// table has no such column, or when the definition the table was built from
// does not determine the column's collation.
func (t *TableInfo) GetColumnCollation(col string) (collation string, ok bool) {
	if _, ok := t.columnsMySQLTps[col]; !ok {
		return "", false
	}
	if t.unknownCollations[col] {
		return "", false
	}
	return t.columnCollations[col], true
}

// GetColumnCharset returns the charset a column carries, spelled as
// GetColumnCollation spells collations. It is only ever lower-case letters and
// digits, so it is safe to splice into SQL. It is empty for a column that carries
// no charset. ok is false when the table has no such column, or when the
// definition the table was built from does not determine the column's charset.
// A column's charset can be known when its collation is not.
func (t *TableInfo) GetColumnCharset(col string) (charset string, ok bool) {
	if _, ok := t.columnsMySQLTps[col]; !ok {
		return "", false
	}
	if t.unknownCharsets[col] {
		return "", false
	}
	return t.columnCharsets[col], true
}

// canonicalCollationName spells a collation the way GetColumnCollation reports
// it. MySQL releases before 8.0.30 name the 3-byte UTF-8 collations utf8_*;
// later ones name the same collations utf8mb3_*.
func canonicalCollationName(collation string) string {
	collation = strings.ToLower(collation)
	if rest, ok := strings.CutPrefix(collation, "utf8_"); ok {
		return "utf8mb3_" + rest
	}
	return collation
}

// canonicalCharsetName is canonicalCollationName for the charset: MySQL
// releases before 8.0.30 name the 3-byte UTF-8 charset utf8.
func canonicalCharsetName(charset string) string {
	charset = strings.ToLower(charset)
	if charset == "utf8" {
		return "utf8mb3"
	}
	return charset
}

// HasEnumOrSetColumns reports whether any column on this table is an
// ENUM or SET.
//
// Deprecated: gate DecodeBinlogRow calls on NeedsBinlogRowDecoding
// instead — DecodeBinlogRow now also re-pads BINARY(N) values, which
// this predicate does not account for.
func (t *TableInfo) HasEnumOrSetColumns() bool {
	return len(t.enumSetElements) > 0
}

// NeedsBinlogRowDecoding reports whether DecodeBinlogRow would do any
// work for this table: it has ENUM/SET columns (ordinal/bitmask
// decoding), fixed-width BINARY columns (trailing 0x00 re-padding) or
// FLOAT columns (widening to float64).
// Used to skip the per-row decoding hot path when there's nothing to
// decode.
func (t *TableInfo) NeedsBinlogRowDecoding() bool {
	return len(t.enumSetElements) > 0 || len(t.binaryColumnWidths) > 0 || len(t.floatColumns) > 0
}

// DecodeBinlogRow normalizes a binlog row image in place so the
// buffered replay path can feed it to the applier as a
// REPLACE INTO ... VALUES:
//
//   - ENUM and SET values are converted from their integer wire format
//     (ENUM ordinal / SET bitmask) back to the string form. The
//     go-mysql binlog reader yields them as int64s; if the target
//     column has been migrated to a non-ENUM type (e.g. VARCHAR),
//     MySQL would insert those integers as literal values instead of
//     the original strings, corrupting data.
//   - BINARY(N) values are right-padded with 0x00 back to their
//     declared width. MySQL strips trailing pad bytes from the row
//     image (Field_string::pack) and expects the reader to re-pad;
//     without this, values with trailing zeros are replayed short into
//     targets that don't re-pad server-side (e.g. VARBINARY), and
//     binary primary-key lookups miss. See pkg/table/binarypad.go and
//     block/spirit#945.
//   - FLOAT values are widened from float32 to float64, so they are
//     written as the float32's exact value (see the comment in the body).
//
// nil values (NULL columns) and rows with nothing to decode are a
// no-op. If the table has no ENUM/SET/BINARY/FLOAT columns at all, callers
// should gate with NeedsBinlogRowDecoding and skip the call entirely.
func (t *TableInfo) DecodeBinlogRow(row []any) error {
	// The binlog decodes FLOAT as a float32, which Datum prints as the
	// shortest string that round-trips as a float32 ("0.1"). MySQL parses
	// that literal as a DOUBLE, so it is not the value the source holds: a
	// DOUBLE target stores 0.1 where ALTER TABLE stores 0.10000000149011612,
	// and FLT_MAX is out of range (warning 1264). The float64 holds the
	// float32's exact value, which a FLOAT target stores back unchanged.
	for _, ord := range t.floatColumns {
		if ord >= len(row) {
			continue
		}
		if f, ok := row[ord].(float32); ok {
			row[ord] = float64(f)
		}
	}
	for ord, width := range t.binaryColumnWidths {
		if ord >= len(row) {
			continue
		}
		row[ord] = padBinaryValue(row[ord], width)
	}
	for ord, elements := range t.enumSetElements {
		if ord >= len(row) {
			continue
		}
		raw := row[ord]
		if raw == nil {
			continue
		}
		intVal, ok := raw.(int64)
		if !ok {
			continue
		}
		colName := ""
		if ord < len(t.Columns) {
			colName = t.Columns[ord]
		}
		mysqlType := t.columnsMySQLTps[colName]
		var decoded string
		var derr error
		if utils.IsSetType(mysqlType) {
			decoded, derr = decodeSetBitmask(intVal, elements)
		} else {
			decoded, derr = decodeEnumOrdinal(intVal, elements)
		}
		if derr != nil {
			return fmt.Errorf("decoding %s.%s column %q: %w", t.SchemaName, t.TableName, colName, derr)
		}
		row[ord] = decoded
	}
	return nil
}

// GetColumnOrdinal returns the ordinal position (0-indexed) of a column by name.
// This is useful for extracting values from row slices where the position matters.
// Returns an error if the column is not found.
func (t *TableInfo) GetColumnOrdinal(columnName string) (int, error) {
	for i, col := range t.Columns {
		if col == columnName {
			return i, nil
		}
	}
	return -1, fmt.Errorf("column %s not found in table %s", columnName, t.TableName)
}

// GetNonGeneratedColumnOrdinal returns the ordinal position (0-indexed) of a column by name
// within the NonGeneratedColumns slice. This is useful when working with row data that only
// contains non-generated columns (e.g., from SELECT statements that exclude generated columns).
// Returns an error if the column is not found or if it's a generated column.
func (t *TableInfo) GetNonGeneratedColumnOrdinal(columnName string) (int, error) {
	for i, col := range t.NonGeneratedColumns {
		if col == columnName {
			return i, nil
		}
	}
	return -1, fmt.Errorf("column %s not found in non-generated columns of table %s", columnName, t.TableName)
}

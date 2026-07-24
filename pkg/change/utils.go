package change

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

func encodeSchemaTable(schema, table string) string {
	return schema + "." + table
}

// getTableIdentity extracts the schema and table name from an AST node that has table information
func getTableIdentity(defaultSchema string, node ast.Node) (string, string) {
	var schema, table string
	switch t := node.(type) {
	case *ast.TableName:
		schema = t.Schema.String()
		table = t.Name.String()
	case *ast.TableSource:
		if tn, ok := t.Source.(*ast.TableName); ok {
			schema = tn.Schema.String()
			table = tn.Name.String()
		}
	}
	if schema == "" {
		schema = defaultSchema
	}
	return schema, table
}

// schemaTable is a parsed schema and table name pair extracted from a DDL statement.
type schemaTable struct {
	schema string
	table  string
}

// extractTablesFromDDLStmts extracts table names from DDL statements.
// The logic is based on canal: https://github.com/go-mysql-org/go-mysql/blob/34b6b0998dde44e51dff0bbcc1ac88339f57f830/canal/sync.go#L195-L245
func extractTablesFromDDLStmts(defaultSchema string, statements string) ([]schemaTable, error) {
	p := parser.New()
	stmts, _, err := p.Parse(statements, "", "")
	if err != nil {
		return nil, err
	}
	var tables []schemaTable
	for _, stmt := range stmts {
		switch t := stmt.(type) {
		case *ast.RenameTableStmt:
			for _, tableInfo := range t.TableToTables {
				schema, table := getTableIdentity(defaultSchema, tableInfo.OldTable)
				tables = append(tables, schemaTable{schema, table})
			}
		case *ast.DropTableStmt:
			for _, table := range t.Tables {
				schema, tableName := getTableIdentity(defaultSchema, table)
				tables = append(tables, schemaTable{schema, tableName})
			}
		case *ast.AlterTableStmt, *ast.CreateTableStmt, *ast.TruncateTableStmt,
			*ast.CreateIndexStmt, *ast.DropIndexStmt:
			var tableNode *ast.TableName
			switch n := t.(type) {
			case *ast.AlterTableStmt:
				tableNode = n.Table
			case *ast.CreateTableStmt:
				tableNode = n.Table
			case *ast.TruncateTableStmt:
				tableNode = n.Table
			case *ast.CreateIndexStmt:
				tableNode = n.Table
			case *ast.DropIndexStmt:
				tableNode = n.Table
			}
			schema, table := getTableIdentity(defaultSchema, tableNode)
			tables = append(tables, schemaTable{schema, table})
		}
	}
	return tables, nil
}

// toSet converts a string slice to a set (map[string]struct{}) for O(1) lookups.
// Returns nil if the input slice is empty, so callers can use len() to check
// whether filtering is enabled.
func toSet(ss []string) map[string]struct{} {
	if len(ss) == 0 {
		return nil
	}
	m := make(map[string]struct{}, len(ss))
	for _, s := range ss {
		m[s] = struct{}{}
	}
	return m
}

// pkChanged reports whether two PK images differ. The values come from
// the binlog row image as `any` with concrete types that depend on the
// source column (int8/16/32/64, uint*, string, []byte, ...), so the
// helper normalises before comparing:
//
//   - numeric widths (int vs int64, signed vs unsigned of the same
//     magnitude) are equalised by formatting through fmt.Sprintf("%v");
//   - []byte is coerced to string so a column that surfaces as one in
//     one image and the other in the next is still compared correctly
//     (fmt.Sprintf("%v", []byte("a")) is "[97]" which would otherwise
//     spuriously diverge from "a").
//
// In practice go-mysql is consistent about which Go type it emits for a
// given column, so the []byte/string coercion is defensive — but cheap
// enough to keep the helper robust to future decoder changes.
func pkChanged(a, b []any) bool {
	if len(a) != len(b) {
		return true
	}
	for i := range a {
		if !pkValueEqual(a[i], b[i]) {
			return true
		}
	}
	return false
}

func pkValueEqual(a, b any) bool {
	if ab, ok := a.([]byte); ok {
		a = string(ab)
	}
	if bb, ok := b.([]byte); ok {
		b = string(bb)
	}
	// Booleans would otherwise collide with strings "true" / "false" under
	// fmt.Sprintf("%v", ...). PK columns are rarely boolean (BIT or
	// TINYINT(1) some drivers surface as bool), but if one is, exact
	// type-and-value match is required.
	aBool, aIsBool := a.(bool)
	bBool, bIsBool := b.(bool)
	if aIsBool || bIsBool {
		return aIsBool && bIsBool && aBool == bBool
	}
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// checkImmutableColumn returns an error when an UPDATE's before and after
// row images differ at ordinal — the position of a column the subscription
// declared immutable via Subscription.ImmutableColumnOrdinal. An ordinal of
// -1 (no immutable column configured) makes the check a no-op.
//
// This enforces the sharded applier's vindex contract (see
// applier.ShardedApplier.UpsertRows): modifications are tracked by PRIMARY
// KEY only, so an UPDATE that moved a row's sharding-column value would
// flush the new row image to its new shard while the old shard silently
// kept a stale copy of the row. Both processRowsEvent implementations treat
// the returned error as fatal, which cancels the entire operation.
//
// key is the row's before-image PRIMARY KEY, used only for the error
// message. Values are compared with the same normalisation rules as
// pkChanged (see pkValueEqual).
func checkImmutableColumn(tbl *table.TableInfo, ordinal int, beforeRow, afterRow []any, key []any) error {
	if ordinal < 0 {
		return nil
	}
	// PrimaryKeyValues has already validated that both images carry one
	// value per table column, and ordinal indexes into tbl.Columns — but
	// bounds-check anyway so a malformed event fails loudly rather than
	// panicking the stream reader.
	if ordinal >= len(beforeRow) || ordinal >= len(afterRow) {
		return fmt.Errorf("immutable column ordinal %d exceeds row image length (before: %d, after: %d) for table %s.%s",
			ordinal, len(beforeRow), len(afterRow), tbl.SchemaName, tbl.TableName)
	}
	if !pkValueEqual(beforeRow[ordinal], afterRow[ordinal]) {
		return fmt.Errorf("update to table %s.%s changes the value of the sharding column %s for the row with PRIMARY KEY %v: the sharding (vindex) column is immutable during a sharded operation, because changes are tracked by primary key only and a stale copy of the row would be left behind on its previous shard",
			tbl.SchemaName, tbl.TableName, tbl.Columns[ordinal], key)
	}
	return nil
}

// errXAUnsupported is the fatal error produced when XA transaction
// activity is observed in the binlog stream. An XA transaction's row
// events are written to the binary log at XA PREPARE time, before the
// transaction's outcome is known: applying them treats the prepare as a
// commit, and a later XA ROLLBACK has no binlog representation that
// could undo them, so the target would diverge permanently. Rather than
// tracking prepared XIDs and buffering until the outcome (full XA
// support), spirit refuses XA workloads outright — the same posture as
// the preflight refusal of non-empty binlog_row_value_options. Both
// change clients treat this as a fatal stream error, aborting before
// any of the XA transaction's row events are buffered.
var errXAUnsupported = errors.New("XA transactions detected in the binlog stream: spirit does not support XA workloads")

// isXAStatement reports whether q (a binlogged statement, whitespace
// already trimmed) is one of the XA transaction statements MySQL writes
// to the binary log as QueryEvents: "XA START", "XA END", "XA COMMIT"
// or "XA ROLLBACK". (Two-phase "XA PREPARE" is logged as an
// XA_PREPARE_LOG_EVENT, not a QueryEvent.) The server rewrites XA
// statements canonically before logging — XA BEGIN 'x' is binlogged as
// "XA START" with a hex-encoded xid — so a keyword prefix match is
// exact, and no other statement the server binlogs begins with the XA
// keyword.
func isXAStatement(q string) bool {
	return hasPrefixFold(q, "XA ")
}

// isMinimalRowImage returns true if the RowsEvent contains a minimal row image,
// i.e. some columns were skipped. This happens when binlog_row_image=MINIMAL or NOBLOB.
// With full row images, SkippedColumns entries are empty slices.
func isMinimalRowImage(e *replication.RowsEvent) bool {
	for _, skipped := range e.SkippedColumns {
		if len(skipped) > 0 {
			return true
		}
	}
	return false
}

// binlogPositionIsImpossible reports whether expectedLogName is no
// longer present on the server (i.e. the binlog file the caller wants
// to resume from has been purged).
//
// Returns:
//   - (true, nil)   — the file is definitely absent: resume from this
//     position cannot succeed.
//   - (false, nil)  — the file is present: the position is resumable.
//   - (_, err)      — could not determine (query / scan / iteration
//     failed). Callers should surface this as a real
//     error rather than treating it as "purged" — a
//     network blip or auth hiccup is recoverable,
//     while reporting "purged" abandons the checkpoint
//     and forces a full re-copy.
//
// pkg/migration.Runner has a near-identical helper (binlogFileExists)
// over the same SHOW BINARY LOGS query. Consolidating into a single
// shared helper would mean exposing it from pkg/change or pulling both
// into a lower-level package; left as a follow-up.
func binlogPositionIsImpossible(ctx context.Context, db *sql.DB, expectedLogName string) (bool, error) {
	rows, err := db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return false, fmt.Errorf("query SHOW BINARY LOGS: %w", err)
	}
	defer utils.CloseAndLog(rows)
	var logname, size, encrypted string
	for rows.Next() {
		if err := rows.Scan(&logname, &size, &encrypted); err != nil {
			return false, fmt.Errorf("scan SHOW BINARY LOGS row: %w", err)
		}
		if logname == expectedLogName {
			return false, nil // file present, position is resumable
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterating SHOW BINARY LOGS: %w", err)
	}
	return true, nil // file definitely not in the result set
}

package utils

import (
	"fmt"
	"strings"
)

const (
	// NameFormatTimestamp is the time.Format layout used in the timestamped
	// _<table>_old_<timestamp> name when SkipDropAfterCutover is set.
	NameFormatTimestamp = "20060102_150405"

	suffixCheckpoint = "_chkpnt"
	suffixNew        = "_new"
	suffixOld        = "_old"
)

// AuxTableName builds a deterministic auxiliary table name for the given
// original table name and suffix (e.g. "_chkpnt", "_new", "_old"). The
// returned name is `_<table><suffix>`, with the table-name portion
// deterministically truncated when needed and the result hard-capped at
// MySQL's 64-character identifier limit (a defensive guard against an
// abusively long suffix; in practice callers pass small suffixes).
//
// Truncation is deterministic: the same (tableName, suffix) input always
// produces the same output. Two distinct table names that share a long
// common prefix can collide; callers must record the original table name
// out-of-band (e.g. in the checkpoint) so collisions can be detected.
func AuxTableName(tableName, suffix string) string {
	truncated := TruncateTableName(tableName, 1+len(suffix))
	result := "_" + truncated + suffix
	if len(result) > MaxTableNameLength {
		result = result[:MaxTableNameLength]
	}
	return result
}

// CheckpointTableName returns the auxiliary checkpoint table name for the
// given original table.
func CheckpointTableName(tableName string) string {
	return AuxTableName(tableName, suffixCheckpoint)
}

// NewTableName returns the auxiliary _new table name for the given original
// table.
func NewTableName(tableName string) string {
	return AuxTableName(tableName, suffixNew)
}

// OldTableName returns the auxiliary _old table name for the given original
// table.
func OldTableName(tableName string) string {
	return AuxTableName(tableName, suffixOld)
}

// OldTableNameWithTimestamp returns the auxiliary _old_<timestamp> table name
// for the given original table and timestamp string. Used when
// SkipDropAfterCutover is set so the renamed-away table is preserved with a
// unique name across multiple migrations.
func OldTableNameWithTimestamp(tableName, timestamp string) string {
	return AuxTableName(tableName, suffixOld+"_"+timestamp)
}

// UnsupportedIdentifierError returns an error when name, a schema or table
// name, contains a character Spirit refuses in those identifiers, and nil
// otherwise. kind describes the identifier in the error message, e.g.
// "table name" or "schema name".
//
// Two characters are refused:
//
//   - '.': the replication client keys each table's subscription by joining
//     its schema and table name with a '.', so `a`.`b.c` and `a.b`.`c` map to
//     the same key and a row change on one table can be applied to the other.
//   - '`': every statement Spirit builds that names the table has to escape
//     the backtick, and a single missed escape produces broken or wrong SQL.
//     Refusing the name removes that class of bug rather than chasing each
//     statement.
func UnsupportedIdentifierError(kind, name string) error {
	if strings.Contains(name, ".") {
		return fmt.Errorf("%s %q contains a '.', which Spirit does not support: "+
			"Spirit identifies tables by joining the schema and table name with a '.', "+
			"so this name can collide with a different table's and changes to one could be applied to the other", kind, name)
	}
	if strings.Contains(name, "`") {
		return fmt.Errorf("%s %q contains a backtick, which Spirit does not support: "+
			"every statement Spirit generates that names the table would need to escape it, "+
			"and a missed escape produces broken or incorrect SQL", kind, name)
	}
	return nil
}

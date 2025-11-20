package check

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
)

const (
	// Max table name length in MySQL
	maxTableNameLength = 64

	// Formats for table names
	NameFormatCheckpoint   = "_%s_chkpnt"
	NameFormatNew          = "_%s_new"
	NameFormatOld          = "_%s_old"
	NameFormatOldTimeStamp = "_%s_old_%s"
	NameFormatTimestamp    = "20060102_150405"
)

var (
	// The number of extra characters needed for table names with all possible
	// formats. These vars are calculated in the `init` function below.
	NameFormatNormalExtraChars    = 0
	NameFormatTimestampExtraChars = 0
)

func init() {
	registerCheck("tablename", tableNameCheck, ScopePreflight)

	// Calculate the number of extra characters needed table names with all possible formats
	for _, format := range []string{NameFormatCheckpoint, NameFormatNew, NameFormatOld} {
		extraChars := len(strings.ReplaceAll(format, "%s", ""))
		if extraChars > NameFormatNormalExtraChars {
			NameFormatNormalExtraChars = extraChars
		}
	}

	// Calculate the number of extra characters needed for table names with the old timestamp format
	NameFormatTimestampExtraChars = len(strings.ReplaceAll(NameFormatOldTimeStamp, "%s", "")) + len(NameFormatTimestamp)
}

func tableNameCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	tableName := r.Table.TableName
	if len(tableName) < 1 {
		return errors.New("table name must be at least 1 character")
	}

	timestampTableNameLength := maxTableNameLength - NameFormatTimestampExtraChars
	if r.SkipDropAfterCutover && len(tableName) > timestampTableNameLength {
		return fmt.Errorf("table name must be less than %d characters when --skip-drop-after-cutover is set", timestampTableNameLength)
	}

	normalTableNameLength := maxTableNameLength - NameFormatNormalExtraChars
	if len(tableName) > normalTableNameLength {
		return fmt.Errorf("table name must be less than %d characters", normalTableNameLength)
	}
	return nil
}

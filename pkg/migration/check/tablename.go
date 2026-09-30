package check

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/block/spirit/pkg/utils"
)

func init() {
	registerCheck("tablename", tableNameCheck, ScopePreflight)
}

func tableNameCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	tableName := r.Table.TableName
	if len(tableName) < 1 {
		return errors.New("table name must be at least 1 character")
	}
	if len(tableName) > utils.MaxTableNameLength {
		return fmt.Errorf("table name must be %d characters or fewer", utils.MaxTableNameLength)
	}
	return nil
}

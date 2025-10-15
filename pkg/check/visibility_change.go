package check

import (
	"context"

	"github.com/siddontang/loggers"
)

func init() {
	registerCheck("visibility", visibilityCheck, ScopePostSetup)
}

// We don't want to allow visibility changes
// This is because we've already attempted MySQL DDL as INPLACE, and it didn't work.
// It likely means the user is combining this operation with other unsafe operations,
// which is not a good idea. We need to protect them by not allowing it.
// https://github.com/block/spirit/issues/283
func visibilityCheck(_ context.Context, r Resources, _ loggers.Advanced) error {
	if err := r.Statement.AlterContainsIndexVisibility(); err != nil {
		return err
	}
	return nil
}

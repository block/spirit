package check

import (
	"context"
	"log/slog"
)

func init() {
	registerCheck("illegalClause", illegalClauseCheck, ScopePreflight)
}

// illegalClauseCheck checks for the presence of specific, unsupported
// clauses in the ALTER statement, such as ALGORITHM= and LOCK=.
func illegalClauseCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	return r.Statement.AlterContainsUnsupportedClause()
}

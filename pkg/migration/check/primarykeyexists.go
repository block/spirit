package check

import (
	"context"
	"errors"
	"log/slog"
)

func init() {
	registerCheck("primarykeyexists", primaryKeyExistsCheck, ScopeStatement)
}

// primaryKeyExistsCheck refuses every ALTER against a table that has no
// primary key. The migration runner requires one to chunk the copy: it fails
// table setup with "no primary key found (not supported)" before attempting
// even MySQL's native DDL, so the refusal holds for every ALTER shape on
// every server — including statements the native DDL could otherwise
// complete, and including ADD PRIMARY KEY itself.
//
// The verdict needs the table's current definition, so the check runs only at
// statement scope, where the caller may supply Resources.Table built from the
// table's DDL; without it the check skips rather than guesses. It is not
// registered at preflight: a migration on such a table fails setup before any
// check runs.
//
// The verdict is only as good as the supplied definition. On MySQL 8.0.30+ a
// generated invisible primary key is a real primary key that SHOW CREATE TABLE
// omits when show_gipk_in_create_table_and_information_schema is disabled; a
// definition collected that way misreports the table as unkeyed, and the check
// would refuse a statement the runner accepts. Collect the definition with
// that variable at its default (ON).
func primaryKeyExistsCheck(ctx context.Context, r Resources, logger *slog.Logger) error {
	if r.Table == nil {
		logger.Debug("skipping check: no table metadata supplied, cannot tell whether the table has a primary key",
			"check", "primarykeyexists")
		return nil
	}
	if len(r.Table.KeyColumns) == 0 {
		return errors.New("altering a table without a primary key is not supported")
	}
	return nil
}

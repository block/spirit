package check

import (
	"database/sql"
	"log/slog"
	"testing"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestEnumReorderCheck(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS enumchk`)
	testutils.RunSQL(t, `CREATE TABLE enumchk (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		status ENUM('active', 'inactive', 'pending') NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "enumchk")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// Reorder ENUM: should fail
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumchk MODIFY COLUMN status ENUM('pending', 'active', 'inactive') NOT NULL")[0],
	}
	err = enumReorderCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "unsafe ENUM value reorder")

	// Append ENUM (safe): should pass
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumchk MODIFY COLUMN status ENUM('active', 'inactive', 'pending', 'archived') NOT NULL")[0],
	}
	err = enumReorderCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)

	// Insert new ENUM value in middle: should fail
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumchk MODIFY COLUMN status ENUM('active', 'suspended', 'inactive', 'pending') NOT NULL")[0],
	}
	err = enumReorderCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "unsafe ENUM value reorder")

	// CHANGE COLUMN with reorder: should fail
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumchk CHANGE COLUMN status status ENUM('pending', 'active', 'inactive') NOT NULL")[0],
	}
	err = enumReorderCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "unsafe ENUM value reorder")

	// Drop ENUM value from the middle: should pass.
	// Retained values keep their relative order; rows that held the dropped
	// value will fail the post-cutover checksum, not the preflight check.
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumchk MODIFY COLUMN status ENUM('active', 'pending') NOT NULL")[0],
	}
	err = enumReorderCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)

	// Drop ENUM value from the start: should pass.
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumchk MODIFY COLUMN status ENUM('inactive', 'pending') NOT NULL")[0],
	}
	err = enumReorderCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)

	// Drop multiple ENUM values: should pass.
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumchk MODIFY COLUMN status ENUM('active') NOT NULL")[0],
	}
	err = enumReorderCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)

	// Drop a value and append a new one: should pass.
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumchk MODIFY COLUMN status ENUM('active', 'pending', 'archived') NOT NULL")[0],
	}
	err = enumReorderCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)

	// Drop a value and re-add it elsewhere: should fail.
	// 'active' is dropped from position 1 and reintroduced at the end —
	// that breaks the relative-order invariant for retained values.
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumchk MODIFY COLUMN status ENUM('inactive', 'pending', 'active') NOT NULL")[0],
	}
	err = enumReorderCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "unsafe ENUM value reorder")

	// Non-ENUM column modification: should pass
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumchk ADD COLUMN name VARCHAR(255)")[0],
	}
	err = enumReorderCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)
}

func TestEnumReorderCheckVarcharToEnum(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS enumchk_varchar`)
	testutils.RunSQL(t, `CREATE TABLE enumchk_varchar (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		state VARCHAR(191) NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "enumchk_varchar")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// VARCHAR→ENUM conversion: should pass (not a reorder)
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumchk_varchar MODIFY COLUMN state ENUM('active', 'inactive', 'pending') NOT NULL")[0],
	}
	err = enumReorderCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)

	// CHANGE COLUMN VARCHAR→ENUM: should pass
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE enumchk_varchar CHANGE COLUMN state state ENUM('active', 'inactive') NOT NULL")[0],
	}
	err = enumReorderCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)
}

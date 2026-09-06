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

func TestSetReorderCheck(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS setchk`)
	testutils.RunSQL(t, `CREATE TABLE setchk (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		flags SET('read', 'write', 'execute') NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "setchk")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// Reorder SET: should fail. The error flags both the unsafe reorder and the
	// checksum failure it would cause.
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE setchk MODIFY COLUMN flags SET('write', 'read', 'execute') NOT NULL")[0],
	}
	err = setReorderCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "unsafe SET value reorder")
	require.ErrorContains(t, err, "checksum")

	// Append SET (safe): should pass
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE setchk MODIFY COLUMN flags SET('read', 'write', 'execute', 'admin') NOT NULL")[0],
	}
	err = setReorderCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)

	// CHANGE COLUMN with reorder: should fail
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE setchk CHANGE COLUMN flags flags SET('execute', 'read', 'write') NOT NULL")[0],
	}
	err = setReorderCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "unsafe SET value reorder")

	// Remove SET value: should fail
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE setchk MODIFY COLUMN flags SET('read', 'execute') NOT NULL")[0],
	}
	err = setReorderCheck(t.Context(), r, slog.Default())
	require.Error(t, err)
	require.ErrorContains(t, err, "unsafe SET value reorder")

	// Non-SET column modification: should pass
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE setchk ADD COLUMN name VARCHAR(255)")[0],
	}
	err = setReorderCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)
}

func TestSetReorderCheckVarcharToSet(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS setchk_varchar`)
	testutils.RunSQL(t, `CREATE TABLE setchk_varchar (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		perms VARCHAR(191) NOT NULL
	)`)

	tbl := table.NewTableInfo(db, "test", "setchk_varchar")
	require.NoError(t, tbl.SetInfo(t.Context()))

	// VARCHAR→SET conversion: should pass (not a reorder)
	r := Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE setchk_varchar MODIFY COLUMN perms SET('read', 'write', 'execute') NOT NULL")[0],
	}
	err = setReorderCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)

	// CHANGE COLUMN VARCHAR→SET: should pass
	r = Resources{
		Table:     tbl,
		Statement: statement.MustNew("ALTER TABLE setchk_varchar CHANGE COLUMN perms perms SET('read', 'write') NOT NULL")[0],
	}
	err = setReorderCheck(t.Context(), r, slog.Default())
	require.NoError(t, err)
}

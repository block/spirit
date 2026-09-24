package check

import (
	"database/sql"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestHasTriggers(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)

	_, err = db.ExecContext(t.Context(), `drop table if exists account`)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `drop trigger if exists ins_sum`)
	require.NoError(t, err)
	sql := `CREATE TABLE account (
		acct_num INT,
		amount DECIMAL (10,2),
		PRIMARY KEY (acct_num)
	);`
	_, err = db.ExecContext(t.Context(), sql)
	require.NoError(t, err)
	sql = `CREATE TRIGGER ins_sum BEFORE INSERT ON account
		FOR EACH ROW SET @sum = @sum + NEW.amount;`
	_, err = db.ExecContext(t.Context(), sql)
	require.NoError(t, err)

	r := Resources{
		DB:        db,
		Table:     &table.TableInfo{SchemaName: "test", TableName: "account"},
		Statement: statement.MustNew("ALTER TABLE account Engine=innodb")[0],
	}

	err = hasTriggersCheck(t.Context(), r, slog.Default())
	require.ErrorContains(t, err, "tables with triggers associated are not supported") // already has a trigger associated.

	_, err = db.ExecContext(t.Context(), `drop trigger if exists ins_sum`)
	require.NoError(t, err)
	err = hasTriggersCheck(t.Context(), r, slog.Default())
	require.NoError(t, err) // no longer said to have trigger associated.
}

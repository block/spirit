package check

import (
	"database/sql"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
)

func TestHasTriggers(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)

	_, err = db.Exec(`drop table if exists account`)
	assert.NoError(t, err)
	_, err = db.Exec(`drop trigger if exists ins_sum`)
	assert.NoError(t, err)
	sql := `CREATE TABLE account (
		acct_num INT,
		amount DECIMAL (10,2),
		PRIMARY KEY (acct_num)
	);`
	_, err = db.Exec(sql)
	assert.NoError(t, err)
	sql = `CREATE TRIGGER ins_sum BEFORE INSERT ON account
		FOR EACH ROW SET @sum = @sum + NEW.amount;`
	_, err = db.Exec(sql)
	assert.NoError(t, err)

	r := Resources{
		DB:        db,
		Table:     &table.TableInfo{SchemaName: "test", TableName: "account"},
		Statement: statement.MustNew("ALTER TABLE account Engine=innodb")[0],
	}

	err = hasTriggersCheck(t.Context(), r, slog.Default())
	assert.ErrorContains(t, err, "tables with triggers associated are not supported") // already has a trigger associated.

	_, err = db.Exec(`drop trigger if exists ins_sum`)
	assert.NoError(t, err)
	err = hasTriggersCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err) // no longer said to have trigger associated.
}

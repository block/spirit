package check

import (
	"database/sql"
	"fmt"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

func TestPrivileges(t *testing.T) {
	config, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	config.User = "root" // needs grant privilege
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	assert.NoError(t, err)

	_, err = db.Exec("DROP USER IF EXISTS testprivsuser")
	assert.NoError(t, err)

	_, err = db.Exec("CREATE USER testprivsuser")
	assert.NoError(t, err)

	config, err = mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	config.User = "testprivsuser"
	config.Passwd = ""

	lowPrivDB, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	assert.NoError(t, err)
	r := Resources{
		DB:    lowPrivDB,
		Table: &table.TableInfo{TableName: "test", SchemaName: "test"},
	}
	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.Error(t, err) // privileges fail, since user has nothing granted.

	_, err = db.Exec("GRANT ALL ON test.* TO testprivsuser")
	assert.NoError(t, err)

	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.Error(t, err) // still not enough, needs replication client

	_, err = db.Exec("GRANT REPLICATION CLIENT, REPLICATION SLAVE, RELOAD ON *.* TO testprivsuser")
	assert.NoError(t, err)

	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err) // it's all good now

	// ForceKill requires additional privileges!
	r.ForceKill = true
	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.Error(t, err)

	_, err = db.Exec("GRANT SELECT on `performance_schema`.* TO testprivsuser")
	assert.NoError(t, err)

	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.Error(t, err) // still not enough, needs connection_admin

	_, err = db.Exec("GRANT CONNECTION_ADMIN ON *.* TO testprivsuser")
	assert.NoError(t, err)

	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.Error(t, err) // still not enough, needs PROCESS
	t.Log(err)

	_, err = db.Exec("GRANT PROCESS ON *.* TO testprivsuser")
	assert.NoError(t, err)

	// Reconnect before checking again.
	// There seems to be a race in MySQL where privileges don't show up immediately
	// That this can work around.
	lowPrivDB.Close()
	lowPrivDB, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.User, config.Passwd, config.Addr, config.DBName))
	assert.NoError(t, err)
	defer lowPrivDB.Close()
	r.DB = lowPrivDB

	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err)

	// Test the root user
	r = Resources{
		DB:    db,
		Table: &table.TableInfo{TableName: "test", SchemaName: "test"},
	}
	err = privilegesCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err) // privileges work fine
}

package migration

import (
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

// Attempt multi changes across different schemas
// This must never happen because we use the schema from
// changes[0] in a lot of places, we tie the checkpoint to
// a schema, and the sentinel etc.
func TestMultiChangesDifferentSchemas(t *testing.T) {
	t.Parallel()

	testutils.RunSQL(t, `DROP DATABASE IF EXISTS multichangedb1`)
	testutils.RunSQL(t, `CREATE DATABASE multichangedb1`)
	testutils.RunSQL(t, `CREATE TABLE multichangedb1.multichange1 (id int not null primary key auto_increment, b INT NOT NULL)`)

	// from the test schema
	testutils.RunSQL(t, `DROP TABLE IF EXISTS multichange2, multichange3`)
	testutils.RunSQL(t, `CREATE TABLE multichange2 (id int not null primary key auto_increment, b INT NOT NULL)`)
	testutils.RunSQL(t, `CREATE TABLE multichange3 (id int not null primary key auto_increment, b INT NOT NULL)`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	migration := &Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         4,
		TargetChunkTime: 2 * time.Second,
		Statement:       "ALTER TABLE multichangedb1.multichange1 ADD COLUMN a INT, ALTER TABLE multichange2 ADD COLUMN a INT; ALTER TABLE multichange3 ADD COLUMN a INT",
		ForceKill:       true,
	}
	assert.Error(t, migration.Run())
	migration.Statement = "ALTER TABLE multichange2 ADD COLUMN a INT; ALTER TABLE multichange3 ADD COLUMN a INT; ALTER TABLE multichangedb1.multichange1 ADD COLUMN a INT"
	assert.Error(t, migration.Run())
	migration.Statement = "ALTER TABLE multichange2 ADD COLUMN a INT; ALTER TABLE multichangedb1.multichange1 ADD COLUMN a INT; ALTER TABLE multichange3 ADD COLUMN a INT"
	assert.Error(t, migration.Run())
	migration.Statement = "ALTER TABLE multichangedb1.multichange1 ADD COLUMN a INT"
	assert.Error(t, migration.Run()) // even this is an error because we have schema + explicit DB.
	migration.Statement = "ALTER TABLE multichange2 ADD COLUMN a INT; ALTER TABLE multichange3 ADD COLUMN a INT"
	assert.NoError(t, migration.Run())
}

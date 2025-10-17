package move

import (
	"database/sql"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

func TestBasicMove(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	src := cfg.Clone()
	src.DBName = "source"
	dest := cfg.Clone()
	dest.DBName = "dest"

	// Convert src and dest back to DSNs.
	sourceDSN := src.FormatDSN()
	targetDSN := dest.FormatDSN()

	// create some data to copy.
	testutils.RunSQL(t, `DROP DATABASE IF EXISTS source`)
	testutils.RunSQL(t, `CREATE DATABASE source`)
	testutils.RunSQL(t, `CREATE TABLE source.t1 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `CREATE TABLE source.t2 (id INT PRIMARY KEY, val VARCHAR(255))`)
	testutils.RunSQL(t, `INSERT INTO source.t1 (id, val) VALUES (1, 'one'), (2, 'two'), (3, 'three')`)
	testutils.RunSQL(t, `INSERT INTO source.t2 (id, val) VALUES (4, 'four'), (5, 'five'), (6, 'six')`)

	// reset the target database.
	db, err := sql.Open("mysql", cfg.FormatDSN())
	assert.NoError(t, err)
	_, err = db.Exec("DROP DATABASE IF EXISTS dest")
	assert.NoError(t, err)
	_, err = db.Exec("CREATE DATABASE dest")
	assert.NoError(t, err)
	defer db.Close()
	// test
	move := &Move{
		SourceDSN:       sourceDSN,
		TargetDSN:       targetDSN,
		TargetChunkTime: 5 * time.Second,
		Threads:         4,
	}
	assert.NoError(t, move.Run())
}

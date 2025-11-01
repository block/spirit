//nolint:dupword
package migration

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/check"
	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVarcharNonBinaryComparable(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS nonbinarycompatt1, _nonbinarycompatt1_new`)
	table := `CREATE TABLE nonbinarycompatt1 (
		uuid varchar(40) NOT NULL,
		name varchar(255) NOT NULL,
		PRIMARY KEY (uuid)
	)`
	testutils.RunSQL(t, table)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "nonbinarycompatt1",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)                // everything is specified.
	assert.NoError(t, m.Run(t.Context())) // it's a non-binary comparable type (varchar)
	assert.NoError(t, m.Close())
}

// TestPartitioningSyntax tests that ALTERs that don't support ALGORITHM assertion
// are still supported. From https://github.com/block/spirit/issues/277
func TestPartitioningSyntax(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS partt1, _partt1_new`)
	table := `CREATE TABLE partt1 (
		id INT NOT NULL PRIMARY KEY auto_increment,
		name varchar(255) NOT NULL
	)`
	testutils.RunSQL(t, table)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "partt1",
		Alter:    "PARTITION BY KEY() PARTITIONS 8",
	})
	assert.NoError(t, err)
	assert.NoError(t, m.Run(t.Context()))
	assert.NoError(t, m.Close())
}

func TestVarbinary(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS varbinaryt1, _varbinaryt1_new`)
	table := `CREATE TABLE varbinaryt1 (
		uuid varbinary(40) NOT NULL,
		name varchar(255) NOT NULL,
		PRIMARY KEY (uuid)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, "INSERT INTO varbinaryt1 (uuid, name) VALUES (UUID(), REPEAT('a', 200))")
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "varbinaryt1",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)                // everything is specified correctly.
	assert.NoError(t, m.Run(t.Context())) // varbinary is compatible.
	assert.False(t, m.usedInstantDDL)     // not possible
	assert.NoError(t, m.Close())
}

// TestDataFromBadSqlMode tests that data previously inserted like 0000-00-00 can still be migrated.
func TestDataFromBadSqlMode(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS badsqlt1, _badsqlt1_new`)
	table := `CREATE TABLE badsqlt1 (
		id int not null primary key auto_increment,
		d date NOT NULL,
		t timestamp NOT NULL
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, "INSERT IGNORE INTO badsqlt1 (d, t) VALUES ('0000-00-00', '0000-00-00 00:00:00'),('2020-02-00', '2020-02-30 00:00:00')")
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "badsqlt1",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)                // everything is specified correctly.
	assert.NoError(t, m.Run(t.Context())) // pk is compatible.
	assert.False(t, m.usedInstantDDL)     // not possible
	assert.NoError(t, m.Close())
}

func TestChangeDatatypeNoData(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS cdatatypemytable`)
	table := `CREATE TABLE cdatatypemytable (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL, -- should be an int
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "cdatatypemytable",
		Alter:    "CHANGE b b INT", //nolint: dupword
	})
	assert.NoError(t, err)                // everything is specified correctly.
	assert.NoError(t, m.Run(t.Context())) // no data so no truncation is possible.
	assert.False(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())
}

func TestChangeDatatypeDataLoss(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS cdatalossmytable`)
	table := `CREATE TABLE cdatalossmytable (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL, -- should be an int
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, "INSERT INTO cdatalossmytable (name, b) VALUES ('a', 'b')")
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "cdatalossmytable",
		Alter:    "CHANGE b b INT", //nolint: dupword
	})
	assert.NoError(t, err)              // everything is specified correctly.
	assert.Error(t, m.Run(t.Context())) // value 'b' can no convert cleanly to int.
	assert.NoError(t, m.Close())
}

func TestOnline(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS testonline`)
	table := `CREATE TABLE testonline (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL, -- should be an int
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "testonline",
		Alter:    "CHANGE COLUMN b b int(11) NOT NULL", //nolint: dupword
	})
	assert.NoError(t, err)
	assert.NoError(t, m.Run(t.Context()))
	assert.False(t, m.usedInplaceDDL) // not possible
	assert.NoError(t, m.Close())

	// Create another table.
	testutils.RunSQL(t, `DROP TABLE IF EXISTS testonline2`)
	table = `CREATE TABLE testonline2 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL, -- should be an int
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "testonline2",
		Alter:    "ADD c int(11) NOT NULL",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.False(t, m.usedInplaceDDL) // uses instant DDL first

	// can only check this against 8.0
	assert.True(t, m.usedInstantDDL)
	assert.NoError(t, m.Close())

	// Finally, this will work.
	testutils.RunSQL(t, `DROP TABLE IF EXISTS testonline3`)
	table = `CREATE TABLE testonline3 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "testonline3",
		Alter:    "ADD INDEX(b)",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.False(t, m.usedInstantDDL) // not possible
	assert.False(t, m.usedInplaceDDL) // ADD INDEX operations now always require copy
	assert.NoError(t, m.Close())

	testutils.RunSQL(t, `DROP TABLE IF EXISTS testonline4`)
	table = `CREATE TABLE testonline4 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		key name (name),
		key b (b),
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "testonline4",
		Alter:    "drop index name, drop index b",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.False(t, m.usedInstantDDL) // unfortunately false in 8.0, see https://bugs.mysql.com/bug.php?id=113355
	assert.True(t, m.usedInplaceDDL)
	assert.NoError(t, m.Close())

	testutils.RunSQL(t, `DROP TABLE IF EXISTS testonline5`)
	table = `CREATE TABLE testonline5 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		key name (name),
		key b (b),
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "testonline5",
		Alter:    "drop index name, add column c int",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.False(t, m.usedInstantDDL) // unfortunately false in 8.0, see https://bugs.mysql.com/bug.php?id=113355
	assert.False(t, m.usedInplaceDDL) // unfortunately false, since it combines INSTANT and INPLACE operations
	assert.NoError(t, m.Close())

	testutils.RunSQL(t, `DROP TABLE IF EXISTS testonline6`)
	table = `CREATE TABLE testonline6 (
		id int(11) NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
		)
		PARTITION BY HASH (id)
		PARTITIONS 4
	`
	testutils.RunSQL(t, table)
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "testonline6",
		Alter:    "add partition partitions 4",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.False(t, m.usedInstantDDL)
	assert.False(t, m.usedInplaceDDL) // false, hash/key partitioned tables require a lock
	assert.NoError(t, m.Close())

	testutils.RunSQL(t, `DROP TABLE IF EXISTS testonline7`)
	table = `CREATE TABLE testonline7 (
		id int(11) NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
		)
		PARTITION BY RANGE (id) (
			PARTITION p0 VALUES LESS THAN (100000), 
			PARTITION p1 VALUES LESS THAN (200000)
		)
	`
	testutils.RunSQL(t, table)
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "testonline7",
		Alter:    "add partition (partition p2 values less than (300000))",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.False(t, m.usedInstantDDL)
	assert.True(t, m.usedInplaceDDL) // true, range/list partitioned tables can run inplace without a lock
	assert.NoError(t, m.Close())
}

func TestTableLength(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS thisisareallylongtablenamethisisareallylongtablename60charac`)
	table := `CREATE TABLE thisisareallylongtablenamethisisareallylongtablename60charac (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "thisisareallylongtablenamethisisareallylongtablename60charac",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "table name must be less than 56 characters")
	assert.NoError(t, m.Close())

	// There is another condition where the error will be in dropping the _old table first
	// if the character limit is exceeded in that query.
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "thisisareallylongtablenamethisisareallylongtablename60charac",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "table name must be less than 56 characters")
	assert.NoError(t, m.Close())
}

func TestBadOptions(t *testing.T) {
	t.Parallel()
	_, err := NewRunner(&Migration{})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "host is required")
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	_, err = NewRunner(&Migration{
		Host: cfg.Addr,
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "schema name is required")
	_, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Database: "mytable",
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "table name is required")
	_, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Database: "mytable",
		Table:    "mytable",
	})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "alter statement is required")
}

func TestBadAlter(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS bot1, bot2`)
	table := `CREATE TABLE bot1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	table = `CREATE TABLE bot2 (
		id int(11) NOT NULL,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "bot1",
		Alter:    "badalter",
	})
	assert.Error(t, err) // parses and fails.
	assert.Nil(t, m)

	// Renames are not supported.
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "bot1",
		Alter:    "RENAME COLUMN name TO name2, ADD INDEX(name)", // need both, otherwise INSTANT algorithm will do the rename
	})
	assert.NoError(t, err) // does not parse alter yet.
	err = m.Run(t.Context())
	assert.Error(t, err) // alter is invalid
	assert.ErrorContains(t, err, "renames are not supported")
	assert.NoError(t, m.Close())

	// This is a different type of rename,
	// which is coming via a change
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "bot1",
		Alter:    "CHANGE name name2 VARCHAR(255), ADD INDEX(name)", // need both, otherwise INSTANT algorithm will do the rename
	})
	assert.NoError(t, err) // does not parse alter yet.
	err = m.Run(t.Context())
	assert.Error(t, err) // alter is invalid
	assert.ErrorContains(t, err, "renames are not supported")
	assert.NoError(t, m.Close())

	// But this is supported (no rename)
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "bot1",
		Alter:    "CHANGE name name VARCHAR(200), ADD INDEX(name)", //nolint: dupword
	})
	assert.NoError(t, err) // does not parse alter yet.
	err = m.Run(t.Context())
	assert.NoError(t, err) // its valid, no rename
	assert.NoError(t, m.Close())

	// Test DROP PRIMARY KEY, change primary key.
	// The REPLACE statement likely relies on the same PRIMARY KEY on the new table,
	// so things get a lot more complicated if the primary key changes.
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "bot2",
		Alter:    "DROP PRIMARY KEY",
	})
	assert.NoError(t, err) // does not parse alter yet.
	err = m.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "dropping primary key")
	assert.NoError(t, m.Close())
}

// TestChangeDatatypeLossyNoAutoInc is a good test of the how much the
// chunker will boil the ocean:
//   - There is a MIN(key)=1 and a MAX(key)=8589934592
//   - There is no auto-increment so the chunker is allowed to expand each chunk
//     based on estimated rows (which is low).
//
// Only the key=8589934592 will fail to be converted. On my system this test
// currently runs in 0.4 seconds which is "acceptable" for chunker performance.
// The generated number of chunks should also be very low because of prefetching.
func TestChangeDatatypeLossyNoAutoInc(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS lossychange2`)
	table := `CREATE TABLE lossychange2 (
					id BIGINT NOT NULL,
					name varchar(255) NOT NULL,
					b varchar(255) NOT NULL,
					PRIMARY KEY (id)
				)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, "INSERT INTO lossychange2 (id, name, b) VALUES (1, 'a', REPEAT('a', 200))")          // will pass in migration
	testutils.RunSQL(t, "INSERT INTO lossychange2 (id, name, b) VALUES (8589934592, 'a', REPEAT('a', 200))") // will fail in migration

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "lossychange2",
		Alter:    "CHANGE COLUMN id id INT NOT NULL auto_increment", //nolint: dupword
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "Out of range value") // Error 1264: Out of range value for column 'id' at row 1
	// Check that the chunker processed fewer than 500 chunks
	_, chunksCopied, _ := m.copier.GetChunker().Progress()
	assert.Less(t, chunksCopied, uint64(500))
	assert.NoError(t, m.Close())
}

// TestChangeDatatypeLossy3 has a data type change that is "lossy" but
// given the current stored data set does not cause errors.
func TestChangeDatatypeLossless(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS lossychange3`)
	table := `CREATE TABLE lossychange3 (
				id BIGINT NOT NULL AUTO_INCREMENT,
				name varchar(255) NOT NULL,
				b varchar(255) NULL,
				PRIMARY KEY (id)
			)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, "INSERT INTO lossychange3 (name, b) VALUES ('a', REPEAT('a', 200))")
	testutils.RunSQL(t, "INSERT INTO lossychange3 (id, name, b) VALUES (8589934592, 'a', REPEAT('a', 200))")

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "lossychange3",
		Alter:    "CHANGE COLUMN b b varchar(200) NOT NULL", //nolint: dupword
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err) // works because there are no violations.
	// Check that the chunker processed fewer than 500 chunks
	_, chunksCopied, _ := m.copier.GetChunker().Progress()
	assert.Less(t, chunksCopied, uint64(500))
	assert.NoError(t, m.Close())
}

// TestChangeDatatypeLossyFailEarly tests a scenario where there is an error
// immediately so the DDL should halt.
// So if it does try to exhaustively run the DDL it will take forever:
// [1, 8589934592] / 1000 = 8589934.592 chunks

func TestChangeDatatypeLossyFailEarly(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS lossychange4`)
	table := `CREATE TABLE lossychange4 (
				id BIGINT NOT NULL AUTO_INCREMENT,
				name varchar(255) NOT NULL,
				b varchar(255) NULL,
				PRIMARY KEY (id)
			)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, "INSERT INTO lossychange4 (name) VALUES ('a')")
	testutils.RunSQL(t, "INSERT INTO lossychange4 (name, b) VALUES ('a', REPEAT('a', 200))")
	testutils.RunSQL(t, "INSERT INTO lossychange4 (id, name, b) VALUES (8589934592, 'a', REPEAT('a', 200))")
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "lossychange4",
		Alter:    "CHANGE COLUMN b b varchar(255) NOT NULL", //nolint: dupword
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.Error(t, err) // there is a violation where row 1 is NULL
	assert.NoError(t, m.Close())
}

// TestAddUniqueIndex is a really interesting test *because* resuming from checkpoint
// will cause duplicate key errors. It's not straight-forward to differentiate between
// duplicate errors from a resume, and a constraint violation. So what we do is:
// 0) *FORCE* checksum to be enabled (regardless now, its always on)
func TestAddUniqueIndexChecksumEnabled(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS uniqmytable`)
	table := `CREATE TABLE uniqmytable (
				id int(11) NOT NULL AUTO_INCREMENT,
				name varchar(255) NOT NULL,
				b varchar(255) NOT NULL,
				PRIMARY KEY (id)
			)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, "INSERT INTO uniqmytable (name, b) VALUES ('a', REPEAT('a', 200))")
	testutils.RunSQL(t, "INSERT INTO uniqmytable (name, b) VALUES ('a', REPEAT('b', 200))")
	testutils.RunSQL(t, "INSERT INTO uniqmytable (name, b) VALUES ('a', REPEAT('c', 200))")
	testutils.RunSQL(t, "INSERT INTO uniqmytable (name, b) VALUES ('a', REPEAT('a', 200))") // duplicate

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "uniqmytable",
		Alter:    "ADD UNIQUE INDEX b (b)",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.Error(t, err)         // not unique
	assert.NoError(t, m.Close()) // need to close now otherwise we'll get an error on re-opening it.

	testutils.RunSQL(t, "DELETE FROM uniqmytable WHERE b = REPEAT('a', 200) LIMIT 1") // make unique
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "uniqmytable",
		Alter:    "ADD UNIQUE INDEX b (b)",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err) // works fine.
	assert.NoError(t, m.Close())
}

// Test int to bigint primary key while resuming from checkpoint.
func TestChangeIntToBigIntPKResumeFromChkPt(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS bigintpk, _bigintpk_chkpnt, _bigintpk_new`)
	table := `CREATE TABLE bigintpk (
			pk int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name varchar(255) NOT NULL,
			b varchar(10) NOT NULL,
            version bigint unsigned NOT NULL DEFAULT '1' COMMENT 'Used for optimistic concurrency.'
		)`
	testutils.RunSQL(t, table)
	// Insert initial data, there needs to be enough that it doesn't just finish
	// the full copy before the first checkpoint can be written.
	testutils.RunSQL(t, "INSERT INTO bigintpk (name, b) VALUES ('a', 'a')")
	testutils.RunSQL(t, `INSERT INTO bigintpk (name, b) SELECT a.name, a.b FROM bigintpk a JOIN bigintpk b JOIN bigintpk c`)
	testutils.RunSQL(t, `INSERT INTO bigintpk (name, b) SELECT a.name, a.b FROM bigintpk a JOIN bigintpk b JOIN bigintpk c`)
	testutils.RunSQL(t, `INSERT INTO bigintpk (name, b) SELECT a.name, a.b FROM bigintpk a JOIN bigintpk b JOIN bigintpk c`)
	testutils.RunSQL(t, `INSERT INTO bigintpk (name, b) SELECT a.name, a.b FROM bigintpk a JOIN bigintpk b JOIN bigintpk c LIMIT 100000`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         1,
		TargetChunkTime: 100 * time.Millisecond,
		Table:           "bigintpk",
		Alter:           "modify column pk bigint unsigned not null auto_increment",
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		err := m.Run(ctx)
		assert.Error(t, err) // it gets interrupted as soon as there is a checkpoint saved.
	}()
	// wait until a checkpoint is saved (which means copy is in progress)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()

	// Add timeout to prevent infinite waiting
	timeout := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for checkpoint to be created")
		case <-ticker.C:
			var rowCount int
			err = db.QueryRow(`SELECT count(*) from _bigintpk_chkpnt`).Scan(&rowCount)
			if err != nil {
				continue // table does not exist yet
			}
			if rowCount > 0 {
				goto checkpointFound
			}
		}
	}
checkpointFound:
	// Between cancel and Close() every resource is freed.
	assert.NoError(t, m.Close())
	cancel()

	// Insert some more dummy data
	testutils.RunSQL(t, "INSERT INTO bigintpk (name,b) VALUES('t', 't')")
	// Start a new migration with the same parameters.
	// Let it complete.
	m2, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "bigintpk",
		Alter:    "modify column pk bigint unsigned not null auto_increment",
	})
	assert.NoError(t, err)
	assert.NotNil(t, m2)

	err = m2.Run(t.Context())
	assert.NoError(t, err)
	assert.True(t, m2.usedResumeFromCheckpoint)
	assert.NoError(t, m2.Close())
}

func TestChangeNonIntPK(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS nonintpk`)
	table := `CREATE TABLE nonintpk (
			pk varbinary(36) NOT NULL PRIMARY KEY,
			name varchar(255) NOT NULL,
			b varchar(10) NOT NULL -- change to varchar(255)
		)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, "INSERT INTO nonintpk (pk, name, b) VALUES (UUID(), 'a', REPEAT('a', 5))")
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "nonintpk",
		Alter:    "CHANGE COLUMN b b VARCHAR(255) NOT NULL", //nolint: dupword
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.NoError(t, m.Close())
}

func TestCheckpoint(t *testing.T) {
	tbl := `CREATE TABLE cpt1 (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		id2 INT NOT NULL,
		pad VARCHAR(100) NOT NULL default 0)`
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS cpt1, _cpt1_new, _cpt1_chkpnt`)
	testutils.RunSQL(t, tbl)
	testutils.RunSQL(t, `insert into cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM dual`)
	testutils.RunSQL(t, `insert into cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1`)
	testutils.RunSQL(t, `insert into cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1 a JOIN cpt1 b JOIN cpt1 c`)
	testutils.RunSQL(t, `insert into cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1 a JOIN cpt1 b JOIN cpt1 c`)
	testutils.RunSQL(t, `insert into cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1 a JOIN cpt1 LIMIT 100000`) // ~100k rows

	testLogger := newTestLogger()

	preSetup := func() *Runner {
		r, err := NewRunner(&Migration{
			Host:     cfg.Addr,
			Username: cfg.User,
			Password: cfg.Passwd,
			Database: cfg.DBName,
			Threads:  2,
			Table:    "cpt1",
			Alter:    "ENGINE=InnoDB",
		})
		assert.NoError(t, err)
		assert.Equal(t, "initial", r.status.Get().String())
		r.SetLogger(testLogger)
		// Usually we would call r.Run() but we want to step through
		// the migration process manually.
		r.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
		assert.NoError(t, err)
		r.dbConfig = dbconn.NewDBConfig()

		// Get Table Info
		r.changes[0].table = table.NewTableInfo(r.db, r.migration.Database, r.migration.Table)
		err = r.changes[0].table.SetInfo(t.Context())
		assert.NoError(t, err)
		assert.NoError(t, r.changes[0].dropOldTable(t.Context()))
		go r.dumpStatus(t.Context()) // start periodically writing status
		return r
	}
	r := preSetup()
	// migrationRunner.Run usually calls r.Setup() here.
	// Which first checks if the table can be restored from checkpoint.
	// Because this is the first run, it can't.
	assert.Error(t, r.resumeFromCheckpoint(t.Context()))
	// So we proceed with the initial steps.
	assert.NoError(t, r.newMigration(t.Context()))

	// Now we are ready to start copying rows.
	// Instead of calling r.copyRows() we will step through it manually.
	// Since we want to checkpoint after a few chunks.

	r.status.Set(status.CopyRows)
	assert.Equal(t, "copyRows", r.status.Get().String())

	time.Sleep(time.Second) // wait for status to be updated.
	testLogger.Lock()
	assert.Contains(t, testLogger.lastInfof, `migration status: state=copyRows copy-progress=0/101040 0.00% binlog-deltas=0`)
	testLogger.Unlock()

	// first chunk.
	chunk1, err := r.copyChunker.Next()
	assert.NoError(t, err)

	chunk2, err := r.copyChunker.Next()
	assert.NoError(t, err)

	chunk3, err := r.copyChunker.Next()
	assert.NoError(t, err)

	// Assert there is no watermark yet, because we've not finished
	// copying any of the chunks.
	_, err = r.copyChunker.GetLowWatermark()
	assert.Error(t, err)
	// Dump checkpoint also returns an error for the same reason.
	assert.Error(t, r.dumpCheckpoint(t.Context()))

	ccopier, ok := r.copier.(*copier.Unbuffered)
	assert.True(t, ok)

	// Because it's multi-threaded, we can't guarantee the order of the chunks.
	// Let's complete them in the order of 2, 1, 3. When 2 phones home first
	// it should be queued. Then when 1 phones home it should apply and de-queue 2.
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk2))
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk1))
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk3))

	time.Sleep(time.Second) // wait for status to be updated.
	testLogger.Lock()
	assert.Contains(t, testLogger.lastInfof, `migration status: state=copyRows copy-progress=3000/101040 2.97% binlog-deltas=0`)
	testLogger.Unlock()

	// The watermark should exist now, because migrateChunk()
	// gives feedback back to table.
	watermark, err := r.copyChunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"1001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2001\"],\"Inclusive\":false}}", watermark)
	// Dump a checkpoint
	assert.NoError(t, r.dumpCheckpoint(t.Context()))

	// Clean up first runner
	assert.NoError(t, r.Close())

	// Now lets imagine that everything fails and we need to start
	// from checkpoint again.

	r = preSetup()
	defer r.Close()
	// Start the binary log feed just before copy rows starts.
	// replClient.Run() is already called in resumeFromCheckpoint.
	assert.NoError(t, r.resumeFromCheckpoint(t.Context()))
	// This opens the table at the checkpoint (table.OpenAtWatermark())
	// which sets the chunkPtr at the LowerBound. It also has to position
	// the watermark to this point so new watermarks "align" correctly.
	// So lets now call NextChunk to verify.

	ccopier, ok = r.copier.(*copier.Unbuffered)
	assert.True(t, ok)

	chunk, err := r.copyChunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "1001", chunk.LowerBound.Value[0].String())
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk))

	// It's ideally not typical but you can still dump checkpoint from
	// a restored checkpoint state. We won't have advanced anywhere from
	// the last checkpoint because on restore, the LowerBound is taken.
	watermark, err = r.copyChunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"1001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2001\"],\"Inclusive\":false}}", watermark)
	// Dump a checkpoint
	assert.NoError(t, r.dumpCheckpoint(t.Context()))

	// Let's confirm we do advance the watermark.
	for range 10 {
		chunk, err = r.copyChunker.Next()
		assert.NoError(t, err)
		assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk))
	}

	watermark, err = r.copyChunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"11001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"12001\"],\"Inclusive\":false}}", watermark)
}

func TestCheckpointRestore(t *testing.T) {
	t.Parallel()
	tbl := `CREATE TABLE cpt2 (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		id2 INT NOT NULL,
		pad VARCHAR(100) NOT NULL default 0)`
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS cpt2, _cpt2_new, _cpt2_chkpnt`)
	testutils.RunSQL(t, tbl)

	r, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "cpt2",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	assert.Equal(t, "initial", r.status.Get().String())
	// Usually we would call r.Run() but we want to step through
	// the migration process manually.
	r.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	r.dbConfig = dbconn.NewDBConfig()
	// Get Table Info
	r.changes[0].table = table.NewTableInfo(r.db, r.migration.Database, r.migration.Table)
	err = r.changes[0].table.SetInfo(t.Context())
	assert.NoError(t, err)
	assert.NoError(t, r.changes[0].dropOldTable(t.Context()))

	// Proceed with the initial steps.
	assert.NoError(t, r.newMigration(t.Context()))

	// Now insert a fake checkpoint, this uses a known bad value
	// from issue #125
	watermark := "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\":[\"53926425\"],\"Inclusive\":true},\"UpperBound\":{\"Value\":[\"53926425\"],\"Inclusive\":false}}"
	binlog := r.replClient.GetBinlogApplyPosition()
	err = dbconn.Exec(t.Context(), r.db, `INSERT INTO %n.%n
	(copier_watermark, checksum_watermark, binlog_name, binlog_pos, statement)
	VALUES
	(%?,  %?, %?, %?, %?)`,
		r.checkpointTable.SchemaName,
		r.checkpointTable.TableName,
		watermark,
		"",
		binlog.Name,
		binlog.Pos,
		r.migration.Statement,
	)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())

	r2, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "cpt2",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	err = r2.Run(t.Context())
	assert.NoError(t, err)
	assert.True(t, r2.usedResumeFromCheckpoint)
	assert.NoError(t, r2.Close())
}

// https://github.com/block/spirit/issues/381
func TestCheckpointRestoreBinaryPK(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	tbl := `CREATE TABLE binarypk (
 main_id varbinary(16) NOT NULL,
 sub_id varchar(36) CHARACTER SET latin1 COLLATE latin1_swedish_ci GENERATED ALWAYS AS (jsonbody->>'$._id') STORED NOT NULL,
 jsonbody json NOT NULL,
 PRIMARY KEY (main_id,sub_id)
);`
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS binarypk, _binarypk_new, _binarypk_chkpnt`)
	testutils.RunSQL(t, tbl)
	testutils.RunSQL(t, `INSERT INTO binarypk (main_id, jsonbody) SELECT RANDOM_BYTES(16), JSON_OBJECT('_id', "0xabc", 'name', 'bbb', 'randombytes', HEX(RANDOM_BYTES(1024))) from dual`)
	testutils.RunSQL(t, `INSERT INTO binarypk (main_id, jsonbody) SELECT RANDOM_BYTES(16), JSON_OBJECT('_id', "0xabc", 'name', 'bbb', 'randombytes', HEX(RANDOM_BYTES(1024))) from binarypk a JOIN binarypk b JOIN binarypk c LIMIT 10000;`)
	testutils.RunSQL(t, `INSERT INTO binarypk (main_id, jsonbody) SELECT RANDOM_BYTES(16), JSON_OBJECT('_id', "0xabc", 'name', 'bbb', 'randombytes', HEX(RANDOM_BYTES(1024))) from binarypk a JOIN binarypk b JOIN binarypk c LIMIT 10000;`)
	testutils.RunSQL(t, `INSERT INTO binarypk (main_id, jsonbody) SELECT RANDOM_BYTES(16), JSON_OBJECT('_id', "0xabc", 'name', 'bbb', 'randombytes', HEX(RANDOM_BYTES(1024))) from binarypk a JOIN binarypk b JOIN binarypk c LIMIT 10000;`)
	testutils.RunSQL(t, `INSERT INTO binarypk (main_id, jsonbody) SELECT RANDOM_BYTES(16), JSON_OBJECT('_id', "0xabc", 'name', 'bbb', 'randombytes', HEX(RANDOM_BYTES(1024))) from binarypk a JOIN binarypk b JOIN binarypk c LIMIT 10000;`)

	r, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		Table:    "binarypk",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	assert.Equal(t, "initial", r.status.Get().String())
	// Usually we would call r.Run() but we want to step through
	// the migration process manually.
	r.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	r.dbConfig = dbconn.NewDBConfig()
	// Get Table Info
	r.changes[0].table = table.NewTableInfo(r.db, r.migration.Database, r.migration.Table)
	err = r.changes[0].table.SetInfo(ctx)
	assert.NoError(t, err)
	assert.NoError(t, r.changes[0].dropOldTable(ctx))

	assert.NoError(t, r.newMigration(t.Context()))

	ccopier, ok := r.copier.(*copier.Unbuffered)
	assert.True(t, ok)

	for range 3 {
		chunk, err := r.copyChunker.Next()
		assert.NoError(t, err)
		assert.NoError(t, ccopier.CopyChunk(ctx, chunk))
	}
	// Dump checkpoint and close runner.
	assert.NoError(t, r.dumpCheckpoint(t.Context()))
	assert.NoError(t, r.Close())
	// Try and resume and then check if we used a checkpoint
	// for resuming.
	r2, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "binarypk",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	err = r2.Run(t.Context())
	assert.NoError(t, err)
	assert.True(t, r2.usedResumeFromCheckpoint) // managed to resume.
	assert.NoError(t, r2.Close())
}

func TestCheckpointResumeDuringChecksum(t *testing.T) {
	t.Parallel()

	// Create unique database for this test
	dbName := testutils.CreateUniqueTestDatabase(t)

	tbl := `CREATE TABLE cptresume (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		id2 INT NOT NULL,
		pad VARCHAR(100) NOT NULL default 0)`
	cfg, err := mysql.ParseDSN(testutils.DSNForDatabase(dbName))
	assert.NoError(t, err)
	testutils.RunSQLInDatabase(t, dbName, `DROP TABLE IF EXISTS cptresume, _cptresume_new, _cptresume_chkpnt`)
	testutils.RunSQLInDatabase(t, dbName, tbl)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE _spirit_sentinel (id INT NOT NULL PRIMARY KEY)`)
	testutils.RunSQLInDatabase(t, dbName, `insert into cptresume (id2,pad) SELECT 1, REPEAT('a', 100) FROM dual`)
	testutils.RunSQLInDatabase(t, dbName, `insert into cptresume (id2,pad) SELECT 1, REPEAT('a', 100) FROM cptresume`)
	testutils.RunSQLInDatabase(t, dbName, `insert into cptresume (id2,pad) SELECT 1, REPEAT('a', 100) FROM cptresume a JOIN cptresume b JOIN cptresume c`)

	r, err := NewRunner(&Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		Database:        dbName,
		Threads:         4,
		TargetChunkTime: 100 * time.Millisecond,
		Table:           "cptresume",
		Alter:           "ENGINE=InnoDB",
		RespectSentinel: true,
	})
	assert.NoError(t, err)

	// Call r.Run() with our context in a go-routine.
	// When we see that we are waiting on the sentinel table,
	// we then manually start the first bits of checksum, and then close()
	// We should be able to resume from the checkpoint into the checksum state.
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		err := r.Run(ctx)
		assert.Error(t, err) // context cancelled
	}()
	for r.status.Get() < status.WaitingOnSentinelTable {
		// Wait for the sentinel table.
		time.Sleep(time.Millisecond)
	}

	assert.NoError(t, r.checksum(t.Context()))       // run the checksum, the original Run is blocked on sentinel.
	assert.NoError(t, r.dumpCheckpoint(t.Context())) // dump a checkpoint with the watermark.
	cancel()                                         // unblock the original waiting on sentinel.
	assert.NoError(t, r.Close())                     // close the run.

	// drop the sentinel table.
	testutils.RunSQLInDatabase(t, dbName, `DROP TABLE _spirit_sentinel`)

	// insert a couple more rows (should not change anything)
	testutils.RunSQLInDatabase(t, dbName, `insert into cptresume (id2,pad) SELECT 1, REPEAT('b', 100) FROM dual`)
	testutils.RunSQLInDatabase(t, dbName, `insert into cptresume (id2,pad) SELECT 1, REPEAT('c', 100) FROM dual`)

	// Start again as a new runner,
	r2, err := NewRunner(&Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         4,
		TargetChunkTime: 100 * time.Millisecond,
		Table:           "cptresume",
		Alter:           "ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	err = r2.Run(t.Context())
	assert.NoError(t, err)
	defer r2.Close()
	assert.True(t, r2.usedResumeFromCheckpoint)
}

func TestCheckpointDifferentRestoreOptions(t *testing.T) {
	t.Parallel()
	tbl := `CREATE TABLE cpt1difft1 (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		id2 INT NOT NULL,
		pad VARCHAR(100) NOT NULL default 0)`

	testutils.RunSQL(t, `DROP TABLE IF EXISTS cpt1difft1, cpt1difft1_new, _cpt1difft1_chkpnt`)
	testutils.RunSQL(t, tbl)
	testutils.RunSQL(t, `insert into cpt1difft1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM dual`)
	testutils.RunSQL(t, `insert into cpt1difft1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1difft1`)
	testutils.RunSQL(t, `insert into cpt1difft1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1difft1 a JOIN cpt1difft1 b JOIN cpt1difft1 c`)
	testutils.RunSQL(t, `insert into cpt1difft1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1difft1 a JOIN cpt1difft1 b JOIN cpt1difft1 c`)
	testutils.RunSQL(t, `insert into cpt1difft1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1difft1 a JOIN cpt1difft1 LIMIT 100000`) // ~100k rows
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	preSetup := func(alter string) *Runner {
		m, err := NewRunner(&Migration{
			Host:     cfg.Addr,
			Username: cfg.User,
			Password: cfg.Passwd,
			Database: cfg.DBName,
			Threads:  2,
			Table:    "cpt1difft1",
			Alter:    alter,
		})
		assert.NoError(t, err)
		assert.Equal(t, "initial", m.status.Get().String())
		// Usually we would call m.Run() but we want to step through
		// the migration process manually.
		m.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
		assert.NoError(t, err)
		m.dbConfig = dbconn.NewDBConfig()
		// Get Table Info
		m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.migration.Table)
		err = m.changes[0].table.SetInfo(t.Context())
		assert.NoError(t, err)
		assert.NoError(t, m.changes[0].dropOldTable(t.Context()))
		return m
	}

	m := preSetup("ADD COLUMN id3 INT NOT NULL DEFAULT 0, ADD INDEX(id2)")
	// migrationRunner.Run usually calls m.Setup() here.
	// Which first checks if the table can be restored from checkpoint.
	// Because this is the first run, it can't.

	assert.Error(t, m.resumeFromCheckpoint(t.Context()))

	assert.NoError(t, m.newMigration(t.Context()))

	// Now we are ready to start copying rows.
	// Instead of calling m.copyRows() we will step through it manually.
	// Since we want to checkpoint after a few chunks.

	// m.copier.StartTime = time.Now()
	m.status.Set(status.CopyRows)
	assert.Equal(t, "copyRows", m.status.Get().String())

	// first chunk.
	chunk1, err := m.copyChunker.Next()
	assert.NoError(t, err)

	chunk2, err := m.copyChunker.Next()
	assert.NoError(t, err)

	chunk3, err := m.copyChunker.Next()
	assert.NoError(t, err)

	// There is no watermark yet.
	_, err = m.copyChunker.GetLowWatermark()
	assert.Error(t, err)
	// Dump checkpoint also returns an error for the same reason.
	assert.Error(t, m.dumpCheckpoint(t.Context()))

	ccopier, ok := m.copier.(*copier.Unbuffered)
	assert.True(t, ok)

	// Because it's multi-threaded, we can't guarantee the order of the chunks.
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk2))
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk1))
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk3))

	// The watermark should exist now, because migrateChunk()
	// gives feedback back to table.

	watermark, err := m.copyChunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"1001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2001\"],\"Inclusive\":false}}", watermark)
	// Dump a checkpoint
	assert.NoError(t, m.dumpCheckpoint(t.Context()))

	// Close m
	assert.NoError(t, m.Close())

	// Now lets imagine that everything fails and we need to start
	// from checkpoint again.

	m = preSetup("ADD COLUMN id4 INT NOT NULL DEFAULT 0, ADD INDEX(id2)")
	assert.Error(t, m.resumeFromCheckpoint(t.Context())) // it should error because the ALTER does not match.
	assert.NoError(t, m.Close())
}

// TestE2EBinlogSubscribingCompositeKey and TestE2EBinlogSubscribingNonCompositeKey tests
// are complex tests that uses the lower level interface
// to step through the table while subscribing to changes that we will
// be making to the table between chunks. It is effectively an
// end-to-end test with concurrent operations on the table.
func TestE2EBinlogSubscribingCompositeKey(t *testing.T) {
	tbl := `CREATE TABLE e2et1 (
		id1 int NOT NULL,
		id2 int not null,
		pad int NOT NULL  default 0,
		PRIMARY KEY (id1, id2))`
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS e2et1, _e2et1_new`)
	testutils.RunSQL(t, tbl)
	testutils.RunSQL(t, `insert into e2et1 (id1, id2) values (1,1),(2,1),(3,1),(4,1),(5,1),(6,1),(7,1),(8,1),(9,1),(10,1),
	(11,1),(12,1),(13,1),(14,1),(15,1),(16,1),(17,1),(18,1),(19,1),(20,1),(21,1),(22,1),(23,1),(24,1),(25,1),(26,1),
	(27,1),(28,1),(29,1),(30,1),(31,1),(32,1),(33,1),(34,1),(35,1),(36,1),(37,1),(38,1),(39,1),(40,1),(41,1),(42,1),
	(43,1),(44,1),(45,1),(46,1),(47,1),(48,1),(49,1),(50,1),(51,1),(52,1),(53,1),(54,1),(55,1),(56,1),(57,1),(58,1),
	(59,1),(60,1),(61,1),(62,1),(63,1),(64,1),(65,1),(66,1),(67,1),(68,1),(69,1),(70,1),(71,1),(72,1),(73,1),(74,1),
	(75,1),(76,1),(77,1),(78,1),(79,1),(80,1),(81,1),(82,1),(83,1),(84,1),(85,1),(86,1),(87,1),(88,1),(89,1),(90,1),
	(91,1),(92,1),(93,1),(94,1),(95,1),(96,1),(97,1),(98,1),(99,1),(100,1),(101,1),(102,1),(103,1),(104,1),(105,1),
	(106,1),(107,1),(108,1),(109,1),(110,1),(111,1),(112,1),(113,1),(114,1),(115,1),(116,1),(117,1),(118,1),(119,1),
	(120,1),(121,1),(122,1),(123,1),(124,1),(125,1),(126,1),(127,1),(128,1),(129,1),(130,1),(131,1),(132,1),(133,1),
	(134,1),(135,1),(136,1),(137,1),(138,1),(139,1),(140,1),(141,1),(142,1),(143,1),(144,1),(145,1),(146,1),(147,1),
	(148,1),(149,1),(150,1),(151,1),(152,1),(153,1),(154,1),(155,1),(156,1),(157,1),(158,1),(159,1),(160,1),(161,1),
	(162,1),(163,1),(164,1),(165,1),(166,1),(167,1),(168,1),(169,1),(170,1),(171,1),(172,1),(173,1),(174,1),(175,1),
	(176,1),(177,1),(178,1),(179,1),(180,1),(181,1),(182,1),(183,1),(184,1),(185,1),(186,1),(187,1),(188,1),(189,1),
	(190,1),(191,1),(192,1),(193,1),(194,1),(195,1),(196,1),(197,1),(198,1),(199,1),(200,1),(201,1),(202,1),(203,1),
	(204,1),(205,1),(206,1),(207,1),(208,1),(209,1),(210,1),(211,1),(212,1),(213,1),(214,1),(215,1),(216,1),(217,1),
	(218,1),(219,1),(220,1),(221,1),(222,1),(223,1),(224,1),(225,1),(226,1),(227,1),(228,1),(229,1),(230,1),(231,1),
	(232,1),(233,1),(234,1),(235,1),(236,1),(237,1),(238,1),(239,1),(240,1),(241,1),(242,1),(243,1),(244,1),(245,1),
	(246,1),(247,1),(248,1),(249,1),(250,1),(251,1),(252,1),(253,1),(254,1),(255,1),(256,1),(257,1),(258,1),(259,1),
	(260,1),(261,1),(262,1),(263,1),(264,1),(265,1),(266,1),(267,1),(268,1),(269,1),(270,1),(271,1),(272,1),(273,1),
	(274,1),(275,1),(276,1),(277,1),(278,1),(279,1),(280,1),(281,1),(282,1),(283,1),(284,1),(285,1),(286,1),(287,1),
	(288,1),(289,1),(290,1),(291,1),(292,1),(293,1),(294,1),(295,1),(296,1),(297,1),(298,1),(299,1),(300,1),(301,1),
	(302,1),(303,1),(304,1),(305,1),(306,1),(307,1),(308,1),(309,1),(310,1),(311,1),(312,1),(313,1),(314,1),(315,1),
	(316,1),(317,1),(318,1),(319,1),(320,1),(321,1),(322,1),(323,1),(324,1),(325,1),(326,1),(327,1),(328,1),(329,1),
	(330,1),(331,1),(332,1),(333,1),(334,1),(335,1),(336,1),(337,1),(338,1),(339,1),(340,1),(341,1),(342,1),(343,1),
	(344,1),(345,1),(346,1),(347,1),(348,1),(349,1),(350,1),(351,1),(352,1),(353,1),(354,1),(355,1),(356,1),(357,1),
	(358,1),(359,1),(360,1),(361,1),(362,1),(363,1),(364,1),(365,1),(366,1),(367,1),(368,1),(369,1),(370,1),(371,1),
	(372,1),(373,1),(374,1),(375,1),(376,1),(377,1),(378,1),(379,1),(380,1),(381,1),(382,1),(383,1),(384,1),(385,1),
	(386,1),(387,1),(388,1),(389,1),(390,1),(391,1),(392,1),(393,1),(394,1),(395,1),(396,1),(397,1),(398,1),(399,1),
	(400,1),(401,1),(402,1),(403,1),(404,1),(405,1),(406,1),(407,1),(408,1),(409,1),(410,1),(411,1),(412,1),(413,1),
	(414,1),(415,1),(416,1),(417,1),(418,1),(419,1),(420,1),(421,1),(422,1),(423,1),(424,1),(425,1),(426,1),(427,1),
	(428,1),(429,1),(430,1),(431,1),(432,1),(433,1),(434,1),(435,1),(436,1),(437,1),(438,1),(439,1),(440,1),(441,1),
	(442,1),(443,1),(444,1),(445,1),(446,1),(447,1),(448,1),(449,1),(450,1),(451,1),(452,1),(453,1),(454,1),(455,1),
	(456,1),(457,1),(458,1),(459,1),(460,1),(461,1),(462,1),(463,1),(464,1),(465,1),(466,1),(467,1),(468,1),(469,1),
	(470,1),(471,1),(472,1),(473,1),(474,1),(475,1),(476,1),(477,1),(478,1),(479,1),(480,1),(481,1),(482,1),(483,1),
	(484,1),(485,1),(486,1),(487,1),(488,1),(489,1),(490,1),(491,1),(492,1),(493,1),(494,1),(495,1),(496,1),(497,1),
	(498,1),(499,1),(500,1),(501,1),(502,1),(503,1),(504,1),(505,1),(506,1),(507,1),(508,1),(509,1),(510,1),(511,1),
	(512,1),(513,1),(514,1),(515,1),(516,1),(517,1),(518,1),(519,1),(520,1),(521,1),(522,1),(523,1),(524,1),(525,1),
	(526,1),(527,1),(528,1),(529,1),(530,1),(531,1),(532,1),(533,1),(534,1),(535,1),(536,1),(537,1),(538,1),(539,1),
	(540,1),(541,1),(542,1),(543,1),(544,1),(545,1),(546,1),(547,1),(548,1),(549,1),(550,1),(551,1),(552,1),(553,1),
	(554,1),(555,1),(556,1),(557,1),(558,1),(559,1),(560,1),(561,1),(562,1),(563,1),(564,1),(565,1),(566,1),(567,1),
	(568,1),(569,1),(570,1),(571,1),(572,1),(573,1),(574,1),(575,1),(576,1),(577,1),(578,1),(579,1),(580,1),(581,1),
	(582,1),(583,1),(584,1),(585,1),(586,1),(587,1),(588,1),(589,1),(590,1),(591,1),(592,1),(593,1),(594,1),(595,1),
	(596,1),(597,1),(598,1),(599,1),(600,1),(601,1),(602,1),(603,1),(604,1),(605,1),(606,1),(607,1),(608,1),(609,1),
	(610,1),(611,1),(612,1),(613,1),(614,1),(615,1),(616,1),(617,1),(618,1),(619,1),(620,1),(621,1),(622,1),(623,1),
	(624,1),(625,1),(626,1),(627,1),(628,1),(629,1),(630,1),(631,1),(632,1),(633,1),(634,1),(635,1),(636,1),(637,1),
	(638,1),(639,1),(640,1),(641,1),(642,1),(643,1),(644,1),(645,1),(646,1),(647,1),(648,1),(649,1),(650,1),(651,1),
	(652,1),(653,1),(654,1),(655,1),(656,1),(657,1),(658,1),(659,1),(660,1),(661,1),(662,1),(663,1),(664,1),(665,1),
	(666,1),(667,1),(668,1),(669,1),(670,1),(671,1),(672,1),(673,1),(674,1),(675,1),(676,1),(677,1),(678,1),(679,1),
	(680,1),(681,1),(682,1),(683,1),(684,1),(685,1),(686,1),(687,1),(688,1),(689,1),(690,1),(691,1),(692,1),(693,1),
	(694,1),(695,1),(696,1),(697,1),(698,1),(699,1),(700,1),(701,1),(702,1),(703,1),(704,1),(705,1),(706,1),(707,1),
	(708,1),(709,1),(710,1),(711,1),(712,1),(713,1),(714,1),(715,1),(716,1),(717,1),(718,1),(719,1),(720,1),(721,1),
	(722,1),(723,1),(724,1),(725,1),(726,1),(727,1),(728,1),(729,1),(730,1),(731,1),(732,1),(733,1),(734,1),(735,1),
	(736,1),(737,1),(738,1),(739,1),(740,1),(741,1),(742,1),(743,1),(744,1),(745,1),(746,1),(747,1),(748,1),(749,1),
	(750,1),(751,1),(752,1),(753,1),(754,1),(755,1),(756,1),(757,1),(758,1),(759,1),(760,1),(761,1),(762,1),(763,1),
	(764,1),(765,1),(766,1),(767,1),(768,1),(769,1),(770,1),(771,1),(772,1),(773,1),(774,1),(775,1),(776,1),(777,1),
	(778,1),(779,1),(780,1),(781,1),(782,1),(783,1),(784,1),(785,1),(786,1),(787,1),(788,1),(789,1),(790,1),(791,1),
	(792,1),(793,1),(794,1),(795,1),(796,1),(797,1),(798,1),(799,1),(800,1),(801,1),(802,1),(803,1),(804,1),(805,1),
	(806,1),(807,1),(808,1),(809,1),(810,1),(811,1),(812,1),(813,1),(814,1),(815,1),(816,1),(817,1),(818,1),(819,1),
	(820,1),(821,1),(822,1),(823,1),(824,1),(825,1),(826,1),(827,1),(828,1),(829,1),(830,1),(831,1),(832,1),(833,1),
	(834,1),(835,1),(836,1),(837,1),(838,1),(839,1),(840,1),(841,1),(842,1),(843,1),(844,1),(845,1),(846,1),(847,1),
	(848,1),(849,1),(850,1),(851,1),(852,1),(853,1),(854,1),(855,1),(856,1),(857,1),(858,1),(859,1),(860,1),(861,1),
	(862,1),(863,1),(864,1),(865,1),(866,1),(867,1),(868,1),(869,1),(870,1),(871,1),(872,1),(873,1),(874,1),(875,1),
	(876,1),(877,1),(878,1),(879,1),(880,1),(881,1),(882,1),(883,1),(884,1),(885,1),(886,1),(887,1),(888,1),(889,1),
	(890,1),(891,1),(892,1),(893,1),(894,1),(895,1),(896,1),(897,1),(898,1),(899,1),(900,1),(901,1),(902,1),(903,1),
	(904,1),(905,1),(906,1),(907,1),(908,1),(909,1),(910,1),(911,1),(912,1),(913,1),(914,1),(915,1),(916,1),(917,1),
	(918,1),(919,1),(920,1),(921,1),(922,1),(923,1),(924,1),(925,1),(926,1),(927,1),(928,1),(929,1),(930,1),(931,1),
	(932,1),(933,1),(934,1),(935,1),(936,1),(937,1),(938,1),(939,1),(940,1),(941,1),(942,1),(943,1),(944,1),(945,1),
	(946,1),(947,1),(948,1),(949,1),(950,1),(951,1),(952,1),(953,1),(954,1),(955,1),(956,1),(957,1),(958,1),(959,1),
	(960,1),(961,1),(962,1),(963,1),(964,1),(965,1),(966,1),(967,1),(968,1),(969,1),(970,1),(971,1),(972,1),(973,1),
	(974,1),(975,1),(976,1),(977,1),(978,1),(979,1),(980,1),(981,1),(982,1),(983,1),(984,1),(985,1),(986,1),(987,1),
	(988,1),(989,1),(990,1),(991,1),(992,1),(993,1),(994,1),(995,1),(996,1),(997,1),(998,1),(999,1),(1000,1),(1001,1),
	(1002,1),(1003,1),(1004,1),(1005,1),(1006,1),(1007,1),(1008,1),(1009,1),(1010,1),(1011,1),(1012,1),(1013,1),
	(1014,1),(1015,1),(1016,1),(1017,1),(1018,1),(1019,1),(1020,1),(1021,1),(1022,1),(1023,1),(1024,1),(1025,1),
	(1026,1),(1027,1),(1028,1),(1029,1),(1030,1),(1031,1),(1032,1),(1033,1),(1034,1),(1035,1),(1036,1),(1037,1),
	(1038,1),(1039,1),(1040,1),(1041,1),(1042,1),(1043,1),(1044,1),(1045,1),(1046,1),(1047,1),(1048,1),(1049,1),
	(1050,1),(1051,1),(1052,1),(1053,1),(1054,1),(1055,1),(1056,1),(1057,1),(1058,1),(1059,1),(1060,1),(1061,1),
	(1062,1),(1063,1),(1064,1),(1065,1),(1066,1),(1067,1),(1068,1),(1069,1),(1070,1),(1071,1),(1072,1),(1073,1),
	(1074,1),(1075,1),(1076,1),(1077,1),(1078,1),(1079,1),(1080,1),(1081,1),(1082,1),(1083,1),(1084,1),(1085,1),
	(1086,1),(1087,1),(1088,1),(1089,1),(1090,1),(1091,1),(1092,1),(1093,1),(1094,1),(1095,1),(1096,1),(1097,1),
	(1098,1),(1099,1),(1100,1),(1101,1),(1102,1),(1103,1),(1104,1),(1105,1),(1106,1),(1107,1),(1108,1),(1109,1),
	(1110,1),(1111,1),(1112,1),(1113,1),(1114,1),(1115,1),(1116,1),(1117,1),(1118,1),(1119,1),(1120,1),(1121,1),
	(1122,1),(1123,1),(1124,1),(1125,1),(1126,1),(1127,1),(1128,1),(1129,1),(1130,1),(1131,1),(1132,1),(1133,1),
	(1134,1),(1135,1),(1136,1),(1137,1),(1138,1),(1139,1),(1140,1),(1141,1),(1142,1),(1143,1),(1144,1),(1145,1),
	(1146,1),(1147,1),(1148,1),(1149,1),(1150,1),(1151,1),(1152,1),(1153,1),(1154,1),(1155,1),(1156,1),(1157,1),
	(1158,1),(1159,1),(1160,1),(1161,1),(1162,1),(1163,1),(1164,1),(1165,1),(1166,1),(1167,1),(1168,1),(1169,1),
	(1170,1),(1171,1),(1172,1),(1173,1),(1174,1),(1175,1),(1176,1),(1177,1),(1178,1),(1179,1),(1180,1),(1181,1),
	(1182,1),(1183,1),(1184,1),(1185,1),(1186,1),(1187,1),(1188,1),(1189,1),(1190,1),(1191,1),(1192,1),(1193,1),
	(1194,1),(1195,1),(1196,1),(1197,1),(1198,1),(1199,1),(1200,1);`)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "e2et1",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	defer m.Close()
	assert.Equal(t, "initial", m.status.Get().String())
	assert.Equal(t, status.Progress{CurrentState: "initial", Summary: ""}, m.GetProgress())

	// Usually we would call m.Run() but we want to step through
	// the migration process manually.
	m.startTime = time.Now()
	m.dbConfig = dbconn.NewDBConfig()
	m.db, err = dbconn.New(testutils.DSN(), m.dbConfig)
	assert.NoError(t, err)
	defer m.db.Close()
	// Get Table Info
	m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.migration.Table)
	err = m.changes[0].table.SetInfo(t.Context())
	assert.NoError(t, err)
	assert.NoError(t, m.setup(t.Context()))

	// Now we are ready to start copying rows.
	// Instead of calling m.copyRows() we will step through it manually.
	// Since we want to checkpoint after a few chunks.
	m.status.Set(status.CopyRows)
	assert.Equal(t, "copyRows", m.status.Get().String())

	// We expect 2 chunks to be copied.
	ccopier, ok := m.copier.(*copier.Unbuffered)
	assert.True(t, ok)

	// first chunk.
	chunk, err := m.copyChunker.Next()
	assert.NoError(t, err)
	assert.NotNil(t, chunk)
	assert.Equal(t, "((`id1` < 1001)\n OR (`id1` = 1001 AND `id2` < 1))", chunk.String())
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk))
	assert.Equal(t, status.Progress{CurrentState: status.CopyRows.String(), Summary: "1000/1200 83.33% copyRows ETA TBD"}, m.GetProgress())

	// Now insert some data.
	testutils.RunSQL(t, `insert into e2et1 (id1, id2) values (1002, 2)`)

	// The composite chunker does not support keyAboveHighWatermark
	// so it will show up as a delta.
	assert.NoError(t, m.replClient.BlockWait(t.Context()))
	assert.Equal(t, 1, m.replClient.GetDeltaLen())

	// Second chunk
	chunk, err = m.copyChunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "((`id1` > 1001)\n OR (`id1` = 1001 AND `id2` >= 1))", chunk.String())
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk))
	assert.Equal(t, status.Progress{CurrentState: status.CopyRows.String(), Summary: "1201/1200 100.08% copyRows ETA DUE"}, m.GetProgress())

	// Now insert some data.
	// This should be picked up by the binlog subscription
	// because it is within chunk size range of the second chunk.
	testutils.RunSQL(t, `insert into e2et1 (id1, id2) values (5, 2)`)
	assert.NoError(t, m.replClient.BlockWait(t.Context()))
	assert.Equal(t, 2, m.replClient.GetDeltaLen())

	testutils.RunSQL(t, `delete from e2et1 where id1 = 1`)
	assert.False(t, m.copyChunker.KeyAboveHighWatermark(1))
	assert.NoError(t, m.replClient.BlockWait(t.Context()))
	assert.Equal(t, 3, m.replClient.GetDeltaLen())

	// Some data is inserted later, even though the last chunk is done.
	// We still care to pick it up because it could be inserted during checkpoint.
	testutils.RunSQL(t, `insert into e2et1 (id1, id2) values (5000, 1)`)
	assert.False(t, m.copyChunker.KeyAboveHighWatermark(int64(math.MaxInt64)))

	// Now that copy rows is done, we flush the changeset until trivial.
	// and perform the optional checksum.
	assert.NoError(t, m.replClient.Flush(t.Context()))
	m.status.Set(status.ApplyChangeset)
	assert.Equal(t, "applyChangeset", m.status.Get().String())
	m.status.Set(status.Checksum)
	m.dbConfig = dbconn.NewDBConfig()
	assert.NoError(t, m.checksum(t.Context()))
	assert.Equal(t, "postChecksum", m.status.Get().String())
	assert.Equal(t, status.Progress{CurrentState: status.PostChecksum.String(), Summary: "Applying Changeset Deltas=0"}, m.GetProgress())

	// All done!
	assert.Equal(t, 0, m.db.Stats().InUse) // all connections are returned.
}

func TestE2EBinlogSubscribingNonCompositeKey(t *testing.T) {
	t.Parallel()
	tbl := `CREATE TABLE e2et2 (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		pad int NOT NULL default 0)`

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS e2et2, _e2et2_new`)
	testutils.RunSQL(t, tbl)
	testutils.RunSQL(t, `insert into e2et2 (id) values (1)`)
	testutils.RunSQL(t, `insert into e2et2 (id) values (2)`)
	testutils.RunSQL(t, `insert into e2et2 (id) values (3)`)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "e2et2",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	defer m.Close()
	assert.Equal(t, "initial", m.status.Get().String())

	// Usually we would call m.Run() but we want to step through
	// the migration process manually.
	m.dbConfig = dbconn.NewDBConfig()
	m.startTime = time.Now()
	m.db, err = dbconn.New(testutils.DSN(), m.dbConfig)
	assert.NoError(t, err)
	defer m.db.Close()
	// Get Table Info
	m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.migration.Table)
	err = m.changes[0].table.SetInfo(t.Context())
	assert.NoError(t, err)
	assert.NoError(t, m.setup(t.Context()))
	// Now we are ready to start copying rows.
	// Instead of calling m.copyRows() we will step through it manually.
	// Since we want to checkpoint after a few chunks.

	m.status.Set(status.CopyRows)
	assert.Equal(t, "copyRows", m.status.Get().String())

	// We expect 3 chunks to be copied.
	// The special first and last case and middle case.
	ccopier, ok := m.copier.(*copier.Unbuffered)
	assert.True(t, ok)

	// first chunk.
	chunk, err := m.copyChunker.Next()
	assert.NoError(t, err)
	assert.NotNil(t, chunk)
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk))
	assert.Equal(t, "`id` < 1", chunk.String())

	// Now insert some data.
	// This will be ignored by the binlog subscription.
	// Because it's ahead of the high watermark.
	testutils.RunSQL(t, `insert into e2et2 (id) values (4)`)
	assert.True(t, m.copyChunker.KeyAboveHighWatermark(4))
	assert.NoError(t, m.replClient.BlockWait(t.Context()))
	assert.Equal(t, 0, m.replClient.GetDeltaLen())

	// second chunk is between min and max value.
	// retrieve it first. We'll copy it after we insert data.
	chunk, err = m.copyChunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "`id` >= 1 AND `id` < 1001", chunk.String())

	// Now insert some data.
	// This should be picked up by the binlog subscription
	// because it is within chunk size range of the second chunk.
	// but until we copy the chunk it is *not* below the low watermark
	// and can't be flushed.
	testutils.RunSQL(t, `insert into e2et2 (id) values (5)`)
	assert.False(t, m.copyChunker.KeyAboveHighWatermark(5))
	assert.False(t, m.copyChunker.KeyBelowLowWatermark(5))
	assert.NoError(t, m.replClient.BlockWait(t.Context()))
	assert.Equal(t, 1, m.replClient.GetDeltaLen())
	assert.NoError(t, m.replClient.Flush(t.Context()))
	assert.Equal(t, 1, m.replClient.GetDeltaLen())

	// Now copy it and because the CopyChunk() calls Feedback()
	// it will mean that the low watermark is advanced and
	// we can safely flush all changes.
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk))
	assert.True(t, m.copyChunker.KeyBelowLowWatermark(5))
	assert.NoError(t, m.replClient.Flush(t.Context()))
	assert.Equal(t, 0, m.replClient.GetDeltaLen())

	// delete some data.
	testutils.RunSQL(t, `delete from e2et2 where id = 1`)
	assert.False(t, m.copyChunker.KeyAboveHighWatermark(1))
	assert.True(t, m.copyChunker.KeyBelowLowWatermark(1))
	assert.NoError(t, m.replClient.BlockWait(t.Context()))
	assert.Equal(t, 1, m.replClient.GetDeltaLen())

	// We are able to flush, but we still have deltas
	// *because* of keyBelowLowWatermark.
	assert.NoError(t, m.replClient.Flush(t.Context()))
	assert.Equal(t, 0, m.replClient.GetDeltaLen())

	// third (and last) chunk is open ended,
	// so anything after it will be picked up by the binlog.
	chunk, err = m.copyChunker.Next()
	assert.NoError(t, err)
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk))
	assert.Equal(t, "`id` >= 1001", chunk.String())

	// Some data is inserted later, even though the last chunk is done.
	// We still care to pick it up because it could be inserted during checkpoint.
	testutils.RunSQL(t, `insert into e2et2 (id) values (6)`)
	// the pointer should be at maxint64 for safety. this ensures
	// that any keyAboveHighWatermark checks return false
	assert.False(t, m.copyChunker.KeyAboveHighWatermark(int64(math.MaxInt64)))

	// Now that copy rows is done, we flush the changeset until trivial.
	// and perform the optional checksum.
	assert.NoError(t, m.replClient.Flush(t.Context()))
	m.status.Set(status.ApplyChangeset)
	assert.Equal(t, "applyChangeset", m.status.Get().String())
	m.status.Set(status.Checksum)
	m.dbConfig = dbconn.NewDBConfig()
	assert.NoError(t, m.checksum(t.Context()))
	assert.Equal(t, "postChecksum", m.status.Get().String())
	// All done!
}

// TestForRemainingTableArtifacts tests that the table is left after
// the migration is complete, but no _chkpnt or _new or _old table.
func TestForRemainingTableArtifacts(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS remainingtbl, _remainingtbl_new, _remainingtbl_old, _remainingtbl_chkpnt`)
	table := `CREATE TABLE remainingtbl (
		id INT NOT NULL PRIMARY KEY,
		name varchar(255) NOT NULL
	)`
	testutils.RunSQL(t, table)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "remainingtbl",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)                // everything is specified.
	assert.NoError(t, m.Run(t.Context())) // it's an accepted type.
	assert.NoError(t, m.Close())

	// Now we should have a _remainingtbl_old table and a remainingtbl table
	// but no _remainingtbl_new table or _remainingtbl_chkpnt table.
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()
	stmt := `SELECT GROUP_CONCAT(table_name) FROM information_schema.tables where table_schema='test' and table_name LIKE '%remainingtbl%' ORDER BY table_name;`
	var tables string
	assert.NoError(t, db.QueryRow(stmt).Scan(&tables))
	assert.Equal(t, "remainingtbl", tables)
}

func TestDropColumn(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS dropcol, _dropcol_new`)
	table := `CREATE TABLE dropcol (
		id int(11) NOT NULL AUTO_INCREMENT,
		a varchar(255) NOT NULL,
		b varchar(255) NOT NULL,
		c varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `insert into dropcol (id, a,b,c) values (1, 'a', 'b', 'c')`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "dropcol",
		Alter:    "DROP COLUMN b, ENGINE=InnoDB", // need both to ensure it is not instant!
	})
	assert.NoError(t, err)
	assert.NoError(t, m.Run(t.Context()))

	assert.False(t, m.usedInstantDDL) // need to ensure it uses full process.
	assert.NoError(t, m.Close())
}

func TestDefaultPort(t *testing.T) {
	t.Parallel()
	m, err := NewRunner(&Migration{
		Host:     "localhost",
		Username: "root",
		Password: "mypassword",
		Database: "test",
		Threads:  2,
		Table:    "t1",
		Alter:    "DROP COLUMN b, ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	assert.Equal(t, "localhost:3306", m.migration.Host)
	m.SetLogger(logrus.New())
}

func TestNullToNotNull(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS autodatetime`)
	table := `CREATE TABLE autodatetime (
		id INT NOT NULL AUTO_INCREMENT,
		created_at DATETIME(3) NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `INSERT INTO autodatetime (created_at) VALUES (NULL)`)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "autodatetime",
		Alter:    "modify column created_at datetime(3) not null default current_timestamp(3)",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "Column 'created_at' cannot be null")
	assert.NoError(t, m.Close())
}

func TestChunkerPrefetching(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS prefetchtest`)
	table := `CREATE TABLE prefetchtest (
		id BIGINT NOT NULL AUTO_INCREMENT,
		created_at DATETIME(3) NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	// insert about 11K rows.
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) VALUES (NULL)`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b JOIN prefetchtest c`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b JOIN prefetchtest c`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b JOIN prefetchtest c`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b LIMIT 10000`)

	// the max id should be able 11040
	// lets insert one far off ID: 300B
	// and then continue inserting at greater than the max dynamic chunk size.
	testutils.RunSQL(t, `INSERT INTO prefetchtest (id, created_at) VALUES (300000000000, NULL)`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b LIMIT 300000`)

	// and then another big gap
	// and then continue inserting at greater than the max dynamic chunk size.
	testutils.RunSQL(t, `INSERT INTO prefetchtest (id, created_at) VALUES (600000000000, NULL)`)
	testutils.RunSQL(t, `INSERT INTO prefetchtest (created_at) SELECT NULL FROM prefetchtest a JOIN prefetchtest b LIMIT 300000`)
	// and then one final value which is way out there.
	testutils.RunSQL(t, `INSERT INTO prefetchtest (id, created_at) VALUES (900000000000, NULL)`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "prefetchtest",
		Alter:    "engine=innodb",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.NoError(t, m.Close())
}

func TestTpConversion(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS tpconvert")
	testutils.RunSQL(t, `CREATE TABLE tpconvert (
	id bigint NOT NULL AUTO_INCREMENT primary key,
	created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	issued_at timestamp NULL DEFAULT NULL,
	activated_at timestamp NULL DEFAULT NULL,
	deactivated_at timestamp NULL DEFAULT NULL,
	intasstring varchar(255) NULL DEFAULT NULL,
	floatcol FLOAT NULL DEFAULT NULL
	)`)
	testutils.RunSQL(t, `INSERT INTO tpconvert (created_at, updated_at, issued_at, activated_at, deactivated_at, intasstring, floatcol) VALUES
	('2023-05-18 09:28:46', '2023-05-18 09:33:27', '2023-05-18 09:28:45', '2023-05-18 09:28:45', NULL, '0001', 9.3),
	('2023-05-18 09:34:38', '2023-05-24 07:38:25', '2023-05-18 09:34:37', '2023-05-18 09:34:37', '2023-05-24 07:38:25', '10', 9.3),
	('2023-05-24 07:34:36', '2023-05-24 07:34:36', '2023-05-24 07:34:35', NULL, null, '01234', 9.3),
	('2023-05-24 07:41:05', '2023-05-25 06:15:37', '2023-05-24 07:41:04', '2023-05-24 07:41:04', '2023-05-25 06:15:37', '10', 2.2),
	('2023-05-25 06:17:30', '2023-05-25 06:17:30', '2023-05-25 06:17:29', '2023-05-25 06:17:29', NULL, '10', 9.3),
	('2023-05-25 06:18:33', '2023-05-25 06:41:13', '2023-05-25 06:18:32', '2023-05-25 06:18:32', '2023-05-25 06:41:13', '10', 1.1),
	('2023-05-25 06:24:23', '2023-05-25 06:24:23', '2023-05-25 06:24:22', NULL, null, '10', 9.3),
	('2023-05-25 06:41:35', '2023-05-28 23:45:09', '2023-05-25 06:41:34', '2023-05-25 06:41:34', '2023-05-28 23:45:09', '10', 9.3),
	('2023-05-25 06:44:41', '2023-05-28 23:45:03', '2023-05-25 06:44:40', '2023-05-25 06:46:48', '2023-05-28 23:45:03', '10', 9.3),
	('2023-05-26 06:24:24', '2023-05-28 23:45:01', '2023-05-26 06:24:23', '2023-05-26 06:24:42', '2023-05-28 23:45:01', '10', 9.3),
	('2023-05-28 23:46:07', '2023-05-29 00:57:55', '2023-05-28 23:46:05', '2023-05-28 23:46:05', NULL, '10', 9.3),
	('2023-05-28 23:53:34', '2023-05-29 00:57:56', '2023-05-28 23:53:33', '2023-05-28 23:58:09', NULL, '10', 9.3);`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "tpconvert",
		Alter: `MODIFY COLUMN created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
		MODIFY COLUMN updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
		MODIFY COLUMN issued_at TIMESTAMP(6) NULL,
		MODIFY COLUMN activated_at TIMESTAMP(6) NULL,
		MODIFY COLUMN deactivated_at TIMESTAMP(6) NULL,
		MODIFY COLUMN intasstring INT NULL DEFAULT NULL
		`,
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.NoError(t, m.Close())
}

func TestResumeFromCheckpointE2E(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS chkpresumetest, _chkpresumetest_old, _chkpresumetest_chkpnt`)
	table := `CREATE TABLE chkpresumetest (
		id int(11) NOT NULL AUTO_INCREMENT,
		pad varbinary(1024) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	// Insert dummy data.
	testutils.RunSQL(t, "INSERT INTO chkpresumetest (pad) SELECT RANDOM_BYTES(1024) FROM dual")
	testutils.RunSQL(t, "INSERT INTO chkpresumetest (pad) SELECT RANDOM_BYTES(1024) FROM chkpresumetest a, chkpresumetest b, chkpresumetest c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO chkpresumetest (pad) SELECT RANDOM_BYTES(1024) FROM chkpresumetest a, chkpresumetest b, chkpresumetest c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO chkpresumetest (pad) SELECT RANDOM_BYTES(1024) FROM chkpresumetest a, chkpresumetest b, chkpresumetest c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO chkpresumetest (pad) SELECT RANDOM_BYTES(1024) FROM chkpresumetest a, chkpresumetest b, chkpresumetest c LIMIT 100000")
	alterSQL := "ADD INDEX(pad);"
	// use as slow as possible here: we want the copy to be still running
	// when we kill it once we have a checkpoint saved.
	migration.Host = cfg.Addr
	migration.Username = cfg.User
	migration.Password = cfg.Passwd
	migration.Database = cfg.DBName
	migration.Threads = 1
	migration.Table = "chkpresumetest"
	migration.Alter = alterSQL
	migration.TargetChunkTime = 100 * time.Millisecond

	runner, err := NewRunner(migration)
	assert.NoError(t, err)

	go func() {
		err := runner.Run(t.Context())
		assert.Error(t, err) // it gets interrupted as soon as there is a checkpoint saved.
	}()

	// wait until a checkpoint is saved (which means copy is in progress)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()
	for {
		var rowCount int
		err = db.QueryRow(`SELECT count(*) from _chkpresumetest_chkpnt`).Scan(&rowCount)
		if err != nil {
			continue // table does not exist yet
		}
		if rowCount > 0 {
			break
		}
	}
	// Between cancelFunc() and Close() every resource is freed.
	runner.cancelFunc()
	assert.NoError(t, runner.Close())

	// Insert some more dummy data
	testutils.RunSQL(t, "INSERT INTO chkpresumetest (pad) SELECT RANDOM_BYTES(1024) FROM chkpresumetest LIMIT 1000")
	// Start a new migration with the same parameters.
	// Let it complete.
	newmigration := &Migration{}
	newmigration.Host = cfg.Addr
	newmigration.Username = cfg.User
	newmigration.Password = cfg.Passwd
	newmigration.Database = cfg.DBName
	newmigration.Threads = 4
	newmigration.Table = "chkpresumetest"
	newmigration.Alter = alterSQL
	newmigration.TargetChunkTime = 5 * time.Second

	m, err := NewRunner(newmigration)
	assert.NoError(t, err)
	assert.NotNil(t, m)

	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.True(t, m.usedResumeFromCheckpoint)
	assert.NoError(t, m.Close())
}

func TestResumeFromCheckpointE2ECompositeVarcharPK(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS compositevarcharpk, _compositevarcharpk_chkpnt`)
	testutils.RunSQL(t, `CREATE TABLE compositevarcharpk (
  token varchar(128) NOT NULL,
  version varchar(255) NOT NULL,
  state varchar(255) NOT NULL,
  source varchar(128) NOT NULL,
  created_at datetime(3) NOT NULL,
  updated_at datetime(3) NOT NULL,
  PRIMARY KEY (token,version)
	);`)
	testutils.RunSQL(t, `INSERT INTO compositevarcharpk VALUES
 (HEX(RANDOM_BYTES(60)), '1', 'active', 'test', NOW(3), NOW(3))`)
	testutils.RunSQL(t, `INSERT INTO compositevarcharpk SELECT
 HEX(RANDOM_BYTES(60)), '1', 'active', 'test', NOW(3), NOW(3)
FROM compositevarcharpk a JOIN compositevarcharpk b JOIN compositevarcharpk c LIMIT 10000`)
	testutils.RunSQL(t, `INSERT INTO compositevarcharpk SELECT
 HEX(RANDOM_BYTES(60)), '1', 'active', 'test', NOW(3), NOW(3)
FROM compositevarcharpk a JOIN compositevarcharpk b JOIN compositevarcharpk c LIMIT 10000`)
	testutils.RunSQL(t, `INSERT INTO compositevarcharpk SELECT
 HEX(RANDOM_BYTES(60)), '1', 'active', 'test', NOW(3), NOW(3)
FROM compositevarcharpk a JOIN compositevarcharpk b JOIN compositevarcharpk c LIMIT 10000`)
	testutils.RunSQL(t, `INSERT INTO compositevarcharpk SELECT
 HEX(RANDOM_BYTES(60)), '1', 'active', 'test', NOW(3), NOW(3)
FROM compositevarcharpk a JOIN compositevarcharpk b JOIN compositevarcharpk c LIMIT 10000`)
	testutils.RunSQL(t, `INSERT INTO compositevarcharpk SELECT
 a.token, '2', 'active', 'test', NOW(3), NOW(3)
FROM compositevarcharpk a WHERE version='1'`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	migration := &Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         1,
		Table:           "compositevarcharpk",
		Alter:           "ENGINE=InnoDB",
		TargetChunkTime: 100 * time.Millisecond,
	}
	runner, err := NewRunner(migration)
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		err := runner.Run(ctx)
		assert.Error(t, err) // it gets interrupted as soon as there is a checkpoint saved.
	}()

	// wait until a checkpoint is saved (which means copy is in progress)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()
	for {
		var rowCount int
		err = db.QueryRow(`SELECT count(*) from _compositevarcharpk_chkpnt`).Scan(&rowCount)
		if err != nil {
			continue // table does not exist yet
		}
		if rowCount > 0 {
			break
		}
	}
	// Between cancel and Close() every resource is freed.
	cancel()
	assert.NoError(t, runner.Close())

	newmigration := &Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         2,
		Table:           "compositevarcharpk",
		Alter:           "ENGINE=InnoDB",
		TargetChunkTime: 5 * time.Second,
	}
	m2, err := NewRunner(newmigration)
	assert.NoError(t, err)
	assert.NotNil(t, m2)

	err = m2.Run(t.Context())
	assert.NoError(t, err)
	assert.True(t, m2.usedResumeFromCheckpoint)
	assert.NoError(t, m2.Close())
}

func TestResumeFromCheckpointStrict(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS resumestricttest, _resumestricttest_old, _resumestricttest_chkpnt`)
	table := `CREATE TABLE resumestricttest (
		id int(11) NOT NULL AUTO_INCREMENT,
		pad varbinary(1024) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)

	// Insert dummy data.
	testutils.RunSQL(t, "INSERT INTO resumestricttest (pad) SELECT RANDOM_BYTES(1024) FROM dual")
	testutils.RunSQL(t, "INSERT INTO resumestricttest (pad) SELECT RANDOM_BYTES(1024) FROM resumestricttest a, resumestricttest b, resumestricttest c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO resumestricttest (pad) SELECT RANDOM_BYTES(1024) FROM resumestricttest a, resumestricttest b, resumestricttest c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO resumestricttest (pad) SELECT RANDOM_BYTES(1024) FROM resumestricttest a, resumestricttest b, resumestricttest c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO resumestricttest (pad) SELECT RANDOM_BYTES(1024) FROM resumestricttest a, resumestricttest b, resumestricttest c LIMIT 100000")

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	alterSQL := "ADD INDEX(pad);"

	migration := &Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         1,
		Table:           "resumestricttest",
		Alter:           alterSQL,
		TargetChunkTime: 100 * time.Millisecond,
		Strict:          true,
	}

	// Kick off a migration with --strict enabled and let it run until the first checkpoint is available

	ctx, cancel := context.WithCancel(t.Context())

	runner, err := NewRunner(migration)
	assert.NoError(t, err)

	go func() {
		err := runner.Run(ctx)
		assert.Error(t, err) // it gets interrupted as soon as there is a checkpoint saved.
	}()

	// wait until a checkpoint is saved (which means copy is in progress)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()
	for {
		var rowCount int
		err = db.QueryRow(`SELECT count(*) from _resumestricttest_chkpnt`).Scan(&rowCount)
		if err != nil {
			continue // table does not exist yet
		}
		if rowCount > 0 {
			break
		}
	}
	// Between cancel and Close() every resource is freed.
	cancel()
	assert.NoError(t, runner.Close())

	// Insert some more dummy data
	testutils.RunSQL(t, "INSERT INTO resumestricttest (pad) SELECT RANDOM_BYTES(1024) FROM resumestricttest LIMIT 1000")

	// Start a _different_ migration on the same table. We don't expect this to work when --strict is enabled
	// since the --alter doesn't match what is recorded in the checkpoint table

	migrationB := &Migration{
		Host:            migration.Host,
		Username:        migration.Username,
		Password:        migration.Password,
		Database:        migration.Database,
		Threads:         migration.Threads,
		Table:           migration.Table,
		Alter:           "ENGINE=INNODB",
		TargetChunkTime: migration.TargetChunkTime,
		Strict:          migration.Strict,
	}

	runner, err = NewRunner(migrationB)
	assert.NoError(t, err)

	err = runner.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorIs(t, err, status.ErrMismatchedAlter)

	assert.NoError(t, runner.Close())

	// We should be able to force the migration to run even though there's a mismatched --alter
	// by disabling --strict

	migrationB.Strict = false
	migrationB.Threads = 4    // to make the test run faster
	migrationB.Statement = "" // reset for validation

	runner, err = NewRunner(migrationB)
	assert.NoError(t, err)

	err = runner.Run(t.Context())
	assert.NoError(t, err)
	assert.False(t, runner.usedResumeFromCheckpoint)

	assert.NoError(t, runner.Close())
}

// TestE2ERogueValues tests that PRIMARY KEY
// values that contain single quotes are escaped correctly
// by the repl feed applier, the copier and checksum.
func TestE2ERogueValues(t *testing.T) {
	// table cointains a reserved word too.
	tbl := "CREATE TABLE e2erogue ( `datetime` varbinary(40) NOT NULL,`col2` int NOT NULL  default 0, primary key (`datetime`, `col2`))"
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS e2erogue, _e2erogue_new`)
	testutils.RunSQL(t, tbl)
	testutils.RunSQL(t, `insert into e2erogue values ("1 \". ",1),("2 \". ",1),("3 \". ",1),("4 \". ",1),("5 \". ",1),("6 \". ",1),("7 \". ",1),("8 \". ",1),("9 \". ",1),("10 \". ",1),("11 \". ",1),("12 \". ",1),("13 \". ",1),("14 \". ",1),("15 \". ",1),("16 \". ",1),
	("17 \". ",1),("18 \". ",1),("19 \". ",1),("'20 \". ",1),("21 \". ",1),("22 \". ",1),("23 \". ",1),("24 \". ",1),("25 \". ",1),("26 \". ",1),("27 \". ",1),("28 \". ",1),("29 \". ",1),("30 \". ",1),("31 \". ",1),
	("32 \". ",1),("33 \". ",1),("34 \". ",1),("35 \". ",1),("3'6 \". ",1),("37 \". ",1),("38 \". ",1),("39 \". ",1),("40 \". ",1),("41 \". ",1),("42 \". ",1),("43 \". ",1),("44 \". ",1),("45 \". ",1),("46 \". ",1),
	("47 \". ",1),("48 \". ",1),("49 \". ",1),("50 \". ",1),("51 \". ",1),("52 \". ",1),("53 \". ",1),("54 \". ",1),("55 \". ",1),("56 \". ",1),("57 \". ",1),("58 \". ",1),("59 \". ",1),("60 \". ",1),("61 \". ",1),
	("62 \". ",1),("63 \". ",1),("64 \". ",1),("65 \". ",1),("66 \". ",1),("67 \". ",1),("68 \". ",1),("69 \". ",1),("70 \". ",1),("71 \". ",1),("72 \". ",1),("73 \". ",1),("74 \". ",1),("75 \". ",1),("76 \". ",1),
	("77 \". ",1),("78 \". ",1),("79 \". ",1),("80 \". ",1),("81 \". ",1),("82 \". ",1),("83 \". ",1),("84 \". ",1),("85 \". ",1),("86 \". ",1),("87 \". ",1),("88 \". ",1),("89 \". ",1),("90 \". ",1),("91 \". ",1),
	("92 \". ",1),("93 \". ",1),("94 \". ",1),("95 \". ",1),("96 \". ",1),("97 \". ",1),("98 \". ",1),("99 \". ",1),("100 \". ",1),("10\"1 \". ",1),("102 \". ",1),("103 \". ",1),("104 \". ",1),("105 \". ",1),
	("106 \". ",1),("107 \". ",1),("108 \". ",1),("109 \". ",1),("110 \". ",1),("111 \". ",1),("112 \". ",1),("113 \". ",1),("114 \". ",1),("115 \". ",1),("116 \". ",1),("117 \". ",1),("118 \". ",1),
	("119 \". ",1),("120 \". ",1),("121 \". ",1),("122 \". ",1),("123 \". ",1),("124 \". ",1),("125 \". ",1),("126 \". ",1),("127 \". ",1),("128 \". ",1),("129 \". ",1),("130 \". ",1),("131 \". ",1),("132 \". ",1),
	("133 \". ",1),("134 \". ",1),("135 \". ",1),("136 \". ",1),("137 \". ",1),("138 \". ",1),("139 \". ",1),("140 \". ",1),("141 \". ",1),("142 \". ",1),("143 \". ",1),("144 \". ",1),("145 \". ",1),("146 \". ",1),
	("147 \". ",1),("148 \". ",1),("149 \". ",1),("150 \". ",1),("151 \". ",1),("152 \". ",1),("153 \". ",1),("154 \". ",1),("155 \". ",1),("156 \". ",1),("157 \". ",1),("158 \". ",1),("159 \". ",1),("160 \". ",1),
	("161 \". ",1),("162 \". ",1),("163 \". ",1),("164 \". ",1),("165 \". ",1),("166 \". ",1),("167 \". ",1),("168 \". ",1),("169 \". ",1),("170 \". ",1),("171 \". ",1),("172 \". ",1),("173 \". ",1),("174 \". ",1),
	("175 \". ",1),("176 \". ",1),("177 \". ",1),("178 \". ",1),("179 \". ",1),("180 \". ",1),("181 \". ",1),("182 \". ",1),("183 \". ",1),("184 \". ",1),("185 \". ",1),("186 \". ",1),("187 \". ",1),("188 \". ",1),
	("189 \". ",1),("190 \". ",1),("191 \". ",1),("192 \". ",1),("193 \". ",1),("194 \". ",1),("195 \". ",1),("196 \". ",1),("197 \". ",1),("198 \". ",1),("199 \". ",1),("200 \". ",1),("201 \". ",1),("202 \". ",1),
	("203 \". ",1),("204 \". ",1),("205 \". ",1),("206 \". ",1),("207 \". ",1),("208 \". ",1),("209 \". ",1),("210 \". ",1),("211 \". ",1),("212 \". ",1),("213 \". ",1),("214 \". ",1),("215 \". ",1),("216 \". ",1),
	("217 \". ",1),("218 \". ",1),("219 \". ",1),("220 \". ",1),("221 \". ",1),("222 \". ",1),("223 \". ",1),("224 \". ",1),("225 \". ",1),("226 \". ",1),("227 \". ",1),("228 \". ",1),("229 \". ",1),("230 \". ",1),
	("231 \". ",1),("232 \". ",1),("233 \". ",1),("234 \". ",1),("235 \". ",1),("236 \". ",1),("237 \". ",1),("238 \". ",1),("239 \". ",1),("240 \". ",1),("241 \". ",1),("242 \". ",1),("243 \". ",1),("244 \". ",1),
	("245 \". ",1),("246 \". ",1),("247 \". ",1),("248 \". ",1),("249 \". ",1),("250 \". ",1),("251 \". ",1),("252 \". ",1),("253 \". ",1),("254 \". ",1),("255 \". ",1),("256 \". ",1),("257 \". ",1),("258 \". ",1),
	("259 \". ",1),("260 \". ",1),("261 \". ",1),("262 \". ",1),("263 \". ",1),("264 \". ",1),("265 \". ",1),("266 \". ",1),("267 \". ",1),("268 \". ",1),("269 \". ",1),("270 \". ",1),("271 \". ",1),("272 \". ",1),
	("273 \". ",1),("274 \". ",1),("275 \". ",1),("276 \". ",1),("277 \". ",1),("278 \". ",1),("279 \". ",1),("280 \". ",1),("281 \". ",1),("282 \". ",1),("283 \". ",1),("284 \". ",1),("285 \". ",1),("286 \". ",1),
	("287 \". ",1),("288 \". ",1),("289 \". ",1),("290 \". ",1),("291 \". ",1),("292 \". ",1),("293 \". ",1),("294 \". ",1),("295 \". ",1),("296 \". ",1),("297 \". ",1),("298 \". ",1),("299 \". ",1),("300 \". ",1),
	("301 \". ",1),("302 \". ",1),("303 \". ",1),("304 \". ",1),("305 \". ",1),("306 \". ",1),("307 \". ",1),("308 \". ",1),("309 \". ",1),("310 \". ",1),("311 \". ",1),("312 \". ",1),("313 \". ",1),("314 \". ",1),
	("315 \". ",1),("316 \". ",1),("317 \". ",1),("318 \". ",1),("319 \". ",1),("320 \". ",1),("321 \". ",1),("322 \". ",1),("323 \". ",1),("324 \". ",1),("325 \". ",1),("326 \". ",1),("327 \". ",1),("328 \". ",1),
	("329 \". ",1),("330 \". ",1),("331 \". ",1),("332 \". ",1),("333 \". ",1),("334 \". ",1),("335 \". ",1),("336 \". ",1),("337 \". ",1),("338 \". ",1),("339 \". ",1),("340 \". ",1),("341 \". ",1),("342 \". ",1),
	("343 \". ",1),("344 \". ",1),("345 \". ",1),("346 \". ",1),("347 \". ",1),("348 \". ",1),("349 \". ",1),("350 \". ",1),("351 \". ",1),("352 \". ",1),("353 \". ",1),("354 \". ",1),("355 \". ",1),("356 \". ",1),
	("357 \". ",1),("358 \". ",1),("359 \". ",1),("360 \". ",1),("361 \". ",1),("362 \". ",1),("363 \". ",1),("364 \". ",1),("365 \". ",1),("366 \". ",1),("367 \". ",1),("368 \". ",1),("369 \". ",1),("370 \". ",1),
	("371 \". ",1),("372 \". ",1),("373 \". ",1),("374 \". ",1),("375 \". ",1),("376 \". ",1),("377 \". ",1),("378 \". ",1),("379 \". ",1),("380 \". ",1),("381 \". ",1),("382 \". ",1),("383 \". ",1),("384 \". ",1),
	("385 \". ",1),("386 \". ",1),("387 \". ",1),("388 \". ",1),("389 \". ",1),("390 \". ",1),("391 \". ",1),("392 \". ",1),("393 \". ",1),("394 \". ",1),("395 \". ",1),("396 \". ",1),("397 \". ",1),("398 \". ",1),
	("399 \". ",1),("400 \". ",1),("401 \". ",1),("402 \". ",1),("403 \". ",1),("404 \". ",1),("405 \". ",1),("406 \". ",1),("407 \". ",1),("408 \". ",1),("409 \". ",1),("410 \". ",1),("411 \". ",1),("412 \". ",1),
	("413 \". ",1),("414 \". ",1),("415 \". ",1),("416 \". ",1),("417 \". ",1),("418 \". ",1),("419 \". ",1),("420 \". ",1),("421 \". ",1),("422 \". ",1),("423 \". ",1),("424 \". ",1),("425 \". ",1),("426 \". ",1),
	("427 \". ",1),("428 \". ",1),("429 \". ",1),("430 \". ",1),("431 \". ",1),("432 \". ",1),("433 \". ",1),("434 \". ",1),("435 \". ",1),("436 \". ",1),("437 \". ",1),("438 \". ",1),("439 \". ",1),("440 \". ",1),
	("441 \". ",1),("442 \". ",1),("443 \". ",1),("444 \". ",1),("445 \". ",1),("446 \". ",1),("447 \". ",1),("448 \". ",1),("449 \". ",1),("450 \". ",1),("451 \". ",1),("452 \". ",1),("453 \". ",1),("454 \". ",1),
	("455 \". ",1),("456 \". ",1),("457 \". ",1),("458 \". ",1),("459 \". ",1),("460 \". ",1),("461 \". ",1),("462 \". ",1),("463 \". ",1),("464 \". ",1),("465 \". ",1),("466 \". ",1),("467 \". ",1),("468 \". ",1),
	("469 \". ",1),("470 \". ",1),("471 \". ",1),("472 \". ",1),("473 \". ",1),("474 \". ",1),("475 \". ",1),("476 \". ",1),("477 \". ",1),("478 \". ",1),("479 \". ",1),("480 \". ",1),("481 \". ",1),("482 \". ",1),
	("483 \". ",1),("484 \". ",1),("485 \". ",1),("486 \". ",1),("487 \". ",1),("488 \". ",1),("489 \". ",1),("490 \". ",1),("491 \". ",1),("492 \". ",1),("493 \". ",1),("494 \". ",1),("495 \". ",1),("496 \". ",1),
	("497 \". ",1),("498 \". ",1),("499 \". ",1),("500 \". ",1),("501 \". ",1),("502 \". ",1),("503 \". ",1),("504 \". ",1),("505 \". ",1),("506 \". ",1),("507 \". ",1),("508 \". ",1),("509 \". ",1),("510 \". ",1),
	("511 \". ",1),("512 \". ",1),("513 \". ",1),("514 \". ",1),("515 \". ",1),("516 \". ",1),("517 \". ",1),("518 \". ",1),("519 \". ",1),("520 \". ",1),("521 \". ",1),("522 \". ",1),("523 \". ",1),("524 \". ",1),
	("525 \". ",1),("526 \". ",1),("527 \". ",1),("528 \". ",1),("529 \". ",1),("530 \". ",1),("531 \". ",1),("532 \". ",1),("533 \". ",1),("534 \". ",1),("535 \". ",1),("536 \". ",1),("537 \". ",1),("538 \". ",1),
	("539 \". ",1),("540 \". ",1),("541 \". ",1),("542 \". ",1),("543 \". ",1),("544 \". ",1),("545 \". ",1),("546 \". ",1),("547 \". ",1),("548 \". ",1),("549 \". ",1),("550 \". ",1),("551 \". ",1),("552 \". ",1),
	("553 \". ",1),("554 \". ",1),("555 \". ",1),("556 \". ",1),("557 \". ",1),("558 \". ",1),("559 \". ",1),("560 \". ",1),("561 \". ",1),("562 \". ",1),("563 \". ",1),("564 \". ",1),("565 \". ",1),("566 \". ",1),
	("567 \". ",1),("568 \". ",1),("569 \". ",1),("570 \". ",1),("571 \". ",1),("572 \". ",1),("573 \". ",1),("574 \". ",1),("575 \". ",1),("576 \". ",1),("577 \". ",1),("578 \". ",1),("579 \". ",1),("580 \". ",1),
	("581 \". ",1),("582 \". ",1),("583 \". ",1),("584 \". ",1),("585 \". ",1),("586 \". ",1),("587 \". ",1),("588 \". ",1),("589 \". ",1),("590 \". ",1),("591 \". ",1),("592 \". ",1),("593 \". ",1),("594 \". ",1),
	("595 \". ",1),("596 \". ",1),("597 \". ",1),("598 \". ",1),("599 \". ",1),("600 \". ",1),("601 \". ",1),("602 \". ",1),("603 \". ",1),("604 \". ",1),("605 \". ",1),("606 \". ",1),("607 \". ",1),("608 \". ",1),
	("609 \". ",1),("610 \". ",1),("611 \". ",1),("612 \". ",1),("613 \". ",1),("614 \". ",1),("615 \". ",1),("616 \". ",1),("617 \". ",1),("618 \". ",1),("619 \". ",1),("620 \". ",1),("621 \". ",1),("622 \". ",1),
	("623 \". ",1),("624 \". ",1),("625 \". ",1),("626 \". ",1),("627 \". ",1),("628 \". ",1),("629 \". ",1),("630 \". ",1),("631 \". ",1),("632 \". ",1),("633 \". ",1),("634 \". ",1),("635 \". ",1),("636 \". ",1),
	("637 \". ",1),("638 \". ",1),("639 \". ",1),("640 \". ",1),("641 \". ",1),("642 \". ",1),("643 \". ",1),("644 \". ",1),("645 \". ",1),("646 \". ",1),("647 \". ",1),("648 \". ",1),("649 \". ",1),("650 \". ",1),
	("651 \". ",1),("652 \". ",1),("653 \". ",1),("654 \". ",1),("655 \". ",1),("656 \". ",1),("657 \". ",1),("658 \". ",1),("659 \". ",1),("660 \". ",1),("661 \". ",1),("662 \". ",1),("663 \". ",1),("664 \". ",1),
	("665 \". ",1),("666 \". ",1),("667 \". ",1),("668 \". ",1),("669 \". ",1),("670 \". ",1),("671 \". ",1),("672 \". ",1),("673 \". ",1),("674 \". ",1),("675 \". ",1),("676 \". ",1),("677 \". ",1),("678 \". ",1),
	("679 \". ",1),("680 \". ",1),("681 \". ",1),("682 \". ",1),("683 \". ",1),("684 \". ",1),("685 \". ",1),("686 \". ",1),("687 \". ",1),("688 \". ",1),("689 \". ",1),("690 \". ",1),("691 \". ",1),("692 \". ",1),
	("693 \". ",1),("694 \". ",1),("695 \". ",1),("696 \". ",1),("697 \". ",1),("698 \". ",1),("699 \". ",1),("700 \". ",1),("701 \". ",1),("702 \". ",1),("703 \". ",1),("704 \". ",1),("705 \". ",1),("706 \". ",1),
	("707 \". ",1),("708 \". ",1),("709 \". ",1),("710 \". ",1),("711 \". ",1),("712 \". ",1),("713 \". ",1),("714 \". ",1),("715 \". ",1),("716 \". ",1),("717 \". ",1),("718 \". ",1),("719 \". ",1),("720 \". ",1),
	("721 \". ",1),("722 \". ",1),("723 \". ",1),("724 \". ",1),("725 \". ",1),("726 \". ",1),("727 \". ",1),("728 \". ",1),("729 \". ",1),("730 \". ",1),("731 \". ",1),("732 \". ",1),("733 \". ",1),("734 \". ",1),
	("735 \". ",1),("736 \". ",1),("737 \". ",1),("738 \". ",1),("739 \". ",1),("740 \". ",1),("741 \". ",1),("742 \". ",1),("743 \". ",1),("744 \". ",1),("745 \". ",1),("746 \". ",1),("747 \". ",1),("748 \". ",1),
	("749 \". ",1),("750 \". ",1),("751 \". ",1),("752 \". ",1),("753 \". ",1),("754 \". ",1),("755 \". ",1),("756 \". ",1),("757 \". ",1),("758 \". ",1),("759 \". ",1),("760 \". ",1),("761 \". ",1),("762 \". ",1),
	("763 \". ",1),("764 \". ",1),("765 \". ",1),("766 \". ",1),("767 \". ",1),("768 \". ",1),("769 \". ",1),("770 \". ",1),("771 \". ",1),("772 \". ",1),("773 \". ",1),("774 \". ",1),("775 \". ",1),("776 \". ",1),
	("777 \". ",1),("778 \". ",1),("779 \". ",1),("780 \". ",1),("781 \". ",1),("782 \". ",1),("783 \". ",1),("784 \". ",1),("785 \". ",1),("786 \". ",1),("787 \". ",1),("788 \". ",1),("789 \". ",1),("790 \". ",1),
	("791 \". ",1),("792 \". ",1),("793 \". ",1),("794 \". ",1),("795 \". ",1),("796 \". ",1),("797 \". ",1),("798 \". ",1),("799 \". ",1),("800 \". ",1),("801 \". ",1),("802 \". ",1),("803 \". ",1),("804 \". ",1),
	("805 \". ",1),("806 \". ",1),("807 \". ",1),("808 \". ",1),("809 \". ",1),("810 \". ",1),("811 \". ",1),("812 \". ",1),("813 \". ",1),("814 \". ",1),("815 \". ",1),("816 \". ",1),("817 \". ",1),("818 \". ",1),
	("819 \". ",1),("820 \". ",1),("821 \". ",1),("822 \". ",1),("823 \". ",1),("824 \". ",1),("825 \". ",1),("826 \". ",1),("827 \". ",1),("828 \". ",1),("829 \". ",1),("830 \". ",1),("831 \". ",1),("832 \". ",1),
	("833 \". ",1),("834 \". ",1),("835 \". ",1),("836 \". ",1),("837 \". ",1),("838 \". ",1),("839 \". ",1),("840 \". ",1),("841 \". ",1),("842 \". ",1),("843 \". ",1),("844 \". ",1),("845 \". ",1),("846 \". ",1),
	("847 \". ",1),("848 \". ",1),("849 \". ",1),("850 \". ",1),("851 \". ",1),("852 \". ",1),("853 \". ",1),("854 \". ",1),("855 \". ",1),("856 \". ",1),("857 \". ",1),("858 \". ",1),("859 \". ",1),("860 \". ",1),
	("861 \". ",1),("862 \". ",1),("863 \". ",1),("864 \". ",1),("865 \". ",1),("866 \". ",1),("867 \". ",1),("868 \". ",1),("869 \". ",1),("870 \". ",1),("871 \". ",1),("872 \". ",1),("873 \". ",1),("874 \". ",1),
	("875 \". ",1),("876 \". ",1),("877 \". ",1),("878 \". ",1),("879 \". ",1),("880 \". ",1),("881 \". ",1),("882 \". ",1),("883 \". ",1),("884 \". ",1),("885 \". ",1),("886 \". ",1),("887 \". ",1),("888 \". ",1),
	("889 \". ",1),("890 \". ",1),("891 \". ",1),("892 \". ",1),("893 \". ",1),("894 \". ",1),("895 \". ",1),("896 \". ",1),("897 \". ",1),("898 \". ",1),("899 \". ",1),("900 \". ",1),("901 \". ",1),("902 \". ",1),
	("903 \". ",1),("904 \". ",1),("905 \". ",1),("906 \". ",1),("907 \". ",1),("908 \". ",1),("909 \". ",1),("910 \". ",1),("911 \". ",1),("912 \". ",1),("913 \". ",1),("914 \". ",1),("915 \". ",1),("916 \". ",1),
	("917 \". ",1),("918 \". ",1),("919 \". ",1),("920 \". ",1),("921 \". ",1),("922 \". ",1),("923 \". ",1),("924 \". ",1),("925 \". ",1),("926 \". ",1),("927 \". ",1),("928 \". ",1),("929 \". ",1),("930 \". ",1),
	("931 \". ",1),("932 \". ",1),("933 \". ",1),("934 \". ",1),("935 \". ",1),("936 \". ",1),("937 \". ",1),("938 \". ",1),("939 \". ",1),("940 \". ",1),("941 \". ",1),("942 \". ",1),("943 \". ",1),("944 \". ",1),
	("945 \". ",1),("946 \". ",1),("947 \". ",1),("948 \". ",1),("949 \". ",1),("950 \". ",1),("951 \". ",1),("952 \". ",1),("953 \". ",1),("954 \". ",1),("955 \". ",1),("956 \". ",1),("957 \". ",1),("958 \". ",1),
	("959 \". ",1),("960 \". ",1),("961 \". ",1),("962 \". ",1),("963 \". ",1),("964 \". ",1),("965 \". ",1),("966 \". ",1),("967 \". ",1),("968 \". ",1),("969 \". ",1),("970 \". ",1),("971 \". ",1),("972 \". ",1),
	("973 \". ",1),("974 \". ",1),("975 \". ",1),("976 \". ",1),("977 \". ",1),("978 \". ",1),("979 \". ",1),("980 \". ",1),("981 \". ",1),("982 \". ",1),("983 \". ",1),("984 \". ",1),("985 \". ",1),("986 \". ",1),
	("987 \". ",1),("988 \". ",1),("989 \". ",1),("990 \". ",1),("991 \". ",1),("992 \". ",1),("993 \". ",1),("994 \". ",1),("995 \". ",1),("996 \". ",1),("997 \". ",1),("998 \". ",1),("999 \". ",1),("1000 \". ",1),
	("1001 \". ",1),("1002 \". ",1),("1003 \". ",1),("1004 \". ",1),("1005 \". ",1),("1006 \". ",1),("1007 \". ",1),("1008 \". ",1),("1009 \". ",1),("1010 \". ",1),("1011 \". ",1),("1012 \". ",1),
	("1013 \". ",1),("1014 \". ",1),("1015 \". ",1),("1016 \". ",1),("1017 \". ",1),("1018 \". ",1),("1019 \". ",1),("1020 \". ",1),("1021 \". ",1),("1022 \". ",1),("1023 \". ",1),("1024 \". ",1),
	("1025 \". ",1),("1026 \". ",1),("1027 \". ",1),("1028 \". ",1),("1029 \". ",1),("1030 \". ",1),("1031 \". ",1),("1032 \". ",1),("1033 \". ",1),("1034 \". ",1),("1035 \". ",1),("1036 \". ",1),
	("1037 \". ",1),("1038 \". ",1),("1039 \". ",1),("1040 \". ",1),("1041 \". ",1),("1042 \". ",1),("1043 \". ",1),("1044 \". ",1),("1045 \". ",1),("1046 \". ",1),("1047 \". ",1),("1048 \". ",1),
	("1049 \". ",1),("1050 \". ",1),("1051 \". ",1),("1052 \". ",1),("1053 \". ",1),("1054 \". ",1),("1055 \". ",1),("1056 \". ",1),("1057 \". ",1),("1058 \". ",1),("1059 \". ",1),("1060 \". ",1),
	("1061 \". ",1),("1062 \". ",1),("1063 \". ",1),("1064 \". ",1),("1065 \". ",1),("1066 \". ",1),("1067 \". ",1),("1068 \". ",1),("1069 \". ",1),("1070 \". ",1),("1071 \". ",1),("1072 \". ",1),
	("1073 \". ",1),("1074 \". ",1),("1075 \". ",1),("1076 \". ",1),("1077 \". ",1),("1078 \". ",1),("1079 \". ",1),("1080 \". ",1),("1081 \". ",1),("1082 \". ",1),("1083 \". ",1),("1084 \". ",1),
	("1085 \". ",1),("1086 \". ",1),("1087 \". ",1),("1088 \". ",1),("1089 \". ",1),("1090 \". ",1),("1091 \". ",1),("1092 \". ",1),("1093 \". ",1),("1094 \". ",1),("1095 \". ",1),("1096 \". ",1),
	("1097 \". ",1),("1098 \". ",1),("1099 \". ",1),("1100 \". ",1),("1101 \". ",1),("1102 \". ",1),("1103 \". ",1),("1104 \". ",1),("1105 \". ",1),("1106 \". ",1),("1107 \". ",1),("1108 \". ",1),
	("1109 \". ",1),("1110 \". ",1),("1111 \". ",1),("1112 \". ",1),("1113 \". ",1),("1114 \". ",1),("1115 \". ",1),("1116 \". ",1),("1117 \". ",1),("1118 \". ",1),("1119 \". ",1),("1120 \". ",1),
	("1121 \". ",1),("1122 \". ",1),("1123 \". ",1),("1124 \". ",1),("1125 \". ",1),("1126 \". ",1),("1127 \". ",1),("1128 \". ",1),("1129 \". ",1),("1130 \". ",1),("1131 \". ",1),("1132 \". ",1),
	("1133 \". ",1),("1134 \". ",1),("1135 \". ",1),("1136 \". ",1),("1137 \". ",1),("1138 \". ",1),("1139 \". ",1),("1140 \". ",1),("1141 \". ",1),("1142 \". ",1),("1143 \". ",1),("1144 \". ",1),
	("1145 \". ",1),("1146 \". ",1),("1147 \". ",1),("1148 \". ",1),("1149 \". ",1),("1150 \". ",1),("1151 \". ",1),("1152 \". ",1),("1153 \". ",1),("1154 \". ",1),("1155 \". ",1),("1156 \". ",1),
	("1157 \". ",1),("1158 \". ",1),("1159 \". ",1),("1160 \". ",1),("1161 \". ",1),("1162 \". ",1),("1163 \". ",1),("1164 \". ",1),("1165 \". ",1),("1166 \". ",1),("1167 \". ",1),("1168 \". ",1),
	("1169 \". ",1),("1170 \". ",1),("1171 \". ",1),("1172 \". ",1),("1173 \". ",1),("1174 \". ",1),("1175 \". ",1),("1176 \". ",1),("1177 \". ",1),("1178 \". ",1),("1179 \". ",1),("1180 \". ",1),
	("1181 \". ",1),("11'82 \". ",1),("118\"3 \". ",1),("1184 \". ",1),("1185 \". ",1),("1186 \". ",1),("1187 \". ",1),("1188 \". ",1),("1189 \". ",1),("1190 \". ",1),("1191 \". ",1),
	("1192 \". ",1),("1193 \". ",1),("1194 \". ",1),("1195 \". ",1),("119\"\"6 \". ",1),("1197 \". ",1),("1198 \". ",1),("1199 \". ",1),("1200 \". ",1);`)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "e2erogue",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	defer m.Close()
	assert.Equal(t, "initial", m.status.Get().String())

	// Usually we would call m.Run() but we want to step through
	// the migration process manually.
	m.dbConfig = dbconn.NewDBConfig()
	m.startTime = time.Now()
	m.db, err = dbconn.New(testutils.DSN(), m.dbConfig)
	assert.NoError(t, err)
	defer m.db.Close()
	// Get Table Info
	m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.migration.Table)
	err = m.changes[0].table.SetInfo(t.Context())
	assert.NoError(t, err)

	assert.NoError(t, m.setup(t.Context()))

	// Now we are ready to start copying rows.
	// Instead of calling m.copyRows() we will step through it manually.
	// Since we want to checkpoint after a few chunks.

	m.status.Set(status.CopyRows)
	assert.Equal(t, "copyRows", m.status.Get().String())

	// We expect 2 chunks to be copied.
	ccopier, ok := m.copier.(*copier.Unbuffered)
	assert.True(t, ok)

	// first chunk.
	chunk, err := m.copyChunker.Next()
	assert.NoError(t, err)
	assert.NotNil(t, chunk)
	assert.Contains(t, chunk.String(), ` < "819 \". "`)
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk))

	// Now insert some data, for binary type it will always say its
	// below the watermark.
	testutils.RunSQL(t, `insert into e2erogue values ("zz'z\"z", 2)`)
	assert.False(t, m.copyChunker.KeyAboveHighWatermark("zz'z\"z"))

	// Second chunk
	chunk, err = m.copyChunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "((`datetime` > \"819 \\\". \")\n OR (`datetime` = \"819 \\\". \" AND `col2` >= 1))", chunk.String())
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk))

	// Now insert some data.
	// This should be picked up by the binlog subscription
	testutils.RunSQL(t, `insert into e2erogue values (5, 2)`)
	assert.False(t, m.copyChunker.KeyAboveHighWatermark(5))
	assert.NoError(t, m.replClient.BlockWait(t.Context()))
	assert.Equal(t, 2, m.replClient.GetDeltaLen())

	testutils.RunSQL(t, "delete from e2erogue where `datetime` like '819%'")
	assert.NoError(t, m.replClient.BlockWait(t.Context()))
	assert.Equal(t, 3, m.replClient.GetDeltaLen())

	// Now that copy rows is done, we flush the changeset until trivial.
	// and perform the optional checksum.
	assert.NoError(t, m.replClient.Flush(t.Context()))
	m.status.Set(status.ApplyChangeset)
	assert.Equal(t, "applyChangeset", m.status.Get().String())
	m.status.Set(status.Checksum)
	m.dbConfig = dbconn.NewDBConfig()
	assert.NoError(t, m.checksum(t.Context()))
	assert.Equal(t, "postChecksum", m.status.Get().String())
	// All done!
}

func TestPartitionedTable(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS part1, _part1_new`)
	table := `CREATE TABLE part1 (
			id bigint(20) NOT NULL AUTO_INCREMENT,
			partition_id smallint(6) NOT NULL,
			created_at timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
			updated_at timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
			initiated_at timestamp(3) NULL DEFAULT NULL,
			version int(11) NOT NULL DEFAULT '0',
			type varchar(50) DEFAULT NULL,
			token varchar(255) DEFAULT NULL,
			PRIMARY KEY (id,partition_id),
			UNIQUE KEY idx_token (token,partition_id)
		  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC
		  /*!50100 PARTITION BY LIST (partition_id)
		  (PARTITION p0 VALUES IN (0) ENGINE = InnoDB,
		   PARTITION p1 VALUES IN (1) ENGINE = InnoDB,
		   PARTITION p2 VALUES IN (2) ENGINE = InnoDB,
		   PARTITION p3 VALUES IN (3) ENGINE = InnoDB,
		   PARTITION p4 VALUES IN (4) ENGINE = InnoDB,
		   PARTITION p5 VALUES IN (5) ENGINE = InnoDB,
		   PARTITION p6 VALUES IN (6) ENGINE = InnoDB,
		   PARTITION p7 VALUES IN (7) ENGINE = InnoDB) */`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `insert into part1 values (1, 1, NOW(), NOW(), NOW(), 1, 'type', 'token'),(1, 2, NOW(), NOW(), NOW(), 1, 'type', 'token'),(1, 3, NOW(), NOW(), NOW(), 1, 'type', 'token2')`) //nolint: dupword

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "part1",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)                // everything is specified.
	assert.NoError(t, m.Run(t.Context())) // should work.
	assert.NoError(t, m.Close())
}

// TestResumeFromCheckpointPhantom tests that there is not a phantom row issue
// when resuming from checkpoint. i.e. consider the following scenario:
// 1) A new row is inserted at the end of the table, and the copier copies it.. but the low watermark never advances past this point
// 2) The row is then deleted after it’s been copied (but the binary log doesn't get to this point)
// 3) A resume occurs
// 4) The insert and delete tracking ignore the row because it’s above the high watermark.
// 5) The INSERT..SELECT only inserts new rows, it doesn't delete non-conflicting existing rows.
// This leaves a broken state because the _new table has a row that should have been deleted.
//
// The fix for this is simple:
// - When resuming from checkpoint, we need to initialize the high watermark from a SELECT MAX(key) FROM the _new table.
// - If this is done correctly, then on resume the DELETE will no longer be ignored.
func TestResumeFromCheckpointPhantom(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS phantomtest, _phantomtest_old, _phantomtest_chkpnt`)
	tbl := `CREATE TABLE phantomtest (
		id int(11) NOT NULL AUTO_INCREMENT,
		pad varbinary(1024) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, tbl)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	// Insert dummy data.
	testutils.RunSQL(t, "INSERT INTO phantomtest (pad) SELECT RANDOM_BYTES(1024) FROM dual")
	testutils.RunSQL(t, "INSERT INTO phantomtest (pad) SELECT RANDOM_BYTES(1024) FROM phantomtest a, phantomtest b, phantomtest c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO phantomtest (pad) SELECT RANDOM_BYTES(1024) FROM phantomtest a, phantomtest b, phantomtest c LIMIT 100000")

	m, err := NewRunner(&Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         2,
		Table:           "phantomtest",
		Alter:           "ENGINE=InnoDB",
		TargetChunkTime: 100 * time.Millisecond,
	})
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())

	// Do the initial setup.
	m.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	m.dbConfig = dbconn.NewDBConfig()
	m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.migration.Table)
	assert.NoError(t, m.changes[0].table.SetInfo(ctx))

	assert.NoError(t, m.newMigration(t.Context()))

	// Now we are ready to start copying rows.
	// We step through this manually using the unbuffered copier, since we want
	// to checkpoint after a few chunks.

	copier, ok := m.copier.(*copier.Unbuffered)
	assert.True(t, ok)

	m.status.Set(status.CopyRows)
	assert.Equal(t, "copyRows", m.status.Get().String())

	// first chunk.
	chunk, err := m.copyChunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "`id` < 1", chunk.String())
	err = copier.CopyChunk(ctx, chunk)
	assert.NoError(t, err)

	// second chunk
	chunk, err = m.copyChunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "`id` >= 1 AND `id` < 1001", chunk.String())
	err = copier.CopyChunk(ctx, chunk)
	assert.NoError(t, err)

	// now we insert a row in the range of the third chunk
	testutils.RunSQL(t, "INSERT INTO phantomtest (id, pad) VALUES (1002, RANDOM_BYTES(1024))")

	// we copy it but we don't feedback it (a hack)
	testutils.RunSQL(t, "INSERT INTO _phantomtest_new (id, pad) SELECT * FROM phantomtest WHERE id = 1002")

	// delete the row (but not from the _new table)
	// when it gets to recopy it will not be there.
	testutils.RunSQL(t, "DELETE FROM phantomtest WHERE id = 1002")

	// then we save the checkpoint without the feedback.
	assert.NoError(t, m.dumpCheckpoint(ctx))
	// assert there is a checkpoint
	var rowCount int
	err = m.db.QueryRow(`SELECT count(*) from _phantomtest_chkpnt`).Scan(&rowCount)
	assert.NoError(t, err)
	assert.Equal(t, 1, rowCount)

	// kill it.
	cancel()
	assert.NoError(t, m.Close())

	// Resume the migration using and apply all of the replication
	// changes before starting the copier.
	ctx = t.Context()
	m, err = NewRunner(&Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         2,
		Table:           "phantomtest",
		Alter:           "ENGINE=InnoDB",
		TargetChunkTime: 100 * time.Millisecond,
	})
	assert.NoError(t, err)
	m.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	m.dbConfig = dbconn.NewDBConfig()
	m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.migration.Table)
	assert.NoError(t, m.changes[0].table.SetInfo(ctx))
	// check we can resume from checkpoint
	// this is normally done in m.setup() but we want to call it in isolation.
	assert.NoError(t, m.resumeFromCheckpoint(ctx))
	// This is normally done in m.setup()
	m.replClient.SetKeyAboveWatermarkOptimization(true)
	// doublecheck that the highPtr is 1002 in the _new table and not in the original table.
	assert.Equal(t, "10", m.changes[0].table.MaxValue().String())
	assert.Equal(t, "1002", m.changes[0].newTable.MaxValue().String())

	// flush the replication changes
	// if the bug exists, this would cause the breakage.
	assert.NoError(t, m.replClient.Flush(ctx))
	// start the copier.
	assert.NoError(t, m.copier.Run(ctx))
	// the checksum runs in prepare for cutover.
	// previously it would fail, but it should work as long as the resumeFromCheckpoint()
	// correctly finds the high watermark.
	err = m.checksum(ctx)
	assert.NoError(t, err)
	assert.NoError(t, m.Close())
}

func TestVarcharE2E(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS varchart1`)
	table := `CREATE TABLE varchart1 (
				pk varchar(255) NOT NULL,
				b varchar(255) NOT NULL,
				PRIMARY KEY (pk)
			)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, "INSERT INTO varchart1 SELECT UUID(), 'abcd' FROM dual ")
	testutils.RunSQL(t, "INSERT INTO varchart1 SELECT UUID(), 'abcd' FROM varchart1 a, varchart1 b, varchart1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO varchart1 SELECT UUID(), 'abcd' FROM varchart1 a, varchart1 b, varchart1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO varchart1 SELECT UUID(), 'abcd' FROM varchart1 a, varchart1 b, varchart1 c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO varchart1 SELECT UUID(), 'abcd' FROM varchart1 a, varchart1 b, varchart1 c LIMIT 100000")

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  2,
		Table:    "varchart1",
		Alter:    "ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.NoError(t, m.Close())
}

func TestSkipDropAfterCutover(t *testing.T) {
	t.Parallel()
	tableName := `skipdrop_test`

	testutils.RunSQL(t, "DROP TABLE IF EXISTS "+tableName)
	table := fmt.Sprintf(`CREATE TABLE %s (
		pk int UNSIGNED NOT NULL,
		PRIMARY KEY(pk)
	)`, tableName)

	testutils.RunSQL(t, table)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:                 cfg.Addr,
		Username:             cfg.User,
		Password:             cfg.Passwd,
		Database:             cfg.DBName,
		Threads:              4,
		Table:                "skipdrop_test",
		Alter:                "ENGINE=InnoDB",
		SkipDropAfterCutover: true,
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)

	sql := fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, m.changes[0].oldTableName())
	var tableCount int
	err = m.db.QueryRow(sql).Scan(&tableCount)
	assert.NoError(t, err)
	assert.Equal(t, 1, tableCount)
	assert.NoError(t, m.Close())
}

func TestDropAfterCutover(t *testing.T) {
	t.Parallel()
	tableName := `drop_test`
	testutils.RunSQL(t, "DROP TABLE IF EXISTS "+tableName)
	table := fmt.Sprintf(`CREATE TABLE %s (
		pk int UNSIGNED NOT NULL,
		PRIMARY KEY(pk)
	)`, tableName)

	testutils.RunSQL(t, table)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:                 cfg.Addr,
		Username:             cfg.User,
		Password:             cfg.Passwd,
		Database:             cfg.DBName,
		Threads:              4,
		Table:                "drop_test",
		Alter:                "ENGINE=InnoDB",
		SkipDropAfterCutover: false,
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)

	sql := fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, m.changes[0].oldTableName())
	var tableCount int
	err = m.db.QueryRow(sql).Scan(&tableCount)
	assert.NoError(t, err)
	assert.Equal(t, 0, tableCount)
	assert.NoError(t, m.Close())
}

func TestDeferCutOver(t *testing.T) {
	t.Parallel()

	// Create unique database for this test
	dbName := testutils.CreateUniqueTestDatabase(t)

	tableName := `deferred_cutover`
	newName := fmt.Sprintf("_%s_new", tableName)
	checkpointTableName := fmt.Sprintf("_%s_chkpnt", tableName)

	dropStmt := `DROP TABLE IF EXISTS %s`
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, checkpointTableName))

	table := fmt.Sprintf(`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tableName)

	testutils.RunSQLInDatabase(t, dbName, table)
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s () values (),(),(),(),(),(),(),(),(),()", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s (id) select null from %s a, %s b, %s c limit 1000", tableName, tableName, tableName, tableName))

	cfg, err := mysql.ParseDSN(testutils.DSNForDatabase(dbName))
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:                 cfg.Addr,
		Username:             cfg.User,
		Password:             cfg.Passwd,
		Database:             dbName,
		Threads:              4,
		Table:                "deferred_cutover",
		Alter:                "ENGINE=InnoDB",
		SkipDropAfterCutover: false,
		DeferCutOver:         true,
		RespectSentinel:      true,
	})
	assert.NoError(t, err)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = m.Run(t.Context())
		assert.Error(t, err)
		assert.ErrorContains(t, err, "timed out waiting for sentinel table to be dropped")
	}()

	// While it's waiting, check the Progress.
	for m.status.Get() != status.WaitingOnSentinelTable {
		time.Sleep(100 * time.Millisecond)
	}
	wg.Wait()

	sql := fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, newName)
	var tableCount int
	err = m.db.QueryRow(sql).Scan(&tableCount)
	assert.NoError(t, err)
	assert.Equal(t, 1, tableCount)
	assert.NoError(t, m.Close())
}

func TestDeferCutOverE2E(t *testing.T) {
	t.Parallel()

	// Create unique database for this test
	dbName := testutils.CreateUniqueTestDatabase(t)

	c := make(chan error)
	tableName := `deferred_cutover_e2e`
	checkpointTableName := fmt.Sprintf("_%s_chkpnt", tableName)

	dropStmt := `DROP TABLE IF EXISTS %s`
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, checkpointTableName))
	table := fmt.Sprintf(`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tableName)

	testutils.RunSQLInDatabase(t, dbName, table)
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s () values (),(),(),(),(),(),(),(),(),()", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s (id) select null from %s a, %s b, %s c limit 1000", tableName, tableName, tableName, tableName))

	cfg, err := mysql.ParseDSN(testutils.DSNForDatabase(dbName))
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:                 cfg.Addr,
		Username:             cfg.User,
		Password:             cfg.Passwd,
		Database:             dbName,
		Threads:              1,
		Table:                "deferred_cutover_e2e",
		Alter:                "ENGINE=InnoDB",
		SkipDropAfterCutover: false,
		DeferCutOver:         true,
		RespectSentinel:      true,
	})
	assert.NoError(t, err)
	go func() {
		err := m.Run(t.Context())
		assert.NoError(t, err)
		c <- err
	}()

	// wait until the sentinel table exists
	db, err := dbconn.New(testutils.DSNForDatabase(dbName), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()
	for {
		var rowCount int
		sql := fmt.Sprintf(
			`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
			WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'`, dbName, sentinelTableName)
		err = db.QueryRow(sql).Scan(&rowCount)
		assert.NoError(t, err)
		if rowCount > 0 {
			break
		}
	}
	assert.NoError(t, err)

	testutils.RunSQLInDatabase(t, dbName, "DROP TABLE "+sentinelTableName)

	err = <-c // wait for the migration to finish
	assert.NoError(t, err)

	sql := fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, m.changes[0].oldTableName())
	var tableCount int
	err = db.QueryRow(sql).Scan(&tableCount)
	assert.NoError(t, err)
	assert.Equal(t, 0, tableCount)
	assert.NoError(t, m.Close())
}

func TestDeferCutOverE2EBinlogAdvance(t *testing.T) {
	// Create unique database for this test
	dbName := testutils.CreateUniqueTestDatabase(t)

	c := make(chan error)
	tableName := `deferred_cutover_e2e_stage`
	checkpointTableName := fmt.Sprintf("_%s_chkpnt", tableName)

	dropStmt := `DROP TABLE IF EXISTS %s`
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, checkpointTableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s () values (),(),(),(),(),(),(),(),(),()", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s (id) select null from %s a, %s b, %s c limit 1000", tableName, tableName, tableName, tableName))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:                 cfg.Addr,
		Username:             cfg.User,
		Password:             cfg.Passwd,
		Database:             dbName,
		Threads:              1,
		Table:                "deferred_cutover_e2e_stage",
		Alter:                "ENGINE=InnoDB",
		SkipDropAfterCutover: false,
		DeferCutOver:         true,
		RespectSentinel:      true,
	})
	assert.NoError(t, err)
	go func() {
		err := m.Run(t.Context())
		assert.NoError(t, err)
		c <- err
	}()

	// wait until the sentinel table exists
	db, err := dbconn.New(testutils.DSNForDatabase(dbName), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()
	for m.status.Get() != status.WaitingOnSentinelTable {
	}
	assert.NoError(t, err)

	binlogPos := m.replClient.GetBinlogApplyPosition()
	for range 4 {
		testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s (id) select null from %s a, %s b, %s c limit 1000", tableName, tableName, tableName, tableName))
		time.Sleep(1 * time.Second)
		m.replClient.Flush(t.Context())
		newBinlogPos := m.replClient.GetBinlogApplyPosition()
		assert.Equal(t, 1, newBinlogPos.Compare(binlogPos))
		binlogPos = newBinlogPos
	}

	testutils.RunSQLInDatabase(t, dbName, "DROP TABLE "+sentinelTableName)

	err = <-c // wait for the migration to finish
	assert.NoError(t, err)

	sql := fmt.Sprintf(
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='%s'`, m.changes[0].oldTableName())
	var tableCount int
	err = db.QueryRow(sql).Scan(&tableCount)
	assert.NoError(t, err)
	assert.Equal(t, 0, tableCount)
	assert.NoError(t, m.Close())
}

func TestResumeFromCheckpointE2EWithManualSentinel(t *testing.T) {
	t.Parallel()
	// This test is similar to TestResumeFromCheckpointE2E but it adds a sentinel table
	// created after the migration begins and is interrupted.
	// The migration itself runs with DeferCutOver=false
	// so we test to make sure a sentinel table created manually by the operator
	// blocks cutover.

	dbName := testutils.CreateUniqueTestDatabase(t)
	tableName := `resume_checkpoint_e2e_w_sentinel`
	tableInfo := table.TableInfo{SchemaName: dbName, TableName: tableName}
	lockTables := []*table.TableInfo{&tableInfo}

	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`DROP TABLE IF EXISTS %s, _%s_old, _%s_chkpnt`, tableName, tableName, tableName))
	table := fmt.Sprintf(`CREATE TABLE %s (
		id int(11) NOT NULL AUTO_INCREMENT,
		pad varbinary(1024) NOT NULL,
		PRIMARY KEY (id)
	)`, tableName)
	testutils.RunSQLInDatabase(t, dbName, table)
	migration := &Migration{}
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	// Insert dummy data.
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (pad) SELECT RANDOM_BYTES(1024) FROM dual", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (pad) SELECT RANDOM_BYTES(1024) FROM %s a, %s b, %s c LIMIT 100000", tableName, tableName, tableName, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (pad) SELECT RANDOM_BYTES(1024) FROM %s a, %s b, %s c LIMIT 100000", tableName, tableName, tableName, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (pad) SELECT RANDOM_BYTES(1024) FROM %s a, %s b, %s c LIMIT 100000", tableName, tableName, tableName, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (pad) SELECT RANDOM_BYTES(1024) FROM %s a, %s b, %s c LIMIT 100000", tableName, tableName, tableName, tableName))
	alterSQL := "ADD INDEX(pad);"
	// use as slow as possible here: we want the copy to be still running
	// when we kill it once we have a checkpoint saved.
	migration.Host = cfg.Addr
	migration.Username = cfg.User
	migration.Password = cfg.Passwd
	migration.Database = dbName
	migration.Threads = 1
	migration.Table = tableName
	migration.Alter = alterSQL
	migration.TargetChunkTime = 100 * time.Millisecond
	migration.DeferCutOver = false
	migration.RespectSentinel = true

	runner, err := NewRunner(migration)
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())

	go func() {
		err := runner.Run(ctx)
		assert.Error(t, err) // it gets interrupted as soon as there is a checkpoint saved.
	}()

	// wait until a checkpoint is saved (which means copy is in progress)
	db, err := dbconn.New(testutils.DSNForDatabase(dbName), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()
	for {
		var rowCount int
		err = db.QueryRow(fmt.Sprintf(`SELECT count(*) from _%s_chkpnt`, tableName)).Scan(&rowCount)
		if err != nil {
			continue // table does not exist yet
		}
		if rowCount > 0 {
			// Test that it's not possible to acquire metadata lock with name
			// as tablename while the migration is running.
			lock, err := dbconn.NewMetadataLock(ctx, testutils.DSN(),
				lockTables, dbconn.NewDBConfig(), &testLogger{})
			assert.Error(t, err)
			if lock != nil {
				assert.ErrorContains(t, err, fmt.Sprintf("could not acquire metadata lock for %s, lock is held by another connection", lock.GetLockName()))
			}
			assert.Nil(t, lock)
			break
		}
	}
	// Between cancel and Close() every resource is freed.
	cancel()
	assert.NoError(t, runner.Close())

	// Manually create the sentinel table.
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE _spirit_sentinel (id int unsigned primary key)")

	// Insert some more dummy data
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (pad) SELECT RANDOM_BYTES(1024) FROM %s LIMIT 1000", tableName, tableName))
	// Start a new migration with the same parameters.
	// Let it complete.
	newmigration := &Migration{}
	newmigration.Host = cfg.Addr
	newmigration.Username = cfg.User
	newmigration.Password = cfg.Passwd
	newmigration.Database = dbName
	newmigration.Threads = 4
	newmigration.Table = tableName
	newmigration.Alter = alterSQL
	newmigration.TargetChunkTime = 5 * time.Second
	newmigration.DeferCutOver = false
	newmigration.RespectSentinel = true

	m, err := NewRunner(newmigration)
	assert.NoError(t, err)
	assert.NotNil(t, m)

	err = m.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "timed out waiting for sentinel table to be dropped")
	assert.True(t, m.usedResumeFromCheckpoint)
	assert.NoError(t, m.Close())
}

func TestPreRunChecksE2E(t *testing.T) {
	t.Parallel()
	// We test the checks in tests for that package, but we also want to test
	// that the checks run correctly when instantiating a migration.

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		Table:    "test_checks_e2e",
		Alter:    "engine=innodb",
	})
	assert.NoError(t, err)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()
	err = m.runChecks(t.Context(), check.ScopePreRun)
	assert.NoError(t, err)
}

// From https://github.com/block/spirit/issues/241
// If an ALTER qualifies as instant, but an instant can't apply, don't burn an instant version.
func TestForNonInstantBurn(t *testing.T) {
	t.Parallel()
	// We skip this test in MySQL 8.0.28. It uses INSTANT_COLS instead of total_row_versions
	// and it supports instant add col, but not instant drop col.
	// It's safe to skip, but we need 8.0.28 in tests because it's the minor version
	// used by Aurora's LTS.
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()
	var version string
	err = db.QueryRow(`SELECT version()`).Scan(&version)
	assert.NoError(t, err)
	if version == "8.0.28" {
		t.Skip("Skiping this test for MySQL 8.0.28")
	}
	// Continue with the test.
	testutils.RunSQL(t, `DROP TABLE IF EXISTS instantburn`)
	table := `CREATE TABLE instantburn (
		id int(11) NOT NULL AUTO_INCREMENT,
		pad varbinary(1024) NOT NULL,
		PRIMARY KEY (id)
	)`
	rowVersions := func() int {
		// Check that the number of total_row_versions is Zero (i'e doesn't burn)
		db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
		assert.NoError(t, err)
		defer db.Close()
		var rowVersions int
		err = db.QueryRow(`SELECT total_row_versions FROM INFORMATION_SCHEMA.INNODB_TABLES where name='test/instantburn'`).Scan(&rowVersions)
		assert.NoError(t, err)
		return rowVersions
	}

	testutils.RunSQL(t, table)
	for range 32 { // requires 64 instants
		testutils.RunSQL(t, "ALTER TABLE instantburn ALGORITHM=INSTANT, ADD newcol INT")
		testutils.RunSQL(t, "ALTER TABLE instantburn ALGORITHM=INSTANT, DROP newcol")
	}
	assert.Equal(t, 64, rowVersions()) // confirm all 64 are used.
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		Table:    "instantburn",
		Alter:    "add newcol2 int",
	})
	assert.NoError(t, err)
	defer m.Close()
	err = m.Run(t.Context())
	assert.NoError(t, err)

	assert.False(t, m.usedInstantDDL) // it would have had to apply a copy.
	assert.Equal(t, 0, rowVersions()) // confirm we reset to zero, not 1 (no burn)
}

// From https://github.com/block/spirit/issues/283
// ALTER INDEX .. VISIBLE is INPLACE which is really weird.
// it only makes sense to be instant, so we attempt it as a "safe inplace".
// If it's not with a set of safe changes, then we error.
// This means the user is expected to split their DDL into two separate ALTERs.
//
// There is a partial workaround for users to use --force-inplace, which would
// help only if the other included changes are also INPLACE and not copy.
// We *do* document this under --force-inplace docs, but it's
// really not a typical use case to ever mix invisible with any other change.
// i.e. if anything it's more a side-effect than a workaround.
func TestIndexVisibility(t *testing.T) {
	t.Parallel()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS indexvisibility`)
	table := `CREATE TABLE indexvisibility (
		id int(11) NOT NULL AUTO_INCREMENT,
		b INT NOT NULL,
		c INT NOT NULL,
		PRIMARY KEY (id),
		INDEX (b)
	)`
	testutils.RunSQL(t, table)
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		Table:    "indexvisibility",
		Alter:    "ALTER INDEX b INVISIBLE",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)

	assert.True(t, m.usedInplaceDDL) // expected to count as safe.
	assert.NoError(t, m.Close())

	// Test again with visible
	m, err = NewRunner(&Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Table:     "indexvisibility",
		Alter:     "ALTER INDEX b VISIBLE",
		ForceKill: true,
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.True(t, m.usedInplaceDDL) // expected to count as safe.
	assert.NoError(t, m.Close())

	// Test again but include an unsafe INPLACE change at the same time.
	// This won't work by default.
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		Table:    "indexvisibility",
		Alter:    "ALTER INDEX b VISIBLE, ADD INDEX (c)",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.Error(t, err)
	assert.NoError(t, m.Close()) // it's errored, we don't need to try again. We can close.

	// The above should now fail with enhanced automatic detection.
	// Index visibility mixed with table-rebuilding operations should be rejected.

	// Index visibility mixed with table-rebuilding operations should fail.
	// This is important because invisible should never be mixed with copy
	// (the semantics are weird since it's for experiments).
	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Threads:  1,
		Table:    "indexvisibility",
		Alter:    "ALTER INDEX b VISIBLE, CHANGE c cc BIGINT NOT NULL",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.Error(t, err)
	assert.NoError(t, m.Close()) // it's errored, we don't need to try again. We can close.
}

// TestPreventConcurrentRuns ensures that metadata locking
// prevents two concurrent migrations on the same table.
// We use DeferCutOver=true option to force the first migration
// to stay running.
func TestPreventConcurrentRuns(t *testing.T) {
	t.Parallel()

	dbName := testutils.CreateUniqueTestDatabase(t)
	tableName := `prevent_concurrent_runs`

	dropStmt := `DROP TABLE IF EXISTS %s`
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(dropStmt, checkpointTableName))

	table := fmt.Sprintf(`CREATE TABLE %s (id bigint unsigned not null auto_increment, primary key(id))`, tableName)

	testutils.RunSQLInDatabase(t, dbName, table)
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s () values (),(),(),(),(),(),(),(),(),()", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("insert into %s (id) select null from %s a, %s b, %s c limit 1000", tableName, tableName, tableName, tableName))

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:                 cfg.Addr,
		Username:             cfg.User,
		Password:             cfg.Passwd,
		Database:             dbName,
		Threads:              4,
		Table:                tableName,
		Alter:                "ENGINE=InnoDB",
		SkipDropAfterCutover: false,
		DeferCutOver:         true,
		RespectSentinel:      true,
	})
	assert.NoError(t, err)
	defer m.Close()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Shadow err to avoid a data race
		err := m.Run(t.Context())
		assert.Error(t, err)
		// The error can either be context cancelled or timed out.
		// Both are acceptable
		if !errors.Is(err, context.Canceled) {
			assert.ErrorContains(t, err, "timed out waiting for sentinel table to be dropped")
		}
	}()

	// While it's waiting, start another run and confirm it fails.
	time.Sleep(1 * time.Second)
	m2, err := NewRunner(&Migration{
		Host:                 cfg.Addr,
		Username:             cfg.User,
		Password:             cfg.Passwd,
		Database:             dbName,
		Threads:              4,
		Table:                tableName,
		Alter:                "ENGINE=InnoDB",
		SkipDropAfterCutover: false,
	})
	assert.NoError(t, err)
	err = m2.Run(t.Context())
	defer m2.Close()
	assert.Error(t, err)
	assert.ErrorContains(t, err, "could not acquire metadata lock")
	wg.Wait()
}

func TestStatementWorkflowStillInstant(t *testing.T) {
	t.Parallel()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS stmtworkflow`)
	table := `CREATE TABLE stmtworkflow (
		id int(11) NOT NULL AUTO_INCREMENT,
		b INT NOT NULL,
		c INT NOT NULL,
		PRIMARY KEY (id),
		INDEX (b)
	)`
	testutils.RunSQL(t, table)
	m, err := NewRunner(&Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  cfg.Passwd,
		Database:  cfg.DBName,
		Threads:   1,
		Statement: "ALTER TABLE stmtworkflow ADD newcol INT",
	})
	assert.NoError(t, err)
	err = m.Run(t.Context())
	assert.NoError(t, err)

	assert.True(t, m.usedInstantDDL) // expected to count as instant.
	assert.NoError(t, m.Close())
}

func TestTrailingSemicolon(t *testing.T) {
	t.Parallel()
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS multiSecondary`)
	testutils.RunSQL(t, `CREATE TABLE multiSecondary (
			  id int unsigned NOT NULL AUTO_INCREMENT,
			  v varchar(32) DEFAULT NULL,
			  PRIMARY KEY (id),
			  KEY idx5 (v),
			  KEY idx1 (v),
			  KEY idx2 (v),
			  KEY idx3 (v),
			  KEY idx4 (v)
			)`)
	dropIndexesAlter := "drop index idx1, drop index idx2, drop index idx3, drop index idx4"
	m, err := NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Table:    "multiSecondary",
		Alter:    dropIndexesAlter,
		Threads:  1,
	})
	require.NoError(t, err)
	err = m.Run(t.Context())
	require.NoError(t, err)

	assert.True(t, m.usedInplaceDDL) // DROP INDEX operations now use INPLACE for better performance
	assert.NoError(t, m.Close())

	m, err = NewRunner(&Migration{
		Host:      cfg.Addr,
		Username:  cfg.User,
		Password:  cfg.Passwd,
		Database:  cfg.DBName,
		Statement: "alter table multiSecondary add index idx1(v), add index idx2(v), add index idx3(v), add index idx4(v);",
		Threads:   1,
	})
	require.NoError(t, err)
	err = m.Run(t.Context())
	require.NoError(t, err)

	require.False(t, m.usedInplaceDDL) // ADD INDEX operations now use copy process for replica safety
	require.NoError(t, m.Close())

	m, err = NewRunner(&Migration{
		Host:     cfg.Addr,
		Username: cfg.User,
		Password: cfg.Passwd,
		Database: cfg.DBName,
		Table:    "multiSecondary",
		// https://github.com/block/spirit/issues/384
		Alter:   dropIndexesAlter + "; ",
		Threads: 1,
	})
	require.NoError(t, err)
	err = m.Run(t.Context())
	require.NoError(t, err)

	require.True(t, m.usedInplaceDDL)
	require.NoError(t, m.Close())
}
func TestAlterExtendVarcharE2E(t *testing.T) {
	t.Parallel()
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1extendvarchar, _t1extendvarchar_new`)
	table := `CREATE TABLE t1extendvarchar (
		id int not null primary key auto_increment,
		col1 varchar(10),
		col2 varchar(10)
	) character set utf8mb4`
	testutils.RunSQL(t, table)

	type alterAttempt struct {
		Statement string
		Error     bool
		InPlace   bool
	}
	alters := []alterAttempt{
		{Statement: `ALTER TABLE t1extendvarchar MODIFY col1 varchar(20)`, InPlace: true},
		{Statement: `ALTER TABLE t1extendvarchar CHANGE col1 col1 varchar(21)`, InPlace: true},
		{Statement: `ALTER TABLE t1extendvarchar MODIFY col1 varchar(22), CHANGE col2 col2 varchar(22) `, InPlace: true},
		{Statement: `ALTER TABLE t1extendvarchar MODIFY col1 varchar(23), CHANGE col2 col2 varchar(200) `, InPlace: false},
		{Statement: `ALTER TABLE t1extendvarchar MODIFY col1 varchar(200)`, InPlace: false},
	}

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	for _, attempt := range alters {
		m, err := NewRunner(&Migration{
			Host:      cfg.Addr,
			Username:  cfg.User,
			Password:  cfg.Passwd,
			Database:  cfg.DBName,
			Threads:   1,
			Statement: attempt.Statement,
		})
		require.NoError(t, err)
		err = m.Run(t.Context())
		require.NoError(t, err)
		assert.Equal(t, attempt.InPlace, m.usedInplaceDDL)

		// go test howls about resource leaks if we don't close all these things
		err = m.Close()
		assert.NoError(t, err)
	}
}

func TestMigrationCancelledFromTableModification(t *testing.T) {
	t.Parallel()
	// This test covers the case where a migration is running
	// and the user modifies the table (e.g. with another ALTER).
	// The migration should detect this and cancel itself.
	// We use a long-running copy phase to give us time to do the modification.
	testutils.RunSQL(t, `DROP TABLE IF EXISTS t1modification, _t1modification_new`)
	table := `CREATE TABLE t1modification (
		id int not null primary key auto_increment,
		col1 varbinary(1024),
		col2 varbinary(1024)
	) character set utf8mb4`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, "INSERT INTO t1modification (col1, col2) SELECT RANDOM_BYTES(1024), RANDOM_BYTES(1024) FROM dual ")
	testutils.RunSQL(t, "INSERT INTO t1modification (col1, col2) SELECT RANDOM_BYTES(1024), RANDOM_BYTES(1024) FROM t1modification a, t1modification b, t1modification c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO t1modification (col1, col2) SELECT RANDOM_BYTES(1024), RANDOM_BYTES(1024) FROM t1modification a, t1modification b, t1modification c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO t1modification (col1, col2) SELECT RANDOM_BYTES(1024), RANDOM_BYTES(1024) FROM t1modification a, t1modification b, t1modification c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO t1modification (col1, col2) SELECT RANDOM_BYTES(1024), RANDOM_BYTES(1024) FROM t1modification a, t1modification b, t1modification c LIMIT 100000")

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	m, err := NewRunner(&Migration{
		Host:            cfg.Addr,
		Username:        cfg.User,
		Password:        cfg.Passwd,
		Database:        cfg.DBName,
		Threads:         1,
		TargetChunkTime: 100 * time.Millisecond, // weak performance at copying.
		Statement:       "ALTER TABLE t1modification ENGINE=InnoDB",
	})
	require.NoError(t, err)

	// Start the migration in a goroutine
	wg := sync.WaitGroup{}
	wg.Add(1)
	var gErr error
	go func() {
		defer wg.Done()
		gErr = m.Run(t.Context())
	}()

	// Wait until the copy phase has started.
	for m.status.Get() != status.CopyRows {
		time.Sleep(1 * time.Millisecond)
	}

	// Now modify the table
	// instant DDL (applies quickly and will cause the migration to cancel)
	testutils.RunSQL(t, "ALTER TABLE t1modification ADD col3 INT")

	wg.Wait() // wait for the error to occur.

	require.Error(t, gErr)
	require.NoError(t, m.Close())
}

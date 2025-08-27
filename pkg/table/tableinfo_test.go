package table

import (
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	"go.uber.org/goleak"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
	os.Exit(m.Run())
}

func TestOpenOnBinaryType(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1.EstimatedRows = 1000000
	t1.KeyColumns = []string{"id"}
	t1.keyColumnsMySQLTp = []string{"varbinary"}
	t1.keyDatums = []datumTp{binaryType}
	t1.KeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	chunker, err := newChunker(t1, ChunkerDefaultTarget, logrus.New())
	assert.NoError(t, err)
	assert.NoError(t, chunker.Open())
}

func TestOpenOnNoMinMax(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1.EstimatedRows = 1000000
	t1.KeyColumns = []string{"id"}
	t1.keyColumnsMySQLTp = []string{"varbinary"}
	t1.keyDatums = []datumTp{binaryType}
	t1.KeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	chunker, err := newChunker(t1, 100, logrus.New())
	assert.NoError(t, err)
	assert.NoError(t, chunker.Open())
}

func TestCallingNextChunkWithoutOpen(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1.EstimatedRows = 1000000
	t1.KeyColumns = []string{"id"}
	t1.keyColumnsMySQLTp = []string{"varbinary"}
	t1.keyDatums = []datumTp{binaryType}
	t1.KeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	chunker, err := newChunker(t1, 100, logrus.New())
	assert.NoError(t, err)

	_, err = chunker.Next()
	assert.Error(t, err)

	assert.NoError(t, chunker.Open())
	_, err = chunker.Next()
	assert.NoError(t, err)
}

func newTableInfo4Test(schema, table string) *TableInfo { //nolint: unparam
	t1 := NewTableInfo(nil, schema, table)
	return t1
}

func TestDiscovery(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS discoveryt1`)
	table := `CREATE TABLE discoveryt1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `insert into discoveryt1 values (1, 'a'), (2, 'b'), (3, 'c')`)

	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer db.Close()

	t1 := NewTableInfo(db, "test", "discoveryt1")
	assert.NoError(t, t1.SetInfo(t.Context()))

	assert.Equal(t, "discoveryt1", t1.TableName)
	assert.Equal(t, "test", t1.SchemaName)
	assert.Equal(t, "id", t1.KeyColumns[0])

	// normalize for mysql 5.7 and 8.0
	assert.Equal(t, "int", removeWidth(t1.columnsMySQLTps["id"]))
	assert.Equal(t, "CAST(`id` AS signed)", t1.WrapCastType("id"))
	assert.Equal(t, "CAST(`name` AS char CHARACTER SET utf8mb4)", t1.WrapCastType("name"))

	assert.Equal(t, "1", t1.minValue.String())
	assert.Equal(t, "3", t1.maxValue.String())

	// Can't check estimated rows (depends on MySQL version etc)
	assert.Equal(t, []string{"int"}, t1.keyColumnsMySQLTp)
	assert.True(t, t1.KeyIsAutoInc)
	assert.Len(t, t1.Columns, 2)
}

func TestDiscoveryUInt(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS discoveryuintt1`)
	table := `CREATE TABLE discoveryuintt1 (
		id int(11) unsigned NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `insert into discoveryuintt1 values (1, 'a'), (2, 'b'), (3, 'c')`)

	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer db.Close()

	t1 := NewTableInfo(db, "test", "discoveryuintt1")
	assert.NoError(t, t1.SetInfo(t.Context()))

	assert.Equal(t, "discoveryuintt1", t1.TableName)
	assert.Equal(t, "test", t1.SchemaName)
	assert.Equal(t, "id", t1.KeyColumns[0])

	assert.Equal(t, "1", t1.minValue.String())
	assert.Equal(t, "3", t1.maxValue.String())

	// Can't check estimated rows (depends on MySQL version etc)
	assert.Equal(t, []string{"int unsigned"}, t1.keyColumnsMySQLTp)
	assert.True(t, t1.KeyIsAutoInc)
	assert.Len(t, t1.Columns, 2)
}

func TestDiscoveryNoKeyColumnsOrNoTable(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS discoverynokeyst1`)
	table := `CREATE TABLE discoverynokeyst1 (
		id int(11) NOT NULL,
		name varchar(255) NOT NULL
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `insert into discoverynokeyst1 values (1, 'a'), (2, 'b'), (3, 'c')`)

	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer db.Close()

	t1 := NewTableInfo(db, "test", "discoverynokeyst1")
	assert.ErrorContains(t, t1.SetInfo(t.Context()), "no primary key found")

	t2 := NewTableInfo(db, "test", "t2fdsfds")
	assert.ErrorContains(t, t2.SetInfo(t.Context()), "table test.t2fdsfds does not exist")
}

func TestDiscoveryBalancesTable(t *testing.T) {
	// This is not a bad test, since there is a PRIMARY KEY and a UNIQUE KEY
	// and the discovery has to discover the primary key as the constraint
	// not the unique key.
	table := `CREATE TABLE balances (
		id bigint NOT NULL AUTO_INCREMENT,
		b_token varbinary(255) NOT NULL,
		c_token varbinary(255) NOT NULL,
		version int NOT NULL DEFAULT '0',
		cents bigint NOT NULL,
		currency varbinary(3) NOT NULL,
		c1 varchar(50) NOT NULL,
		c2 varchar(120) DEFAULT NULL,
		b1 tinyint NOT NULL,
		created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		PRIMARY KEY (id),
		UNIQUE KEY b_token (b_token),
		KEY c_token (c_token)
	  ) ENGINE=InnoDB AUTO_INCREMENT=3000001 DEFAULT CHARSET=utf8mb4`
	testutils.RunSQL(t, `drop table if exists balances`)
	testutils.RunSQL(t, table)

	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer db.Close()

	t1 := NewTableInfo(db, "test", "balances")
	assert.NoError(t, t1.SetInfo(t.Context()))

	assert.True(t, t1.KeyIsAutoInc)
	assert.Equal(t, []string{"bigint"}, t1.keyColumnsMySQLTp)
	assert.Equal(t, []string{"id"}, t1.KeyColumns)
	assert.Equal(t, "0", t1.minValue.String())
	assert.Equal(t, "0", t1.maxValue.String())

	chunker, err := newChunker(t1, 100, logrus.New())
	assert.NoError(t, err)

	assert.NoError(t, chunker.Open())
	assert.Equal(t, "0", t1.minValue.String())
	assert.Equal(t, "0", t1.maxValue.String())
}

func TestDiscoveryCompositeNonComparable(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS compnoncomparable`)
	table := `CREATE TABLE compnoncomparable (
		id int(11) unsigned NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id, name)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `insert into compnoncomparable values (1, 'a'), (2, 'b'), (3, 'c')`)

	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer db.Close()

	t1 := NewTableInfo(db, "test", "compnoncomparable")
	assert.NoError(t, t1.SetInfo(t.Context()))         // still discovers the primary key
	assert.Error(t, t1.PrimaryKeyIsMemoryComparable()) // but its non comparable
}

func TestDiscoveryCompositeComparable(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS compcomparable`)
	table := `CREATE TABLE compcomparable (
		id int(11) unsigned NOT NULL AUTO_INCREMENT,
		age int(11) NOT NULL,
		PRIMARY KEY (id, age)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `insert into compcomparable values (1, 1), (2, 2), (3, 3)`)

	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer db.Close()

	t1 := NewTableInfo(db, "test", "compcomparable")
	assert.NoError(t, t1.SetInfo(t.Context()))

	assert.True(t, t1.KeyIsAutoInc)
	assert.Equal(t, []string{"int unsigned", "int"}, t1.keyColumnsMySQLTp)
	assert.Equal(t, []string{"id", "age"}, t1.KeyColumns)
}

func TestStatisticsUpdate(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer db.Close()

	testutils.RunSQL(t, `DROP TABLE IF EXISTS statsupdate`)
	table := `CREATE TABLE statsupdate (
		id int(11) unsigned NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id, name)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `insert into statsupdate values (1, 'a'), (2, 'b'), (3, 'c')`)

	t1 := &TableInfo{
		minValue:          NewDatum(1, signedType),
		maxValue:          NewDatum(1000000, signedType),
		EstimatedRows:     1000000,
		SchemaName:        "test",
		TableName:         "statsupdate",
		QuotedName:        "`test`.`statsupdate`",
		KeyColumns:        []string{"id"},
		keyColumnsMySQLTp: []string{"int"},
		keyDatums:         []datumTp{signedType},
		KeyIsAutoInc:      true,
		Columns:           []string{"id", "name"},
		db:                db,
	}
	t1.statisticsLastUpdated = time.Now()

	go t1.AutoUpdateStatistics(t.Context(), time.Millisecond*10, logrus.New())
	time.Sleep(time.Millisecond * 100)

	t1.Close()
}

func TestKeyColumnsValuesExtraction(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer db.Close()

	testutils.RunSQL(t, `DROP TABLE IF EXISTS colvaluest1`)
	table := `CREATE TABLE colvaluest1 (
		id int(11) unsigned NOT NULL AUTO_INCREMENT,
		name varchar(115),
		age int(11) NOT NULL,
		PRIMARY KEY (id, age)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `insert into colvaluest1 values (1, 'a', 15), (2, 'b', 20), (3, 'c', 25)`)

	t1 := NewTableInfo(db, "test", "colvaluest1")
	assert.NoError(t, t1.SetInfo(t.Context()))

	var id, age int
	var name string

	err = db.QueryRow("SELECT * FROM `test`.`colvaluest1` ORDER BY id DESC LIMIT 1").Scan(&id, &name, &age)
	assert.NoError(t, err)

	row := []any{id, name, age}
	pkVals, err := t1.PrimaryKeyValues(row)
	assert.Equal(t, id, pkVals[0])
	assert.Equal(t, age, pkVals[1])
	assert.NoError(t, err)
}

func TestDiscoveryGeneratedCols(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS generatedcolst1`)
	table := `CREATE TABLE generatedcolst1 (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		b INT NOT NULL,
		c1 int GENERATED ALWAYS AS  (b + 1),
		c2 int GENERATED ALWAYS AS  (b + 1) VIRTUAL,
		c3 int GENERATED ALWAYS AS  (b + 1) STORED,
		d datetime(3) not null default current_timestamp(3),
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)

	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer db.Close()

	t1 := NewTableInfo(db, "test", "generatedcolst1")
	assert.NoError(t, t1.SetInfo(t.Context()))

	// Can't check estimated rows (depends on MySQL version etc)
	assert.Equal(t, []string{"id", "name", "b", "c1", "c2", "c3", "d"}, t1.Columns)
	assert.Equal(t, []string{"id", "name", "b", "d"}, t1.NonGeneratedColumns)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS generatedcolst2`)
	table = `CREATE TABLE generatedcolst2 (
  id bigint NOT NULL AUTO_INCREMENT,
  pa bigint DEFAULT NULL,
  p1 bigint DEFAULT NULL,
  p2 bigint DEFAULT NULL,
  s1 tinyint(1) GENERATED ALWAYS AS (if((pa is not null),1,NULL)) STORED,
  s2 tinyint(1) GENERATED ALWAYS AS (if((p1 is not null),1,NULL)) STORED,
  s3 tinyint(1) GENERATED ALWAYS AS (if((p2 is not null),1,NULL)) STORED,
  s4 tinyint(1) GENERATED ALWAYS AS (if((pa <> p2),1,NULL)) STORED,
  PRIMARY KEY (id)
);`
	testutils.RunSQL(t, table)
	t2 := NewTableInfo(db, "test", "generatedcolst2")
	assert.NoError(t, t2.SetInfo(t.Context()))

	// Can't check estimated rows (depends on MySQL version etc)
	assert.Equal(t, []string{"id", "pa", "p1", "p2", "s1", "s2", "s3", "s4"}, t2.Columns)
	assert.Equal(t, []string{"id", "pa", "p1", "p2"}, t2.NonGeneratedColumns)
}

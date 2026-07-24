package table

import (
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/testutils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestOpenOnBinaryType(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1.EstimatedRows = 1000000
	t1.KeyColumns = []string{"id"}
	t1.keyColumnsMySQLTp = []string{"varbinary"}
	t1.keyDatums = []datumTp{binaryType}
	t1.KeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	chunker, err := NewChunker(t1, ChunkerConfig{})
	require.NoError(t, err)
	require.NoError(t, chunker.Open())
}

func TestOpenOnNoMinMax(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1.EstimatedRows = 1000000
	t1.KeyColumns = []string{"id"}
	t1.keyColumnsMySQLTp = []string{"varbinary"}
	t1.keyDatums = []datumTp{binaryType}
	t1.KeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	chunker, err := NewChunker(t1, ChunkerConfig{TargetChunkTime: 100})
	require.NoError(t, err)
	require.NoError(t, chunker.Open())
}

func TestCallingNextChunkWithoutOpen(t *testing.T) {
	t1 := NewTableInfo(nil, "test", "t1")
	t1.EstimatedRows = 1000000
	t1.KeyColumns = []string{"id"}
	t1.keyColumnsMySQLTp = []string{"varbinary"}
	t1.keyDatums = []datumTp{binaryType}
	t1.KeyIsAutoInc = true
	t1.Columns = []string{"id", "name"}
	chunker, err := NewChunker(t1, ChunkerConfig{TargetChunkTime: 100})
	require.NoError(t, err)

	_, err = chunker.Next()
	require.Error(t, err)

	require.NoError(t, chunker.Open())
	_, err = chunker.Next()
	require.NoError(t, err)
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
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()
	t1 := NewTableInfo(db, "test", "discoveryt1")
	require.NoError(t, t1.SetInfo(t.Context()))

	require.Equal(t, "discoveryt1", t1.TableName)
	require.Equal(t, "test", t1.SchemaName)
	require.Equal(t, "id", t1.KeyColumns[0])

	// normalize for mysql 5.7 and 8.0
	require.Equal(t, "int", removeWidth(t1.columnsMySQLTps["id"]))
	castID, err := t1.wrapCastType("id", castSource)
	require.NoError(t, err)
	require.Equal(t, "CAST(`id` AS signed)", castID)
	castName, err := t1.wrapCastType("name", castTarget)
	require.NoError(t, err)
	require.Equal(t, "CAST(`name` AS char CHARACTER SET utf8mb4)", castName)

	require.Equal(t, "1", t1.minValue.String())
	require.Equal(t, "3", t1.maxValue.String())

	// Can't check estimated rows (depends on MySQL version etc)
	require.Equal(t, []string{"int"}, t1.keyColumnsMySQLTp)
	require.True(t, t1.KeyIsAutoInc)
	require.Len(t, t1.Columns, 2)
}

// TestDiscoveryDisableAnalyze verifies that SetInfo works with DisableAnalyze
// set: it must skip ANALYZE TABLE (which needs a writable server) and still
// discover the schema and populate EstimatedRows straight from
// information_schema. This is the path a read-only / replica source uses.
func TestDiscoveryDisableAnalyze(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS discoverynoanalyze`)
	table := `CREATE TABLE discoverynoanalyze (
		id int(11) NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `insert into discoverynoanalyze values (1, 'a'), (2, 'b'), (3, 'c')`)
	// Refresh the optimizer stats up front (the writable path) so the estimate
	// is populated; DisableAnalyze must then read it back without re-analyzing.
	testutils.RunSQL(t, `ANALYZE TABLE discoverynoanalyze`)

	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		_ = db.Close()
	}()

	t1 := NewTableInfo(db, "test", "discoverynoanalyze")
	t1.DisableAnalyze = true
	require.NoError(t, t1.SetInfo(t.Context()))

	require.Equal(t, "id", t1.KeyColumns[0])
	require.True(t, t1.KeyIsAutoInc)
	require.Len(t, t1.Columns, 2)
	require.Equal(t, "1", t1.minValue.String())
	require.Equal(t, "3", t1.maxValue.String())
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
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	t1 := NewTableInfo(db, "test", "discoveryuintt1")
	require.NoError(t, t1.SetInfo(t.Context()))

	require.Equal(t, "discoveryuintt1", t1.TableName)
	require.Equal(t, "test", t1.SchemaName)
	require.Equal(t, "id", t1.KeyColumns[0])

	require.Equal(t, "1", t1.minValue.String())
	require.Equal(t, "3", t1.maxValue.String())

	// Can't check estimated rows (depends on MySQL version etc)
	require.Equal(t, []string{"int unsigned"}, t1.keyColumnsMySQLTp)
	require.True(t, t1.KeyIsAutoInc)
	require.Len(t, t1.Columns, 2)
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
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	t1 := NewTableInfo(db, "test", "discoverynokeyst1")
	require.ErrorContains(t, t1.SetInfo(t.Context()), "no primary key found")

	t2 := NewTableInfo(db, "test", "t2fdsfds")
	require.ErrorContains(t, t2.SetInfo(t.Context()), "table test.t2fdsfds does not exist")
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
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	t1 := NewTableInfo(db, "test", "balances")
	require.NoError(t, t1.SetInfo(t.Context()))

	require.True(t, t1.KeyIsAutoInc)
	require.Equal(t, []string{"bigint"}, t1.keyColumnsMySQLTp)
	require.Equal(t, []string{"id"}, t1.KeyColumns)
	require.Equal(t, "0", t1.minValue.String())
	require.Equal(t, "0", t1.maxValue.String())

	chunker, err := NewChunker(t1, ChunkerConfig{TargetChunkTime: 100})
	require.NoError(t, err)

	require.NoError(t, chunker.Open())
	require.Equal(t, "0", t1.minValue.String())
	require.Equal(t, "0", t1.maxValue.String())
}

// TestPrimaryKeyIsMemoryComparableRejectsBIT regresses a gap exposed when
// BIT was reclassified from unknownType to unsignedType in
// mySQLTypeToDatumTp: the change makes BIT-keyed tables pass the original
// unknownType check, but the chunker's setMinMax path returns BIT as raw
// big-endian bytes that newDatumFromMySQL can't parse as decimal. Until
// the min/max read path is BIT-aware, BIT primary keys must be rejected
// upfront with ErrUnsupportedPKType.
func TestPrimaryKeyIsMemoryComparableRejectsBIT(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS bitpk`)
	testutils.RunSQL(t, `CREATE TABLE bitpk (b BIT(8) NOT NULL, v INT NOT NULL, PRIMARY KEY (b))`)
	testutils.RunSQL(t, `INSERT INTO bitpk (b, v) VALUES (b'00000001', 1), (b'00000010', 2)`)

	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	t1 := NewTableInfo(db, "test", "bitpk")
	require.NoError(t, t1.SetInfo(t.Context()))
	require.ErrorIs(t, t1.PrimaryKeyIsMemoryComparable(), ErrUnsupportedPKType)
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
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	t1 := NewTableInfo(db, "test", "compnoncomparable")
	require.NoError(t, t1.SetInfo(t.Context()))         // still discovers the primary key
	require.Error(t, t1.PrimaryKeyIsMemoryComparable()) // but its non comparable
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
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	t1 := NewTableInfo(db, "test", "compcomparable")
	require.NoError(t, t1.SetInfo(t.Context()))

	require.True(t, t1.KeyIsAutoInc)
	require.Equal(t, []string{"int unsigned", "int"}, t1.keyColumnsMySQLTp)
	require.Equal(t, []string{"id", "age"}, t1.KeyColumns)
}

func TestStatisticsUpdate(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	testutils.RunSQL(t, `DROP TABLE IF EXISTS statsupdate`)
	table := `CREATE TABLE statsupdate (
		id int(11) unsigned NOT NULL AUTO_INCREMENT,
		name varchar(255) NOT NULL,
		PRIMARY KEY (id, name)
	)`
	testutils.RunSQL(t, table)
	testutils.RunSQL(t, `insert into statsupdate values (1, 'a'), (2, 'b'), (3, 'c')`)

	t1 := &TableInfo{
		minValue:          Datum{Val: int64(1), Tp: signedType},
		maxValue:          Datum{Val: int64(1000000), Tp: signedType},
		EstimatedRows:     1000000,
		SchemaName:        "test",
		TableName:         "statsupdate",
		QuotedTableName:   "`statsupdate`",
		KeyColumns:        []string{"id"},
		keyColumnsMySQLTp: []string{"int"},
		keyDatums:         []datumTp{signedType},
		KeyIsAutoInc:      true,
		Columns:           []string{"id", "name"},
		db:                db,
	}
	t1.statisticsLastUpdated = time.Now()

	go t1.AutoUpdateStatistics(t.Context(), time.Millisecond*10, slog.Default())
	time.Sleep(time.Millisecond * 100)

	require.NoError(t, t1.Close())
}

func TestKeyColumnsValuesExtraction(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

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
	require.NoError(t, t1.SetInfo(t.Context()))

	var id, age int
	var name string

	err = db.QueryRowContext(t.Context(), "SELECT * FROM `test`.`colvaluest1` ORDER BY id DESC LIMIT 1").Scan(&id, &name, &age)
	require.NoError(t, err)

	row := []any{id, name, age}
	pkVals, err := t1.PrimaryKeyValues(row)
	require.Equal(t, id, pkVals[0])
	require.Equal(t, age, pkVals[1])
	require.NoError(t, err)
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
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close db: %v", err)
		}
	}()

	t1 := NewTableInfo(db, "test", "generatedcolst1")
	require.NoError(t, t1.SetInfo(t.Context()))

	// Can't check estimated rows (depends on MySQL version etc)
	require.Equal(t, []string{"id", "name", "b", "c1", "c2", "c3", "d"}, t1.Columns)
	require.Equal(t, []string{"id", "name", "b", "d"}, t1.NonGeneratedColumns)

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
	require.NoError(t, t2.SetInfo(t.Context()))

	// Can't check estimated rows (depends on MySQL version etc)
	require.Equal(t, []string{"id", "pa", "p1", "p2", "s1", "s2", "s3", "s4"}, t2.Columns)
	require.Equal(t, []string{"id", "pa", "p1", "p2"}, t2.NonGeneratedColumns)
}

func TestGetColumnOrdinal(t *testing.T) {
	// Create a simple TableInfo for testing
	t1 := NewTableInfo(nil, "test", "testtable")
	t1.Columns = []string{"id", "name", "age", "email"}

	// Test finding existing columns
	ordinal, err := t1.GetColumnOrdinal("id")
	require.NoError(t, err)
	require.Equal(t, 0, ordinal)

	ordinal, err = t1.GetColumnOrdinal("name")
	require.NoError(t, err)
	require.Equal(t, 1, ordinal)

	ordinal, err = t1.GetColumnOrdinal("age")
	require.NoError(t, err)
	require.Equal(t, 2, ordinal)

	ordinal, err = t1.GetColumnOrdinal("email")
	require.NoError(t, err)
	require.Equal(t, 3, ordinal)

	// Test finding non-existent column
	ordinal, err = t1.GetColumnOrdinal("nonexistent")
	require.Error(t, err)
	require.Equal(t, -1, ordinal)
	require.ErrorContains(t, err, "column nonexistent not found in table testtable")
}

func TestGetNonGeneratedColumnOrdinal(t *testing.T) {
	// Create a TableInfo with generated columns
	t1 := NewTableInfo(nil, "test", "testtable")
	// All columns including generated ones
	t1.Columns = []string{"id", "name", "name_reversed", "age", "email"}
	// Only non-generated columns (name_reversed is generated)
	t1.NonGeneratedColumns = []string{"id", "name", "age", "email"}

	// Test finding existing non-generated columns
	// Note: ordinals are relative to NonGeneratedColumns, not Columns
	ordinal, err := t1.GetNonGeneratedColumnOrdinal("id")
	require.NoError(t, err)
	require.Equal(t, 0, ordinal, "id should be at position 0 in NonGeneratedColumns")

	ordinal, err = t1.GetNonGeneratedColumnOrdinal("name")
	require.NoError(t, err)
	require.Equal(t, 1, ordinal, "name should be at position 1 in NonGeneratedColumns")

	ordinal, err = t1.GetNonGeneratedColumnOrdinal("age")
	require.NoError(t, err)
	require.Equal(t, 2, ordinal, "age should be at position 2 in NonGeneratedColumns (skipping name_reversed)")

	ordinal, err = t1.GetNonGeneratedColumnOrdinal("email")
	require.NoError(t, err)
	require.Equal(t, 3, ordinal, "email should be at position 3 in NonGeneratedColumns")

	// Test finding a generated column (should fail)
	ordinal, err = t1.GetNonGeneratedColumnOrdinal("name_reversed")
	require.Error(t, err)
	require.Equal(t, -1, ordinal)
	require.ErrorContains(t, err, "column name_reversed not found in non-generated columns of table testtable")

	// Test finding non-existent column
	ordinal, err = t1.GetNonGeneratedColumnOrdinal("nonexistent")
	require.Error(t, err)
	require.Equal(t, -1, ordinal)
	require.ErrorContains(t, err, "column nonexistent not found in non-generated columns of table testtable")
}

func TestQualifiedName(t *testing.T) {
	// Without Host, returns schema.table
	ti := &TableInfo{SchemaName: "mydb", TableName: "orders"}
	require.Equal(t, "mydb.orders", ti.QualifiedName())

	// With Host, returns host.schema.table
	ti.Host = "server1:3306"
	require.Equal(t, "server1:3306.mydb.orders", ti.QualifiedName())

	// Two tables with same schema.table but different hosts are distinct
	ti2 := &TableInfo{SchemaName: "mydb", TableName: "orders", Host: "server2:3306"}
	require.NotEqual(t, ti.QualifiedName(), ti2.QualifiedName())

	// Two tables with same name, no host, different schemas are distinct
	ti3 := &TableInfo{SchemaName: "db1", TableName: "t1"}
	ti4 := &TableInfo{SchemaName: "db2", TableName: "t1"}
	require.NotEqual(t, ti3.QualifiedName(), ti4.QualifiedName())
}

// TestPrimaryKeyValuesNonSliceRow covers the checked type-assertion error path
// in PrimaryKeyValues: a row image that is not a []any must produce a clean
// error rather than panic. No DB is needed; the column/key metadata is set by
// hand.
func TestPrimaryKeyValuesNonSliceRow(t *testing.T) {
	ti := &TableInfo{
		SchemaName: "test",
		TableName:  "t1",
		Columns:    []string{"id", "name"},
		KeyColumns: []string{"id"},
	}

	// A non-[]any row must return an error, not panic.
	_, err := ti.PrimaryKeyValues("not-a-slice")
	require.ErrorContains(t, err, "expected []any row")

	_, err = ti.PrimaryKeyValues(42)
	require.ErrorContains(t, err, "expected []any row")

	// A []any shorter than the table's column count must return an error, not
	// panic with index-out-of-range.
	_, err = ti.PrimaryKeyValues([]any{int64(7)})
	require.ErrorContains(t, err, "fewer than")

	// Sanity: a well-formed []any row extracts the PK value.
	pk, err := ti.PrimaryKeyValues([]any{int64(7), "alice"})
	require.NoError(t, err)
	require.Equal(t, []any{int64(7)}, pk)
}

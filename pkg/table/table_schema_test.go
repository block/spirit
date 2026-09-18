package table

import (
	"database/sql"
	"regexp"
	"strings"
	"testing"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestLoadSchemaFromDB(t *testing.T) {
	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE users (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		name varchar(100) DEFAULT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE orders (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		user_id bigint unsigned NOT NULL,
		amount decimal(10,2) NOT NULL,
		PRIMARY KEY (id),
		KEY idx_user_id (user_id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)

	db, err := sql.Open("block-mysql", testutils.DSNForDatabase(dbName))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	tables, err := LoadSchemaFromDB(t.Context(), db)
	require.NoError(t, err)
	require.Len(t, tables, 2)

	// Build a map for easier assertions
	byName := make(map[string]TableSchema)
	for _, ts := range tables {
		byName[ts.Name] = ts
	}

	require.Contains(t, byName, "users")
	require.Contains(t, byName, "orders")
	require.Contains(t, byName["users"].Schema, "CREATE TABLE")
	require.Contains(t, byName["users"].Schema, "`name` varchar(100)")
	require.Contains(t, byName["orders"].Schema, "`amount` decimal(10,2)")
}

func TestLoadSchemaFromDB_EmptyDatabase(t *testing.T) {
	dbName, _ := testutils.CreateUniqueTestDatabase(t)

	db, err := sql.Open("block-mysql", testutils.DSNForDatabase(dbName))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	tables, err := LoadSchemaFromDB(t.Context(), db)
	require.NoError(t, err)
	require.Empty(t, tables)
}

func TestLoadSchemaFromDB_PreservesAutoIncrement(t *testing.T) {
	// Verify that LoadSchemaFromDB returns the raw DDL including AUTO_INCREMENT
	// values. Consumers that need to strip it (e.g. for diffing) do so themselves.
	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE counters (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	) ENGINE=InnoDB AUTO_INCREMENT=1000 DEFAULT CHARSET=utf8mb4`)

	db, err := sql.Open("block-mysql", testutils.DSNForDatabase(dbName))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	tables, err := LoadSchemaFromDB(t.Context(), db)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	require.Equal(t, "counters", tables[0].Name)
	require.Contains(t, tables[0].Schema, "AUTO_INCREMENT=")
}

func TestLoadSchemaFromDB_FilterUnderscoreTables(t *testing.T) {
	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE users (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE _vt_shadow (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE _pending_drops (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB`)

	db, err := sql.Open("block-mysql", testutils.DSNForDatabase(dbName))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Without filter: all 3 tables returned.
	all, err := LoadSchemaFromDB(t.Context(), db)
	require.NoError(t, err)
	require.Len(t, all, 3)

	// With underscore filter: only "users" returned.
	filtered, err := LoadSchemaFromDB(t.Context(), db, WithoutUnderscoreTables)
	require.NoError(t, err)
	require.Len(t, filtered, 1)
	require.Equal(t, "users", filtered[0].Name)
}

func TestLoadSchemaFromDB_FilterArchiveTables(t *testing.T) {
	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE users (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE users_archive_2024 (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE orders_archive_2024_01 (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE logs_archive_2024_01_15 (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB`)

	db, err := sql.Open("block-mysql", testutils.DSNForDatabase(dbName))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Without filter: all 4 tables returned.
	all, err := LoadSchemaFromDB(t.Context(), db)
	require.NoError(t, err)
	require.Len(t, all, 4)

	// With archive filter: only "users" returned.
	filtered, err := LoadSchemaFromDB(t.Context(), db, WithoutArchiveTables)
	require.NoError(t, err)
	require.Len(t, filtered, 1)
	require.Equal(t, "users", filtered[0].Name)
}

// WithStrippedAutoIncrement must remove the counter and nothing else: a table
// that carries one and a table that does not have to come back in the same
// format, or one schema read returns two different formats. The DDL MySQL emits
// is multi-line, and it stays that way.
func TestLoadSchemaFromDB_StripAutoIncrement(t *testing.T) {
	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE counters (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		note varchar(64) DEFAULT 'auto_increment=999',
		PRIMARY KEY (id)
	) ENGINE=InnoDB AUTO_INCREMENT=1000 DEFAULT CHARSET=utf8mb4 COMMENT='keep auto_increment=999'`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE plain (
		id bigint unsigned NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)

	db, err := sql.Open("block-mysql", testutils.DSNForDatabase(dbName))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	raw, err := LoadSchemaFromDB(t.Context(), db)
	require.NoError(t, err)
	stripped, err := LoadSchemaFromDB(t.Context(), db, WithStrippedAutoIncrement)
	require.NoError(t, err)
	rawByName := schemasByName(raw)
	strippedByName := schemasByName(stripped)
	require.Len(t, strippedByName, 2)

	// The table with no counter round-trips byte for byte.
	require.Equal(t, rawByName["plain"], strippedByName["plain"])
	require.Contains(t, rawByName["plain"], "\n")

	// The table with a counter loses only AUTO_INCREMENT=N.
	counterRegexp := regexp.MustCompile(` AUTO_INCREMENT=\d+`)
	require.Contains(t, rawByName["counters"], " AUTO_INCREMENT=")
	require.Equal(t, counterRegexp.ReplaceAllString(rawByName["counters"], ""), strippedByName["counters"])

	// The column-level attribute and the literals spelling the counter survive,
	// and the DDL is still the multi-line form SHOW CREATE TABLE emitted.
	require.Contains(t, strippedByName["counters"], "`id` bigint unsigned NOT NULL AUTO_INCREMENT")
	require.Contains(t, strippedByName["counters"], "DEFAULT 'auto_increment=999'")
	require.Contains(t, strippedByName["counters"], "COMMENT='keep auto_increment=999'")
	require.Equal(t,
		strings.Count(rawByName["counters"], "\n"),
		strings.Count(strippedByName["counters"], "\n"))
}

func schemasByName(tables []TableSchema) map[string]string {
	byName := make(map[string]string, len(tables))
	for _, ts := range tables {
		byName[ts.Name] = ts.Schema
	}
	return byName
}

func TestLoadSchemaFromDB_CombinedFilters(t *testing.T) {
	dbName, _ := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE users (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	) ENGINE=InnoDB AUTO_INCREMENT=500 DEFAULT CHARSET=utf8mb4`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE _shadow (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB`)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE users_archive_2024 (id bigint NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB`)

	db, err := sql.Open("block-mysql", testutils.DSNForDatabase(dbName))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	filtered, err := LoadSchemaFromDB(t.Context(), db,
		WithoutUnderscoreTables,
		WithoutArchiveTables,
		WithStrippedAutoIncrement,
	)
	require.NoError(t, err)
	require.Len(t, filtered, 1)
	require.Equal(t, "users", filtered[0].Name)
	require.NotContains(t, filtered[0].Schema, "AUTO_INCREMENT=")
}

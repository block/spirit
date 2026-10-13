package lint

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestLoadSchemaFromDir_Basic(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, dir, "users.sql", `CREATE TABLE users (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		name varchar(100) DEFAULT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	writeFile(t, dir, "orders.sql", `CREATE TABLE orders (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		user_id bigint unsigned NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	tables, err := LoadSchemaFromDir(dir)
	require.NoError(t, err)
	require.Len(t, tables, 2)

	// Verify table names are parsed
	names := make(map[string]bool)
	for _, ct := range tables {
		names[ct.TableName] = true
	}
	require.True(t, names["users"])
	require.True(t, names["orders"])
}

func TestLoadSchemaFromDir_SkipsNonSQL(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, dir, "users.sql", `CREATE TABLE users (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	writeFile(t, dir, "README.md", "This is not SQL")
	writeFile(t, dir, "notes.txt", "This is not SQL either")

	tables, err := LoadSchemaFromDir(dir)
	require.NoError(t, err)
	require.Len(t, tables, 1)
	require.Equal(t, "users", tables[0].TableName)
}

func TestLoadSchemaFromDir_SkipsDirectories(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, dir, "users.sql", `CREATE TABLE users (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	// Create a subdirectory
	require.NoError(t, os.Mkdir(filepath.Join(dir, "subdir"), 0o755))

	tables, err := LoadSchemaFromDir(dir)
	require.NoError(t, err)
	require.Len(t, tables, 1)
}

func TestLoadSchemaFromDir_Empty(t *testing.T) {
	dir := t.TempDir()

	tables, err := LoadSchemaFromDir(dir)
	require.NoError(t, err)
	require.Empty(t, tables)
}

func TestLoadSchemaFromDir_InvalidSQL(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, dir, "bad.sql", "THIS IS NOT VALID SQL")

	_, err := LoadSchemaFromDir(dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "bad.sql")
}

func TestLoadSchemaFromDir_NonexistentDir(t *testing.T) {
	_, err := LoadSchemaFromDir("/nonexistent/path")
	require.Error(t, err)
}

func TestLoadSchemaFromDir_CaseInsensitiveExtension(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, dir, "users.SQL", `CREATE TABLE users (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	writeFile(t, dir, "orders.Sql", `CREATE TABLE orders (
		id bigint unsigned NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`)

	tables, err := LoadSchemaFromDir(dir)
	require.NoError(t, err)
	require.Len(t, tables, 2)
}

// writeFile is a test helper that creates a file with the given content.
func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644)
	require.NoError(t, err)
}

func TestLoadSchemaFromDSN(t *testing.T) {
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	_, err := db.ExecContext(t.Context(), "CREATE TABLE users (id bigint unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, name varchar(100))")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "CREATE TABLE orders (id bigint unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY)")
	require.NoError(t, err)

	tables, err := LoadSchemaFromDSN(t.Context(), testutils.DSNForDatabase(dbName))
	require.NoError(t, err)
	names := make([]string, 0, len(tables))
	for _, ct := range tables {
		names = append(names, ct.TableName)
	}
	require.ElementsMatch(t, []string{"users", "orders"}, names)

	// loadServerSchema reads lower_case_table_names over the same connection.
	// The expected value is the server's own, so the test holds on servers
	// configured with any setting.
	var want int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@lower_case_table_names").Scan(&want))
	tables, lowerCaseTableNames, err := loadServerSchema(t.Context(), testutils.DSNForDatabase(dbName))
	require.NoError(t, err)
	require.Len(t, tables, 2)
	require.Equal(t, want, lowerCaseTableNames)
}

func TestLoadSchemaFromDSN_BadDSN(t *testing.T) {
	_, err := LoadSchemaFromDSN(t.Context(), "not a dsn")
	require.Error(t, err)
}

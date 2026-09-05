package fmt

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := testutils.DSN()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// Unit tests for parseAndPrepare

func TestParseAndPrepare(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		wantTable     string
		wantErr       bool
		wantSchemaStr bool // if true, execSQL should differ from input (schema stripped)
	}{
		{
			name:      "simple",
			input:     "CREATE TABLE `users` (id INT)",
			wantTable: "users",
		},
		{
			name:      "unquoted",
			input:     "CREATE TABLE users (id INT)",
			wantTable: "users",
		},
		{
			name:      "if not exists",
			input:     "CREATE TABLE IF NOT EXISTS `orders` (id INT)",
			wantTable: "orders",
		},
		{
			name:          "schema qualified",
			input:         "CREATE TABLE mydb.mytable (id INT)",
			wantTable:     "mytable",
			wantSchemaStr: true,
		},
		{
			name:          "schema qualified backtick",
			input:         "CREATE TABLE `mydb`.`mytable` (id INT)",
			wantTable:     "mytable",
			wantSchemaStr: true,
		},
		{
			name:    "not create table",
			input:   "ALTER TABLE users ADD COLUMN x INT",
			wantErr: true,
		},
		{
			name:    "invalid SQL",
			input:   "THIS IS NOT VALID SQL",
			wantErr: true,
		},
		{
			name:    "multiple statements",
			input:   "CREATE TABLE a (id INT); CREATE TABLE b (id INT);",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tableName, execSQL, err := parseAndPrepare(tc.input)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantTable, tableName)
			if tc.wantSchemaStr {
				// Schema should be stripped from execSQL
				require.NotContains(t, execSQL, "mydb")
				require.Contains(t, execSQL, tc.wantTable)
			} else {
				// Without schema qualifier, original SQL is used as-is
				require.Equal(t, tc.input, execSQL)
			}
		})
	}
}

// Unit tests for collectSQLFiles

func TestCollectSQLFiles(t *testing.T) {
	dir := t.TempDir()

	// Create some files.
	writeTestFile(t, dir, "users.sql", "CREATE TABLE users (id INT)")
	writeTestFile(t, dir, "orders.SQL", "CREATE TABLE orders (id INT)")
	writeTestFile(t, dir, "README.md", "not sql")
	require.NoError(t, os.Mkdir(filepath.Join(dir, "subdir"), 0o755))

	t.Run("directory", func(t *testing.T) {
		files, err := collectSQLFiles([]string{dir})
		require.NoError(t, err)
		require.Len(t, files, 2)
	})

	t.Run("single file", func(t *testing.T) {
		files, err := collectSQLFiles([]string{filepath.Join(dir, "users.sql")})
		require.NoError(t, err)
		require.Len(t, files, 1)
	})

	t.Run("non-sql file silently skipped", func(t *testing.T) {
		files, err := collectSQLFiles([]string{filepath.Join(dir, "README.md")})
		require.NoError(t, err)
		require.Empty(t, files)
	})

	t.Run("mixed glob", func(t *testing.T) {
		// Simulates `spirit fmt *` in a directory with mixed file types.
		files, err := collectSQLFiles([]string{
			filepath.Join(dir, "users.sql"),
			filepath.Join(dir, "README.md"),
			filepath.Join(dir, "orders.SQL"),
		})
		require.NoError(t, err)
		require.Len(t, files, 2)
	})

	t.Run("nonexistent path", func(t *testing.T) {
		_, err := collectSQLFiles([]string{"/nonexistent/path"})
		require.Error(t, err)
	})

	t.Run("empty directory", func(t *testing.T) {
		emptyDir := t.TempDir()
		files, err := collectSQLFiles([]string{emptyDir})
		require.NoError(t, err)
		require.Empty(t, files)
	})
}

func TestBuildDSN(t *testing.T) {
	require.Equal(t, "root@tcp(127.0.0.1:3306)/test", buildDSN("root", "", "127.0.0.1:3306", "test"))
	require.Equal(t, "user:pass@tcp(host:3306)/db", buildDSN("user", "pass", "host:3306", "db"))
}

// Integration tests — require a running MySQL server.

func TestCanonicalize_Boolean(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	// MySQL converts BOOLEAN to tinyint(1) and FALSE to '0'.
	input := "CREATE TABLE `bool_test` (\n  `id` int NOT NULL,\n  `active` boolean DEFAULT FALSE,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

	canonical, err := canonicalize(ctx, db, input)
	require.NoError(t, err)

	// The canonical form should have tinyint(1) instead of boolean.
	require.Contains(t, canonical, "tinyint(1)")
	require.NotContains(t, canonical, "boolean")
	// Table name should be preserved.
	require.Contains(t, canonical, "`bool_test`")
	// Should end with newline.
	require.Equal(t, byte('\n'), canonical[len(canonical)-1])
}

func TestCanonicalize_NoChange(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	// Create a table, get the canonical form, then canonicalize it again.
	// The second pass should produce the same result.
	input := "CREATE TABLE `stable_test` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(100) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

	first, err := canonicalize(ctx, db, input)
	require.NoError(t, err)

	second, err := canonicalize(ctx, db, first)
	require.NoError(t, err)

	require.Equal(t, first, second, "canonicalize should be idempotent")
}

func TestCanonicalize_Serial(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	// SERIAL is an alias for BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE.
	input := "CREATE TABLE `serial_test` (\n  `id` SERIAL,\n  `name` varchar(100) DEFAULT NULL\n) ENGINE=InnoDB"

	canonical, err := canonicalize(ctx, db, input)
	require.NoError(t, err)

	require.Contains(t, canonical, "bigint unsigned")
	require.Contains(t, canonical, "`serial_test`")
}

func TestCanonicalize_PreservesTableName(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	input := "CREATE TABLE `my_important_table` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB"

	canonical, err := canonicalize(ctx, db, input)
	require.NoError(t, err)

	require.Contains(t, canonical, "`my_important_table`")
}

func TestCanonicalize_StripsAutoIncrement(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	input := "CREATE TABLE `autoinc_test` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB AUTO_INCREMENT=12345"

	canonical, err := canonicalize(ctx, db, input)
	require.NoError(t, err)

	require.NotContains(t, canonical, "AUTO_INCREMENT=")
	require.NotContains(t, canonical, "12345")
	// But the column should still have AUTO_INCREMENT.
	require.Contains(t, canonical, "AUTO_INCREMENT")
}

func TestCanonicalize_SchemaQualified(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	// A CREATE TABLE with a database qualifier should have it stripped
	// and still be canonicalized correctly.
	input := "CREATE TABLE `somedb`.`qualified_test` (\n  `id` int NOT NULL,\n  `active` boolean DEFAULT FALSE,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB"

	canonical, err := canonicalize(ctx, db, input)
	require.NoError(t, err)

	// Schema qualifier should not appear in canonical output.
	require.NotContains(t, canonical, "somedb")
	// Table name should be preserved.
	require.Contains(t, canonical, "`qualified_test`")
	// Boolean should be canonicalized.
	require.Contains(t, canonical, "tinyint(1)")
}

func TestCanonicalize_InvalidSQL(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	_, err := canonicalize(ctx, db, "THIS IS NOT VALID SQL")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a valid CREATE TABLE")
}

func TestCanonicalize_NotCreateTable(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	_, err := canonicalize(ctx, db, "ALTER TABLE users ADD COLUMN x INT")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a valid CREATE TABLE")
}

func TestFormatFile_Changed(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	dir := t.TempDir()

	// Write a file with BOOLEAN (non-canonical).
	path := filepath.Join(dir, "test.sql")
	writeTestFile(t, dir, "test.sql", "CREATE TABLE `fmt_file_test` (\n  `id` int NOT NULL,\n  `active` boolean DEFAULT FALSE,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")

	changed, err := formatFile(ctx, db, path)
	require.NoError(t, err)
	require.True(t, changed, "file should have been changed")

	// Read back and verify.
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(content), "tinyint(1)")
	require.NotContains(t, string(content), "boolean")
}

func TestFormatFile_Unchanged(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	dir := t.TempDir()

	// First, get the canonical form.
	input := "CREATE TABLE `fmt_unchanged` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	canonical, err := canonicalize(ctx, db, input)
	require.NoError(t, err)

	// Write the canonical form to a file.
	path := filepath.Join(dir, "test.sql")
	require.NoError(t, os.WriteFile(path, []byte(canonical), 0o644))

	changed, err := formatFile(ctx, db, path)
	require.NoError(t, err)
	require.False(t, changed, "file should not have been changed")
}

func TestFormatFile_InvalidSQL(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	dir := t.TempDir()

	path := filepath.Join(dir, "bad.sql")
	writeTestFile(t, dir, "bad.sql", "THIS IS NOT VALID SQL")

	_, err := formatFile(ctx, db, path)
	require.Error(t, err)
}

func TestFormatFile_Directory(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	dir := t.TempDir()

	// Write two files: one canonical, one not.
	canonical := "CREATE TABLE `already_good` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	canonicalForm, err := canonicalize(ctx, db, canonical)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "good.sql"), []byte(canonicalForm), 0o644))

	writeTestFile(t, dir, "needs_fix.sql", "CREATE TABLE `needs_fix` (\n  `id` int NOT NULL,\n  `flag` boolean DEFAULT FALSE,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")

	// Collect and format all files.
	files, err := collectSQLFiles([]string{dir})
	require.NoError(t, err)
	require.Len(t, files, 2)

	var changedFiles []string
	for _, f := range files {
		changed, err := formatFile(ctx, db, f)
		require.NoError(t, err)
		if changed {
			changedFiles = append(changedFiles, f)
		}
	}

	require.Len(t, changedFiles, 1)
	require.Contains(t, changedFiles[0], "needs_fix.sql")
}

func writeTestFile(t *testing.T, dir, name, content string) {
	t.Helper()
	err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644)
	require.NoError(t, err)
}

// Formatting must not change SQL literals or identifiers that happen to look
// like a table counter, even when the statement also contains a real counter.
func TestFormatFile_PreservesAutoIncrementLiterals(t *testing.T) {
	tests := []struct {
		name   string
		option string
	}{
		{name: "without table counter"},
		{name: "with table counter", option: " AUTO_INCREMENT=98765"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := testDB(t)
			input := "CREATE TABLE `fmt_AUTO_INCREMENT=123` (" +
				"id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
				"val VARCHAR(100) DEFAULT 'AUTO_INCREMENT=123' COMMENT 'keep AUTO_INCREMENT = 456', " +
				"`AUTO_INCREMENT=789` INT DEFAULT 1" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci" + tt.option + " COMMENT='keep auto_increment=321'"
			dir := t.TempDir()
			path := filepath.Join(dir, "schema.sql")
			writeTestFile(t, dir, "schema.sql", input)
			_, err := formatFile(t.Context(), db, path)
			require.NoError(t, err)
			content, err := os.ReadFile(path)
			require.NoError(t, err)
			canonical := string(content)
			for _, preserved := range []string{"`fmt_AUTO_INCREMENT=123`", "DEFAULT 'AUTO_INCREMENT=123'", "COMMENT 'keep AUTO_INCREMENT = 456'", "`AUTO_INCREMENT=789`", "COMMENT='keep auto_increment=321'"} {
				require.Contains(t, canonical, preserved)
			}
			require.NotContains(t, canonical, "98765")
			tt := testutils.NewTestTable(t, "fmt_AUTO_INCREMENT=123", canonical)
			_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO `fmt_AUTO_INCREMENT=123` () VALUES ()")
			require.NoError(t, err)
			var id int
			var val string
			require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT id, val FROM `fmt_AUTO_INCREMENT=123`").Scan(&id, &val))
			require.Equal(t, 1, id)
			require.Equal(t, "AUTO_INCREMENT=123", val)
			changed, err := formatFile(t.Context(), db, path)
			require.NoError(t, err)
			after, readErr := os.ReadFile(path)
			require.NoError(t, readErr)
			require.Equal(t, canonical, string(after))
			require.False(t, changed)
			require.True(t, strings.HasSuffix(canonical, "\n"))
		})
	}
}

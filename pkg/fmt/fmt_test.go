package fmt

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// testDB opens the test server the way FmtCmd.Run does, with dbconn and
// formatSession, so canonicalize runs under the production session settings
// (sql_mode, time_zone, charset).
func testDB(t *testing.T) *sql.Conn {
	t.Helper()
	cfg := dbconn.NewDBConfig()
	cfg.MaxOpenConnections = 1
	db, err := dbconn.New(testutils.DSN(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	conn, err := formatSession(t.Context(), db)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// Unit tests for parseAndPrepare

func TestParseAndPrepare(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		wantTable     string
		wantErr       bool
		wantSchemaStr bool // if true, execSQL should differ from input (schema stripped)
		wantReset     bool
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
			name:      "unqualified counter preserves input",
			input:     "CREATE TABLE `counter` (`id` bigint AUTO_INCREMENT PRIMARY KEY, `note` varchar(255) DEFAULT 'AUTO_INCREMENT=42') ENGINE=InnoDB AUTO_INCREMENT=123 COMMENT='keep AUTO_INCREMENT=999'",
			wantTable: "counter",
			wantReset: true,
		},
		{
			name:          "qualified counter strips only schema",
			input:         "CREATE TABLE `mydb`.`counter` (`id` bigint AUTO_INCREMENT PRIMARY KEY, `note` varchar(255) DEFAULT 'AUTO_INCREMENT=42') ENGINE=InnoDB AUTO_INCREMENT=123 COMMENT='keep AUTO_INCREMENT=999'",
			wantTable:     "counter",
			wantSchemaStr: true,
			wantReset:     true,
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
			tableName, execSQL, resetAutoIncrement, err := parseAndPrepare(tc.input)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantTable, tableName)
			require.Equal(t, tc.wantReset, resetAutoIncrement)
			if tc.wantSchemaStr {
				// Schema should be stripped from execSQL
				require.NotContains(t, execSQL, "mydb")
				require.Contains(t, execSQL, tc.wantTable)
			} else {
				// Without schema qualifier, original SQL is used as-is
				require.Equal(t, tc.input, execSQL)
			}
			if tc.wantReset {
				require.Contains(t, execSQL, "AUTO_INCREMENT=42")
				require.Contains(t, execSQL, "AUTO_INCREMENT=999")
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
	require.Equal(t, "root@tcp(127.0.0.1:3306)/", buildDSN("root", "", "127.0.0.1:3306", ""))

	// Every field must parse back intact, including a password using the
	// DSN's own delimiters and a database name the DSN would otherwise read
	// as parameters ('?') or an escape ('%').
	for _, tc := range []struct{ password, database string }{
		{"p@ss", "db"},
		{"p:ss", "db"},
		{"p/ss", "db"},
		{"a@b:c/d)e?f", "db"},
		{"pass", "spirit_fmt?x=1"},
		{"pass", "spirit%20fmt"},
		{"pass", "spirit/fmt"},
	} {
		cfg, err := mysql.ParseDSN(buildDSN("user", tc.password, "host:3306", tc.database))
		require.NoError(t, err, tc)
		require.Equal(t, "user", cfg.User)
		require.Equal(t, tc.password, cfg.Passwd)
		require.Equal(t, "tcp", cfg.Net)
		require.Equal(t, "host:3306", cfg.Addr)
		require.Equal(t, tc.database, cfg.DBName)
	}
}

// TestCanonicalize_SpiritSQLMode pins a decision: fmt canonicalizes under
// spirit's session sql_mode (NO_AUTO_VALUE_ON_ZERO) plus formatSQLMode. The
// zero-date checks stay off, so a zero-date default is accepted and
// canonicalized the way spirit sees it.
func TestCanonicalize_SpiritSQLMode(t *testing.T) {
	db := testDB(t)
	var sqlMode string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@SESSION.sql_mode").Scan(&sqlMode))
	require.Equal(t, "NO_AUTO_VALUE_ON_ZERO,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION", sqlMode)

	input := "CREATE TABLE `fmt_zero_date` (\n  `id` int NOT NULL,\n  `d` date NOT NULL DEFAULT '0000-00-00',\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB"
	canonical, err := canonicalize(t.Context(), db, input)
	require.NoError(t, err)
	require.Contains(t, canonical, "`d` date NOT NULL DEFAULT '0000-00-00'")
}

// TestCanonicalize_RejectsDefinitionsTheServerWouldRewrite checks that a
// definition MySQL only accepts by changing it is an error, never a different
// table written back into the user's file.
func TestCanonicalize_RejectsDefinitionsTheServerWouldRewrite(t *testing.T) {
	db := testDB(t)
	for name, input := range map[string]string{
		"index too long":  "CREATE TABLE `fmt_long_key` (\n  `id` int NOT NULL,\n  `a` varchar(2000) CHARACTER SET utf8mb4 DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `a` (`a`)\n) ENGINE=InnoDB",
		"varchar too big": "CREATE TABLE `fmt_big_varchar` (\n  `id` int NOT NULL,\n  `b` varchar(70000) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB",
		"unknown engine":  "CREATE TABLE `fmt_bad_engine` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=NDBCLUSTER",
	} {
		t.Run(name, func(t *testing.T) {
			canonical, err := canonicalize(t.Context(), db, input)
			require.Error(t, err, canonical)
		})
	}
}

// TestFmtCmd_Run runs the command end to end, as a user whose password uses
// characters the DSN syntax reserves, against a working database whose name
// contains a '?'. The DSN used to be built by concatenation, which read
// everything after that '?' as driver parameters.
func TestFmtCmd_Run(t *testing.T) {
	rootCfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	rootCfg.User = "root" // CREATE USER and GRANT
	rootCfg.DBName = ""
	rootDB, err := sql.Open(dbconn.DriverName, rootCfg.FormatDSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rootDB.Close() })

	const user = "spirit_fmt_pw_user"
	const password = "p@ss:w/rd)?x"
	const database = "spirit_fmt_dsn?test"
	cleanup := func() {
		_, _ = rootDB.ExecContext(context.Background(), "DROP USER IF EXISTS "+user)
		_, _ = rootDB.ExecContext(context.Background(), "DROP DATABASE IF EXISTS `"+database+"`")
	}
	cleanup()
	t.Cleanup(cleanup)
	_, err = rootDB.ExecContext(t.Context(), "CREATE USER "+user+" IDENTIFIED BY '"+password+"'")
	require.NoError(t, err)
	_, err = rootDB.ExecContext(t.Context(), "GRANT ALL ON `"+database+"`.* TO "+user)
	require.NoError(t, err)

	dir := t.TempDir()
	writeTestFile(t, dir, "flags.sql", "CREATE TABLE `fmt_run_flags` (\n  `id` int NOT NULL,\n  `active` boolean DEFAULT FALSE,\n  `d` date NOT NULL DEFAULT '0000-00-00',\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB")
	cmd := &FmtCmd{
		Files:    []string{dir},
		Host:     rootCfg.Addr,
		Username: user,
		Password: password,
		Database: database,
	}
	require.NoError(t, cmd.Run())
	content, err := os.ReadFile(filepath.Join(dir, "flags.sql"))
	require.NoError(t, err)
	require.Contains(t, string(content), "`active` tinyint(1) DEFAULT '0'")
	require.Contains(t, string(content), "DEFAULT '0000-00-00'")

	// Run formats in strict mode: a definition MySQL would only accept by
	// changing it (here a too-long index, into a prefix index) is an error,
	// and the file is left as it was.
	strictDir := t.TempDir()
	const longKey = "CREATE TABLE `fmt_run_long_key` (\n  `id` int NOT NULL,\n  `a` varchar(2000) CHARACTER SET utf8mb4 DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `a` (`a`)\n) ENGINE=InnoDB"
	writeTestFile(t, strictDir, "long_key.sql", longKey)
	cmd.Files = []string{strictDir}
	require.Error(t, cmd.Run())
	content, err = os.ReadFile(filepath.Join(strictDir, "long_key.sql"))
	require.NoError(t, err)
	require.Equal(t, longKey, string(content))
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

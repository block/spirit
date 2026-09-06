package statement

import (
	"context"
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// Tests for the secondary-index rewriting helpers in secondary_indexes.go.

// TestRemoveSecondaryIndexes tests the RemoveSecondaryIndexes function
func TestRemoveSecondaryIndexes(t *testing.T) {
	testCases := []struct {
		name                string
		sql                 string
		shouldKeepPrimary   bool
		shouldKeepUnique    bool
		shouldKeepFulltext  bool
		shouldRemoveRegular bool
	}{
		{
			name: "Table with all index types",
			sql: `CREATE TABLE test_all (
				id INT,
				email VARCHAR(255),
				name VARCHAR(100),
				description TEXT,
				PRIMARY KEY (id),
				UNIQUE KEY uk_email (email),
				INDEX idx_name (name),
				FULLTEXT idx_description (description)
			)`,
			shouldKeepPrimary:   true,
			shouldKeepUnique:    true,
			shouldKeepFulltext:  true,
			shouldRemoveRegular: true,
		},
		{
			name: "Table with only regular indexes",
			sql: `CREATE TABLE test_regular (
				id INT,
				name VARCHAR(100),
				email VARCHAR(255),
				INDEX idx_name (name),
				INDEX idx_email (email)
			)`,
			shouldRemoveRegular: true,
		},
		{
			name: "Table with no indexes",
			sql: `CREATE TABLE test_no_indexes (
				id INT,
				name VARCHAR(100)
			)`,
		},
		{
			name: "Table with PRIMARY KEY and UNIQUE only",
			sql: `CREATE TABLE test_pk_unique (
				id INT PRIMARY KEY,
				email VARCHAR(255) UNIQUE
			)`,
			shouldKeepPrimary: true,
			shouldKeepUnique:  true,
		},
		{
			name: "Complex table with multiple index types",
			sql: `CREATE TABLE test_complex (
				id BIGINT PRIMARY KEY AUTO_INCREMENT,
				uuid CHAR(36) NOT NULL,
				name VARCHAR(255) NOT NULL,
				description TEXT,
				content TEXT,
				email VARCHAR(255),
				status VARCHAR(50),
				created_at TIMESTAMP,
				
				UNIQUE KEY uk_uuid (uuid),
				INDEX idx_name (name),
				INDEX idx_status (status) INVISIBLE,
				INDEX idx_created (created_at) USING BTREE,
				FULLTEXT idx_description (description),
				FULLTEXT idx_content (content) WITH PARSER ngram
			)`,
			shouldKeepPrimary:   true,
			shouldKeepUnique:    true,
			shouldKeepFulltext:  true,
			shouldRemoveRegular: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Get original indexes using GetIndexes() which includes column-level indexes
			originalCT, err := ParseCreateTable(tc.sql)
			require.NoError(t, err)
			originalIndexes := originalCT.GetIndexes()

			// Remove secondary indexes
			modifiedSQL, err := RemoveSecondaryIndexes(tc.sql)
			require.NoError(t, err)
			require.NotEmpty(t, modifiedSQL)

			// Parse the modified SQL
			modifiedCT, err := ParseCreateTable(modifiedSQL)
			require.NoError(t, err)
			modifiedIndexes := modifiedCT.GetIndexes()

			// Check that regular indexes were removed
			if tc.shouldRemoveRegular {
				for _, idx := range originalIndexes {
					if idx.Type == "INDEX" {
						// This index should not be in the modified list
						found := false
						for _, modIdx := range modifiedIndexes {
							if modIdx.Name == idx.Name {
								found = true
								break
							}
						}
						require.False(t, found, "Regular index %s should have been removed", idx.Name)
					}
				}
			}

			// Check that PRIMARY KEY is kept
			if tc.shouldKeepPrimary {
				foundPrimary := false
				for _, idx := range modifiedIndexes {
					if idx.Type == "PRIMARY KEY" {
						foundPrimary = true
						break
					}
				}
				require.True(t, foundPrimary, "PRIMARY KEY should be kept")
			}

			// Check that UNIQUE indexes are kept
			if tc.shouldKeepUnique {
				originalUniqueCount := 0
				modifiedUniqueCount := 0
				for _, idx := range originalIndexes {
					if idx.Type == "UNIQUE" {
						originalUniqueCount++
					}
				}
				for _, idx := range modifiedIndexes {
					if idx.Type == "UNIQUE" {
						modifiedUniqueCount++
					}
				}
				require.Equal(t, originalUniqueCount, modifiedUniqueCount, "UNIQUE indexes should be kept")
			}

			// Check that FULLTEXT indexes are kept
			if tc.shouldKeepFulltext {
				originalFulltextCount := 0
				modifiedFulltextCount := 0
				for _, idx := range originalIndexes {
					if idx.Type == "FULLTEXT" {
						originalFulltextCount++
					}
				}
				for _, idx := range modifiedIndexes {
					if idx.Type == "FULLTEXT" {
						modifiedFulltextCount++
					}
				}
				require.Equal(t, originalFulltextCount, modifiedFulltextCount, "FULLTEXT indexes should be kept")
			}

			// Verify the modified SQL is valid by checking it can be parsed
			require.NotEmpty(t, modifiedSQL)
			require.Contains(t, modifiedSQL, "CREATE TABLE")
		})
	}
}

func TestGetMissingSecondaryIndexes(t *testing.T) {
	testCases := []struct {
		name              string
		sourceCreateTable string
		targetCreateTable string
		tableName         string
		expectedAlter     string
		expectEmpty       bool
		expectError       bool
	}{
		{
			name: "No missing indexes - target has all indexes",
			sourceCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				email VARCHAR(255),
				name VARCHAR(100),
				INDEX idx_email (email),
				INDEX idx_name (name)
			)`,
			targetCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				email VARCHAR(255),
				name VARCHAR(100),
				INDEX idx_email (email),
				INDEX idx_name (name)
			)`,
			tableName:   "users",
			expectEmpty: true,
		},
		{
			name: "Single missing index",
			sourceCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				email VARCHAR(255),
				name VARCHAR(100),
				INDEX idx_email (email),
				INDEX idx_name (name)
			)`,
			targetCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				email VARCHAR(255),
				name VARCHAR(100),
				INDEX idx_email (email)
			)`,
			tableName:     "users",
			expectedAlter: "ALTER TABLE `users` ADD INDEX `idx_name` (`name`)",
		},
		{
			name: "Multiple missing indexes",
			sourceCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				email VARCHAR(255),
				name VARCHAR(100),
				status VARCHAR(50),
				INDEX idx_email (email),
				INDEX idx_name (name),
				INDEX idx_status (status)
			)`,
			targetCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				email VARCHAR(255),
				name VARCHAR(100),
				status VARCHAR(50)
			)`,
			tableName:     "users",
			expectedAlter: "ALTER TABLE `users` ADD INDEX `idx_email` (`email`), ADD INDEX `idx_name` (`name`), ADD INDEX `idx_status` (`status`)",
		},
		{
			name: "Missing index with options",
			sourceCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				name VARCHAR(100),
				INDEX idx_name (name) USING BTREE COMMENT 'Name index' INVISIBLE
			)`,
			targetCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				name VARCHAR(100)
			)`,
			tableName:     "users",
			expectedAlter: "ALTER TABLE `users` ADD INDEX `idx_name` (`name`) USING BTREE COMMENT 'Name index' INVISIBLE",
		},
		{
			name: "Missing FULLTEXT index with parser",
			sourceCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				bio TEXT,
				FULLTEXT INDEX ft_bio (bio) WITH PARSER ngram
			)`,
			targetCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				bio TEXT
			)`,
			tableName:     "users",
			expectedAlter: "ALTER TABLE `users` ADD FULLTEXT INDEX `ft_bio` (`bio`) WITH PARSER ngram",
		},
		{
			name: "Composite index missing",
			sourceCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				first_name VARCHAR(100),
				last_name VARCHAR(100),
				INDEX idx_name (first_name, last_name)
			)`,
			targetCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				first_name VARCHAR(100),
				last_name VARCHAR(100)
			)`,
			tableName:     "users",
			expectedAlter: "ALTER TABLE `users` ADD INDEX `idx_name` (`first_name`, `last_name`)",
		},
		{
			name: "Prefix index missing",
			sourceCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				description TEXT,
				INDEX idx_description (description(100))
			)`,
			targetCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				description TEXT
			)`,
			tableName:     "users",
			expectedAlter: "ALTER TABLE `users` ADD INDEX `idx_description` (`description`(100))",
		},
		{
			name: "PRIMARY KEY ignored, regular index detected",
			sourceCreateTable: `CREATE TABLE users (
				id INT,
				email VARCHAR(255),
				name VARCHAR(100),
				PRIMARY KEY (id),
				UNIQUE KEY uk_email (email),
				INDEX idx_name (name)
			)`,
			targetCreateTable: `CREATE TABLE users (
				id INT,
				email VARCHAR(255),
				name VARCHAR(100),
				PRIMARY KEY (id),
				UNIQUE KEY uk_email (email)
			)`,
			tableName:     "users",
			expectedAlter: "ALTER TABLE `users` ADD INDEX `idx_name` (`name`)",
		},
		{
			name: "Missing UNIQUE index",
			sourceCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				email VARCHAR(255),
				name VARCHAR(100),
				UNIQUE KEY uk_email (email),
				INDEX idx_name (name)
			)`,
			targetCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				email VARCHAR(255),
				name VARCHAR(100),
				INDEX idx_name (name)
			)`,
			tableName:     "users",
			expectedAlter: "ALTER TABLE `users` ADD UNIQUE INDEX `uk_email` (`email`)",
		},
		{
			name: "Missing FULLTEXT index",
			sourceCreateTable: `CREATE TABLE articles (
				id INT PRIMARY KEY,
				content TEXT,
				name VARCHAR(100),
				FULLTEXT idx_content (content),
				INDEX idx_name (name)
			)`,
			targetCreateTable: `CREATE TABLE articles (
				id INT PRIMARY KEY,
				content TEXT,
				name VARCHAR(100),
				INDEX idx_name (name)
			)`,
			tableName:     "articles",
			expectedAlter: "ALTER TABLE `articles` ADD FULLTEXT INDEX `idx_content` (`content`)",
		},
		{
			name: "Multiple missing index types (UNIQUE, FULLTEXT, regular INDEX)",
			sourceCreateTable: `CREATE TABLE mixed (
				id INT PRIMARY KEY,
				email VARCHAR(255),
				content TEXT,
				name VARCHAR(100),
				UNIQUE KEY uk_email (email),
				FULLTEXT idx_content (content),
				INDEX idx_name (name)
			)`,
			targetCreateTable: `CREATE TABLE mixed (
				id INT PRIMARY KEY,
				email VARCHAR(255),
				content TEXT,
				name VARCHAR(100)
			)`,
			tableName:     "mixed",
			expectedAlter: "ALTER TABLE `mixed` ADD UNIQUE INDEX `uk_email` (`email`), ADD FULLTEXT INDEX `idx_content` (`content`), ADD INDEX `idx_name` (`name`)",
		},
		{
			name: "Missing SPATIAL index",
			sourceCreateTable: `CREATE TABLE places (
				id INT PRIMARY KEY,
				name VARCHAR(100),
				location GEOMETRY NOT NULL SRID 4326,
				SPATIAL INDEX idx_location (location)
			)`,
			targetCreateTable: `CREATE TABLE places (
				id INT PRIMARY KEY,
				name VARCHAR(100),
				location GEOMETRY NOT NULL SRID 4326
			)`,
			tableName:     "places",
			expectedAlter: "ALTER TABLE `places` ADD SPATIAL INDEX `idx_location` (`location`)",
		},
		{
			name: "No indexes on either table",
			sourceCreateTable: `CREATE TABLE simple (
				id INT PRIMARY KEY,
				name VARCHAR(100)
			)`,
			targetCreateTable: `CREATE TABLE simple (
				id INT PRIMARY KEY,
				name VARCHAR(100)
			)`,
			tableName:   "simple",
			expectEmpty: true,
		},
		{
			name: "Target has more indexes than source",
			sourceCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				email VARCHAR(255),
				INDEX idx_email (email)
			)`,
			targetCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				email VARCHAR(255),
				name VARCHAR(100),
				INDEX idx_email (email),
				INDEX idx_name (name)
			)`,
			tableName:   "users",
			expectEmpty: true,
		},
		{
			name: "Index comment with special characters requiring SQL escaping",
			sourceCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				name VARCHAR(100),
				INDEX idx_name (name) COMMENT 'User''s name index with "quotes" and backslash: \\'
			)`,
			targetCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				name VARCHAR(100)
			)`,
			tableName:     "users",
			expectedAlter: "ALTER TABLE `users` ADD INDEX `idx_name` (`name`) COMMENT 'User\\'s name index with \\\"quotes\\\" and backslash: \\\\'",
		},
		{
			name: "Index comment with SQL injection attempt",
			sourceCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				email VARCHAR(255),
				INDEX idx_email (email) COMMENT 'Email index''; DROP TABLE users; --'
			)`,
			targetCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				email VARCHAR(255)
			)`,
			tableName:     "users",
			expectedAlter: "ALTER TABLE `users` ADD INDEX `idx_email` (`email`) COMMENT 'Email index\\'; DROP TABLE users; --'",
		},
		{
			name:              "Index comment with newline and carriage return",
			sourceCreateTable: "CREATE TABLE users (\n\tid INT PRIMARY KEY,\n\tname VARCHAR(100),\n\tINDEX idx_name (name) COMMENT 'Line 1\nLine 2\rLine 3'\n)",
			targetCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				name VARCHAR(100)
			)`,
			tableName:     "users",
			expectedAlter: "ALTER TABLE `users` ADD INDEX `idx_name` (`name`) COMMENT 'Line 1\\nLine 2\\rLine 3'",
		},
		{
			name:              "Index comment with null byte (should be escaped)",
			sourceCreateTable: "CREATE TABLE users (\n\tid INT PRIMARY KEY,\n\tdata VARCHAR(100),\n\tINDEX idx_data (data) COMMENT 'Data\x00null'\n)",
			targetCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				data VARCHAR(100)
			)`,
			tableName:     "users",
			expectedAlter: "ALTER TABLE `users` ADD INDEX `idx_data` (`data`) COMMENT 'Data\\0null'",
		},
		{
			name:              "Index comment with Ctrl-Z character",
			sourceCreateTable: "CREATE TABLE users (\n\tid INT PRIMARY KEY,\n\tdata VARCHAR(100),\n\tINDEX idx_data (data) COMMENT 'Data\x1aCtrlZ'\n)",
			targetCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				data VARCHAR(100)
			)`,
			tableName:     "users",
			expectedAlter: "ALTER TABLE `users` ADD INDEX `idx_data` (`data`) COMMENT 'Data\\ZCtrlZ'",
		},
		{
			name: "Index comment with multiple special characters",
			sourceCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				name VARCHAR(100),
				INDEX idx_name (name) COMMENT 'Test: apostrophe'' and "quotes" and backslash\\'
			)`,
			targetCreateTable: `CREATE TABLE users (
				id INT PRIMARY KEY,
				name VARCHAR(100)
			)`,
			tableName:     "users",
			expectedAlter: "ALTER TABLE `users` ADD INDEX `idx_name` (`name`) COMMENT 'Test: apostrophe\\' and \\\"quotes\\\" and backslash\\\\'",
		},
		{
			// A backtick inside an index or column name must be doubled,
			// the same way the table name already is, or the generated
			// ALTER TABLE breaks out of the identifier quoting.
			name: "Index and column names containing a backtick are escaped",
			sourceCreateTable: "CREATE TABLE t (\n" +
				"  `c``1` INT,\n" +
				"  INDEX `i``x` (`c``1`)\n" +
				")",
			targetCreateTable: "CREATE TABLE t (\n" +
				"  `c``1` INT\n" +
				")",
			tableName:     "t",
			expectedAlter: "ALTER TABLE `t` ADD INDEX `i``x` (`c``1`)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := GetMissingSecondaryIndexes(tc.sourceCreateTable, tc.targetCreateTable, tc.tableName)

			if tc.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tc.expectEmpty {
				require.Empty(t, result, "Expected empty result but got: %s", result)
			} else {
				require.Equal(t, tc.expectedAlter, result)
			}
		})
	}
}

// TestGetMissingSecondaryIndexes_FunctionalIndexes covers functional
// (expression) index parts, where the TiDB AST sets Expr instead of Column
// on the IndexPartSpecification. Previously this caused a nil-pointer panic.
func TestGetMissingSecondaryIndexes_FunctionalIndexes(t *testing.T) {
	testCases := []struct {
		name              string
		sourceCreateTable string
		targetCreateTable string
		tableName         string
		expectedAlter     string
		expectEmpty       bool
	}{
		{
			name: "Functional index missing on target",
			sourceCreateTable: `CREATE TABLE t1 (
				a INT NOT NULL PRIMARY KEY,
				b VARCHAR(255),
				KEY fidx ((lower(b)))
			)`,
			targetCreateTable: `CREATE TABLE t1 (
				a INT NOT NULL PRIMARY KEY,
				b VARCHAR(255)
			)`,
			tableName:     "t1",
			expectedAlter: "ALTER TABLE `t1` ADD INDEX `fidx` ((LOWER(`b`)))",
		},
		{
			name: "Mixed index with column, expression and prefix parts",
			sourceCreateTable: `CREATE TABLE t1 (
				a INT NOT NULL PRIMARY KEY,
				b VARCHAR(255),
				c VARCHAR(255),
				KEY k (a, (lower(b)), c(10))
			)`,
			targetCreateTable: `CREATE TABLE t1 (
				a INT NOT NULL PRIMARY KEY,
				b VARCHAR(255),
				c VARCHAR(255)
			)`,
			tableName:     "t1",
			expectedAlter: "ALTER TABLE `t1` ADD INDEX `k` (`a`, (LOWER(`b`)), `c`(10))",
		},
		{
			name: "Functional index present on both sides",
			sourceCreateTable: `CREATE TABLE t1 (
				a INT NOT NULL PRIMARY KEY,
				b VARCHAR(255),
				KEY fidx ((lower(b)))
			)`,
			targetCreateTable: `CREATE TABLE t1 (
				a INT NOT NULL PRIMARY KEY,
				b VARCHAR(255),
				KEY fidx ((lower(b)))
			)`,
			tableName:   "t1",
			expectEmpty: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := GetMissingSecondaryIndexes(tc.sourceCreateTable, tc.targetCreateTable, tc.tableName)
			require.NoError(t, err)

			if tc.expectEmpty {
				require.Empty(t, result, "Expected empty result but got: %s", result)
			} else {
				require.Equal(t, tc.expectedAlter, result)
			}
		})
	}
}

// TestGetMissingSecondaryIndexes_FunctionalIndexesLiveMySQL verifies that the
// generated ADD INDEX DDL for functional indexes is valid by executing it
// against a real MySQL server (functional indexes require MySQL 8.0.13+).
// This mirrors the move runner's restoreSecondaryIndexes path: SHOW CREATE
// TABLE output is fed into GetMissingSecondaryIndexes and the resulting ALTER
// is executed on the target.
func TestGetMissingSecondaryIndexes_FunctionalIndexesLiveMySQL(t *testing.T) {
	admin, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS test_w2d")
		require.NoError(t, err)
		require.NoError(t, admin.Close())
	})

	_, err = admin.ExecContext(t.Context(), "DROP DATABASE IF EXISTS test_w2d")
	require.NoError(t, err)
	_, err = admin.ExecContext(t.Context(), "CREATE DATABASE test_w2d")
	require.NoError(t, err)

	db, err := sql.Open("block-mysql", testutils.DSNForDatabase("test_w2d"))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Source table with a functional index and a mixed
	// (column + expression + prefix) index.
	_, err = db.ExecContext(t.Context(), `CREATE TABLE src (
		a INT NOT NULL PRIMARY KEY,
		b VARCHAR(255),
		c VARCHAR(255),
		KEY fidx ((lower(b))),
		KEY k (a, (lower(b)), c(10))
	)`)
	require.NoError(t, err)

	// Target table with the same columns but no secondary indexes,
	// as created during a move before indexes are restored.
	_, err = db.ExecContext(t.Context(), `CREATE TABLE dst (
		a INT NOT NULL PRIMARY KEY,
		b VARCHAR(255),
		c VARCHAR(255)
	)`)
	require.NoError(t, err)

	showCreate := func(table string) string {
		var name, createStmt string
		require.NoError(t, db.QueryRowContext(t.Context(), "SHOW CREATE TABLE `"+table+"`").Scan(&name, &createStmt))
		return createStmt
	}

	alterStmt, err := GetMissingSecondaryIndexes(showCreate("src"), showCreate("dst"), "dst")
	require.NoError(t, err)
	require.NotEmpty(t, alterStmt)

	// Execute the generated DDL against MySQL to prove it is valid.
	_, err = db.ExecContext(t.Context(), alterStmt)
	require.NoError(t, err)

	// After applying, no indexes should be reported missing anymore.
	alterStmt, err = GetMissingSecondaryIndexes(showCreate("src"), showCreate("dst"), "dst")
	require.NoError(t, err)
	require.Empty(t, alterStmt, "expected no missing indexes after applying DDL, got: %s", alterStmt)
}

// TestGetMissingSecondaryIndexes_DescendingIndexes covers DESC key parts
// (MySQL 8.0+). Previously the DESC modifier was dropped from the generated
// ADD INDEX, so a deferred KEY k (a DESC) was silently restored as the
// ascending KEY k (a).
func TestGetMissingSecondaryIndexes_DescendingIndexes(t *testing.T) {
	testCases := []struct {
		name              string
		sourceCreateTable string
		targetCreateTable string
		tableName         string
		expectedAlter     string
		expectEmpty       bool
	}{
		{
			name: "Descending index missing on target",
			sourceCreateTable: `CREATE TABLE t1 (
				a INT NOT NULL PRIMARY KEY,
				b INT,
				c VARCHAR(255),
				KEY k (b DESC, c)
			)`,
			targetCreateTable: `CREATE TABLE t1 (
				a INT NOT NULL PRIMARY KEY,
				b INT,
				c VARCHAR(255)
			)`,
			tableName:     "t1",
			expectedAlter: "ALTER TABLE `t1` ADD INDEX `k` (`b` DESC, `c`)",
		},
		{
			name: "Descending prefix and functional parts missing on target",
			sourceCreateTable: `CREATE TABLE t1 (
				a INT NOT NULL PRIMARY KEY,
				b VARCHAR(255),
				KEY k (b(10) DESC, (lower(b)) DESC)
			)`,
			targetCreateTable: `CREATE TABLE t1 (
				a INT NOT NULL PRIMARY KEY,
				b VARCHAR(255)
			)`,
			tableName:     "t1",
			expectedAlter: "ALTER TABLE `t1` ADD INDEX `k` (`b`(10) DESC, (LOWER(`b`)) DESC)",
		},
		{
			name: "Descending index present on both sides",
			sourceCreateTable: `CREATE TABLE t1 (
				a INT NOT NULL PRIMARY KEY,
				b INT,
				KEY k (b DESC)
			)`,
			targetCreateTable: `CREATE TABLE t1 (
				a INT NOT NULL PRIMARY KEY,
				b INT,
				KEY k (b DESC)
			)`,
			tableName:   "t1",
			expectEmpty: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := GetMissingSecondaryIndexes(tc.sourceCreateTable, tc.targetCreateTable, tc.tableName)
			require.NoError(t, err)

			if tc.expectEmpty {
				require.Empty(t, result, "Expected empty result but got: %s", result)
			} else {
				require.Equal(t, tc.expectedAlter, result)
			}
		})
	}
}

// TestGetMissingSecondaryIndexes_DescendingIndexesLiveMySQL verifies the full
// round trip that spirit move performs for descending indexes: the source's
// SHOW CREATE TABLE (which prints KEY `k` (`a` DESC)) is fed into
// GetMissingSecondaryIndexes, the generated ALTER is executed on the target,
// and the target really ends up with a descending index.
func TestGetMissingSecondaryIndexes_DescendingIndexesLiveMySQL(t *testing.T) {
	admin, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS test_descidx")
		require.NoError(t, err)
		require.NoError(t, admin.Close())
	})

	_, err = admin.ExecContext(t.Context(), "DROP DATABASE IF EXISTS test_descidx")
	require.NoError(t, err)
	_, err = admin.ExecContext(t.Context(), "CREATE DATABASE test_descidx")
	require.NoError(t, err)

	db, err := sql.Open("block-mysql", testutils.DSNForDatabase("test_descidx"))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	// Source table with descending key parts, including a functional one.
	_, err = db.ExecContext(t.Context(), `CREATE TABLE src (
		a INT NOT NULL PRIMARY KEY,
		b INT,
		c VARCHAR(255),
		KEY k (b DESC, c),
		KEY fidx ((lower(c)) DESC)
	)`)
	require.NoError(t, err)

	// Target table with the same columns but no secondary indexes,
	// as created during a move before indexes are restored.
	_, err = db.ExecContext(t.Context(), `CREATE TABLE dst (
		a INT NOT NULL PRIMARY KEY,
		b INT,
		c VARCHAR(255)
	)`)
	require.NoError(t, err)

	showCreate := func(table string) string {
		var name, createStmt string
		require.NoError(t, db.QueryRowContext(t.Context(), "SHOW CREATE TABLE `"+table+"`").Scan(&name, &createStmt))
		return createStmt
	}

	alterStmt, err := GetMissingSecondaryIndexes(showCreate("src"), showCreate("dst"), "dst")
	require.NoError(t, err)
	require.NotEmpty(t, alterStmt)

	// Execute the generated DDL against MySQL to prove it is valid.
	_, err = db.ExecContext(t.Context(), alterStmt)
	require.NoError(t, err)

	// The restored indexes must be descending, not silently ascending.
	dstCreate := showCreate("dst")
	require.Contains(t, dstCreate, "KEY `k` (`b` DESC,`c`)")
	require.Contains(t, dstCreate, "KEY `fidx` ((lower(`c`)) DESC)")

	// After applying, no indexes should be reported missing anymore.
	alterStmt, err = GetMissingSecondaryIndexes(showCreate("src"), showCreate("dst"), "dst")
	require.NoError(t, err)
	require.Empty(t, alterStmt, "expected no missing indexes after applying DDL, got: %s", alterStmt)
}

func TestGetMissingSecondaryIndexes_ErrorCases(t *testing.T) {
	testCases := []struct {
		name              string
		sourceCreateTable string
		targetCreateTable string
		tableName         string
	}{
		{
			name:              "Invalid source CREATE TABLE",
			sourceCreateTable: "INVALID SQL",
			targetCreateTable: "CREATE TABLE users (id INT PRIMARY KEY)",
			tableName:         "users",
		},
		{
			name:              "Invalid target CREATE TABLE",
			sourceCreateTable: "CREATE TABLE users (id INT PRIMARY KEY)",
			targetCreateTable: "INVALID SQL",
			tableName:         "users",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := GetMissingSecondaryIndexes(tc.sourceCreateTable, tc.targetCreateTable, tc.tableName)
			require.Error(t, err)
		})
	}
}

func TestRemoveSecondaryIndexes_ErrorCases(t *testing.T) {
	testCases := []struct {
		name        string
		createTable string
	}{
		{
			name:        "Invalid SQL syntax",
			createTable: "INVALID SQL",
		},
		{
			name:        "Empty string",
			createTable: "",
		},
		{
			name:        "Not a CREATE TABLE statement",
			createTable: "SELECT * FROM users",
		},
		{
			name:        "Incomplete CREATE TABLE",
			createTable: "CREATE TABLE",
		},
		{
			name:        "CREATE TABLE with syntax error",
			createTable: "CREATE TABLE users (id INT PRIMARY KEY,)",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := RemoveSecondaryIndexes(tc.createTable)
			require.Error(t, err, "Expected error for invalid CREATE TABLE statement")
		})
	}
}

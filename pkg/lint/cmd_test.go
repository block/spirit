package lint

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveStatement_Cmdline(t *testing.T) {
	tests := []struct {
		name string
		arg  string
	}{
		{
			name: "CREATE TABLE",
			arg:  "CREATE TABLE users (id BIGINT PRIMARY KEY)",
		},
		{
			name: "ALTER TABLE",
			arg:  "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
		},
		{
			name: "multiline SQL",
			arg:  "CREATE TABLE users (\n  id BIGINT PRIMARY KEY\n)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sources, err := resolveStatement(tt.arg)
			require.NoError(t, err)
			require.Len(t, sources, 1)
			assert.Equal(t, "cmdline", sources[0].Origin)
			assert.Equal(t, tt.arg, sources[0].SQL)
		})
	}
}

func TestResolveStatement_Stdin(t *testing.T) {
	// Mock stdin
	oldStdin := os.Stdin

	defer func() { os.Stdin = oldStdin }()

	r, w, _ := os.Pipe()
	os.Stdin = r

	sql := "CREATE TABLE users (id BIGINT PRIMARY KEY)"

	go func() {
		_, _ = w.WriteString(sql)
		w.Close()
	}()

	sources, err := resolveStatement("-")
	require.NoError(t, err)
	require.Len(t, sources, 1)
	assert.Equal(t, "stdin", sources[0].Origin)
	assert.Equal(t, sql, sources[0].SQL)
}

func TestResolveStatement_File(t *testing.T) {
	// Create a temporary file
	tmpfile, err := os.CreateTemp(t.TempDir(), "test_*.sql")
	require.NoError(t, err)

	defer os.Remove(tmpfile.Name())

	sql := "CREATE TABLE users (id BIGINT PRIMARY KEY)"
	_, err = tmpfile.WriteString(sql)
	require.NoError(t, err)
	tmpfile.Close()

	sources, err := resolveStatement("file:" + tmpfile.Name())
	require.NoError(t, err)
	require.Len(t, sources, 1)
	assert.Equal(t, "file:"+tmpfile.Name(), sources[0].Origin)
	assert.Equal(t, sql, sources[0].SQL)
}

func TestResolveStatement_FileNotExists(t *testing.T) {
	sources, err := resolveStatement("file:/nonexistent/path/to/file.sql")
	assert.Error(t, err)
	assert.Nil(t, sources)
	assert.Contains(t, err.Error(), "failed to access")
}

func TestResolveStatement_Directory(t *testing.T) {
	// Create a temporary directory with SQL files
	tmpdir := t.TempDir()

	// Create some files
	sql1 := "CREATE TABLE users (id INT)"
	sql2 := "CREATE TABLE orders (id INT)"

	require.NoError(t, os.WriteFile(filepath.Join(tmpdir, "001_users.sql"), []byte(sql1), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpdir, "002_orders.sql"), []byte(sql2), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpdir, "README.md"), []byte("# Migrations"), 0644))

	// Create a subdirectory with a file
	subdir := filepath.Join(tmpdir, "archived")
	require.NoError(t, os.Mkdir(subdir, 0755))

	sql3 := "CREATE TABLE old (id INT)"
	require.NoError(t, os.WriteFile(filepath.Join(subdir, "old.sql"), []byte(sql3), 0644))

	sources, err := resolveStatement("file:" + tmpdir)
	require.NoError(t, err)
	require.Len(t, sources, 3) // Should find all 3 .sql files recursively

	// Verify origins have file: prefix
	for _, source := range sources {
		assert.True(t, strings.HasPrefix(source.Origin, "file:"))
		assert.True(t, strings.HasSuffix(source.Origin, ".sql"))
	}

	// Verify we got the right content
	sqlContents := []string{sources[0].SQL, sources[1].SQL, sources[2].SQL}
	assert.Contains(t, sqlContents, sql1)
	assert.Contains(t, sqlContents, sql2)
	assert.Contains(t, sqlContents, sql3)
}

func TestResolveStatement_DirectoryEmpty(t *testing.T) {
	tmpdir := t.TempDir()

	sources, err := resolveStatement("file:" + tmpdir)
	assert.Error(t, err)
	assert.Nil(t, sources)
	assert.Contains(t, err.Error(), "no .sql files found")
}

func TestResolveStatement_Glob(t *testing.T) {
	// Create a temporary directory with SQL files
	tmpdir := t.TempDir()

	// Create some files
	sql1 := "CREATE TABLE users (id INT)"
	sql2 := "CREATE TABLE orders (id INT)"
	sql3 := "CREATE TABLE products (id INT)"

	require.NoError(t, os.WriteFile(filepath.Join(tmpdir, "001_users.sql"), []byte(sql1), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpdir, "002_orders.sql"), []byte(sql2), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpdir, "003_products.sql"), []byte(sql3), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpdir, "README.md"), []byte("# Migrations"), 0644))

	// Test glob pattern
	pattern := "file:" + filepath.Join(tmpdir, "*.sql")
	sources, err := resolveStatement(pattern)
	require.NoError(t, err)
	require.Len(t, sources, 3) // Should find all 3 .sql files

	// Verify origins
	for _, source := range sources {
		assert.True(t, strings.HasPrefix(source.Origin, "file:"))
		assert.True(t, strings.HasSuffix(source.Origin, ".sql"))
	}
}

func TestResolveStatement_GlobNoMatches(t *testing.T) {
	tmpdir := t.TempDir()

	pattern := "file:" + filepath.Join(tmpdir, "*.sql")
	sources, err := resolveStatement(pattern)
	assert.Error(t, err)
	assert.Nil(t, sources)
	assert.Contains(t, err.Error(), "no files matched glob pattern")
}

func TestResolveStatement_GlobWithPattern(t *testing.T) {
	tmpdir := t.TempDir()

	// Create files with different patterns
	require.NoError(t, os.WriteFile(filepath.Join(tmpdir, "001_users.sql"), []byte("CREATE TABLE users (id INT)"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpdir, "002_orders.sql"), []byte("CREATE TABLE orders (id INT)"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpdir, "999_old.sql"), []byte("CREATE TABLE old (id INT)"), 0644))

	// Test pattern that matches only 001 and 002
	pattern := "file:" + filepath.Join(tmpdir, "00[12]*.sql")
	sources, err := resolveStatement(pattern)
	require.NoError(t, err)
	require.Len(t, sources, 2)
}

func TestResolveStatement_GlobSkipsDirectories(t *testing.T) {
	tmpdir := t.TempDir()

	// Create a subdirectory that would match the glob
	subdir := filepath.Join(tmpdir, "migrations")
	require.NoError(t, os.Mkdir(subdir, 0755))

	// Create a file
	require.NoError(t, os.WriteFile(filepath.Join(tmpdir, "001.sql"), []byte("CREATE TABLE users (id INT)"), 0644))

	// Glob should skip the directory
	pattern := "file:" + filepath.Join(tmpdir, "*")
	sources, err := resolveStatement(pattern)
	require.NoError(t, err)
	require.Len(t, sources, 1) // Only the file, not the directory
}

func TestResolveStatement_Integration(t *testing.T) {
	// Create a realistic test directory structure
	tmpdir := t.TempDir()

	// Create some files
	require.NoError(t, os.WriteFile(filepath.Join(tmpdir, "001_users.sql"), []byte("CREATE TABLE users (id INT)"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(tmpdir, "002_orders.sql"), []byte("CREATE TABLE orders (id INT)"), 0644))

	tests := []struct {
		name          string
		arg           string
		expectedCount int
	}{
		{
			name:          "specific file",
			arg:           "file:" + filepath.Join(tmpdir, "001_users.sql"),
			expectedCount: 1,
		},
		{
			name:          "directory",
			arg:           "file:" + tmpdir,
			expectedCount: 2,
		},
		{
			name:          "glob all sql",
			arg:           "file:" + filepath.Join(tmpdir, "*.sql"),
			expectedCount: 2,
		},
		{
			name:          "glob with pattern",
			arg:           "file:" + filepath.Join(tmpdir, "001*.sql"),
			expectedCount: 1,
		},
		{
			name:          "cmdline",
			arg:           "CREATE TABLE test (id INT)",
			expectedCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sources, err := resolveStatement(tt.arg)
			require.NoError(t, err)
			assert.Len(t, sources, tt.expectedCount)
		})
	}
}

func TestParseStatementSource_Empty(t *testing.T) {
	source := StatementSource{
		Origin: "cmdline",
		SQL:    "",
	}

	createTables, alterStatements, err := parseStatementSource(source)
	require.NoError(t, err)
	assert.Nil(t, createTables)
	assert.Nil(t, alterStatements)
}

func TestParseStatementSource_WhitespaceOnly(t *testing.T) {
	source := StatementSource{
		Origin: "cmdline",
		SQL:    "   \n\t  ",
	}

	createTables, alterStatements, err := parseStatementSource(source)
	require.NoError(t, err)
	assert.Nil(t, createTables)
	assert.Nil(t, alterStatements)
}

func TestParseStatementSource_SingleCreateTable(t *testing.T) {
	source := StatementSource{
		Origin: "cmdline",
		SQL:    "CREATE TABLE users (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255))",
	}

	createTables, alterStatements, err := parseStatementSource(source)
	require.NoError(t, err)
	require.Len(t, createTables, 1)
	assert.Empty(t, alterStatements)
	assert.Equal(t, "users", createTables[0].GetTableName())
}

func TestParseStatementSource_SingleAlterTable(t *testing.T) {
	source := StatementSource{
		Origin: "cmdline",
		SQL:    "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
	}

	createTables, alterStatements, err := parseStatementSource(source)
	require.NoError(t, err)
	assert.Empty(t, createTables)
	require.Len(t, alterStatements, 1)
	assert.Equal(t, "users", alterStatements[0].Table)
}

func TestParseStatementSource_MultipleAlterStatements(t *testing.T) {
	source := StatementSource{
		Origin: "file:migrations/001.sql",
		SQL: `
			ALTER TABLE users ADD COLUMN email VARCHAR(255);
			ALTER TABLE users ADD INDEX idx_email (email);
		`,
	}

	createTables, alterStatements, err := parseStatementSource(source)
	require.NoError(t, err)
	assert.Empty(t, createTables)
	require.Len(t, alterStatements, 2)
	assert.Equal(t, "users", alterStatements[0].Table)
	assert.Equal(t, "users", alterStatements[1].Table)
}

func TestParseStatementSource_MixedStatements(t *testing.T) {
	source := StatementSource{
		Origin: "file:schema.sql",
		SQL: `
			CREATE TABLE users (id BIGINT UNSIGNED PRIMARY KEY);
			ALTER TABLE users ADD INDEX idx_email (email);
		`,
	}

	// Mixed statements should fail due to statement.New() limitation
	createTables, alterStatements, err := parseStatementSource(source)
	assert.Error(t, err)
	assert.Nil(t, createTables)
	assert.Nil(t, alterStatements)
	assert.Contains(t, err.Error(), "failed to parse file:schema.sql")
}

func TestParseStatementSource_MultipleCreateStatements(t *testing.T) {
	source := StatementSource{
		Origin: "file:schema.sql",
		SQL: `
			CREATE TABLE users (id BIGINT UNSIGNED PRIMARY KEY);
			CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY);
		`,
	}

	// Multiple CREATE statements should fail due to statement.New() limitation
	createTables, alterStatements, err := parseStatementSource(source)
	assert.Error(t, err)
	assert.Nil(t, createTables)
	assert.Nil(t, alterStatements)
	assert.Contains(t, err.Error(), "failed to parse file:schema.sql")
}

func TestParseStatementSource_InvalidSQL(t *testing.T) {
	source := StatementSource{
		Origin: "cmdline",
		SQL:    "INVALID SQL STATEMENT",
	}

	createTables, alterStatements, err := parseStatementSource(source)
	assert.Error(t, err)
	assert.Nil(t, createTables)
	assert.Nil(t, alterStatements)
	assert.Contains(t, err.Error(), "failed to parse cmdline")
}

func TestParseStatementSource_ErrorContext(t *testing.T) {
	source := StatementSource{
		Origin: "file:migrations/bad.sql",
		SQL:    "CREATE TABLE",
	}

	_, _, err := parseStatementSource(source)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "file:migrations/bad.sql")
}

func TestParseStatementSource_WithComments(t *testing.T) {
	source := StatementSource{
		Origin: "file:schema.sql",
		SQL: `
			-- This is a comment
			ALTER TABLE users ADD COLUMN email VARCHAR(255);
			/* Multi-line
			   comment */
			ALTER TABLE users ADD INDEX idx_email (email);
		`,
	}

	createTables, alterStatements, err := parseStatementSource(source)
	require.NoError(t, err)
	assert.Empty(t, createTables)
	require.Len(t, alterStatements, 2)
}

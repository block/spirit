package change

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
)

func TestEncodeSchemaTable(t *testing.T) {
	tests := []struct {
		name     string
		schema   string
		table    string
		expected string
	}{
		{
			name:     "basic case",
			schema:   "test",
			table:    "users",
			expected: "test.users",
		},
		{
			name:     "schema with underscore",
			schema:   "test_db",
			table:    "users",
			expected: "test_db.users",
		},
		{
			name:     "table with underscore",
			schema:   "test",
			table:    "user_data",
			expected: "test.user_data",
		},
		{
			name:     "empty schema",
			schema:   "",
			table:    "users",
			expected: ".users",
		},
		{
			name:     "empty table",
			schema:   "test",
			table:    "",
			expected: "test.",
		},
		{
			name:     "both empty",
			schema:   "",
			table:    "",
			expected: ".",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encodeSchemaTable(tt.schema, tt.table)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractTablesFromDDLStmts(t *testing.T) {
	tests := []struct {
		name          string
		defaultSchema string
		statement     string
		want          []schemaTable
		wantOpensTxn  bool
		wantErr       bool
	}{
		{
			name:          "create table",
			defaultSchema: "test",
			statement:     "CREATE TABLE users (id INT PRIMARY KEY)",
			want:          []schemaTable{{"test", "users"}},
		},
		{
			// The binlog form of CREATE TABLE ... SELECT (8.0.21+, RBR):
			// the row events follow, so the statement opens its group.
			name:          "create table start transaction",
			defaultSchema: "test",
			statement:     "CREATE TABLE `ctas1` (\n  `a` int NOT NULL,\n  `b` int DEFAULT NULL\n) START TRANSACTION",
			want:          []schemaTable{{"test", "ctas1"}},
			wantOpensTxn:  true,
		},
		{
			name:          "start transaction",
			defaultSchema: "test",
			statement:     "START TRANSACTION",
			wantOpensTxn:  true,
		},
		{
			name:          "create table with schema",
			defaultSchema: "test",
			statement:     "CREATE TABLE mydb.users (id INT PRIMARY KEY)",
			want:          []schemaTable{{"mydb", "users"}},
		},
		{
			name:          "drop table",
			defaultSchema: "test",
			statement:     "DROP TABLE users",
			want:          []schemaTable{{"test", "users"}},
		},
		{
			name:          "drop multiple tables",
			defaultSchema: "test",
			statement:     "DROP TABLE users, orders",
			want:          []schemaTable{{"test", "users"}, {"test", "orders"}},
		},
		{
			name:          "alter table",
			defaultSchema: "test",
			statement:     "ALTER TABLE users ADD COLUMN age INT",
			want:          []schemaTable{{"test", "users"}},
		},
		{
			name:          "rename table",
			defaultSchema: "test",
			statement:     "RENAME TABLE users TO new_users",
			want:          []schemaTable{{"test", "users"}},
		},
		{
			name:          "truncate table",
			defaultSchema: "test",
			statement:     "TRUNCATE TABLE users",
			want:          []schemaTable{{"test", "users"}},
		},
		{
			name:          "create index",
			defaultSchema: "test",
			statement:     "CREATE INDEX idx_name ON users (name)",
			want:          []schemaTable{{"test", "users"}},
		},
		{
			name:          "drop index",
			defaultSchema: "test",
			statement:     "DROP INDEX idx_name ON users",
			want:          []schemaTable{{"test", "users"}},
		},
		{
			name:          "multiple statements",
			defaultSchema: "test",
			statement:     "CREATE TABLE users (id INT); CREATE INDEX idx_name ON users (name);",
			want:          []schemaTable{{"test", "users"}, {"test", "users"}},
		},
		{
			name:          "invalid statement",
			defaultSchema: "test",
			statement:     "INVALID SQL",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, opensTransaction, err := extractTablesFromDDLStmts(tt.defaultSchema, tt.statement)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantOpensTxn, opensTransaction)
		})
	}
}

func TestToSet(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected map[string]struct{}
	}{
		{
			name:     "nil input",
			input:    nil,
			expected: nil,
		},
		{
			name:     "empty slice",
			input:    []string{},
			expected: nil,
		},
		{
			name:  "single element",
			input: []string{"a"},
			expected: map[string]struct{}{
				"a": {},
			},
		},
		{
			name:  "multiple elements",
			input: []string{"a", "b", "c"},
			expected: map[string]struct{}{
				"a": {},
				"b": {},
				"c": {},
			},
		},
		{
			name:  "duplicate elements",
			input: []string{"a", "b", "a"},
			expected: map[string]struct{}{
				"a": {},
				"b": {},
			},
		},
		{
			name:  "schema.table style strings",
			input: []string{"test.users", "test.orders"},
			expected: map[string]struct{}{
				"test.users":  {},
				"test.orders": {},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toSet(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestExtractTablesFromDDLStmtsComplex tests more complex DDL statements
func TestExtractTablesFromDDLStmtsComplex(t *testing.T) {
	tests := []struct {
		name          string
		defaultSchema string
		statement     string
		want          []schemaTable
		wantErr       bool
	}{
		{
			name:          "create table with complex columns",
			defaultSchema: "test",
			statement: `CREATE TABLE users (
				id INT AUTO_INCREMENT PRIMARY KEY,
				name VARCHAR(255) NOT NULL,
				email VARCHAR(255) UNIQUE,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
			)`,
			want: []schemaTable{{"test", "users"}},
		},
		{
			name:          "alter table with multiple changes",
			defaultSchema: "test",
			statement: `ALTER TABLE users 
				ADD COLUMN age INT,
				DROP COLUMN old_field,
				MODIFY email VARCHAR(320),
				ADD INDEX idx_age (age),
				DROP INDEX idx_old`,
			want: []schemaTable{{"test", "users"}},
		},
		{
			name:          "create table with foreign keys",
			defaultSchema: "test",
			statement: `CREATE TABLE orders (
				id INT PRIMARY KEY,
				user_id INT,
				FOREIGN KEY (user_id) REFERENCES users(id)
			)`,
			want: []schemaTable{{"test", "orders"}},
		},
		{
			name:          "multiple schema references",
			defaultSchema: "test",
			statement: `CREATE TABLE shop.orders (
				id INT PRIMARY KEY,
				user_id INT,
				FOREIGN KEY (user_id) REFERENCES auth.users(id)
			)`,
			want: []schemaTable{{"shop", "orders"}},
		},
		{
			name:          "partition definition",
			defaultSchema: "test",
			statement: `CREATE TABLE sales (
				id INT,
				amount DECIMAL(10,2),
				sale_date DATE
			)
			PARTITION BY RANGE (YEAR(sale_date)) (
				PARTITION p0 VALUES LESS THAN (2020),
				PARTITION p1 VALUES LESS THAN (2021),
				PARTITION p2 VALUES LESS THAN (2022)
			)`,
			want: []schemaTable{{"test", "sales"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := extractTablesFromDDLStmts(tt.defaultSchema, tt.statement)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestPkChanged exercises the helper that decides whether the before- and
// after-image of a binlog UPDATE event represent a primary key update.
// Values arrive in []any from the binlog row image with concrete types
// like int8/int16/int32/int64 depending on the source column, so the
// helper compares via fmt.Sprintf("%v", ...) rather than reflect.DeepEqual.
func TestPkChanged(t *testing.T) {
	t.Parallel()

	t.Run("both nil is unchanged", func(t *testing.T) {
		require.False(t, pkChanged(nil, nil))
	})

	t.Run("nil and empty slice are unchanged", func(t *testing.T) {
		// len(nil) == 0 == len([]any{}), so the helper treats them as equal.
		require.False(t, pkChanged(nil, []any{}))
		require.False(t, pkChanged([]any{}, nil))
	})

	t.Run("length mismatch is changed", func(t *testing.T) {
		require.True(t, pkChanged([]any{1}, []any{1, 2}))
		require.True(t, pkChanged([]any{1, 2}, []any{1}))
	})

	t.Run("single-column integer PK unchanged", func(t *testing.T) {
		require.False(t, pkChanged([]any{int64(42)}, []any{int64(42)}))
	})

	t.Run("single-column integer PK changed", func(t *testing.T) {
		require.True(t, pkChanged([]any{int64(42)}, []any{int64(43)}))
	})

	t.Run("single-column string PK unchanged", func(t *testing.T) {
		require.False(t, pkChanged([]any{"abc"}, []any{"abc"}))
	})

	t.Run("single-column string PK changed", func(t *testing.T) {
		require.True(t, pkChanged([]any{"abc"}, []any{"abd"}))
	})

	t.Run("composite PK all equal is unchanged", func(t *testing.T) {
		require.False(t, pkChanged([]any{int64(1), "x"}, []any{int64(1), "x"}))
	})

	t.Run("composite PK first column changed", func(t *testing.T) {
		require.True(t, pkChanged([]any{int64(1), "x"}, []any{int64(2), "x"}))
	})

	t.Run("composite PK last column changed", func(t *testing.T) {
		require.True(t, pkChanged([]any{int64(1), "x"}, []any{int64(1), "y"}))
	})

	t.Run("numeric type equivalence: int vs int64 same value is unchanged", func(t *testing.T) {
		// MySQL binlog may surface the same underlying value as different Go
		// integer types depending on the source column width. fmt.Sprintf
		// "%v" normalises these to identical text, so the helper must not
		// see int(5) and int64(5) as a PK change.
		require.False(t, pkChanged([]any{int(5)}, []any{int64(5)}))
		require.False(t, pkChanged([]any{int32(5)}, []any{int64(5)}))
		require.False(t, pkChanged([]any{uint32(5)}, []any{int64(5)}))
	})

	t.Run("numeric type equivalence: signed and unsigned same value", func(t *testing.T) {
		require.False(t, pkChanged([]any{int64(7)}, []any{uint64(7)}))
	})

	t.Run("byte slice and string with same content are unchanged", func(t *testing.T) {
		// fmt.Sprintf alone renders []byte and string differently
		// ("[97 98]" vs "ab"), so the helper coerces []byte to string
		// before formatting. Defensive against decoder changes that
		// might surface a column as []byte in one image and string in
		// the next.
		require.False(t, pkChanged([]any{[]byte("abc")}, []any{"abc"}))
		require.False(t, pkChanged([]any{"abc"}, []any{[]byte("abc")}))
		require.False(t, pkChanged([]any{[]byte("abc")}, []any{[]byte("abc")}))
		require.True(t, pkChanged([]any{[]byte("abc")}, []any{"abd"}))
	})

	t.Run("typed nil is treated as a value", func(t *testing.T) {
		// fmt.Sprintf("%v", nil) == "<nil>". Two nils render the same,
		// so a column transitioning NULL -> NULL doesn't count as changed.
		// (PRIMARY KEY columns are NOT NULL in MySQL, but be conservative.)
		require.False(t, pkChanged([]any{nil}, []any{nil}))
		require.True(t, pkChanged([]any{nil}, []any{int64(0)}))
	})

	t.Run("boolean and string-of-bool do not collide", func(t *testing.T) {
		// fmt.Sprintf("%v", true) == "true" and fmt.Sprintf("%v", "true")
		// == "true". Without the bool special-case, a TINYINT(1) PK
		// column surfaced as bool would compare equal to a VARCHAR PK
		// holding the literal string "true". Vanishingly rare in
		// practice but worth a defensive test.
		require.True(t, pkChanged([]any{true}, []any{"true"}))
		require.True(t, pkChanged([]any{false}, []any{"false"}))
		require.False(t, pkChanged([]any{true}, []any{true}))
		require.False(t, pkChanged([]any{false}, []any{false}))
		require.True(t, pkChanged([]any{true}, []any{false}))
	})
}

// TestIsMinimalRowImage verifies the per-event detection used by the
// binlog client to refuse running against a source configured with
// binlog_row_image other than FULL. SkippedColumns is a [][]int per
// row from go-mysql; a row with len > 0 inside indicates that some
// columns were omitted, which is the MINIMAL/NOBLOB image form.
func TestIsMinimalRowImage(t *testing.T) {
	t.Parallel()

	t.Run("nil SkippedColumns is full image", func(t *testing.T) {
		e := &replication.RowsEvent{SkippedColumns: nil}
		require.False(t, isMinimalRowImage(e))
	})

	t.Run("empty per-row slices is full image", func(t *testing.T) {
		// go-mysql preallocates one slice per row, all empty when the
		// source has binlog_row_image=FULL.
		e := &replication.RowsEvent{SkippedColumns: [][]int{{}, {}, {}}}
		require.False(t, isMinimalRowImage(e))
	})

	t.Run("any non-empty per-row slice flags minimal", func(t *testing.T) {
		// A single row with at least one skipped column ordinal is enough.
		e := &replication.RowsEvent{SkippedColumns: [][]int{{}, {2}, {}}}
		require.True(t, isMinimalRowImage(e))
	})

	t.Run("multiple skipped columns also flags minimal", func(t *testing.T) {
		e := &replication.RowsEvent{SkippedColumns: [][]int{{1, 2, 3}}}
		require.True(t, isMinimalRowImage(e))
	})
}

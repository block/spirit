package lint

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/require"
)

// findTable returns the post-state table with the given name (case-insensitive)
// or nil if none matches. PostState keys its internal map by lower-cased name
// but preserves the original casing on TableName, so callers shouldn't have to
// guess the case the post-state produced.
func findTable(tables []*statement.CreateTable, name string) *statement.CreateTable {
	for _, t := range tables {
		if strings.EqualFold(t.TableName, name) {
			return t
		}
	}
	return nil
}

// TestPostState_RenameTable verifies that ALTER … RENAME TO surfaces the
// table under its new name in the post-state, and that the old name is gone.
func TestPostState_RenameTable(t *testing.T) {
	existing, err := statement.ParseCreateTable("CREATE TABLE Foo (id INT PRIMARY KEY)")
	require.NoError(t, err)

	rename, err := statement.New("ALTER TABLE Foo RENAME TO bar")
	require.NoError(t, err)

	post := PostState([]*statement.CreateTable{existing}, rename)
	require.Len(t, post, 1)
	require.Equal(t, "bar", post[0].TableName)
}

// TestPostState_TableOptionEngine verifies that ALTER TABLE … ENGINE=… is
// applied to TableOptions in the post-state.
func TestPostState_TableOptionEngine(t *testing.T) {
	existing, err := statement.ParseCreateTable("CREATE TABLE t1 (id INT PRIMARY KEY) ENGINE=MyISAM")
	require.NoError(t, err)
	require.NotNil(t, existing.TableOptions)
	require.NotNil(t, existing.TableOptions.Engine)
	require.Equal(t, "MyISAM", *existing.TableOptions.Engine)

	alter, err := statement.New("ALTER TABLE t1 ENGINE=InnoDB")
	require.NoError(t, err)

	post := PostState([]*statement.CreateTable{existing}, alter)
	t1 := findTable(post, "t1")
	require.NotNil(t, t1)
	require.NotNil(t, t1.TableOptions)
	require.NotNil(t, t1.TableOptions.Engine)
	require.Equal(t, "InnoDB", *t1.TableOptions.Engine)
}

// TestPostState_TableOptionAutoIncrement verifies that ALTER TABLE …
// AUTO_INCREMENT=… is applied to TableOptions in the post-state.
func TestPostState_TableOptionAutoIncrement(t *testing.T) {
	existing, err := statement.ParseCreateTable("CREATE TABLE t1 (id INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (id)) AUTO_INCREMENT=100")
	require.NoError(t, err)

	alter, err := statement.New("ALTER TABLE t1 AUTO_INCREMENT=1000000000")
	require.NoError(t, err)

	post := PostState([]*statement.CreateTable{existing}, alter)
	t1 := findTable(post, "t1")
	require.NotNil(t, t1)
	require.NotNil(t, t1.TableOptions)
	require.NotNil(t, t1.TableOptions.AutoIncrement)
	require.Equal(t, uint64(1000000000), *t1.TableOptions.AutoIncrement)
}

// TestPostState_AddDropForeignKey verifies that ADD CONSTRAINT FOREIGN KEY
// and DROP FOREIGN KEY mutate the Constraints slice in the post-state.
func TestPostState_AddDropForeignKey(t *testing.T) {
	existing, err := statement.ParseCreateTable(`CREATE TABLE orders (
		id INT PRIMARY KEY,
		user_id INT,
		CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id)
	)`)
	require.NoError(t, err)
	require.Len(t, existing.Constraints, 1)

	drop, err := statement.New("ALTER TABLE orders DROP FOREIGN KEY fk_user")
	require.NoError(t, err)
	postAfterDrop := PostState([]*statement.CreateTable{existing}, drop)
	t1 := findTable(postAfterDrop, "orders")
	require.NotNil(t, t1)
	fkCount := 0
	for _, c := range t1.Constraints {
		if c.Type == "FOREIGN KEY" {
			fkCount++
		}
	}
	require.Equal(t, 0, fkCount, "DROP FOREIGN KEY should remove the constraint from post-state")

	clean, err := statement.ParseCreateTable(`CREATE TABLE orders (
		id INT PRIMARY KEY,
		user_id INT
	)`)
	require.NoError(t, err)
	add, err := statement.New("ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id)")
	require.NoError(t, err)
	postAfterAdd := PostState([]*statement.CreateTable{clean}, add)
	t1 = findTable(postAfterAdd, "orders")
	require.NotNil(t, t1)
	fkCount = 0
	for _, c := range t1.Constraints {
		if c.Type == "FOREIGN KEY" {
			fkCount++
		}
	}
	require.Equal(t, 1, fkCount, "ADD CONSTRAINT FOREIGN KEY should appear in post-state")
}

// TestPostState_MigratedLinters_FixingAltersSilenceWarnings verifies the
// post-state convention across the six linters migrated in this change:
// an ALTER that fixes the legacy issue should silence the warning.
func TestPostState_MigratedLinters_FixingAltersSilenceWarnings(t *testing.T) {
	t.Run("allow_engine: ALTER ENGINE=InnoDB silences legacy MyISAM", func(t *testing.T) {
		existing, err := statement.ParseCreateTable("CREATE TABLE t1 (id INT PRIMARY KEY) ENGINE=MyISAM")
		require.NoError(t, err)
		fix, err := statement.New("ALTER TABLE t1 ENGINE=InnoDB")
		require.NoError(t, err)

		linter := &AllowEngine{}
		require.NoError(t, linter.Configure(linter.DefaultConfig()))
		require.Empty(t, linter.Lint([]*statement.CreateTable{existing}, fix))
	})

	t.Run("allow_charset: MODIFY COLUMN to utf8mb4 silences legacy latin1", func(t *testing.T) {
		existing, err := statement.ParseCreateTable("CREATE TABLE t1 (id INT PRIMARY KEY, n VARCHAR(50) CHARACTER SET latin1)")
		require.NoError(t, err)
		fix, err := statement.New("ALTER TABLE t1 MODIFY COLUMN n VARCHAR(50) CHARACTER SET utf8mb4")
		require.NoError(t, err)

		linter := &AllowCharset{charsets: []string{"utf8mb4"}}
		require.Empty(t, linter.Lint([]*statement.CreateTable{existing}, fix))
	})

	t.Run("has_foreign_key: DROP FOREIGN KEY silences the FK warning", func(t *testing.T) {
		existing, err := statement.ParseCreateTable(`CREATE TABLE orders (
			id INT PRIMARY KEY,
			user_id INT,
			CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id)
		)`)
		require.NoError(t, err)
		fix, err := statement.New("ALTER TABLE orders DROP FOREIGN KEY fk_user")
		require.NoError(t, err)

		linter := &HasFKLinter{}
		require.Empty(t, linter.Lint([]*statement.CreateTable{existing}, fix))
	})

	t.Run("name_case: RENAME to lowercase silences legacy uppercase", func(t *testing.T) {
		existing, err := statement.ParseCreateTable("CREATE TABLE USERS (id INT PRIMARY KEY)")
		require.NoError(t, err)
		fix, err := statement.New("ALTER TABLE USERS RENAME TO users")
		require.NoError(t, err)

		linter := &NameCaseLinter{}
		require.Empty(t, linter.Lint([]*statement.CreateTable{existing}, fix))
	})

	t.Run("reserved_words: DROP COLUMN silences legacy reserved column name", func(t *testing.T) {
		existing, err := statement.ParseCreateTable("CREATE TABLE t1 (id INT PRIMARY KEY, `select` INT)")
		require.NoError(t, err)
		fix, err := statement.New("ALTER TABLE t1 DROP COLUMN `select`")
		require.NoError(t, err)

		linter := &ReservedWordsLinter{}
		require.Empty(t, linter.Lint([]*statement.CreateTable{existing}, fix))
	})

	t.Run("auto_inc_capacity: ALTER MODIFY widens column type silences capacity warning", func(t *testing.T) {
		// Legacy table: SMALLINT auto-inc at 60000 (~91% of 65535, over the 85% default threshold).
		existing, err := statement.ParseCreateTable("CREATE TABLE t1 (id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT, PRIMARY KEY (id)) AUTO_INCREMENT=60000")
		require.NoError(t, err)
		linter := &AutoIncCapacityLinter{}
		require.NoError(t, linter.Configure(linter.DefaultConfig()))
		// Sanity: linter fires without any change.
		require.NotEmpty(t, linter.Lint([]*statement.CreateTable{existing}, nil))

		// Widen to BIGINT: post-state column is BIGINT, 60000 is far below threshold.
		fix, err := statement.New("ALTER TABLE t1 MODIFY COLUMN id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT")
		require.NoError(t, err)
		require.Empty(t, linter.Lint([]*statement.CreateTable{existing}, fix))
	})
}

// TestPostState_ColumnCharsetCollation verifies that ADD/MODIFY COLUMN carries
// the column's charset and collation into the post-state column, whether they
// were written on the type or as a separate COLLATE option. type_pedantic's
// collation rule reads these, so without them an ALTER that converges a
// collation would keep reporting the violation it fixes.
func TestPostState_ColumnCharsetCollation(t *testing.T) {
	existing, err := statement.ParseCreateTable("CREATE TABLE t1 (id INT PRIMARY KEY, n VARCHAR(50)) DEFAULT CHARSET=latin1")
	require.NoError(t, err)

	t.Run("charset and collation on the type", func(t *testing.T) {
		fix, err := statement.New("ALTER TABLE t1 MODIFY COLUMN n VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci")
		require.NoError(t, err)
		post := findTable(PostState([]*statement.CreateTable{existing}, fix), "t1")
		require.NotNil(t, post)
		col := post.Columns.ByName("n")
		require.NotNil(t, col)
		require.NotNil(t, col.Charset)
		require.Equal(t, "utf8mb4", *col.Charset)
		require.NotNil(t, col.Collation)
		require.Equal(t, "utf8mb4_general_ci", *col.Collation)
	})

	t.Run("distinct COLLATE options in one ALTER do not alias", func(t *testing.T) {
		// ColumnDef.Options is a slice of *ColumnOption, so &opt.StrValue
		// addresses a field inside a distinct heap object per option — no
		// iteration can overwrite another's value.
		fix, err := statement.New("ALTER TABLE t1 ADD COLUMN x VARCHAR(50) COLLATE utf8mb4_bin, ADD COLUMN y VARCHAR(50) COLLATE utf8mb4_general_ci")
		require.NoError(t, err)
		post := findTable(PostState([]*statement.CreateTable{existing}, fix), "t1")
		require.NotNil(t, post)
		for name, want := range map[string]string{"x": "utf8mb4_bin", "y": "utf8mb4_general_ci"} {
			col := post.Columns.ByName(name)
			require.NotNil(t, col, "column %s", name)
			require.NotNil(t, col.Collation, "column %s", name)
			require.Equal(t, want, *col.Collation, "column %s", name)
		}
	})

	t.Run("bare COLLATE option", func(t *testing.T) {
		fix, err := statement.New("ALTER TABLE t1 ADD COLUMN n2 VARCHAR(50) COLLATE utf8mb4_0900_ai_ci")
		require.NoError(t, err)
		post := findTable(PostState([]*statement.CreateTable{existing}, fix), "t1")
		require.NotNil(t, post)
		col := post.Columns.ByName("n2")
		require.NotNil(t, col)
		require.NotNil(t, col.Collation)
		require.Equal(t, "utf8mb4_0900_ai_ci", *col.Collation)
	})
}

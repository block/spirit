package statement

import (
	"fmt"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// assertChangesContain checks that each expected substring appears in at least
// one of the returned statements.
func assertChangesContain(t *testing.T, changes []*AbstractStatement, expected []string) {
	t.Helper()
	strs := statementsToStrings(changes)
	for _, exp := range expected {
		found := false
		for _, s := range strs {
			if strings.Contains(s, exp) {
				found = true
				break
			}
		}
		require.True(t, found, "expected some statement to contain %q, got %v", exp, strs)
	}
}

func statementsToStrings(changes []*AbstractStatement) []string {
	strs := make([]string, len(changes))
	for i, ch := range changes {
		strs[i] = ch.Statement
	}
	return strs
}

func TestDeclarativeToImperative(t *testing.T) {
	tests := []struct {
		name          string
		current       []table.TableSchema
		desired       []table.TableSchema
		expectedCount int
		contains      []string // substrings that must appear somewhere in the results
	}{
		{
			name: "NoChanges",
			current: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY)"},
			},
			desired: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY)"},
			},
			expectedCount: 0,
		},
		{
			name: "AddColumn",
			current: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY)"},
			},
			desired: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY, b INT)"},
			},
			expectedCount: 1,
			contains:      []string{"ALTER TABLE `t1` ADD COLUMN"},
		},
		{
			name: "NewTable",
			current: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY)"},
			},
			desired: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY)"},
				{Name: "t2", Schema: "CREATE TABLE t2 (id INT PRIMARY KEY, name VARCHAR(100))"},
			},
			expectedCount: 1,
			contains:      []string{"CREATE TABLE"},
		},
		{
			name: "DropTable",
			current: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY)"},
				{Name: "t2", Schema: "CREATE TABLE t2 (id INT PRIMARY KEY)"},
			},
			desired: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY)"},
			},
			expectedCount: 1,
			contains:      []string{"DROP TABLE `t2`"},
		},
		{
			name: "MixedChanges",
			current: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY)"},
				{Name: "t2", Schema: "CREATE TABLE t2 (id INT PRIMARY KEY)"},
			},
			desired: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY, b INT)"},
				{Name: "t3", Schema: "CREATE TABLE t3 (id INT PRIMARY KEY)"},
			},
			expectedCount: 3,
			contains: []string{
				"ALTER TABLE `t1`",
				"CREATE TABLE",
				"DROP TABLE `t2`",
			},
		},
		{
			name:          "EmptyCurrentAndDesired",
			current:       nil,
			desired:       nil,
			expectedCount: 0,
		},
		{
			name:    "EmptyCurrentNewTables",
			current: nil,
			desired: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY)"},
			},
			expectedCount: 1,
			contains:      []string{"CREATE TABLE"},
		},
		{
			name: "EmptyDesiredDropAll",
			current: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY)"},
			},
			desired:       nil,
			expectedCount: 1,
			contains:      []string{"DROP TABLE `t1`"},
		},
		{
			name: "AddIndex",
			current: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))"},
			},
			desired: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name))"},
			},
			expectedCount: 1,
			contains:      []string{"ALTER TABLE `t1` ADD INDEX"},
		},
		{
			name: "DropIndex",
			current: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name))"},
			},
			desired: []table.TableSchema{
				{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))"},
			},
			expectedCount: 1,
			contains:      []string{"ALTER TABLE `t1` DROP INDEX"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changes, err := DeclarativeToImperative(tt.current, tt.desired, nil)
			require.NoError(t, err)

			require.Len(t, changes, tt.expectedCount)
			if tt.contains != nil {
				assertChangesContain(t, changes, tt.contains)
			}
		})
	}
}

func TestDeclarativeToImperativeWithOptions(t *testing.T) {
	// With default options, AUTO_INCREMENT differences are ignored
	current := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT) AUTO_INCREMENT=100"},
	}
	desired := []table.TableSchema{
		{Name: "t1", Schema: "CREATE TABLE t1 (id INT PRIMARY KEY AUTO_INCREMENT) AUTO_INCREMENT=200"},
	}

	changes, err := DeclarativeToImperative(current, desired, NewDiffOptions())
	require.NoError(t, err)
	require.Empty(t, changes, "AUTO_INCREMENT differences should be ignored with default options")
}

// TestDeclarativeToImperative_DuplicateTableRefused: a schema that declares the
// same table twice is refused instead of planned from whichever definition
// came last. Under a case-insensitive target, names that differ only in case
// are the same table; under lower_case_table_names=0 they are two tables.
func TestDeclarativeToImperative_DuplicateTableRefused(t *testing.T) {
	folding := NewDiffOptions()
	folding.LowerCaseTableNames = 1
	tests := []struct {
		name    string
		current []table.TableSchema
		desired []table.TableSchema
		opts    *DiffOptions
		wantErr string
	}{
		{
			name: "desired declares a table twice",
			desired: []table.TableSchema{
				{Name: "orders", Schema: "CREATE TABLE orders (id INT PRIMARY KEY)"},
				{Name: "orders", Schema: "CREATE TABLE orders (id INT PRIMARY KEY, total INT)"},
			},
			opts:    NewDiffOptions(),
			wantErr: `desired schema declares table "orders" more than once`,
		},
		{
			name: "desired names one table in two cases on a case-insensitive target",
			desired: []table.TableSchema{
				{Name: "orders", Schema: "CREATE TABLE orders (id INT PRIMARY KEY)"},
				{Name: "Orders", Schema: "CREATE TABLE Orders (id INT PRIMARY KEY, total INT)"},
			},
			opts:    folding,
			wantErr: `desired schema declares tables "orders" and "Orders", which are the same table when lower_case_table_names=1`,
		},
		{
			name: "current lists one table twice",
			current: []table.TableSchema{
				{Name: "orders", Schema: "CREATE TABLE orders (id INT PRIMARY KEY)"},
				{Name: "orders", Schema: "CREATE TABLE orders (id INT PRIMARY KEY)"},
			},
			opts:    nil,
			wantErr: `current schema declares table "orders" more than once`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changes, err := DeclarativeToImperative(tt.current, tt.desired, tt.opts)
			require.EqualError(t, err, tt.wantErr)
			require.Nil(t, changes)
		})
	}

	t.Run("names that differ in case are two tables on a case-sensitive target", func(t *testing.T) {
		desired := []table.TableSchema{
			{Name: "orders", Schema: "CREATE TABLE orders (id INT PRIMARY KEY)"},
			{Name: "Orders", Schema: "CREATE TABLE Orders (id INT PRIMARY KEY)"},
		}
		changes, err := DeclarativeToImperative(nil, desired, NewDiffOptions())
		require.NoError(t, err)
		require.Len(t, changes, 2)
		assertChangesContain(t, changes, []string{"CREATE TABLE orders", "CREATE TABLE Orders"})
	})
}

// TestDeclarativeToImperative_LowerCaseTableNames: on a target that compares
// table names case-insensitively, a desired table matches the current table
// whose name differs only in case. It is diffed under the name it exists by,
// rather than created beside it (a CREATE MySQL refuses) and the existing one
// dropped with its rows; dropped tables keep the case they exist in, and
// foreign key references order new tables whatever case they are written in.
func TestDeclarativeToImperative_LowerCaseTableNames(t *testing.T) {
	for _, lctn := range []int{1, 2} {
		opts := NewDiffOptions()
		opts.LowerCaseTableNames = lctn

		t.Run(fmt.Sprintf("lower_case_table_names=%d/case-only rename diffs the existing table", lctn), func(t *testing.T) {
			current := []table.TableSchema{{Name: "orders", Schema: "CREATE TABLE orders (id INT PRIMARY KEY)"}}
			desired := []table.TableSchema{{Name: "Orders", Schema: "CREATE TABLE Orders (id INT PRIMARY KEY, total INT)"}}
			changes, err := DeclarativeToImperative(current, desired, opts)
			require.NoError(t, err)
			require.Equal(t, []string{"ALTER TABLE `orders` ADD COLUMN `total` int NULL"}, statementsToStrings(changes))
		})

		t.Run(fmt.Sprintf("lower_case_table_names=%d/dropped table keeps its case", lctn), func(t *testing.T) {
			current := []table.TableSchema{{Name: "Orders", Schema: "CREATE TABLE Orders (id INT PRIMARY KEY)"}}
			changes, err := DeclarativeToImperative(current, nil, opts)
			require.NoError(t, err)
			require.Equal(t, []string{"DROP TABLE `Orders`"}, statementsToStrings(changes))
		})

		t.Run(fmt.Sprintf("lower_case_table_names=%d/reference in another case orders the creates", lctn), func(t *testing.T) {
			desired := []table.TableSchema{
				{Name: "child", Schema: "CREATE TABLE child (id INT PRIMARY KEY, pid INT, CONSTRAINT fk FOREIGN KEY (pid) REFERENCES Parent (id))"},
				{Name: "parent", Schema: "CREATE TABLE parent (id INT PRIMARY KEY)"},
			}
			changes, err := DeclarativeToImperative(nil, desired, opts)
			require.NoError(t, err)
			require.Len(t, changes, 2)
			require.Equal(t, "parent", changes[0].Table)
			require.Equal(t, "child", changes[1].Table)
		})
	}

	t.Run("lower_case_table_names=0 creates and drops", func(t *testing.T) {
		current := []table.TableSchema{{Name: "orders", Schema: "CREATE TABLE orders (id INT PRIMARY KEY)"}}
		desired := []table.TableSchema{{Name: "Orders", Schema: "CREATE TABLE Orders (id INT PRIMARY KEY, total INT)"}}
		changes, err := DeclarativeToImperative(current, desired, NewDiffOptions())
		require.NoError(t, err)
		require.Len(t, changes, 2)
		require.Equal(t, "Orders", changes[0].Table)
		require.True(t, changes[0].IsCreateTable())
		require.Equal(t, "DROP TABLE `orders`", changes[1].Statement)
	})
}

// TestDeclarativeToImperative_PrimaryKeyDeclaresNull: a desired primary key
// column that explicitly declares NULL or DEFAULT NULL is a table MySQL refuses
// to create, so it is rejected at plan time for new and existing tables alike.
// A key column that merely omits NOT NULL is implicitly NOT NULL and is
// accepted, as is the expression default (NULL).
func TestDeclarativeToImperative_PrimaryKeyDeclaresNull(t *testing.T) {
	const current = "CREATE TABLE t1 (a int NOT NULL, b int DEFAULT NULL, PRIMARY KEY (a))"
	tests := []struct {
		name    string
		desired string
		wantErr bool
	}{
		{"ImplicitNotNull", "CREATE TABLE t1 (a INT, b INT, PRIMARY KEY (a))", false},
		{"ExplicitNull", "CREATE TABLE t1 (a INT NULL, b INT, PRIMARY KEY (a))", true},
		{"DefaultNull", "CREATE TABLE t1 (a INT DEFAULT NULL, b INT, PRIMARY KEY (a))", true},
		{"ExplicitNullInline", "CREATE TABLE t1 (a INT NULL PRIMARY KEY, b INT)", true},
		{"ExplicitNullInComposite", "CREATE TABLE t1 (a INT, b INT NULL, PRIMARY KEY (a, b))", true},
		{"ExplicitNullThenNotNull", "CREATE TABLE t1 (a INT NULL NOT NULL, b INT, PRIMARY KEY (a))", true},
		{"NotNullDefaultNull", "CREATE TABLE t1 (a INT NOT NULL DEFAULT NULL, b INT, PRIMARY KEY (a))", true},
		{"ExpressionDefaultNull", "CREATE TABLE t1 (a INT DEFAULT (NULL), b INT, PRIMARY KEY (a))", false},
		// Moving the key off a column frees it to declare NULL.
		{"FormerKeyColumnDeclaresNull", "CREATE TABLE t1 (a INT NULL, b INT NOT NULL, PRIMARY KEY (b))", false},
	}
	for _, tt := range tests {
		for _, existing := range []bool{true, false} {
			name := tt.name + "/NewTable"
			var cur []table.TableSchema
			if existing {
				name = tt.name + "/ExistingTable"
				cur = []table.TableSchema{{Name: "t1", Schema: current}}
			}
			t.Run(name, func(t *testing.T) {
				_, err := DeclarativeToImperative(cur, []table.TableSchema{{Name: "t1", Schema: tt.desired}}, nil)
				if !tt.wantErr {
					require.NoError(t, err)
					return
				}
				require.ErrorContains(t, err, "is part of the PRIMARY KEY but declares NULL")
			})
		}
	}
}

func TestDeclarativeToImperative_OrderingCreateAlterBeforeDrop(t *testing.T) {
	// Verify the correctness property: statements are ordered as
	// CREATE → ALTER → DROP. This ensures safe sequential execution (e.g.
	// an ALTER adding a FK to a newly-created table runs after the CREATE,
	// and a table referenced by a FK won't be dropped before the
	// referencing ALTER runs).
	current := []table.TableSchema{
		{Name: "a_table", Schema: "CREATE TABLE a_table (id INT PRIMARY KEY)"},
		{Name: "b_table", Schema: "CREATE TABLE b_table (id INT PRIMARY KEY)"},
		{Name: "c_drop_me", Schema: "CREATE TABLE c_drop_me (id INT PRIMARY KEY)"},
		{Name: "d_drop_me", Schema: "CREATE TABLE d_drop_me (id INT PRIMARY KEY)"},
	}
	desired := []table.TableSchema{
		{Name: "a_table", Schema: "CREATE TABLE a_table (id INT PRIMARY KEY, name VARCHAR(100))"},
		{Name: "b_table", Schema: "CREATE TABLE b_table (id INT PRIMARY KEY)"},
		{Name: "e_new", Schema: "CREATE TABLE e_new (id INT PRIMARY KEY)"},
	}

	changes, err := DeclarativeToImperative(current, desired, nil)
	require.NoError(t, err)

	// We expect: CREATE e_new, ALTER a_table, DROP c_drop_me, DROP d_drop_me
	require.Len(t, changes, 4)

	// Classify each statement and verify strict CREATE → ALTER → DROP ordering.
	const (
		phaseCreate = iota
		phaseAlter
		phaseDrop
	)
	phase := phaseCreate
	for i, ch := range changes {
		var stmtPhase int
		switch {
		case strings.Contains(ch.Statement, "CREATE TABLE"):
			stmtPhase = phaseCreate
		case strings.Contains(ch.Statement, "ALTER TABLE"):
			stmtPhase = phaseAlter
		case strings.Contains(ch.Statement, "DROP TABLE"):
			stmtPhase = phaseDrop
		default:
			t.Fatalf("unexpected statement at index %d: %s", i, ch.Statement)
		}
		if stmtPhase < phase {
			t.Fatalf("ordering violation at index %d: got %s after phase %d", i, ch.Statement, phase)
		}
		phase = stmtPhase
	}

	// Additionally verify the exact order of statement types.
	require.Contains(t, changes[0].Statement, "CREATE TABLE")
	require.Contains(t, changes[1].Statement, "ALTER TABLE")
	require.Contains(t, changes[2].Statement, "DROP TABLE")
	require.Contains(t, changes[3].Statement, "DROP TABLE")
}

// TestDeclarativeToImperative_CreateOrderFollowsForeignKeys verifies that a
// new table is created after the new tables its foreign keys reference, even
// when alphabetical order says otherwise: MySQL refuses to create a child
// whose parent does not exist yet (error 1824).
func TestDeclarativeToImperative_CreateOrderFollowsForeignKeys(t *testing.T) {
	tests := []struct {
		name    string
		desired []table.TableSchema
		want    []string // table names in CREATE order
	}{
		{
			name: "child named before parent",
			desired: []table.TableSchema{
				{Name: "a_child", Schema: "CREATE TABLE a_child (id INT PRIMARY KEY, pid INT, CONSTRAINT fk FOREIGN KEY (pid) REFERENCES b_parent (id))"},
				{Name: "b_parent", Schema: "CREATE TABLE b_parent (id INT PRIMARY KEY)"},
			},
			want: []string{"b_parent", "a_child"},
		},
		{
			name: "chain of three",
			desired: []table.TableSchema{
				{Name: "a", Schema: "CREATE TABLE a (id INT PRIMARY KEY, bid INT, CONSTRAINT fk_a FOREIGN KEY (bid) REFERENCES b (id))"},
				{Name: "b", Schema: "CREATE TABLE b (id INT PRIMARY KEY, cid INT, CONSTRAINT fk_b FOREIGN KEY (cid) REFERENCES c (id))"},
				{Name: "c", Schema: "CREATE TABLE c (id INT PRIMARY KEY)"},
			},
			want: []string{"c", "b", "a"},
		},
		{
			name: "unrelated tables stay alphabetical",
			desired: []table.TableSchema{
				{Name: "c", Schema: "CREATE TABLE c (id INT PRIMARY KEY)"},
				{Name: "a", Schema: "CREATE TABLE a (id INT PRIMARY KEY)"},
				{Name: "b", Schema: "CREATE TABLE b (id INT PRIMARY KEY)"},
			},
			want: []string{"a", "b", "c"},
		},
		{
			name: "self reference imposes no order",
			desired: []table.TableSchema{
				{Name: "b_tree", Schema: "CREATE TABLE b_tree (id INT PRIMARY KEY, parent_id INT, CONSTRAINT fk FOREIGN KEY (parent_id) REFERENCES b_tree (id))"},
				{Name: "a", Schema: "CREATE TABLE a (id INT PRIMARY KEY)"},
			},
			want: []string{"a", "b_tree"},
		},
		{
			name: "reference to an existing table imposes no order",
			desired: []table.TableSchema{
				{Name: "b", Schema: "CREATE TABLE b (id INT PRIMARY KEY)"},
				{Name: "a", Schema: "CREATE TABLE a (id INT PRIMARY KEY, xid INT, CONSTRAINT fk FOREIGN KEY (xid) REFERENCES existing (id))"},
			},
			want: []string{"a", "b"},
		},
		{
			// MySQL cannot create either table of a cycle without
			// FOREIGN_KEY_CHECKS=0; the output is still complete and
			// deterministic.
			name: "cycle falls back to alphabetical",
			desired: []table.TableSchema{
				{Name: "b", Schema: "CREATE TABLE b (id INT PRIMARY KEY, aid INT, CONSTRAINT fk_b FOREIGN KEY (aid) REFERENCES a (id))"},
				{Name: "a", Schema: "CREATE TABLE a (id INT PRIMARY KEY, bid INT, CONSTRAINT fk_a FOREIGN KEY (bid) REFERENCES b (id))"},
			},
			want: []string{"a", "b"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changes, err := DeclarativeToImperative(nil, tt.desired, nil)
			require.NoError(t, err)
			var got []string
			for _, ch := range changes {
				require.True(t, ch.IsCreateTable(), ch.Statement)
				got = append(got, ch.Table)
			}
			require.Equal(t, tt.want, got)
		})
	}
}

// TestDeclarativeToImperative_DropOrderFollowsForeignKeys verifies that a
// table is dropped before the tables that reference it: MySQL refuses to drop
// a parent while a child still references it (error 3730). The order comes
// from the current schemas, so a current schema that does not parse keeps
// its DROP and only its place in the order.
func TestDeclarativeToImperative_DropOrderFollowsForeignKeys(t *testing.T) {
	current := []table.TableSchema{
		{Name: "a_parent", Schema: "CREATE TABLE a_parent (id INT PRIMARY KEY)"},
		{Name: "b_child", Schema: "CREATE TABLE b_child (id INT PRIMARY KEY, pid INT, CONSTRAINT fk_b FOREIGN KEY (pid) REFERENCES a_parent (id))"},
		{Name: "c_grandchild", Schema: "CREATE TABLE c_grandchild (id INT PRIMARY KEY, cid INT, CONSTRAINT fk_c FOREIGN KEY (cid) REFERENCES b_child (id))"},
		{Name: "d_unparsed", Schema: "not a create table"},
		{Name: "e_kept", Schema: "CREATE TABLE e_kept (id INT PRIMARY KEY, pid INT, CONSTRAINT fk_e FOREIGN KEY (pid) REFERENCES a_parent (id))"},
	}
	desired := []table.TableSchema{
		// The surviving child drops its reference first (ALTER before DROP).
		{Name: "e_kept", Schema: "CREATE TABLE e_kept (id INT PRIMARY KEY, pid INT)"},
	}
	changes, err := DeclarativeToImperative(current, desired, nil)
	require.NoError(t, err)
	got := statementsToStrings(changes)
	require.Equal(t, []string{
		"ALTER TABLE `e_kept` DROP FOREIGN KEY `fk_e`",
		"DROP TABLE `c_grandchild`",
		"DROP TABLE `b_child`",
		"DROP TABLE `a_parent`",
		"DROP TABLE `d_unparsed`",
	}, got)
}

// TestDeclarativeToImperativeMySQLForeignKeyOrder runs the emitted statements
// against MySQL: the creates in order, then the drops in order. Alphabetical
// order would fail both ways (errors 1824 and 3730).
func TestDeclarativeToImperativeMySQLForeignKeyOrder(t *testing.T) {
	_, db := testutils.CreateUniqueTestDatabase(t)
	desired := []table.TableSchema{
		{Name: "a_child", Schema: "CREATE TABLE a_child (id INT PRIMARY KEY, pid INT, KEY pid (pid), CONSTRAINT fk_a FOREIGN KEY (pid) REFERENCES b_parent (id))"},
		{Name: "b_parent", Schema: "CREATE TABLE b_parent (id INT PRIMARY KEY, gid INT, KEY gid (gid), CONSTRAINT fk_b FOREIGN KEY (gid) REFERENCES c_grandparent (id))"},
		{Name: "c_grandparent", Schema: "CREATE TABLE c_grandparent (id INT PRIMARY KEY)"},
	}

	creates, err := DeclarativeToImperative(nil, desired, nil)
	require.NoError(t, err)
	require.Len(t, creates, 3)
	execStatements(t, db, creates)

	current := make([]table.TableSchema, 0, len(desired))
	for _, want := range desired {
		current = append(current, table.TableSchema{Name: want.Name, Schema: showCreateTable(t, db, want.Name)})
	}
	drops, err := DeclarativeToImperative(current, nil, nil)
	require.NoError(t, err)
	require.Len(t, drops, 3)
	execStatements(t, db, drops)

	var count int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()").Scan(&count))
	require.Zero(t, count)
}

func TestToTableSchema(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100) NOT NULL)")
	require.NoError(t, err)

	ts, err := ct.ToTableSchema()
	require.NoError(t, err)
	require.Equal(t, "t1", ts.Name)
	require.Contains(t, ts.Schema, "CREATE TABLE")
	require.Contains(t, ts.Schema, "t1")
	require.Contains(t, ts.Schema, "id")
	require.Contains(t, ts.Schema, "name")
}

// A restored CREATE TABLE must be one line and keep every literal's value.
// Before the fix a newline inside a COMMENT was emitted raw (so a consumer that
// executes the schema line-by-line split the statement mid-literal) and a
// backslash inside a DEFAULT was emitted unescaped (so 'a\\b' came back as
// 'a\b', which MySQL reads as a<BACKSPACE>).
func TestToTableSchema_EscapesControlCharsAndBackslashes(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE t1 (" +
		"id INT PRIMARY KEY, " +
		`c VARCHAR(20) DEFAULT 'a\\b' COMMENT 'line one\nline two', ` +
		"KEY idx_c (c) COMMENT 'idx\\rcomment'" +
		") COMMENT 'tbl\\ncomment'")
	require.NoError(t, err)

	ts, err := ct.ToTableSchema()
	require.NoError(t, err)
	require.NotContains(t, ts.Schema, "\n", "restored schema must be a single line")
	require.NotContains(t, ts.Schema, "\r", "restored schema must be a single line")
	require.Contains(t, ts.Schema, `'a\\b'`)
	require.Contains(t, ts.Schema, `'line one\nline two'`)
	require.Contains(t, ts.Schema, `'idx\rcomment'`)
	require.Contains(t, ts.Schema, `'tbl\ncomment'`)

	// The escaped form parses back to the same values...
	again, err := ParseCreateTable(ts.Schema)
	require.NoError(t, err)
	require.Equal(t, `a\b`, *again.Columns[1].Default)
	require.Equal(t, "line one\nline two", *again.Columns[1].Comment)
	require.Equal(t, "idx\rcomment", *again.Indexes[0].Comment)
	require.Equal(t, "tbl\ncomment", *again.TableOptions.Comment)

	// ...so a re-diff of the two is a no-op.
	diff, err := ct.Diff(again, nil)
	require.NoError(t, err)
	require.Nil(t, diff)
}

package statement

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/table"
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

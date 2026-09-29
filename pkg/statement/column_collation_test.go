package statement

import (
	"strings"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collationCases are ALTERs run against a table keyed on a character column,
// with the collation MySQL leaves the key column under. Each is executed by
// TestColumnCollationChangeMatchesMySQL, so wantAfter is MySQL's answer rather
// than a restatement of the resolution rules under test.
var collationCases = []struct {
	name   string
	create string // %s is the table name
	alter  string // %s is the table name
	column string

	wantAfter      string
	wantChanged    bool
	wantDeclaredAs string
}{
	{
		name:           "MODIFY declaring a different collation",
		create:         "CREATE TABLE %s (a varchar(20) NOT NULL, b int, PRIMARY KEY (a)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		alter:          "ALTER TABLE %s MODIFY COLUMN a varchar(20) COLLATE utf8mb4_bin NOT NULL",
		column:         "a",
		wantAfter:      "utf8mb4_bin",
		wantChanged:    true,
		wantDeclaredAs: "a",
	},
	{
		name:           "MODIFY that omits an explicit column collation drops it",
		create:         "CREATE TABLE %s (a varchar(20) COLLATE utf8mb4_bin NOT NULL, PRIMARY KEY (a)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		alter:          "ALTER TABLE %s MODIFY COLUMN a varchar(40) NOT NULL",
		column:         "a",
		wantAfter:      "utf8mb4_0900_ai_ci",
		wantChanged:    true,
		wantDeclaredAs: "a",
	},
	{
		name:           "MODIFY inheriting a default the same statement changes",
		create:         "CREATE TABLE %s (a varchar(20) NOT NULL, PRIMARY KEY (a)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		alter:          "ALTER TABLE %s DEFAULT COLLATE=utf8mb4_bin, MODIFY COLUMN a varchar(20) NOT NULL",
		column:         "a",
		wantAfter:      "utf8mb4_bin",
		wantChanged:    true,
		wantDeclaredAs: "a",
	},
	{
		name:           "MODIFY declaring only a charset takes its default collation",
		create:         "CREATE TABLE %s (a varchar(20) NOT NULL, PRIMARY KEY (a)) DEFAULT CHARSET=latin1 COLLATE=latin1_bin",
		alter:          "ALTER TABLE %s MODIFY COLUMN a varchar(20) CHARACTER SET latin1 NOT NULL",
		column:         "a",
		wantAfter:      "latin1_swedish_ci",
		wantChanged:    true,
		wantDeclaredAs: "a",
	},
	{
		name:           "MODIFY with the BINARY attribute takes the charset's binary collation",
		create:         "CREATE TABLE %s (a varchar(20) NOT NULL, PRIMARY KEY (a)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		alter:          "ALTER TABLE %s MODIFY COLUMN a varchar(20) BINARY NOT NULL",
		column:         "a",
		wantAfter:      "utf8mb4_bin",
		wantChanged:    true,
		wantDeclaredAs: "a",
	},
	{
		name:           "CHANGE matched on the old name",
		create:         "CREATE TABLE %s (a varchar(20) NOT NULL, PRIMARY KEY (a)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		alter:          "ALTER TABLE %s CHANGE COLUMN A a varchar(20) COLLATE utf8mb4_bin NOT NULL",
		column:         "a",
		wantAfter:      "utf8mb4_bin",
		wantChanged:    true,
		wantDeclaredAs: "A",
	},
	{
		name:        "CONVERT TO re-collates a column it does not name",
		create:      "CREATE TABLE %s (a varchar(20) NOT NULL, b int, PRIMARY KEY (a)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		alter:       "ALTER TABLE %s CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci",
		column:      "a",
		wantAfter:   "utf8mb4_general_ci",
		wantChanged: true,
	},
	{
		name:        "CONVERT TO without a collation takes the charset's default",
		create:      "CREATE TABLE %s (a varchar(20) COLLATE latin1_bin NOT NULL, PRIMARY KEY (a)) DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci",
		alter:       "ALTER TABLE %s CONVERT TO CHARACTER SET latin1",
		column:      "a",
		wantAfter:   "latin1_swedish_ci",
		wantChanged: true,
	},
	{
		name:           "CONVERT TO overrides the collation a MODIFY in the same statement declares",
		create:         "CREATE TABLE %s (a varchar(20) NOT NULL, PRIMARY KEY (a)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		alter:          "ALTER TABLE %s MODIFY COLUMN a varchar(20) COLLATE utf8mb4_bin NOT NULL, CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci",
		column:         "a",
		wantAfter:      "utf8mb4_general_ci",
		wantChanged:    true,
		wantDeclaredAs: "a",
	},
	{
		name:      "CONVERT TO leaves a binary string column alone",
		create:    "CREATE TABLE %s (a varbinary(20) NOT NULL, b varchar(20), PRIMARY KEY (a)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		alter:     "ALTER TABLE %s CONVERT TO CHARACTER SET latin1",
		column:    "a",
		wantAfter: "",
	},
	{
		name:           "MODIFY that only widens the column",
		create:         "CREATE TABLE %s (a varchar(20) NOT NULL, PRIMARY KEY (a)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		alter:          "ALTER TABLE %s MODIFY COLUMN a varchar(40) NOT NULL",
		column:         "a",
		wantAfter:      "utf8mb4_0900_ai_ci",
		wantDeclaredAs: "a",
	},
	{
		name:           "MODIFY restating the collation the column already has",
		create:         "CREATE TABLE %s (a varchar(20) COLLATE utf8mb4_bin NOT NULL, PRIMARY KEY (a)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		alter:          "ALTER TABLE %s MODIFY COLUMN a varchar(40) COLLATE utf8mb4_bin NOT NULL",
		column:         "a",
		wantAfter:      "utf8mb4_bin",
		wantDeclaredAs: "a",
	},
	{
		name:           "the legacy utf8 spelling of the column's own collation",
		create:         "CREATE TABLE %s (a varchar(20) NOT NULL, PRIMARY KEY (a)) DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci",
		alter:          "ALTER TABLE %s MODIFY COLUMN a varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL",
		column:         "a",
		wantAfter:      "utf8mb3_general_ci",
		wantDeclaredAs: "a",
	},
	{
		name:      "a table default change leaves an existing column alone",
		create:    "CREATE TABLE %s (a varchar(20) NOT NULL, PRIMARY KEY (a)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		alter:     "ALTER TABLE %s DEFAULT COLLATE=utf8mb4_bin",
		column:    "a",
		wantAfter: "utf8mb4_0900_ai_ci",
	},
	{
		name:      "a change to another column",
		create:    "CREATE TABLE %s (a varchar(20) NOT NULL, b varchar(20), PRIMARY KEY (a)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		alter:     "ALTER TABLE %s MODIFY COLUMN b varchar(20) COLLATE utf8mb4_bin",
		column:    "a",
		wantAfter: "utf8mb4_0900_ai_ci",
	},
	{
		name:           "a change from a character type to a binary string type loses the collation",
		create:         "CREATE TABLE %s (a varchar(20) NOT NULL, PRIMARY KEY (a)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		alter:          "ALTER TABLE %s MODIFY COLUMN a varbinary(80) NOT NULL",
		column:         "a",
		wantAfter:      "",
		wantChanged:    true,
		wantDeclaredAs: "a",
	},
	{
		name:           "a change from an integer to a character type gains a collation",
		create:         "CREATE TABLE %s (a int NOT NULL, PRIMARY KEY (a)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		alter:          "ALTER TABLE %s MODIFY COLUMN a varchar(20) NOT NULL",
		column:         "a",
		wantAfter:      "utf8mb4_0900_ai_ci",
		wantChanged:    true,
		wantDeclaredAs: "a",
	},
	{
		name:           "a change between integer widths",
		create:         "CREATE TABLE %s (a int NOT NULL, PRIMARY KEY (a)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		alter:          "ALTER TABLE %s MODIFY COLUMN a bigint unsigned NOT NULL",
		column:         "a",
		wantAfter:      "",
		wantDeclaredAs: "a",
	},
	{
		name:           "a charset upgrade between collations that sort the same way",
		create:         "CREATE TABLE %s (a varchar(20) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL, PRIMARY KEY (a)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		alter:          "ALTER TABLE %s MODIFY COLUMN a varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL",
		column:         "a",
		wantAfter:      "utf8mb4_bin",
		wantChanged:    true,
		wantDeclaredAs: "a",
	},
}

// TestColumnCollationChangeMatchesMySQL resolves each ALTER against the
// table's SHOW CREATE TABLE, then runs the ALTER and reads the collation MySQL
// actually left the column under. The two must agree: a caller refusing a
// statement on this answer is claiming what MySQL will do.
func TestColumnCollationChangeMatchesMySQL(t *testing.T) {
	for _, tc := range collationCases {
		t.Run(tc.name, func(t *testing.T) {
			const name = "colcollation"
			tt := testutils.NewTestTable(t, name, strings.ReplaceAll(tc.create, "%s", name))

			current, err := ParseCreateTable(showCreateTable(t, tt.DB, name))
			require.NoError(t, err)
			info, err := current.ToTableInfo("test")
			require.NoError(t, err)
			before, ok := info.GetColumnCollation(tc.column)
			require.True(t, ok)

			alter := strings.ReplaceAll(tc.alter, "%s", name)
			change, determined, err := MustNew(alter)[0].ColumnCollationChange(tc.column, before, info.DefaultCollation)
			require.NoError(t, err)
			require.True(t, determined)
			assert.Equal(t, tc.wantAfter, change.After)
			assert.Equal(t, tc.wantChanged, change.Changed())
			assert.Equal(t, tc.wantDeclaredAs, change.DeclaredAs)

			_, err = tt.DB.ExecContext(t.Context(), alter)
			require.NoError(t, err)
			var actual string
			require.NoError(t, tt.DB.QueryRowContext(t.Context(),
				"SELECT IFNULL(collation_name, '') FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=? AND column_name=?",
				name, tc.column).Scan(&actual))
			assert.Equal(t, normalizeCollationName(strings.ToLower(actual)), change.After,
				"the resolved collation must be the one MySQL leaves the column under")
		})
	}
}

// TestColumnCollationChangeUndetermined covers statements whose result
// depends on a default the inputs do not carry. They must be reported as
// undetermined rather than guessed at, so a caller never refuses a statement
// on a collation it made up.
func TestColumnCollationChangeUndetermined(t *testing.T) {
	tests := []struct {
		name           string
		alter          string
		tableCollation string
	}{
		{
			name:           "CONVERT TO the schema's default charset",
			alter:          "ALTER TABLE t CONVERT TO CHARACTER SET DEFAULT",
			tableCollation: "utf8mb4_0900_ai_ci",
		},
		{
			name:  "redeclaration inheriting an unknown table default",
			alter: "ALTER TABLE t MODIFY COLUMN a varchar(20) NOT NULL",
		},
		{
			// The table default the statement sets is the server's
			// default_collation_for_utf8mb4.
			name:           "redeclaration inheriting a utf8mb4 default the same statement sets",
			alter:          "ALTER TABLE t DEFAULT CHARSET=utf8mb4, MODIFY COLUMN a varchar(20) NOT NULL",
			tableCollation: "utf8mb4_bin",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, determined, err := MustNew(tt.alter)[0].ColumnCollationChange("a", "utf8mb4_0900_ai_ci", tt.tableCollation)
			require.NoError(t, err)
			assert.False(t, determined)
		})
	}

	for _, alter := range utf8mb4WithoutCollation {
		t.Run(alter, func(t *testing.T) {
			_, determined, err := MustNew(strings.ReplaceAll(alter, "%s", "t"))[0].ColumnCollationChange("a", "utf8mb4_general_ci", "utf8mb4_0900_ai_ci")
			require.NoError(t, err)
			assert.False(t, determined)

			// Whichever utf8mb4 collation the server picks, it is not a latin1
			// column's.
			change, determined, err := MustNew(strings.ReplaceAll(alter, "%s", "t"))[0].ColumnCollationChange("a", "latin1_swedish_ci", "latin1_swedish_ci")
			require.NoError(t, err)
			require.True(t, determined)
			assert.True(t, change.Changed())
			assert.Equal(t, "utf8mb4", change.AfterCharset)
			assert.Empty(t, change.After)
		})
	}

	// A key that carries no charset gains a collation, whichever one the
	// server picks.
	change, determined, err := MustNew("ALTER TABLE t MODIFY COLUMN a varchar(20) CHARACTER SET utf8mb4 NOT NULL")[0].
		ColumnCollationChange("a", "", "utf8mb4_0900_ai_ci")
	require.NoError(t, err)
	require.True(t, determined)
	assert.True(t, change.Changed())

	// A redeclaration that spells its collation out needs no default.
	change, determined, err = MustNew("ALTER TABLE t MODIFY COLUMN a varchar(20) COLLATE utf8mb4_bin NOT NULL")[0].
		ColumnCollationChange("a", "utf8mb4_0900_ai_ci", "")
	require.NoError(t, err)
	require.True(t, determined)
	assert.True(t, change.Changed())
}

// utf8mb4WithoutCollation are ALTERs that name utf8mb4 but no collation, so the
// collation they give the key column is the server's
// default_collation_for_utf8mb4.
var utf8mb4WithoutCollation = []string{
	"ALTER TABLE %s MODIFY COLUMN a varchar(20) CHARACTER SET utf8mb4 NOT NULL",
	"ALTER TABLE %s CONVERT TO CHARACTER SET utf8mb4",
}

// TestUTF8MB4DefaultCollationIsTheServers runs each statement in
// utf8mb4WithoutCollation against a key under utf8mb4_general_ci, once for each
// value a server can give default_collation_for_utf8mb4. One server changes the
// key's collation and the other leaves it alone, which is what makes reporting
// the statements as undetermined, rather than assuming utf8mb4_0900_ai_ci, the
// correct answer.
func TestUTF8MB4DefaultCollationIsTheServers(t *testing.T) {
	for _, alter := range utf8mb4WithoutCollation {
		t.Run(alter, func(t *testing.T) {
			var results []string
			for _, serverDefault := range []string{"utf8mb4_0900_ai_ci", "utf8mb4_general_ci"} {
				const name = "utf8mb4default"
				tt := testutils.NewTestTable(t, name, "CREATE TABLE "+name+" (a varchar(20) COLLATE utf8mb4_general_ci NOT NULL, PRIMARY KEY (a)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
				conn, err := tt.DB.Conn(t.Context())
				require.NoError(t, err)
				_, err = conn.ExecContext(t.Context(), "SET SESSION default_collation_for_utf8mb4 = ?", serverDefault)
				require.NoError(t, err)
				_, err = conn.ExecContext(t.Context(), strings.ReplaceAll(alter, "%s", name))
				require.NoError(t, err)
				var collation string
				require.NoError(t, conn.QueryRowContext(t.Context(),
					"SELECT collation_name FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=? AND column_name='a'",
					name).Scan(&collation))
				require.NoError(t, conn.Close())
				results = append(results, collation)
			}
			assert.Equal(t, []string{"utf8mb4_0900_ai_ci", "utf8mb4_general_ci"}, results)
		})
	}
}

// TestColumnCollationChangeNotAlter rejects a statement that is not an ALTER
// TABLE: only an ALTER changes an existing column.
func TestColumnCollationChangeNotAlter(t *testing.T) {
	_, _, err := MustNew("CREATE TABLE t (a varchar(20) NOT NULL PRIMARY KEY)")[0].ColumnCollationChange("a", "utf8mb4_0900_ai_ci", "")
	require.ErrorIs(t, err, ErrNotAlterTable)
}

// TestTableDefaultCollation reads the collation a column declared without one
// takes: the table's COLLATE, or its charset's default collation when only the
// charset is declared and every server agrees on that default.
func TestTableDefaultCollation(t *testing.T) {
	tests := map[string]string{
		"CREATE TABLE t (a int) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin": "utf8mb4_bin",
		"CREATE TABLE t (a int) DEFAULT CHARSET=latin1":                      "latin1_swedish_ci",
		"CREATE TABLE t (a int) DEFAULT CHARSET=utf8mb4":                     "",
		"CREATE TABLE t (a int) DEFAULT CHARSET=utf8 COLLATE=utf8_bin":       "utf8mb3_bin",
		"CREATE TABLE t (a int)":                                             "",
	}
	for create, want := range tests {
		t.Run(create, func(t *testing.T) {
			ct, err := ParseCreateTable(create)
			require.NoError(t, err)
			assert.Equal(t, want, ct.TableDefaultCollation())
		})
	}
}

// TestToTableInfoUnknownCollation builds table metadata from hand-written
// definitions that leave a character column's collation to a default the
// definition does not carry. The collation must be reported as unknown, not as
// the empty collation of a column that carries no charset.
func TestToTableInfoUnknownCollation(t *testing.T) {
	for _, create := range []string{
		"CREATE TABLE t (a varchar(20) NOT NULL, n int, PRIMARY KEY (a))",
		"CREATE TABLE t (a varchar(20) NOT NULL, n int, PRIMARY KEY (a)) DEFAULT CHARSET=utf8mb4",
		"CREATE TABLE t (a varchar(20) CHARACTER SET utf8mb4 NOT NULL, n int, PRIMARY KEY (a)) DEFAULT CHARSET=latin1",
	} {
		t.Run(create, func(t *testing.T) {
			ct, err := ParseCreateTable(create)
			require.NoError(t, err)
			info, err := ct.ToTableInfo("test")
			require.NoError(t, err)
			_, ok := info.GetColumnCollation("a")
			assert.False(t, ok)
			collation, ok := info.GetColumnCollation("n")
			require.True(t, ok)
			assert.Empty(t, collation)
		})
	}

	ct, err := ParseCreateTable("CREATE TABLE t (a varchar(20) NOT NULL, PRIMARY KEY (a)) DEFAULT CHARSET=latin1")
	require.NoError(t, err)
	info, err := ct.ToTableInfo("test")
	require.NoError(t, err)
	collation, ok := info.GetColumnCollation("a")
	require.True(t, ok)
	assert.Equal(t, "latin1_swedish_ci", collation, "latin1 takes the same default collation on every server")
}

// TestToTableInfoCollationsMatchSetInfo reads one live table both ways: from
// information_schema, as preflight does, and from its SHOW CREATE TABLE, as a
// statement-scope classifier does. Both must report the same collation for
// every column and the same table default, or the classifier would refuse
// statements preflight accepts, or accept ones it refuses.
func TestToTableInfoCollationsMatchSetInfo(t *testing.T) {
	const name = "collationparity"
	tt := testutils.NewTestTable(t, name, "CREATE TABLE "+name+` (
		token varchar(64) NOT NULL,
		code char(3) COLLATE utf8mb4_bin NOT NULL,
		label varchar(20) BINARY,
		raw varbinary(16) NOT NULL,
		legacy varchar(10) CHARACTER SET latin1,
		old_utf8 varchar(10) CHARACTER SET utf8mb3,
		kind enum('a','b') COLLATE utf8mb4_general_ci,
		amount bigint NOT NULL,
		PRIMARY KEY (token, code)
	) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`)

	live := table.NewTableInfo(tt.DB, "test", name)
	require.NoError(t, live.SetInfo(t.Context()))

	ct, err := ParseCreateTable(showCreateTable(t, tt.DB, name))
	require.NoError(t, err)
	fromDDL, err := ct.ToTableInfo("test")
	require.NoError(t, err)

	assert.Equal(t, live.DefaultCollation, fromDDL.DefaultCollation)
	require.Equal(t, live.Columns, fromDDL.Columns)
	for _, column := range live.Columns {
		want, ok := live.GetColumnCollation(column)
		require.True(t, ok)
		got, ok := fromDDL.GetColumnCollation(column)
		require.True(t, ok)
		assert.Equal(t, want, got, column)
	}
}

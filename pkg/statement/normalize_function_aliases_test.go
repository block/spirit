package statement

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/dbconn/sqlescape"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// erDefaultValNotAllowed is ER_DEFAULT_VAL_GENERATED_NAMED_FUNCTION_IS_NOT_ALLOWED:
// the server refusing to store a particular function inside a default value
// expression. Which functions are on that list is version-dependent, so a case
// that trips it is skipped rather than failed.
const erDefaultValNotAllowed = 3770

func isDisallowedInDefaultErr(err error) bool {
	var myErr *mysql.MySQLError
	return errors.As(err, &myErr) && myErr.Number == erDefaultValNotAllowed
}

// TestFunctionAliasNormalization covers each alias in the map, in the
// expression contexts MySQL rewrites: expression DEFAULTs, generated columns,
// CHECK constraints, functional indexes and partition expressions.
func TestFunctionAliasNormalization(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantExpr string // the canonical text the rule must produce
		get      func(*CreateTable) *string
	}{
		{
			name:     "vector default (issue #1152)",
			sql:      "CREATE TABLE t (v vector(3) DEFAULT (STRING_TO_VECTOR('[1,2,3]')))",
			wantExpr: "to_vector('[1,2,3]')",
			get:      firstColumnDefault,
		},
		{
			name:     "vector_to_string",
			sql:      "CREATE TABLE t (v varchar(64) DEFAULT (VECTOR_TO_STRING(STRING_TO_VECTOR('[1]'))))",
			wantExpr: "from_vector(to_vector('[1]'))",
			get:      firstColumnDefault,
		},
		{
			name:     "lcase",
			sql:      "CREATE TABLE t (c varchar(10) DEFAULT (LCASE('AB')))",
			wantExpr: "lower('AB')",
			get:      firstColumnDefault,
		},
		{
			name:     "ucase",
			sql:      "CREATE TABLE t (c varchar(10) DEFAULT (UCASE('ab')))",
			wantExpr: "upper('ab')",
			get:      firstColumnDefault,
		},
		{
			name:     "mid",
			sql:      "CREATE TABLE t (c varchar(10) DEFAULT (MID('abcdef',1,2)))",
			wantExpr: "substr('abcdef', 1, 2)",
			get:      firstColumnDefault,
		},
		{
			name:     "substring",
			sql:      "CREATE TABLE t (c varchar(10) DEFAULT (SUBSTRING('abcdef',1,2)))",
			wantExpr: "substr('abcdef', 1, 2)",
			get:      firstColumnDefault,
		},
		{
			name:     "power",
			sql:      "CREATE TABLE t (c int DEFAULT (POWER(2,3)))",
			wantExpr: "pow(2, 3)",
			get:      firstColumnDefault,
		},
		{
			name:     "ceil",
			sql:      "CREATE TABLE t (c int DEFAULT (CEIL(1.2)))",
			wantExpr: "ceiling(1.2)",
			get:      firstColumnDefault,
		},
		{
			name:     "character_length",
			sql:      "CREATE TABLE t (c int DEFAULT (CHARACTER_LENGTH('abc')))",
			wantExpr: "char_length('abc')",
			get:      firstColumnDefault,
		},
		{
			name:     "octet_length",
			sql:      "CREATE TABLE t (c int DEFAULT (OCTET_LENGTH('abc')))",
			wantExpr: "length('abc')",
			get:      firstColumnDefault,
		},
		{
			name:     "day",
			sql:      "CREATE TABLE t (c int DEFAULT (DAY('2020-01-01')))",
			wantExpr: "dayofmonth('2020-01-01')",
			get:      firstColumnDefault,
		},
		{
			// POSITION(x IN y) is restored as an infix form, locate(x, y) as a
			// plain call — the rename has to survive the change of shape.
			name:     "position",
			sql:      "CREATE TABLE t (c int DEFAULT (POSITION('a' IN 'abc')))",
			wantExpr: "locate('a', 'abc')",
			get:      firstColumnDefault,
		},
		{
			name:     "session_user",
			sql:      "CREATE TABLE t (c varchar(64) DEFAULT (SESSION_USER()))",
			wantExpr: "user()",
			get:      firstColumnDefault,
		},
		{
			name:     "system_user",
			sql:      "CREATE TABLE t (c varchar(64) DEFAULT (SYSTEM_USER()))",
			wantExpr: "user()",
			get:      firstColumnDefault,
		},
		{
			name:     "current_date",
			sql:      "CREATE TABLE t (c int DEFAULT (YEAR(CURRENT_DATE)))",
			wantExpr: "year(curdate())",
			get:      firstColumnDefault,
		},
		{
			name:     "current_time",
			sql:      "CREATE TABLE t (c int DEFAULT (HOUR(CURRENT_TIME)))",
			wantExpr: "hour(curtime())",
			get:      firstColumnDefault,
		},
		{
			// An expression default keeps the call form, so the whole
			// timestamp family collapses onto now() the way MySQL stores it.
			name:     "current_timestamp inside an expression default",
			sql:      "CREATE TABLE t (c datetime DEFAULT (CURRENT_TIMESTAMP))",
			wantExpr: "now()",
			get:      firstColumnDefault,
		},
		{
			name:     "localtimestamp inside an expression default",
			sql:      "CREATE TABLE t (c datetime DEFAULT (LOCALTIMESTAMP()))",
			wantExpr: "now()",
			get:      firstColumnDefault,
		},
		{
			name:     "nested aliases",
			sql:      "CREATE TABLE t (c varchar(10) DEFAULT (LOWER(MID(UCASE('abc'),1,2))))",
			wantExpr: "lower(substr(upper('abc'), 1, 2))",
			get:      firstColumnDefault,
		},
		{
			name:     "generated column",
			sql:      "CREATE TABLE t (a varchar(10), c varchar(10) GENERATED ALWAYS AS (LCASE(a)) VIRTUAL)",
			wantExpr: "LOWER(`a`)",
			get:      func(ct *CreateTable) *string { return ct.Columns[1].GeneratedExpr },
		},
		{
			name:     "table-level CHECK",
			sql:      "CREATE TABLE t (c varchar(10), CHECK (OCTET_LENGTH(c) > 0))",
			wantExpr: "LENGTH(`c`)>0",
			get:      func(ct *CreateTable) *string { return ct.Constraints[0].Expression },
		},
		{
			// Hoisted to a table-level constraint by columnCheckNormalizer.
			name:     "column-level CHECK",
			sql:      "CREATE TABLE t (c varchar(10) CHECK (LCASE(c) = c))",
			wantExpr: "LOWER(`c`)=`c`",
			get:      func(ct *CreateTable) *string { return ct.Constraints[0].Expression },
		},
		{
			name:     "functional index",
			sql:      "CREATE TABLE t (c varchar(10), KEY idx ((UCASE(c))))",
			wantExpr: "UPPER(`c`)",
			get:      func(ct *CreateTable) *string { return ct.Indexes[0].ColumnList[0].Expression },
		},
		{
			name:     "partition expression",
			sql:      "CREATE TABLE t (dt date NOT NULL, PRIMARY KEY (dt)) PARTITION BY HASH (DAY(dt)) PARTITIONS 2",
			wantExpr: "DAYOFMONTH(`dt`)",
			get:      func(ct *CreateTable) *string { return ct.Partition.Expression },
		},
		{
			name: "subpartition expression",
			sql: "CREATE TABLE t (id int NOT NULL, dt date NOT NULL, PRIMARY KEY (id, dt)) " +
				"PARTITION BY RANGE (YEAR(dt)) SUBPARTITION BY HASH (DAY(dt)) SUBPARTITIONS 2 " +
				"(PARTITION p0 VALUES LESS THAN (2020), PARTITION p1 VALUES LESS THAN MAXVALUE)",
			wantExpr: "dayofmonth(`dt`)",
			get:      func(ct *CreateTable) *string { return ct.Partition.SubPartition.Expression },
		},
		{
			// A stored function is not a builtin: renaming db.day() would call
			// something else entirely.
			name:     "schema-qualified call is left alone",
			sql:      "CREATE TABLE t (c int DEFAULT (mydb.day('2020-01-01')))",
			wantExpr: "`mydb`.`day`('2020-01-01')",
			get:      firstColumnDefault,
		},
		{
			// The value of a string default is data, not an expression: a
			// column defaulting to the text "lcase(x)" keeps it verbatim.
			name:     "string literal default is left alone",
			sql:      "CREATE TABLE t (c text DEFAULT ('lcase(x)'))",
			wantExpr: "lcase(x)",
			get:      firstColumnDefault,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ct, err := ParseCreateTable(tc.sql)
			require.NoError(t, err)
			got := tc.get(ct)
			require.NotNil(t, got)
			assert.Equal(t, tc.wantExpr, *got)
		})
	}
}

func firstColumnDefault(ct *CreateTable) *string { return ct.Columns[0].Default }

// TestFunctionAliasConverges is the payoff: the authored spelling and the
// SHOW CREATE TABLE spelling of the same column must produce no diff. Without
// the rule the diff emits a MODIFY COLUMN that MySQL immediately rewrites back
// — a full table copy on every run, forever (issue #1152).
func TestFunctionAliasConverges(t *testing.T) {
	tests := []struct {
		name     string
		authored string
		live     string
	}{
		{
			name:     "vector default",
			authored: "CREATE TABLE t (id int NOT NULL PRIMARY KEY, v vector(3) DEFAULT (STRING_TO_VECTOR('[1,2,3]')))",
			live:     "CREATE TABLE t (id int NOT NULL PRIMARY KEY, v vector(3) DEFAULT (to_vector(_latin1'[1,2,3]')))",
		},
		{
			name:     "expression default with a charset introducer",
			authored: "CREATE TABLE t (id int NOT NULL PRIMARY KEY, c varchar(10) DEFAULT (LCASE('AB')))",
			live:     "CREATE TABLE t (id int NOT NULL PRIMARY KEY, c varchar(10) DEFAULT (lower(_latin1'AB')))",
		},
		{
			name:     "timestamp expression default",
			authored: "CREATE TABLE t (id int NOT NULL PRIMARY KEY, c datetime DEFAULT (CURRENT_TIMESTAMP))",
			live:     "CREATE TABLE t (id int NOT NULL PRIMARY KEY, c datetime DEFAULT (now()))",
		},
		{
			name:     "generated column",
			authored: "CREATE TABLE t (id int NOT NULL PRIMARY KEY, a varchar(10), c varchar(10) GENERATED ALWAYS AS (MID(a,1,2)) STORED)",
			live:     "CREATE TABLE t (id int NOT NULL PRIMARY KEY, a varchar(10), c varchar(10) GENERATED ALWAYS AS (substr(`a`,1,2)) STORED)",
		},
		{
			name:     "check constraint",
			authored: "CREATE TABLE t (id int NOT NULL PRIMARY KEY, c varchar(10), CONSTRAINT t_chk_1 CHECK (CHARACTER_LENGTH(c) > 0))",
			live:     "CREATE TABLE t (id int NOT NULL PRIMARY KEY, c varchar(10), CONSTRAINT t_chk_1 CHECK ((char_length(`c`) > 0)))",
		},
		{
			name:     "functional index",
			authored: "CREATE TABLE t (id int NOT NULL PRIMARY KEY, c varchar(10), KEY idx ((SUBSTRING(c,1,2))))",
			live:     "CREATE TABLE t (id int NOT NULL PRIMARY KEY, c varchar(10), KEY idx ((substr(`c`,1,2))))",
		},
		{
			name:     "partition expression",
			authored: "CREATE TABLE t (dt date NOT NULL, PRIMARY KEY (dt)) PARTITION BY HASH (DAY(dt)) PARTITIONS 2",
			live:     "CREATE TABLE t (dt date NOT NULL, PRIMARY KEY (dt)) PARTITION BY HASH (dayofmonth(`dt`)) PARTITIONS 2",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			authored, err := ParseCreateTable(tc.authored)
			require.NoError(t, err)
			live, err := ParseCreateTable(tc.live)
			require.NoError(t, err)

			stmts, err := live.Diff(authored, nil)
			require.NoError(t, err)
			assert.Nil(t, stmts, "authored and live spellings must converge")
		})
	}
}

// TestRoundTrip_FunctionAliases is the same convergence check against a real
// server: MySQL, not a hand-written fixture, decides what a stored expression
// reads back as. Every alias in mysqlFunctionAliases is exercised here, so a
// mapping that a future MySQL stops applying (or applies differently) shows up
// as a failure rather than as a diff that never converges in production.
func TestRoundTrip_FunctionAliases(t *testing.T) {
	db := openScratch(t)

	tests := []struct {
		name     string
		columns  string // column list of the authored CREATE TABLE
		suffix   string // clauses after the column list, e.g. PARTITION BY
		needsVec bool
		// mayBeDisallowed marks a case whose function MySQL refuses to store in
		// a default expression on some versions; such a case skips rather than
		// fails when the server rejects it. See erDefaultValNotAllowed.
		mayBeDisallowed bool
	}{
		{name: "lcase", columns: "c varchar(10) DEFAULT (LCASE('AB'))"},
		{name: "ucase", columns: "c varchar(10) DEFAULT (UCASE('ab'))"},
		{name: "mid", columns: "c varchar(10) DEFAULT (MID('abcdef',1,2))"},
		{name: "substring", columns: "c varchar(10) DEFAULT (SUBSTRING('abcdef',1,2))"},
		{name: "substring_from_for", columns: "c varchar(10) DEFAULT (SUBSTRING('abcdef' FROM 2 FOR 3))"},
		{name: "power", columns: "c int DEFAULT (POWER(2,3))"},
		{name: "ceil", columns: "c int DEFAULT (CEIL(1.2))"},
		{name: "character_length", columns: "c int DEFAULT (CHARACTER_LENGTH('abc'))"},
		{name: "octet_length", columns: "c int DEFAULT (OCTET_LENGTH('abc'))"},
		{name: "day", columns: "c int DEFAULT (DAY('2020-01-01'))"},
		{name: "position", columns: "c int DEFAULT (POSITION('a' IN 'abc'))"},
		// Which functions a default expression may contain moved over time:
		// 8.0.28 rejects user() outright (error 3770), while 8.0.46 and 9.7
		// store it. Both spellings resolve to user() before that check runs, so
		// on a server that rejects it neither the authored nor the canonical
		// form is storable and the round trip cannot be observed at all.
		{name: "session_user", columns: "c varchar(64) DEFAULT (SESSION_USER())", mayBeDisallowed: true},
		{name: "system_user", columns: "c varchar(64) DEFAULT (SYSTEM_USER())", mayBeDisallowed: true},
		{name: "current_date", columns: "c int DEFAULT (YEAR(CURRENT_DATE))"},
		{name: "current_time", columns: "c int DEFAULT (HOUR(CURRENT_TIME))"},
		{name: "current_timestamp", columns: "c datetime DEFAULT (CURRENT_TIMESTAMP)"},
		{name: "localtime", columns: "c datetime DEFAULT (LOCALTIME)"},
		{name: "localtimestamp", columns: "c datetime DEFAULT (LOCALTIMESTAMP())"},
		{name: "nested", columns: "c varchar(10) DEFAULT (LOWER(MID(UCASE('abc'),1,2)))"},
		{name: "generated_column", columns: "a varchar(10), c varchar(10) GENERATED ALWAYS AS (LCASE(a)) VIRTUAL"},
		{name: "check_constraint", columns: "c varchar(10), CHECK (CHARACTER_LENGTH(c) > 0)"},
		{name: "functional_index", columns: "c varchar(10), KEY idx ((UCASE(c)))"},
		{
			name:    "partition_expression",
			columns: "dt date NOT NULL, PRIMARY KEY (dt)",
			suffix:  "PARTITION BY HASH (DAY(dt)) PARTITIONS 2",
		},
		// A subpartitioned table has no live case here: diffPartitions does not
		// compare subpartitioning at all, so such a table re-emits a
		// REMOVE PARTITIONING + PARTITION BY pair (which also drops the
		// SUBPARTITION clause) whatever the expressions say. The rule's effect
		// on SubPartition.Expression is covered by the unit test above.
		{
			name:     "string_to_vector",
			columns:  "v vector(3) DEFAULT (STRING_TO_VECTOR('[1,2,3]'))",
			needsVec: true,
		},
		{
			name:     "vector_to_string",
			columns:  "c varchar(64) DEFAULT (VECTOR_TO_STRING(STRING_TO_VECTOR('[1]')))",
			needsVec: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.needsVec {
				testutils.SkipUnlessVectorSupported(t)
			}
			table := "fa_" + tc.name
			// A partitioned case declares its own key (every partitioning
			// column has to be in it), so it opts out of the shared id column.
			idColumn := "id int NOT NULL PRIMARY KEY, "
			if tc.suffix != "" {
				idColumn = ""
			}
			authored := strings.TrimSpace(fmt.Sprintf("CREATE TABLE %s (%s%s) %s",
				sqlescape.EscapeIdentifier(table), idColumn, tc.columns, tc.suffix))

			_, err := db.ExecContext(t.Context(), "DROP TABLE IF EXISTS "+sqlescape.EscapeIdentifier(table))
			require.NoError(t, err)
			_, err = db.ExecContext(t.Context(), authored)
			if tc.mayBeDisallowed && isDisallowedInDefaultErr(err) {
				t.Skipf("skipping: this server does not allow the function in a default expression: %v", err)
			}
			require.NoError(t, err, "MySQL rejected the authored table")
			t.Cleanup(func() {
				_, _ = db.ExecContext(t.Context(), "DROP TABLE IF EXISTS "+sqlescape.EscapeIdentifier(table))
			})

			live, err := ParseCreateTable(showCreate(t, db, table))
			require.NoError(t, err)
			target, err := ParseCreateTable(authored)
			require.NoError(t, err)

			stmts, err := live.Diff(target, nil)
			require.NoError(t, err)
			assert.Nil(t, stmts, "authored schema must not diff against the table MySQL stored")
		})
	}
}

// TestRoundTrip_ExpressionDefaultApplies checks the emission side of the
// expression-default handling: an ALTER that Spirit generates for a
// timestamp-family expression default has to be SQL MySQL accepts. The
// bare-keyword spelling of the literal default — DEFAULT (now) — is not.
func TestRoundTrip_ExpressionDefaultApplies(t *testing.T) {
	db := openScratch(t)

	const table = "fa_expr_default"
	createSQL := "CREATE TABLE " + table + " (id int NOT NULL PRIMARY KEY, c datetime)"
	targetSQL := "CREATE TABLE " + table + " (id int NOT NULL PRIMARY KEY, c datetime DEFAULT (CURRENT_TIMESTAMP))"

	afterCreate := applyAndConverge(t, db, table, createSQL, targetSQL)
	assert.Contains(t, afterCreate, "DEFAULT (now())")
}

// TestFunctionAliasesAreIdempotent guards the property the rule relies on: a
// canonical name is never itself an alias, so normalizing an already-normalized
// definition is a no-op. Were a name both key and value, the rule would map a
// live definition onto something MySQL does not store and the diff would flip
// between two forms instead of converging.
func TestFunctionAliasesAreIdempotent(t *testing.T) {
	for alias, canonical := range mysqlFunctionAliases {
		_, isAlias := mysqlFunctionAliases[canonical]
		assert.False(t, isAlias, "%s maps to %s, which is itself an alias", alias, canonical)
		assert.Equal(t, canonical, strings.ToLower(canonical), "canonical names must be lowercase")
		assert.Equal(t, alias, strings.ToLower(alias), "alias keys are matched against FnName.L, so must be lowercase")
	}
}

// TestFunctionAliasNormalizationIsStable checks the rule is a fixed point:
// feeding a normalized expression back through the parser must not shift it
// again, which is what lets a diff converge in one round rather than oscillate
// between two forms.
func TestFunctionAliasNormalizationIsStable(t *testing.T) {
	tests := []struct {
		template string // one %s, holding the expression
		expr     string
		get      func(*CreateTable) *string
	}{
		{
			template: "CREATE TABLE t (v vector(3) DEFAULT (%s))",
			expr:     "STRING_TO_VECTOR('[1,2,3]')",
			get:      firstColumnDefault,
		},
		{
			template: "CREATE TABLE t (c datetime DEFAULT (%s))",
			expr:     "CURRENT_TIMESTAMP",
			get:      firstColumnDefault,
		},
		{
			template: "CREATE TABLE t (a varchar(10), c varchar(10) GENERATED ALWAYS AS (%s) VIRTUAL)",
			expr:     "LCASE(a)",
			get:      func(ct *CreateTable) *string { return ct.Columns[1].GeneratedExpr },
		},
		{
			template: "CREATE TABLE t (c varchar(10), CHECK (%s))",
			expr:     "OCTET_LENGTH(c) > 0",
			get:      func(ct *CreateTable) *string { return ct.Constraints[0].Expression },
		},
		{
			template: "CREATE TABLE t (c varchar(10), KEY idx ((%s)))",
			expr:     "UCASE(c)",
			get:      func(ct *CreateTable) *string { return ct.Indexes[0].ColumnList[0].Expression },
		},
	}
	for _, tc := range tests {
		t.Run(tc.expr, func(t *testing.T) {
			once, err := ParseCreateTable(fmt.Sprintf(tc.template, tc.expr))
			require.NoError(t, err)
			first := tc.get(once)
			require.NotNil(t, first)

			twice, err := ParseCreateTable(fmt.Sprintf(tc.template, *first))
			require.NoError(t, err)
			second := tc.get(twice)
			require.NotNil(t, second)
			assert.Equal(t, *first, *second)
		})
	}
}

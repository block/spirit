package statement

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// diffContract is one MySQL round-trip contract for CreateTable.Diff. A table
// is created from target and its SHOW CREATE TABLE captured as the expected end
// state; the table is then recreated from source, the live definition is diffed
// against target, every emitted statement is executed, and the resulting
// SHOW CREATE TABLE must equal the expected one byte for byte. A second diff of
// the result against target must then be empty.
//
// Comparing against MySQL's own rendering of the target, rather than only
// checking that the second diff is empty, is what catches a silent omission: an
// attribute the parser drops on both sides diffs clean while the live table
// never had it.
type diffContract struct {
	name   string
	source string // the body of CREATE TABLE t, e.g. "(id INT PRIMARY KEY)"
	target string
	// sourceSessionCharset creates the source table from a session with that
	// character_set_client (SET NAMES), which is how MySQL comes to report a
	// non-utf8mb4 introducer on a literal the user wrote bare.
	sourceSessionCharset string
	// wantNoop asserts that the live source already satisfies target: the diff
	// must be empty, and the SHOW CREATE TABLE comparison is skipped because
	// the two definitions are equivalent rather than identical.
	wantNoop bool
	opts     *DiffOptions // nil selects the defaults
}

// compressedDiffOptions compares ROW_FORMAT and the table-level
// KEY_BLOCK_SIZE, which the defaults ignore.
func compressedDiffOptions() *DiffOptions {
	opts := NewDiffOptions()
	opts.IgnoreRowFormat = false
	return opts
}

func runDiffContract(t *testing.T, c diffContract) {
	t.Helper()
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	ctx := t.Context()
	exec := func(stmt string) {
		t.Helper()
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err, "executing: %s", stmt)
	}

	exec("CREATE TABLE t " + c.target)
	expected := showCreateTable(t, db, "t")
	exec("DROP TABLE t")

	if c.sourceSessionCharset == "" {
		exec("CREATE TABLE t " + c.source)
	} else {
		// A separate pool, so the SET NAMES never leaks into db's connections.
		sdb, err := sql.Open("block-mysql", testutils.DSNForDatabase(dbName))
		require.NoError(t, err)
		conn, err := sdb.Conn(ctx)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "SET NAMES "+c.sourceSessionCharset)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "CREATE TABLE t "+c.source)
		require.NoError(t, err)
		require.NoError(t, conn.Close())
		require.NoError(t, sdb.Close())
	}

	live := showCreateTable(t, db, "t")
	src, err := ParseCreateTable(live)
	require.NoError(t, err, live)
	dst, err := ParseCreateTable("CREATE TABLE t " + c.target)
	require.NoError(t, err)
	stmts, err := src.Diff(dst, c.opts)
	require.NoError(t, err)
	applied := make([]string, 0, len(stmts))
	for _, s := range stmts {
		applied = append(applied, s.Statement)
	}
	if c.wantNoop {
		assert.Empty(t, applied, "live:\n%s", live)
		return
	}
	for _, s := range applied {
		exec(s)
	}
	actual := showCreateTable(t, db, "t")
	require.Equal(t, expected, actual, "live:\n%s\napplied:\n%s", live, strings.Join(applied, "\n"))

	again, err := ParseCreateTable(actual)
	require.NoError(t, err)
	stmts, err = again.Diff(dst, c.opts)
	require.NoError(t, err)
	assert.Empty(t, stmts, "second diff must be empty")
}

// TestDiffMySQLContracts runs the round-trip contracts. Each case is one
// correctness finding; the comment above a group names what used to go wrong.
func TestDiffMySQLContracts(t *testing.T) {
	contracts := []diffContract{
		// Charset introducers. Every introducer used to be stripped from a
		// restored expression, which changed its meaning (CHAR_LENGTH of a
		// _binary literal counts bytes) or made it invalid (a COLLATE whose
		// charset came from the introducer, error 1253). See restoreExprText.
		{
			name:   "generated column keeps a _binary introducer",
			source: "(id INT PRIMARY KEY)",
			target: "(id INT PRIMARY KEY, g INT AS (CHAR_LENGTH(_binary'€') + id) STORED)",
		},
		{
			name:   "check keeps a _binary introducer",
			source: "(id INT PRIMARY KEY, c VARCHAR(10), CHECK (c <> 'q'))",
			target: "(id INT PRIMARY KEY, c VARCHAR(10), CHECK (c <> _binary'q'))",
		},
		{
			name:   "functional index keeps a _binary introducer",
			source: "(id INT PRIMARY KEY, z VARCHAR(10))",
			target: "(id INT PRIMARY KEY, z VARCHAR(10), KEY fk ((CONCAT(z, _binary'x'))))",
		},
		{
			name:   "expression default keeps the introducer under COLLATE",
			source: "(id INT PRIMARY KEY)",
			target: "(id INT PRIMARY KEY, c VARCHAR(10) DEFAULT (_latin1'a' COLLATE latin1_bin))",
		},
		// Every introducer but _utf8mb4 is kept, wherever the literal sits:
		// an introducer can decide the value even of an ASCII literal
		// (UPPER(_latin5'i'), STRCMP under a PAD SPACE collation), and the
		// diff cannot tell. It used to fold an ASCII latin1 or utf8mb3
		// literal away outside COLLATE, which emitted error 1253 under
		// COLLATE and a different value elsewhere.
		// TestDiffIntegrationExpressionDefaultIntroducerValues checks the
		// values. See restoreExprText.
		{
			name:   "expression default keeps the introducer under COLLATE over a function",
			source: "(id INT PRIMARY KEY)",
			target: "(id INT PRIMARY KEY, c VARCHAR(10) DEFAULT (CONCAT(_latin1'a') COLLATE latin1_bin))",
		},
		{
			name:   "expression default keeps the introducer inside CHARSET()",
			source: "(id INT PRIMARY KEY)",
			target: "(id INT PRIMARY KEY, c VARCHAR(64) DEFAULT (CHARSET(_latin1'a')))",
		},
		{
			name:   "expression default keeps the introducer inside COLLATION()",
			source: "(id INT PRIMARY KEY)",
			target: "(id INT PRIMARY KEY, c VARCHAR(64) DEFAULT (COLLATION(_utf8mb3'a')))",
		},
		{
			name:   "generated column keeps the introducer inside WEIGHT_STRING()",
			source: "(id INT PRIMARY KEY)",
			target: "(id INT PRIMARY KEY, g VARCHAR(64) AS (HEX(WEIGHT_STRING(_latin1'a'))) STORED)",
		},
		{
			name:   "CHECK keeps the introducer deep inside CHARSET()",
			source: "(id INT PRIMARY KEY, c VARCHAR(64))",
			target: "(id INT PRIMARY KEY, c VARCHAR(64), CONSTRAINT ck CHECK (c <> CHARSET(IF(id, _latin1'a', _latin1'b'))))",
		},
		{
			// MySQL keeps the _latin1 in the stored expression. The value
			// happens to be the same, but the diff cannot tell (UPPER(_latin5'i')
			// is not UPPER('i')), so the bare spelling diffs once; the MODIFY
			// stores UPPER('a') from a utf8mb4 session, reported as
			// upper(_utf8mb4'a'), which renders bare, and the two agree.
			name:   "an ASCII latin1 literal under UPPER() is replaced by the bare one",
			source: "(id INT PRIMARY KEY, g VARCHAR(10) AS (UPPER(_latin1'a')) STORED)",
			target: "(id INT PRIMARY KEY, g VARCHAR(10) AS (UPPER('a')) STORED)",
		},
		{
			// The same expression stored from a utf8mb3 or a latin1 session
			// reports _utf8mb3 or _latin1 on its literal; the schema's bare
			// literal diffs once and the re-stored expression agrees.
			name:                 "literal stored from a utf8mb3 session is re-stored once",
			source:               "(id INT PRIMARY KEY, c VARCHAR(10), CONSTRAINT ck CHECK (c <> 'A'))",
			target:               "(id INT PRIMARY KEY, c VARCHAR(10), CONSTRAINT ck CHECK (c <> 'A'))",
			sourceSessionCharset: "utf8",
		},
		{
			name:                 "ASCII literal stored from a latin1 session is re-stored once",
			source:               "(id INT PRIMARY KEY, g INT AS (LENGTH('abc')) STORED)",
			target:               "(id INT PRIMARY KEY, g INT AS (LENGTH('abc')) STORED)",
			sourceSessionCharset: "latin1",
		},
		{
			// From a latin1 session the UTF-8 bytes of 'é' are read as two
			// latin1 characters, so the stored expression is a different one.
			name:                 "non-ASCII literal stored from a latin1 session is not the bare literal",
			source:               "(id INT PRIMARY KEY, g INT AS (CHAR_LENGTH('é')) STORED)",
			target:               "(id INT PRIMARY KEY, g INT AS (CHAR_LENGTH('é')) STORED)",
			sourceSessionCharset: "latin1",
		},
		// Column-level CHECKs: every one a column carries is kept, with its
		// enforcement. Only the last used to survive, and NOT ENFORCED was lost.
		{
			name:   "both column checks are added",
			source: "(id INT PRIMARY KEY, c INT)",
			target: "(id INT PRIMARY KEY, c INT CHECK (c > 0) CHECK (c < 10))",
		},
		{
			name:     "both column checks are kept",
			source:   "(id INT PRIMARY KEY, c INT CHECK (c > 0) CHECK (c < 10))",
			target:   "(id INT PRIMARY KEY, c INT CHECK (c > 0) CHECK (c < 10))",
			wantNoop: true,
		},
		{
			name:   "column check stays not enforced",
			source: "(id INT PRIMARY KEY, c INT)",
			target: "(id INT PRIMARY KEY, c INT CONSTRAINT ck CHECK (c > 0) NOT ENFORCED)",
		},
		{
			name:   "column check enforcement is toggled",
			source: "(id INT PRIMARY KEY, c INT CONSTRAINT ck CHECK (c > 0))",
			target: "(id INT PRIMARY KEY, c INT CONSTRAINT ck CHECK (c > 0) NOT ENFORCED)",
		},
		// Invisible columns and the other per-column attributes. They were
		// not modeled, so a change to one was never emitted, and a MODIFY
		// for any other reason silently cleared them.
		{
			name:   "column becomes invisible",
			source: "(id INT PRIMARY KEY, c INT)",
			target: "(id INT PRIMARY KEY, c INT INVISIBLE)",
		},
		{
			name:   "column becomes visible",
			source: "(id INT PRIMARY KEY, c INT INVISIBLE)",
			target: "(id INT PRIMARY KEY, c INT)",
		},
		{
			name:   "invisible column survives a comment change",
			source: "(id INT PRIMARY KEY, c INT INVISIBLE)",
			target: "(id INT PRIMARY KEY, c INT INVISIBLE COMMENT 'x')",
		},
		{
			name:   "secondary engine attribute survives a comment change",
			source: `(id INT PRIMARY KEY, c INT SECONDARY_ENGINE_ATTRIBUTE='{"x":1}')`,
			target: `(id INT PRIMARY KEY, c INT SECONDARY_ENGINE_ATTRIBUTE='{"x":1}' COMMENT 'x')`,
		},
		{
			name:     "secondary engine attribute as MySQL re-serializes it is a no-op",
			source:   `(id INT PRIMARY KEY, c INT SECONDARY_ENGINE_ATTRIBUTE='{"b":1,"a":[1,2]}')`,
			target:   `(id INT PRIMARY KEY, c INT SECONDARY_ENGINE_ATTRIBUTE='{"b":1,"a":[1,2]}')`,
			wantNoop: true,
		},
		{
			name:   "not secondary, column format and storage survive a comment change",
			source: "(id INT PRIMARY KEY, c INT NOT SECONDARY COLUMN_FORMAT FIXED STORAGE DISK)",
			target: "(id INT PRIMARY KEY, c INT NOT SECONDARY COLUMN_FORMAT FIXED STORAGE DISK COMMENT 'x')",
		},
		{
			name:   "generated invisible column is added with its attributes",
			source: "(id INT PRIMARY KEY)",
			target: "(id INT PRIMARY KEY, g INT GENERATED ALWAYS AS (id + 1) STORED NOT NULL INVISIBLE COMMENT 'g')",
		},
		// Column order. Positions are decided by replaying the clauses the
		// way MySQL applies them; a DROP used to suppress the reorder of the
		// column after it.
		{
			name:   "columns are reordered after a drop",
			source: "(id INT PRIMARY KEY, a INT, b INT, c INT, d INT)",
			target: "(id INT PRIMARY KEY, d INT, b INT)",
		},
		{
			name:   "columns are reordered around an added column",
			source: "(id INT PRIMARY KEY, a INT, b INT)",
			target: "(id INT PRIMARY KEY, x INT, b INT, a INT)",
		},
		{
			name:   "columns are rotated",
			source: "(a INT, b INT, c INT, d INT)",
			target: "(c INT, d INT, a INT, b INT)",
		},
		{
			name:   "columns are reversed",
			source: "(a INT, b INT, c INT, d INT)",
			target: "(d INT, c INT, b INT, a INT)",
		},
		// Table options SHOW CREATE TABLE reports and Diff used to discard.
		{
			name:   "table statistics options are applied",
			source: "(id INT PRIMARY KEY)",
			target: "(id INT PRIMARY KEY) STATS_PERSISTENT=0 STATS_AUTO_RECALC=1 STATS_SAMPLE_PAGES=42",
		},
		{
			name:   "table statistics options are reset",
			source: "(id INT PRIMARY KEY) STATS_PERSISTENT=0 STATS_AUTO_RECALC=1 STATS_SAMPLE_PAGES=42",
			target: "(id INT PRIMARY KEY)",
		},
		{
			name:     "AUTOEXTEND_SIZE suffix matches the live byte count",
			source:   "(id INT PRIMARY KEY) AUTOEXTEND_SIZE=4194304",
			target:   "(id INT PRIMARY KEY) AUTOEXTEND_SIZE=4M",
			wantNoop: true,
		},
		{
			name:   "AUTOEXTEND_SIZE is applied",
			source: "(id INT PRIMARY KEY)",
			target: "(id INT PRIMARY KEY) AUTOEXTEND_SIZE=4M",
		},
		{
			name:   "AUTOEXTEND_SIZE is reset",
			source: "(id INT PRIMARY KEY) AUTOEXTEND_SIZE=4M",
			target: "(id INT PRIMARY KEY)",
		},
		{
			name:   "table SECONDARY_ENGINE_ATTRIBUTE is applied",
			source: "(id INT PRIMARY KEY)",
			target: `(id INT PRIMARY KEY) SECONDARY_ENGINE_ATTRIBUTE='{"t":1}'`,
		},
		{
			name:   "table SECONDARY_ENGINE_ATTRIBUTE is reset",
			source: `(id INT PRIMARY KEY) SECONDARY_ENGINE_ATTRIBUTE='{"t":1}'`,
			target: "(id INT PRIMARY KEY)",
		},
		{
			name:     "table SECONDARY_ENGINE_ATTRIBUTE compares as JSON",
			source:   `(id INT PRIMARY KEY) SECONDARY_ENGINE_ATTRIBUTE='{"b":1,"a":[1,2]}'`,
			target:   `(id INT PRIMARY KEY) SECONDARY_ENGINE_ATTRIBUTE='{"a": [1, 2], "b": 1}'`,
			wantNoop: true,
		},
		{
			name:   "table storage hints are applied",
			source: "(id INT PRIMARY KEY)",
			target: "(id INT PRIMARY KEY) MIN_ROWS=10 MAX_ROWS=1000 AVG_ROW_LENGTH=100 PACK_KEYS=1 CHECKSUM=1 DELAY_KEY_WRITE=1",
		},
		{
			name:   "table storage hints are reset",
			source: "(id INT PRIMARY KEY) MIN_ROWS=10 MAX_ROWS=1000 AVG_ROW_LENGTH=100 PACK_KEYS=1 CHECKSUM=1 DELAY_KEY_WRITE=1",
			target: "(id INT PRIMARY KEY)",
		},
		{
			name:   "table KEY_BLOCK_SIZE is applied with the row format",
			source: "(id INT PRIMARY KEY) ROW_FORMAT=COMPRESSED",
			target: "(id INT PRIMARY KEY) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4",
			opts:   &DiffOptions{IgnoreAutoIncrement: true, IgnoreEngine: true, IgnoreRowFormat: false},
		},
		{
			// InnoDB rejects ROW_FORMAT=DYNAMIC while a KEY_BLOCK_SIZE is set,
			// so the clearing KEY_BLOCK_SIZE=0 must travel in the same ALTER.
			name:   "table KEY_BLOCK_SIZE is cleared with the row format",
			source: "(id INT PRIMARY KEY) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4",
			target: "(id INT PRIMARY KEY) ROW_FORMAT=DYNAMIC",
			opts:   &DiffOptions{IgnoreAutoIncrement: true, IgnoreEngine: true, IgnoreRowFormat: false},
		},
		// A row format the target leaves out. The diff emitted nothing for
		// COMPACT -> omitted, and ROW_FORMAT=DEFAULT on the target re-emitted
		// itself forever: MySQL stores it as no row format.
		{
			name:   "row format is cleared when the target omits it",
			source: "(id INT PRIMARY KEY) ROW_FORMAT=COMPACT",
			target: "(id INT PRIMARY KEY)",
			opts:   &DiffOptions{IgnoreAutoIncrement: true, IgnoreEngine: true, IgnoreRowFormat: false},
		},
		{
			name:   "row format is cleared by a target ROW_FORMAT=DEFAULT",
			source: "(id INT PRIMARY KEY) ROW_FORMAT=COMPACT",
			target: "(id INT PRIMARY KEY) ROW_FORMAT=DEFAULT",
			opts:   &DiffOptions{IgnoreAutoIncrement: true, IgnoreEngine: true, IgnoreRowFormat: false},
		},
		{
			name:   "explicit DYNAMIC row format is cleared when the target omits it",
			source: "(id INT PRIMARY KEY) ROW_FORMAT=DYNAMIC",
			target: "(id INT PRIMARY KEY)",
			opts:   &DiffOptions{IgnoreAutoIncrement: true, IgnoreEngine: true, IgnoreRowFormat: false},
		},
		{
			name:   "compressed row format is cleared when the target omits it",
			source: "(id INT PRIMARY KEY) ROW_FORMAT=COMPRESSED",
			target: "(id INT PRIMARY KEY)",
			opts:   &DiffOptions{IgnoreAutoIncrement: true, IgnoreEngine: true, IgnoreRowFormat: false},
		},
		{
			name:   "compressed row format and KEY_BLOCK_SIZE are cleared when the target omits them",
			source: "(id INT PRIMARY KEY) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8",
			target: "(id INT PRIMARY KEY)",
			opts:   &DiffOptions{IgnoreAutoIncrement: true, IgnoreEngine: true, IgnoreRowFormat: false},
		},
		{
			name:     "target ROW_FORMAT=DEFAULT against no row format is a no-op",
			source:   "(id INT PRIMARY KEY)",
			target:   "(id INT PRIMARY KEY) ROW_FORMAT=DEFAULT",
			opts:     &DiffOptions{IgnoreAutoIncrement: true, IgnoreEngine: true, IgnoreRowFormat: false},
			wantNoop: true,
		},
		{
			name:     "row format is left alone by default",
			source:   "(id INT PRIMARY KEY) ROW_FORMAT=COMPACT",
			target:   "(id INT PRIMARY KEY)",
			wantNoop: true,
		},
		// An index's SECONDARY_ENGINE_ATTRIBUTE.
		{
			name:   "index SECONDARY_ENGINE_ATTRIBUTE is applied",
			source: "(id INT PRIMARY KEY, c INT, KEY k (c))",
			target: `(id INT PRIMARY KEY, c INT, KEY k (c) SECONDARY_ENGINE_ATTRIBUTE='{"k":1}')`,
		},
		{
			name:   "index SECONDARY_ENGINE_ATTRIBUTE is removed",
			source: `(id INT PRIMARY KEY, c INT, KEY k (c) SECONDARY_ENGINE_ATTRIBUTE='{"k":1}')`,
			target: "(id INT PRIMARY KEY, c INT, KEY k (c))",
		},
		{
			name:   "index SECONDARY_ENGINE_ATTRIBUTE survives a comment change",
			source: `(id INT PRIMARY KEY, c INT, KEY k (c) COMMENT 'a' SECONDARY_ENGINE_ATTRIBUTE='{"k":1}')`,
			target: `(id INT PRIMARY KEY, c INT, KEY k (c) COMMENT 'b' SECONDARY_ENGINE_ATTRIBUTE='{"k":1}')`,
		},
		{
			name:   "index SECONDARY_ENGINE_ATTRIBUTE survives a visibility change",
			source: `(id INT PRIMARY KEY, c INT, KEY k (c) SECONDARY_ENGINE_ATTRIBUTE='{"k":1}')`,
			target: `(id INT PRIMARY KEY, c INT, KEY k (c) SECONDARY_ENGINE_ATTRIBUTE='{"k":1}' INVISIBLE)`,
		},
		// Index options MySQL accepts and does not store.
		{
			name:     "primary key VISIBLE is a no-op",
			source:   "(id INT PRIMARY KEY)",
			target:   "(id INT, PRIMARY KEY (id) VISIBLE)",
			wantNoop: true,
		},
		{
			name:     "secondary index VISIBLE is a no-op",
			source:   "(id INT PRIMARY KEY, c INT, KEY k (c))",
			target:   "(id INT PRIMARY KEY, c INT, KEY k (c) VISIBLE)",
			wantNoop: true,
		},
		{
			name:     "USING HASH is a no-op on InnoDB",
			source:   "(id INT PRIMARY KEY, c INT, KEY k (c))",
			target:   "(id INT PRIMARY KEY, c INT, KEY k (c) USING HASH)",
			wantNoop: true,
		},
		{
			name:   "USING BTREE is applied",
			source: "(id INT PRIMARY KEY, c INT, KEY k (c))",
			target: "(id INT PRIMARY KEY, c INT, KEY k (c) USING BTREE)",
		},
		{
			name:   "USING BTREE is removed",
			source: "(id INT PRIMARY KEY, c INT, KEY k (c) USING BTREE)",
			target: "(id INT PRIMARY KEY, c INT, KEY k (c))",
		},
		{
			name:     "index KEY_BLOCK_SIZE is a no-op on an uncompressed table",
			source:   "(id INT AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id))",
			target:   "(id INT AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id) KEY_BLOCK_SIZE=8)",
			wantNoop: true,
		},
		{
			name:   "index KEY_BLOCK_SIZE is applied on a compressed table",
			source: "(id INT PRIMARY KEY, c INT, KEY k (c)) ROW_FORMAT=COMPRESSED",
			target: "(id INT PRIMARY KEY, c INT, KEY k (c) KEY_BLOCK_SIZE=8) ROW_FORMAT=COMPRESSED",
		},

		// AUTO_INCREMENT implies NOT NULL (autoIncrementNotNullNormalizer).
		{
			name:     "AUTO_INCREMENT without NOT NULL is NOT NULL",
			source:   "(id INT NOT NULL AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id))",
			target:   "(id INT AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id))",
			wantNoop: true,
		},
		{
			name:     "NULL before AUTO_INCREMENT is NOT NULL",
			source:   "(id INT NOT NULL AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id))",
			target:   "(id INT NULL AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id))",
			wantNoop: true,
		},
		{
			name:     "AUTO_INCREMENT DEFAULT NULL is NOT NULL with no default",
			source:   "(id INT NOT NULL AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id))",
			target:   "(id INT AUTO_INCREMENT DEFAULT NULL, x INT PRIMARY KEY, UNIQUE KEY k (id))",
			wantNoop: true,
		},
		{
			name:   "adding an AUTO_INCREMENT column without NOT NULL",
			source: "(x INT PRIMARY KEY)",
			target: "(x INT PRIMARY KEY, id INT AUTO_INCREMENT, UNIQUE KEY k (id))",
		},

		// Functional index parenthesization and column-reference case
		// (expressionParenNormalizer, columnReferenceCaseNormalizer).
		{
			name:   "functional index converges",
			source: "(id INT PRIMARY KEY, c INT)",
			target: "(id INT PRIMARY KEY, c INT, KEY k ((c+1)))",
		},
		{
			name:     "functional index in another parenthesization is a no-op",
			source:   "(id INT PRIMARY KEY, c INT, KEY k ((c+1)))",
			target:   "(id INT PRIMARY KEY, c INT, KEY k (((c)+(1))))",
			wantNoop: true,
		},
		{
			name:   "functional index expression change",
			source: "(id INT PRIMARY KEY, c INT, KEY k ((c+1)))",
			target: "(id INT PRIMARY KEY, c INT, KEY k ((c+2)))",
		},
		{
			name:   "functional index written in another column case",
			source: "(id INT PRIMARY KEY, c INT)",
			target: "(id INT PRIMARY KEY, c INT, KEY k ((C+1)))",
		},
		{
			name:   "generated column written in another column case",
			source: "(id INT PRIMARY KEY, c INT)",
			target: "(id INT PRIMARY KEY, c INT, g INT AS (C + 1) STORED)",
		},
		{
			name:     "generated column reference case is a no-op",
			source:   "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL)",
			target:   "(id INT PRIMARY KEY, c INT, g INT AS (C + 1) VIRTUAL)",
			wantNoop: true,
		},
		{
			name:     "CHECK reference case is a no-op",
			source:   "(id INT PRIMARY KEY, c INT, CONSTRAINT chk CHECK (c > 0))",
			target:   "(id INT PRIMARY KEY, c INT, CONSTRAINT chk CHECK (C > 0))",
			wantNoop: true,
		},
		{
			name:     "partition expression reference case is a no-op",
			source:   "(id INT PRIMARY KEY) PARTITION BY HASH (id) PARTITIONS 2",
			target:   "(id INT PRIMARY KEY) PARTITION BY HASH (ID) PARTITIONS 2",
			wantNoop: true,
		},

		// Numeric literal defaults (numericDefaultNormalizer): MySQL stores the
		// literal converted to the column's type and reports the result.
		{
			name:   "decimal default converges",
			source: "(id INT PRIMARY KEY, c DECIMAL(6,2))",
			target: "(id INT PRIMARY KEY, c DECIMAL(6,2) DEFAULT 1.2)",
		},
		{
			name:   "decimal default changed",
			source: "(id INT PRIMARY KEY, c DECIMAL(6,2) DEFAULT 1.2)",
			target: "(id INT PRIMARY KEY, c DECIMAL(6,2) DEFAULT 1.235)",
		},
		{
			name:     "decimal default already padded is a no-op",
			source:   "(id INT PRIMARY KEY, c DECIMAL(6,2) DEFAULT 1.2)",
			target:   "(id INT PRIMARY KEY, c DECIMAL(6,2) DEFAULT '1.20')",
			wantNoop: true,
		},
		{
			name:   "decimal default from the keyword converges",
			source: "(id INT PRIMARY KEY, c DECIMAL(4,2) NOT NULL)",
			target: "(id INT PRIMARY KEY, c DECIMAL(4,2) NOT NULL DEFAULT TRUE)",
		},
		{
			name:   "integer default from a padded string converges",
			source: "(id INT PRIMARY KEY, c INT)",
			target: "(id INT PRIMARY KEY, c INT DEFAULT '001')",
		},
		{
			name:   "integer default from a float literal converges",
			source: "(id INT PRIMARY KEY, c INT)",
			target: "(id INT PRIMARY KEY, c INT DEFAULT 2.5e0)",
		},
		{
			name:     "unsigned integer default from a negative string that rounds to zero is a no-op",
			source:   "(id INT PRIMARY KEY, c INT UNSIGNED DEFAULT 0)",
			target:   "(id INT PRIMARY KEY, c INT UNSIGNED DEFAULT '-0.4')",
			wantNoop: true,
		},
		// SECONDARY_ENGINE_ATTRIBUTE compared as JSON with MySQL's number
		// model: integers exact, so a change past 2^53 is a change; 1e2 and
		// 100.0 one value; an integer past uint64 the double MySQL stores.
		{
			name:   "index attribute changing a large integer converges",
			source: `(id INT PRIMARY KEY, c INT, KEY k (c) SECONDARY_ENGINE_ATTRIBUTE='{"x":9007199254740992}')`,
			target: `(id INT PRIMARY KEY, c INT, KEY k (c) SECONDARY_ENGINE_ATTRIBUTE='{"x":9007199254740993}')`,
		},
		{
			name:   "column attribute changing a large integer converges",
			source: `(id INT PRIMARY KEY, c INT SECONDARY_ENGINE_ATTRIBUTE='{"x":9007199254740992}')`,
			target: `(id INT PRIMARY KEY, c INT SECONDARY_ENGINE_ATTRIBUTE='{"x":9007199254740993}')`,
		},
		{
			name:   "table attribute changing a large integer converges",
			source: `(id INT PRIMARY KEY) SECONDARY_ENGINE_ATTRIBUTE='{"x":9007199254740992}'`,
			target: `(id INT PRIMARY KEY) SECONDARY_ENGINE_ATTRIBUTE='{"x":9007199254740993}'`,
		},
		{
			name:     "attribute number forms MySQL re-serializes are a no-op",
			source:   `(id INT PRIMARY KEY, c INT SECONDARY_ENGINE_ATTRIBUTE='{"z":1e2,"y":1.0,"w":12345678901234567890123}')`,
			target:   `(id INT PRIMARY KEY, c INT SECONDARY_ENGINE_ATTRIBUTE='{"w": 1.2345678901234568e22, "y": 1.0, "z": 100.0}')`,
			wantNoop: true,
		},
		// An integer against a double compares exactly: MySQL stores
		// 9007199254740993 as an integer and 9007199254740992.0 as a double
		// (reported as 9.007199254740992e15), and the comparison used to
		// round the integer to a float64 first and see no change.
		{
			name:   "index attribute changing a large integer to a nearby double converges",
			source: `(id INT PRIMARY KEY, c INT, KEY k (c) SECONDARY_ENGINE_ATTRIBUTE='{"x":9007199254740993}')`,
			target: `(id INT PRIMARY KEY, c INT, KEY k (c) SECONDARY_ENGINE_ATTRIBUTE='{"x":9007199254740992.0}')`,
		},
		{
			name:   "column attribute changing a double to the integer it rounds converges",
			source: `(id INT PRIMARY KEY, c INT SECONDARY_ENGINE_ATTRIBUTE='{"x":9007199254740993.0}')`,
			target: `(id INT PRIMARY KEY, c INT SECONDARY_ENGINE_ATTRIBUTE='{"x":9007199254740993}')`,
		},
		// An index whose options alone change is replaced by a swap: the
		// replacement is added under a temporary name and the old index
		// dropped in one statement, then renamed back. The standalone DROP
		// INDEX the two-statement plan used to start with is refused for the
		// only index on an AUTO_INCREMENT column (error 1075) and for the
		// index a foreign key depends on (error 1553).
		{
			name:   "option change on the only index of an AUTO_INCREMENT column",
			source: "(id INT AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id))",
			target: `(id INT AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id) SECONDARY_ENGINE_ATTRIBUTE='{"x":1}')`,
		},
		{
			name:   "option change on the index a foreign key depends on",
			source: "(id INT PRIMARY KEY, p INT, KEY k (p), CONSTRAINT fk_p FOREIGN KEY (p) REFERENCES t (id)) ROW_FORMAT=COMPRESSED",
			target: "(id INT PRIMARY KEY, p INT, KEY k (p) KEY_BLOCK_SIZE=4, CONSTRAINT fk_p FOREIGN KEY (p) REFERENCES t (id)) ROW_FORMAT=COMPRESSED",
		},
		{
			// The replacement is appended after the existing indexes, as any
			// re-created index is, so the swapped index is declared last.
			name:   "option change next to an index under the temporary name",
			source: "(id INT PRIMARY KEY, c INT, KEY _K_new (id), KEY k (c)) ROW_FORMAT=COMPRESSED",
			target: "(id INT PRIMARY KEY, c INT, KEY _K_new (id), KEY k (c) KEY_BLOCK_SIZE=4) ROW_FORMAT=COMPRESSED",
		},
		// A table-level KEY_BLOCK_SIZE change on a table that stays compressed
		// does not converge (MySQL keeps the old size on the existing
		// indexes) and is not a contract; compressed tables are out of scope.
		// Compared under IgnoreRowFormat: false; the default ignores the
		// option.
		{
			name:   "table KEY_BLOCK_SIZE change from the implicit size",
			source: "(id INT PRIMARY KEY, c INT, KEY k (c)) ROW_FORMAT=COMPRESSED",
			target: "(id INT PRIMARY KEY, c INT, KEY k (c)) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4",
			opts:   compressedDiffOptions(),
		},
		{
			name:   "compressed table becoming uncompressed",
			source: "(id INT PRIMARY KEY, c INT, KEY k (c)) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8",
			target: "(id INT PRIMARY KEY, c INT, KEY k (c)) ROW_FORMAT=DYNAMIC",
			opts:   compressedDiffOptions(),
		},
		{
			name:     "index declaring the table's KEY_BLOCK_SIZE is no change",
			source:   "(id INT PRIMARY KEY, c INT, KEY k (c)) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4",
			target:   "(id INT PRIMARY KEY, c INT, KEY k (c) KEY_BLOCK_SIZE=4) ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=4",
			opts:     compressedDiffOptions(),
			wantNoop: true,
		},
		{
			// MySQL rounds a scaled real default as floor(x) + rint(frac·10^D)/10^D
			// (Field_real::truncate), not toward zero: -1e-17 at D=20 stores
			// as 0 and -0.0005 at D=3 as 0.000, while -0.0006 is -0.001 and
			// -2.0005 is -2.001. A model that truncated the fraction would
			// never converge on the first two.
			name:   "scaled double default below the scale converges on zero",
			source: "(id INT PRIMARY KEY, c DOUBLE(30,20))",
			target: "(id INT PRIMARY KEY, c DOUBLE(30,20) DEFAULT -1e-17)",
		},
		{
			name:   "scaled double negative half default converges on zero",
			source: "(id INT PRIMARY KEY, c DOUBLE(10,3))",
			target: "(id INT PRIMARY KEY, c DOUBLE(10,3) DEFAULT -0.0005)",
		},
		{
			name:   "scaled double negative default past half converges",
			source: "(id INT PRIMARY KEY, c DOUBLE(10,3))",
			target: "(id INT PRIMARY KEY, c DOUBLE(10,3) DEFAULT -0.0006)",
		},
		{
			name:   "scaled double negative default above one converges",
			source: "(id INT PRIMARY KEY, c DOUBLE(10,3))",
			target: "(id INT PRIMARY KEY, c DOUBLE(10,3) DEFAULT -2.0005)",
		},
		{
			name:   "double default from an exponent converges",
			source: "(id INT PRIMARY KEY, c DOUBLE)",
			target: "(id INT PRIMARY KEY, c DOUBLE DEFAULT 1e2)",
		},
		{
			name:   "double default in exponent notation converges",
			source: "(id INT PRIMARY KEY, c DOUBLE)",
			target: "(id INT PRIMARY KEY, c DOUBLE DEFAULT 123456789012345678)",
		},
		{
			name:   "double default below the fixed range converges",
			source: "(id INT PRIMARY KEY, c DOUBLE)",
			target: "(id INT PRIMARY KEY, c DOUBLE DEFAULT 0.0000000000000001234)",
		},
		{
			name:   "double default with a scale converges",
			source: "(id INT PRIMARY KEY, c DOUBLE(10,3))",
			target: "(id INT PRIMARY KEY, c DOUBLE(10,3) DEFAULT 2.0005)",
		},
		// A float is compared by its exact value. One whose six-digit SHOW
		// CREATE TABLE text reads back as the same float converges; one
		// spelled past six digits keeps diffing, which
		// TestDiffIntegrationFloatDefaultValueAsWritten pins.
		{
			name:   "float default with six digits converges",
			source: "(id INT PRIMARY KEY, c FLOAT)",
			target: "(id INT PRIMARY KEY, c FLOAT DEFAULT 1.23457)",
		},
		{
			name:   "float default from an exponent converges",
			source: "(id INT PRIMARY KEY, c FLOAT)",
			target: "(id INT PRIMARY KEY, c FLOAT DEFAULT 1e38)",
		},
		{
			name:   "float default changed below six digits",
			source: "(id INT PRIMARY KEY, c FLOAT DEFAULT 1234570)",
			target: "(id INT PRIMARY KEY, c FLOAT DEFAULT 1234560)",
		},
		{
			name:   "float denormal default converges",
			source: "(id INT PRIMARY KEY, c FLOAT)",
			target: "(id INT PRIMARY KEY, c FLOAT DEFAULT 1e-45)",
		},
		{
			name:   "float default with a scale converges",
			source: "(id INT PRIMARY KEY, c FLOAT(7,4))",
			target: "(id INT PRIMARY KEY, c FLOAT(7,4) DEFAULT 1.5)",
		},
		{
			name:   "varchar default from a decimal literal converges",
			source: "(id INT PRIMARY KEY, c VARCHAR(10))",
			target: "(id INT PRIMARY KEY, c VARCHAR(10) DEFAULT 1.50)",
		},
		{
			name:   "varchar default from a float literal converges",
			source: "(id INT PRIMARY KEY, c VARCHAR(10))",
			target: "(id INT PRIMARY KEY, c VARCHAR(10) DEFAULT 1.5E+2)",
		},
		{
			name:   "binary default from a decimal literal is padded",
			source: "(id INT PRIMARY KEY, c BINARY(5))",
			target: "(id INT PRIMARY KEY, c BINARY(5) DEFAULT 1.5)",
		},
		// Temporal literal defaults (temporalDefaultNormalizer): MySQL stores
		// the literal as a date or time and reports the stored value. A
		// fraction past the precision is read only where MySQL's rounding
		// and its TIME_TRUNCATE_FRACTIONAL truncation agree; the literals
		// they disagree on keep diffing, which
		// TestDiffIntegrationTemporalDefaultTruncateFractional pins.
		{
			name:   "datetime default from a date converges",
			source: "(id INT PRIMARY KEY, c DATETIME)",
			target: "(id INT PRIMARY KEY, c DATETIME DEFAULT '2020-1-1')",
		},
		{
			name:   "datetime default changed",
			source: "(id INT PRIMARY KEY, c DATETIME DEFAULT '2020-01-01')",
			target: "(id INT PRIMARY KEY, c DATETIME DEFAULT '2020-01-02 10:00:00')",
		},
		{
			name:     "datetime default already canonical is a no-op",
			source:   "(id INT PRIMARY KEY, c DATETIME DEFAULT '2020-01-01 00:00:00')",
			target:   "(id INT PRIMARY KEY, c DATETIME DEFAULT '2020-01-01')",
			wantNoop: true,
		},
		{
			name:   "datetime default with precision pads the fraction",
			source: "(id INT PRIMARY KEY, c DATETIME(3))",
			target: "(id INT PRIMARY KEY, c DATETIME(3) DEFAULT '2020-01-01 10:00:00')",
		},
		{
			name:   "datetime default with a fraction below the half converges",
			source: "(id INT PRIMARY KEY, c DATETIME)",
			target: "(id INT PRIMARY KEY, c DATETIME DEFAULT '2020-01-01 23:59:59.4')",
		},
		{
			name:   "datetime default from a number converges",
			source: "(id INT PRIMARY KEY, c DATETIME)",
			target: "(id INT PRIMARY KEY, c DATETIME DEFAULT 20200101100000)",
		},
		{
			name:   "datetime default from a compact string converges",
			source: "(id INT PRIMARY KEY, c DATETIME(6))",
			target: "(id INT PRIMARY KEY, c DATETIME(6) DEFAULT '20200101T100000.1234564')",
		},
		{
			name:   "timestamp default converges",
			source: "(id INT PRIMARY KEY, c TIMESTAMP NULL)",
			target: "(id INT PRIMARY KEY, c TIMESTAMP NULL DEFAULT '2020-01-01')",
		},
		{
			name:   "date default from a number converges",
			source: "(id INT PRIMARY KEY, c DATE)",
			target: "(id INT PRIMARY KEY, c DATE DEFAULT 20200101)",
		},
		{
			name:   "date default from a datetime string converges",
			source: "(id INT PRIMARY KEY, c DATE)",
			target: "(id INT PRIMARY KEY, c DATE DEFAULT '2020-01-01 23:59:59.4')",
		},
		{
			name:   "time default converges",
			source: "(id INT PRIMARY KEY, c TIME)",
			target: "(id INT PRIMARY KEY, c TIME DEFAULT '1:2')",
		},
		{
			name:   "time default with days converges",
			source: "(id INT PRIMARY KEY, c TIME)",
			target: "(id INT PRIMARY KEY, c TIME DEFAULT '1 2:3:4.4')",
		},
		{
			name:   "time default from a number converges",
			source: "(id INT PRIMARY KEY, c TIME(1))",
			target: "(id INT PRIMARY KEY, c TIME(1) DEFAULT 1.54)",
		},
		{
			name:   "time default fraction cut at the precision converges",
			source: "(id INT PRIMARY KEY, c TIME(6))",
			target: "(id INT PRIMARY KEY, c TIME(6) DEFAULT '10:00:00.1234564')",
		},
		{
			name:     "time default negative zero is a no-op",
			source:   "(id INT PRIMARY KEY, c TIME DEFAULT '00:00:00')",
			target:   "(id INT PRIMARY KEY, c TIME DEFAULT '-0:00:00.4')",
			wantNoop: true,
		},
		// Operand grouping (expressionParenNormalizer): only a nested AND or
		// OR regroups. A bitwise operator evaluates on binary strings or on
		// integers by operand, so its grouping is the value: the diff used to
		// emit _binary'12' & _binary'21' & 7, which is 0, for a default of 4.
		// TestDiffIntegrationExpressionGroupingValues checks the values.
		{
			name:   "expression default keeps the grouping of bitwise operands",
			source: "(id INT PRIMARY KEY)",
			target: "(id INT PRIMARY KEY, c INT DEFAULT (_binary'12' & (_binary'21' & 7)))",
		},
		{
			name:   "generated column keeps the grouping of bitwise operands",
			source: "(id INT PRIMARY KEY, a INT, b INT)",
			target: "(id INT PRIMARY KEY, a INT, b INT, g INT AS (a | (b | id)) STORED)",
		},
		{
			name:   "check keeps the grouping of bitwise operands",
			source: "(id INT PRIMARY KEY, a INT, b INT)",
			target: "(id INT PRIMARY KEY, a INT, b INT, CONSTRAINT ck CHECK (a ^ (b ^ id) > 0))",
		},
		{
			name:     "nested AND still regroups",
			source:   "(id INT PRIMARY KEY, a INT, b INT, CONSTRAINT ck CHECK (a > 0 AND (b > 0 AND id > 0)))",
			target:   "(id INT PRIMARY KEY, a INT, b INT, CONSTRAINT ck CHECK ((a > 0 AND b > 0) AND id > 0))",
			wantNoop: true,
		},
		// A comparison between quoted literals. The quote stripping that turns
		// DEFAULT ('a') into the value a keyed off the rendered text, so once
		// the canonical form had dropped the parentheses of ('a') = ('a') the
		// default became the unparsable a'='a and the diff's own ALTER failed
		// to parse.
		{
			name:   "expression default comparing quoted literals",
			source: "(id INT PRIMARY KEY)",
			target: "(id INT PRIMARY KEY, c INT DEFAULT (('a') = ('a')), d INT DEFAULT ('a' LIKE 'a%'), e VARCHAR(5) DEFAULT (('a')))",
		},
		// Expression defaults and unary plus (expressionParenNormalizer): MySQL
		// stores an expression in its own parenthesization and drops every
		// unary plus when it parses it.
		{
			name:   "expression default with a negated literal converges",
			source: "(id INT PRIMARY KEY, c INT)",
			target: "(id INT PRIMARY KEY, c INT DEFAULT (-1))",
		},
		{
			name:   "expression default changed",
			source: "(id INT PRIMARY KEY, c INT DEFAULT (-1))",
			target: "(id INT PRIMARY KEY, c INT DEFAULT (-2))",
		},
		{
			name:     "expression default negated as MySQL spells it is a no-op",
			source:   "(id INT PRIMARY KEY, c INT DEFAULT (-(1)))",
			target:   "(id INT PRIMARY KEY, c INT DEFAULT (-1))",
			wantNoop: true,
		},
		{
			name:   "expression default with a unary plus converges",
			source: "(id INT PRIMARY KEY, c INT)",
			target: "(id INT PRIMARY KEY, c INT DEFAULT (+1))",
		},
		{
			name:   "expression default with a negated argument converges",
			source: "(id INT PRIMARY KEY, c INT)",
			target: "(id INT PRIMARY KEY, c INT DEFAULT (abs(-1)))",
		},
		{
			name:   "expression default with a negated call converges",
			source: "(id INT PRIMARY KEY, c DOUBLE)",
			target: "(id INT PRIMARY KEY, c DOUBLE DEFAULT (-pi()))",
		},
		{
			name:   "expression default with a negated string converges",
			source: "(id INT PRIMARY KEY, c INT)",
			target: "(id INT PRIMARY KEY, c INT DEFAULT (-'1'))",
		},
		{
			name:   "expression default with a unary plus on a string converges",
			source: "(id INT PRIMARY KEY, c VARCHAR(10))",
			target: "(id INT PRIMARY KEY, c VARCHAR(10) DEFAULT (+'1'))",
		},
		{
			name:   "expression default with a negated product converges",
			source: "(id INT PRIMARY KEY, c INT)",
			target: "(id INT PRIMARY KEY, c INT DEFAULT (2 * -1))",
		},
		{
			name:   "check constraint with a unary plus converges",
			source: "(id INT PRIMARY KEY, c INT)",
			target: "(id INT PRIMARY KEY, c INT, CONSTRAINT chk CHECK (c > +1))",
		},
		{
			name:   "generated column with a unary plus converges",
			source: "(id INT PRIMARY KEY, c INT)",
			target: "(id INT PRIMARY KEY, c INT, g INT GENERATED ALWAYS AS (+c + +1) VIRTUAL)",
		},
		// Generated columns changing to or from VIRTUAL. The diff used to emit
		// a MODIFY COLUMN, which MySQL rejects (error 3106); the column is
		// dropped and added back, with whatever reads it. See rebuiltColumns.
		// A VIRTUAL column becoming a regular column is staged through STORED
		// so that it keeps its values (see virtualToRegularIntermediate and
		// TestDiffIntegrationVirtualToRegularKeepsValues, which checks them).
		{
			name:   "generated column VIRTUAL to STORED is rebuilt",
			source: "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL)",
			target: "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) STORED)",
		},
		{
			name:   "generated column STORED to VIRTUAL is rebuilt",
			source: "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) STORED)",
			target: "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL)",
		},
		{
			name:   "generated column VIRTUAL to regular is staged through STORED",
			source: "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL)",
			target: "(id INT PRIMARY KEY, c INT, g INT)",
		},
		{
			name:   "generated column VIRTUAL to regular with a type change and other changes",
			source: "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL)",
			target: "(id INT PRIMARY KEY, c INT, g BIGINT NOT NULL DEFAULT 0, d INT)",
		},
		{
			name:   "generated column VIRTUAL to regular while its read column is dropped",
			source: "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL)",
			target: "(id INT PRIMARY KEY, g INT)",
		},
		{
			name:   "generated column VIRTUAL to regular under a functional index and a CHECK",
			source: "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL, KEY kf ((g + 1)), CONSTRAINT ck CHECK (g > 0))",
			target: "(id INT PRIMARY KEY, c INT, g INT, KEY kf ((g + 1)), CONSTRAINT ck CHECK (g > 0))",
		},
		{
			name:   "generated column VIRTUAL to regular with a dependent generated column",
			source: "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL, s INT AS (g + 1) STORED)",
			target: "(id INT PRIMARY KEY, c INT, g INT, s INT AS (g + 1) STORED)",
		},
		{
			name:   "dependent STORED column becoming regular is modified, not rebuilt",
			source: "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL, s INT AS (g + 1) STORED)",
			target: "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) STORED, s INT)",
		},
		{
			name:   "regular column to generated VIRTUAL is rebuilt",
			source: "(id INT PRIMARY KEY, c INT, g INT)",
			target: "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL)",
		},
		{
			name:   "regular column to generated STORED is modified",
			source: "(id INT PRIMARY KEY, c INT, g INT)",
			target: "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) STORED)",
		},
		{
			name:   "generated rebuild keeps the column position",
			source: "(id INT PRIMARY KEY, g INT AS (id + 1) VIRTUAL, c INT)",
			target: "(id INT PRIMARY KEY, g INT AS (id + 1) STORED, c INT)",
		},
		{
			name:   "generated rebuild keeps plain indexes on the column",
			source: "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL, KEY kg (g), UNIQUE KEY ug (g), KEY kcg (c, g))",
			target: "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) STORED, KEY kg (g), UNIQUE KEY ug (g), KEY kcg (c, g))",
		},
		{
			name:   "generated rebuild re-adds a functional index on the column",
			source: "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL, KEY kf ((g + 1)))",
			target: "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) STORED, KEY kf ((g + 1)))",
		},
		{
			name:   "generated rebuild re-adds a single-column CHECK",
			source: "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL, CONSTRAINT ck CHECK (g > 0))",
			target: "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) STORED, CONSTRAINT ck CHECK (g > 0))",
		},
		{
			name:   "generated rebuild re-adds a multi-column CHECK",
			source: "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL, CONSTRAINT ck CHECK (g > c))",
			target: "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) STORED, CONSTRAINT ck CHECK (g > c))",
		},
		{
			name:   "generated rebuild re-adds a column-level CHECK under its server name",
			source: "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) VIRTUAL CHECK (g > 0))",
			target: "(id INT PRIMARY KEY, c INT, g INT AS (c + 1) STORED CHECK (g > 0))",
		},
		{
			name:   "generated rebuild cascades to a dependent generated column",
			source: "(id INT PRIMARY KEY, g INT AS (id + 1) VIRTUAL, h INT AS (g + 1) VIRTUAL, c INT)",
			target: "(id INT PRIMARY KEY, g INT AS (id + 1) STORED, h INT AS (g + 1) VIRTUAL, c INT)",
		},
		// SRID changes under a spatial index. The diff used to emit one
		// MODIFY, which MySQL rejects while the index exists (error 3644),
		// even with a DROP INDEX in the same ALTER. See
		// spatialIndexesBlockingSRIDChange.
		{
			name:   "SRID change under a spatial index",
			source: "(id INT PRIMARY KEY, p POINT NOT NULL SRID 0, SPATIAL KEY k (p))",
			target: "(id INT PRIMARY KEY, p POINT NOT NULL SRID 4326, SPATIAL KEY k (p))",
		},
		{
			name:   "SRID added under a spatial index",
			source: "(id INT PRIMARY KEY, p POINT NOT NULL, SPATIAL KEY k (p))",
			target: "(id INT PRIMARY KEY, p POINT NOT NULL SRID 4326, SPATIAL KEY k (p))",
		},
		{
			name:   "SRID removed under a spatial index",
			source: "(id INT PRIMARY KEY, p POINT NOT NULL SRID 4326, SPATIAL KEY k (p))",
			target: "(id INT PRIMARY KEY, p POINT NOT NULL, SPATIAL KEY k (p))",
		},
		{
			// The SHOW CREATE TABLE comparison is textual, and a re-added
			// index lists after the ones that stayed, in the order the diff
			// adds them (sorted by clause text): kq is declared first, and
			// ka sorts before kb.
			name:   "SRID change under two spatial indexes keeps one on another column",
			source: "(id INT PRIMARY KEY, p POINT NOT NULL SRID 0, q POINT NOT NULL SRID 0, SPATIAL KEY kq (q), SPATIAL KEY ka (p), SPATIAL KEY kb (p))",
			target: "(id INT PRIMARY KEY, p POINT NOT NULL SRID 4326, q POINT NOT NULL SRID 0, SPATIAL KEY kq (q), SPATIAL KEY ka (p), SPATIAL KEY kb (p))",
		},
		{
			name:   "SRID change with the spatial index removed",
			source: "(id INT PRIMARY KEY, p POINT NOT NULL SRID 0, SPATIAL KEY k (p))",
			target: "(id INT PRIMARY KEY, p POINT NOT NULL SRID 4326)",
		},
		{
			name:   "comment change under a spatial index is a plain MODIFY",
			source: "(id INT PRIMARY KEY, p POINT NOT NULL SRID 0, SPATIAL KEY k (p))",
			target: "(id INT PRIMARY KEY, p POINT NOT NULL SRID 0 COMMENT 'x', SPATIAL KEY k (p))",
		},
		// FULLTEXT indexes. InnoDB builds one per ALTER TABLE (error 1795);
		// the diff used to put every addition in the combined ALTER.
		{
			name:   "two FULLTEXT additions",
			source: "(id INT PRIMARY KEY, a TEXT, b TEXT)",
			target: "(id INT PRIMARY KEY, a TEXT, b TEXT, FULLTEXT KEY k1 (a), FULLTEXT KEY k2 (b))",
		},
		{
			name:   "three FULLTEXT additions alongside other changes",
			source: "(id INT PRIMARY KEY, a TEXT, b TEXT)",
			target: "(id INT PRIMARY KEY, a TEXT, b TEXT, c TEXT, KEY kc (c(10)), FULLTEXT KEY k1 (a), FULLTEXT KEY k2 (b), FULLTEXT KEY k3 (c))",
		},
		{
			name:   "FULLTEXT rebuilt and another added",
			source: "(id INT PRIMARY KEY, a TEXT, b TEXT, FULLTEXT KEY k1 (a))",
			target: "(id INT PRIMARY KEY, a TEXT, b TEXT, FULLTEXT KEY k1 (a, b), FULLTEXT KEY k2 (b))",
		},
	}
	for _, c := range contracts {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			runDiffContract(t, c)
		})
	}
}

// TestDiffContractInlineUniqueKeepsLiveName checks the pairing of an inline
// `c INT UNIQUE` with a live unique index under another name: the emitted
// ALTER keeps the live name and brings the index's options in line with the
// declaration, and the result diffs clean. The SHOW CREATE TABLE text differs
// from the declaration's by the index name, which is why this is not a
// diffContract case.
func TestDiffContractInlineUniqueKeepsLiveName(t *testing.T) {
	t.Parallel()
	_, db := testutils.CreateUniqueTestDatabase(t)
	ctx := t.Context()
	_, err := db.ExecContext(ctx, "CREATE TABLE t (id INT PRIMARY KEY, c INT, UNIQUE KEY c_2 (c) COMMENT 'x' INVISIBLE)")
	require.NoError(t, err)
	dst, err := ParseCreateTable("CREATE TABLE t (id INT PRIMARY KEY, c INT UNIQUE)")
	require.NoError(t, err)
	src, err := ParseCreateTable(showCreateTable(t, db, "t"))
	require.NoError(t, err)
	stmts, err := src.Diff(dst, nil)
	require.NoError(t, err)
	require.NotEmpty(t, stmts, "the live index carries a comment and is invisible; the declaration has neither")
	for _, stmt := range stmts {
		_, err = db.ExecContext(ctx, stmt.Statement)
		require.NoError(t, err, stmt.Statement)
	}
	live := showCreateTable(t, db, "t")
	require.Contains(t, live, "UNIQUE KEY `c_2` (`c`)\n", "the live name is kept and the options are cleared")
	again, err := ParseCreateTable(live)
	require.NoError(t, err)
	stmts, err = again.Diff(dst, nil)
	require.NoError(t, err)
	require.Empty(t, stmts)
}

// TestDiffContractNullableAutoIncrement: a NULL written after the
// AUTO_INCREMENT is the one spelling of a nullable AUTO_INCREMENT column, and
// the emitted MODIFY has to keep that order, because MySQL applies the
// attributes in order and `int NULL AUTO_INCREMENT` is stored NOT NULL. This
// is not a diffContract because the result cannot converge: MySQL reports the
// nullable column as `int AUTO_INCREMENT`, which as CREATE TABLE input means
// NOT NULL, so a second diff reads the live column back as NOT NULL.
func TestDiffContractNullableAutoIncrement(t *testing.T) {
	_, db := testutils.CreateUniqueTestDatabase(t)
	ctx := t.Context()
	_, err := db.ExecContext(ctx, "CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT, x INT PRIMARY KEY, UNIQUE KEY k (id))")
	require.NoError(t, err)
	src, err := ParseCreateTable(showCreateTable(t, db, "t"))
	require.NoError(t, err)
	dst, err := ParseCreateTable("CREATE TABLE t (id INT AUTO_INCREMENT NULL, x INT PRIMARY KEY, UNIQUE KEY k (id))")
	require.NoError(t, err)
	stmts, err := src.Diff(dst, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t, "ALTER TABLE `t` MODIFY COLUMN `id` int AUTO_INCREMENT NULL", stmts[0].Statement)
	_, err = db.ExecContext(ctx, stmts[0].Statement)
	require.NoError(t, err)
	var isNullable string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT IS_NULLABLE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 't' AND COLUMN_NAME = 'id'").Scan(&isNullable))
	assert.Equal(t, "YES", isNullable)
	assert.Contains(t, showCreateTable(t, db, "t"), "`id` int AUTO_INCREMENT,\n")
}

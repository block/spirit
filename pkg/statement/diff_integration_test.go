package statement

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"unicode"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// showCreateTable (defined in diff_column_options_test.go) takes (t, db,
// tableName); call it as showCreateTable(t, tt.DB, tt.Name) below.

// TestDiffIntegrationRemoveTableComment verifies that the empty COMMENT
// clause emitted when a table comment is removed actually clears the comment
// on a real MySQL server, and that a re-diff afterwards converges to nil.
func TestDiffIntegrationRemoveTableComment(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_comment_removal",
		"CREATE TABLE diff_comment_removal (a int) COMMENT='old comment'")

	target, err := ParseCreateTable("CREATE TABLE diff_comment_removal (a int)")
	require.NoError(t, err)

	source, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)

	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "ALTER TABLE `diff_comment_removal` COMMENT=''", stmts[0].Statement)

	// Apply the diff and verify the comment is really gone.
	_, err = tt.DB.ExecContext(t.Context(), stmts[0].Statement)
	require.NoError(t, err)
	postAlter := showCreateTable(t, tt.DB, tt.Name)
	require.NotContains(t, postAlter, "COMMENT")

	// Re-diff: the schemas now converge.
	source, err = ParseCreateTable(postAlter)
	require.NoError(t, err)
	stmts, err = source.Diff(target, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)
}

// TestDiffIntegrationFulltextParser verifies that adding WITH PARSER to an
// index whose column list is unchanged is detected, and that executing the
// statements Diff() emits — exactly as Spirit's Runner would — actually applies
// the parser change on a real MySQL server.
//
// This is a regression test for a silent-no-op bug: MySQL treats a combined
// `DROP INDEX x, ADD INDEX x (<same cols>)` in a single ALTER as a no-op and
// keeps the existing index. Diff() therefore emits the DROP and ADD as two
// separate statements. The test executes only those emitted statements (no
// extra manual ALTERs) and asserts the parser change took effect.
func TestDiffIntegrationFulltextParser(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_ft_parser",
		"CREATE TABLE diff_ft_parser (id int primary key, b text, FULLTEXT KEY ft_b (b))")

	target, err := ParseCreateTable("CREATE TABLE diff_ft_parser (id int primary key, b text, FULLTEXT KEY ft_b (b) WITH PARSER ngram)")
	require.NoError(t, err)

	source, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)

	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 2, "option-only index change must be two separate statements")
	require.Equal(t, "ALTER TABLE `diff_ft_parser` DROP INDEX `ft_b`", stmts[0].Statement)
	require.Equal(t, "ALTER TABLE `diff_ft_parser` ADD FULLTEXT INDEX `ft_b` (`b`) WITH PARSER ngram", stmts[1].Statement)

	// Execute the emitted statements exactly as the Runner would, and verify
	// the parser change actually took effect — no extra manual ALTERs.
	for _, stmt := range stmts {
		_, err = tt.DB.ExecContext(t.Context(), stmt.Statement)
		require.NoError(t, err)
	}
	postAlter := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, postAlter, "WITH PARSER `ngram`")

	// Re-diff: the schemas now converge.
	source, err = ParseCreateTable(postAlter)
	require.NoError(t, err)
	stmts, err = source.Diff(target, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)
}

// TestDiffIntegrationFulltextRebuildPreservesParser verifies that an index
// rebuilt for an unrelated reason (a column list change) preserves WITH PARSER
// in the re-add, and that applying the diff to a real MySQL server converges.
//
// Unlike the option-only cases above, the column list genuinely changes here,
// so MySQL really rebuilds the index. A combined `DROP INDEX x, ADD INDEX x
// (<new cols>)` in a single ALTER is therefore NOT a no-op, and Diff() keeps it
// as one statement.
func TestDiffIntegrationFulltextRebuildPreservesParser(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_ft_rebuild",
		"CREATE TABLE diff_ft_rebuild (id int primary key, b text, c text, FULLTEXT KEY ft_b (b) WITH PARSER ngram)")

	target, err := ParseCreateTable("CREATE TABLE diff_ft_rebuild (id int primary key, b text, c text, FULLTEXT KEY ft_b (b, c) WITH PARSER ngram)")
	require.NoError(t, err)

	source, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)

	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "ALTER TABLE `diff_ft_rebuild` DROP INDEX `ft_b`, ADD FULLTEXT INDEX `ft_b` (`b`, `c`) WITH PARSER ngram", stmts[0].Statement)

	// The column list changed, so MySQL really rebuilds the index and the
	// parser must survive the rebuild.
	_, err = tt.DB.ExecContext(t.Context(), stmts[0].Statement)
	require.NoError(t, err)
	postAlter := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, postAlter, "FULLTEXT KEY `ft_b` (`b`,`c`)")
	require.Contains(t, postAlter, "WITH PARSER `ngram`")

	// Re-diff: the schemas now converge.
	source, err = ParseCreateTable(postAlter)
	require.NoError(t, err)
	stmts, err = source.Diff(target, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)
}

// TestDiffIntegrationInlineUnique verifies that a desired schema written with
// a column-level UNIQUE (`c int unique`) diffs cleanly against the live
// canonical form MySQL reports (`c int` + `UNIQUE KEY c (c)`), and that when
// the unique index is missing the emitted DDL creates it exactly once and the
// re-diff converges.
//
// Regression: inline UNIQUE only existed as Column.Unique and was invisible to
// diffIndexes, so this diff used to emit
// `MODIFY COLUMN c int(11) NULL, DROP INDEX c` — silently dropping the live
// uniqueness constraint — and never converged (a MODIFY COLUMN cannot
// re-express UNIQUE).
func TestDiffIntegrationInlineUnique(t *testing.T) {
	// Create the table from the inline form; MySQL canonicalizes it.
	tt := testutils.NewTestTable(t, "diff_inline_unique",
		"CREATE TABLE diff_inline_unique (id int primary key, c int unique)")

	desired, err := ParseCreateTable("CREATE TABLE diff_inline_unique (id int primary key, c int unique)")
	require.NoError(t, err)

	// The server names the canonicalized unique index after the column.
	live := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, live, "UNIQUE KEY `c` (`c`)")

	source, err := ParseCreateTable(live)
	require.NoError(t, err)

	// Headline regression: live-canonical vs desired-inline must be a no-op.
	stmts, err := source.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)

	// Drop the unique index out from under the desired schema; the diff must
	// re-add it, exactly once.
	_, err = tt.DB.ExecContext(t.Context(), "ALTER TABLE diff_inline_unique DROP INDEX c")
	require.NoError(t, err)

	source, err = ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	stmts, err = source.Diff(desired, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "ALTER TABLE `diff_inline_unique` ADD UNIQUE INDEX `c` (`c`)", stmts[0].Statement)

	_, err = tt.DB.ExecContext(t.Context(), stmts[0].Statement)
	require.NoError(t, err)

	// Convergence: the index is back under its canonical name and a re-diff
	// yields nil.
	postAlter := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, postAlter, "UNIQUE KEY `c` (`c`)")
	source, err = ParseCreateTable(postAlter)
	require.NoError(t, err)
	stmts, err = source.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)
}

// TestDiffIntegrationInlineUniqueNameCollision verifies the synthesized names
// follow the server's declaration-order naming when an inline unique and an
// unnamed table-level key share a base name: the inline unique on c claims
// `c` and the unnamed KEY (c, d) is pushed to `c_2`. The live canonical form
// must diff as a no-op against the original inline form.
func TestDiffIntegrationInlineUniqueNameCollision(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_inline_uniq_col",
		"CREATE TABLE diff_inline_uniq_col (id int primary key, c int unique, d int, key (c, d))")

	live := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, live, "UNIQUE KEY `c` (`c`)")
	require.Contains(t, live, "KEY `c_2` (`c`,`d`)",
		"server is expected to push the unnamed key past the inline unique's name")

	desired, err := ParseCreateTable("CREATE TABLE diff_inline_uniq_col (id int primary key, c int unique, d int, key (c, d))")
	require.NoError(t, err)
	source, err := ParseCreateTable(live)
	require.NoError(t, err)

	stmts, err := source.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)
}

// TestDiffIntegrationKeyBlockSize verifies that a KEY_BLOCK_SIZE difference on
// an index whose column list is unchanged is detected, and that executing the
// statements Diff() emits actually applies the change on a real MySQL server.
// The table uses ROW_FORMAT=COMPRESSED because InnoDB silently ignores
// index-level KEY_BLOCK_SIZE on uncompressed tables.
//
// Like the parser case, this is option-only, so Diff() emits a separate DROP
// and ADD; a combined single ALTER would be a MySQL no-op. The test executes
// only the emitted statements (no extra manual ALTERs).
func TestDiffIntegrationKeyBlockSize(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_kbs",
		"CREATE TABLE diff_kbs (id int primary key, b varchar(100), KEY idx_b (b)) ROW_FORMAT=COMPRESSED")

	target, err := ParseCreateTable("CREATE TABLE diff_kbs (id int primary key, b varchar(100), KEY idx_b (b) KEY_BLOCK_SIZE=8) ROW_FORMAT=COMPRESSED")
	require.NoError(t, err)

	source, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)

	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 2, "option-only index change must be two separate statements")
	require.Equal(t, "ALTER TABLE `diff_kbs` DROP INDEX `idx_b`", stmts[0].Statement)
	require.Equal(t, "ALTER TABLE `diff_kbs` ADD INDEX `idx_b` (`b`) KEY_BLOCK_SIZE=8", stmts[1].Statement)

	// Execute the emitted statements exactly as the Runner would, and verify
	// KEY_BLOCK_SIZE actually took effect — no extra manual ALTERs.
	for _, stmt := range stmts {
		_, err = tt.DB.ExecContext(t.Context(), stmt.Statement)
		require.NoError(t, err)
	}
	postAlter := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, postAlter, "KEY `idx_b` (`b`) KEY_BLOCK_SIZE=8")

	// Re-diff: the schemas now converge.
	source, err = ParseCreateTable(postAlter)
	require.NoError(t, err)
	stmts, err = source.Diff(target, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)
}

// A table created from `year(4)` is stored as a plain `year`, so it must diff
// clean against the declaration it was created from. Without
// yearDisplayWidthNormalizer the diff emits `MODIFY COLUMN ... year(4)` on
// every run.
func TestDiffIntegrationYearDisplayWidthCreatedAsDeclared(t *testing.T) {
	const declaredSQL = "CREATE TABLE diff_year_width (" +
		"id int NOT NULL, " +
		"a year(4), " +
		"b year(4) NOT NULL DEFAULT 2024, " +
		"PRIMARY KEY (id))"

	tt := testutils.NewTestTable(t, "diff_year_width", declaredSQL)

	live := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, live, "`a` year DEFAULT NULL")
	require.Contains(t, live, "`b` year NOT NULL DEFAULT '2024'")

	stmts := diffLiveTable(t, tt.DB, tt.Name, declaredSQL)
	require.Nil(t, stmts)
}

// Changing a column to year(4) is a real change: the diff is emitted, MySQL
// stores the column as `year`, and a re-diff is clean.
func TestDiffIntegrationYearDisplayWidthConverges(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_year_width_change",
		"CREATE TABLE diff_year_width_change (id int NOT NULL, a smallint, PRIMARY KEY (id))")

	const targetSQL = "CREATE TABLE diff_year_width_change (id int NOT NULL, a year(4), PRIMARY KEY (id))"

	stmts := diffLiveTable(t, tt.DB, tt.Name, targetSQL)
	require.Len(t, stmts, 1)

	execStatements(t, tt.DB, stmts)
	require.Contains(t, showCreateTable(t, tt.DB, tt.Name), "`a` year DEFAULT NULL")

	requireConverged(t, tt.DB, tt.Name, targetSQL)
}

// A literal default on a year column is stored as a four-digit year, so a table
// created from these declarations must diff clean against them. Without
// yearDefaultNormalizer the diff emits `MODIFY COLUMN ... DEFAULT 99` against
// the live '1999' on every run.
func TestDiffIntegrationYearDefaultCreatedAsDeclared(t *testing.T) {
	const declaredSQL = "CREATE TABLE diff_year_default (" +
		"id int NOT NULL, " +
		"two_digit year DEFAULT 99, " +
		"two_digit_str year DEFAULT '24', " +
		"one_digit year DEFAULT 5, " +
		"num_zero year DEFAULT 0, " +
		"str_zero year DEFAULT '0', " +
		"str_zero4 year DEFAULT '0000', " +
		"hex year DEFAULT 0x07, " +
		"bits year DEFAULT b'111', " +
		"kw_true year NOT NULL DEFAULT TRUE, " +
		"kw_false year NOT NULL DEFAULT FALSE, " +
		"four_digit year DEFAULT 2024, " +
		"plus year DEFAULT +5, " +
		"plus_padded year DEFAULT +0099, " +
		"neg_zero year DEFAULT -0, " +
		"neg_zero_padded year DEFAULT -00, " +
		"low_str year DEFAULT '01901', " +
		"high_str year DEFAULT '02155', " +
		"low_hex year DEFAULT 0x076D, " +
		"high_hex year DEFAULT 0x086B, " +
		"PRIMARY KEY (id))"

	tt := testutils.NewTestTable(t, "diff_year_default", declaredSQL)

	live := showCreateTable(t, tt.DB, tt.Name)
	for _, want := range []string{
		"`two_digit` year DEFAULT '1999'",
		"`two_digit_str` year DEFAULT '2024'",
		"`one_digit` year DEFAULT '2005'",
		"`num_zero` year DEFAULT '0000'",
		"`str_zero` year DEFAULT '2000'",
		"`str_zero4` year DEFAULT '0000'",
		"`hex` year DEFAULT '2007'",
		"`bits` year DEFAULT '2007'",
		"`kw_true` year NOT NULL DEFAULT '2001'",
		"`kw_false` year NOT NULL DEFAULT '0000'",
		"`four_digit` year DEFAULT '2024'",
		"`plus` year DEFAULT '2005'",
		"`plus_padded` year DEFAULT '1999'",
		"`neg_zero` year DEFAULT '0000'",
		"`neg_zero_padded` year DEFAULT '0000'",
		"`low_str` year DEFAULT '1901'",
		"`high_str` year DEFAULT '2155'",
		"`low_hex` year DEFAULT '1901'",
		"`high_hex` year DEFAULT '2155'",
	} {
		require.Contains(t, live, want)
	}

	require.Nil(t, diffLiveTable(t, tt.DB, tt.Name, declaredSQL))
}

// Changing a column to a year with a two-digit default is a real change: the
// diff is emitted, MySQL stores the default as a four-digit year, and a re-diff
// is clean. The zero defaults are included because the numeric and string
// zero store different years ('0000' and '2000'), and the bare year the diff
// emits must store the same one.
func TestDiffIntegrationYearDefaultConverges(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_year_default_change",
		"CREATE TABLE diff_year_default_change (id int NOT NULL, a smallint, b smallint, c smallint, PRIMARY KEY (id))")

	const targetSQL = "CREATE TABLE diff_year_default_change (id int NOT NULL, " +
		"a year DEFAULT 99, b year DEFAULT 0, c year DEFAULT '0', PRIMARY KEY (id))"

	stmts := diffLiveTable(t, tt.DB, tt.Name, targetSQL)
	require.Len(t, stmts, 1)

	execStatements(t, tt.DB, stmts)
	live := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, live, "`a` year DEFAULT '1999'")
	require.Contains(t, live, "`b` year DEFAULT '0000'")
	require.Contains(t, live, "`c` year DEFAULT '2000'")

	requireConverged(t, tt.DB, tt.Name, targetSQL)
}

// TestDiffIntegrationForeignKeyNoAction verifies against a real MySQL server
// that a desired schema spelling out ON DELETE NO ACTION / ON UPDATE NO ACTION
// converges with the live table. MySQL omits NO ACTION from SHOW CREATE TABLE
// output (it is the default action, a synonym for RESTRICT in InnoDB), so
// before the parse-time normalization this produced the same DROP+ADD FOREIGN
// KEY on every diff — an ALTER that never changed SHOW CREATE output.
func TestDiffIntegrationForeignKeyNoAction(t *testing.T) {
	// Parent must be created first; TestTable cleanup is LIFO so the child
	// (created last) is dropped before the parent.
	_ = testutils.NewTestTable(t, "diff_fkna_parent",
		"CREATE TABLE diff_fkna_parent (id int primary key)")
	tt := testutils.NewTestTable(t, "diff_fkna_child",
		"CREATE TABLE diff_fkna_child (id int primary key, pid int, KEY fk_fkna_pid (pid), "+
			"CONSTRAINT fk_fkna_pid FOREIGN KEY (pid) REFERENCES diff_fkna_parent (id) ON DELETE NO ACTION ON UPDATE NO ACTION)")

	// Document the server behavior this fix depends on: SHOW CREATE TABLE
	// omits NO ACTION.
	live := showCreateTable(t, tt.DB, tt.Name)
	require.NotContains(t, live, "NO ACTION")

	desired, err := ParseCreateTable(
		"CREATE TABLE diff_fkna_child (id int primary key, pid int, KEY fk_fkna_pid (pid), " +
			"CONSTRAINT fk_fkna_pid FOREIGN KEY (pid) REFERENCES diff_fkna_parent (id) ON DELETE NO ACTION ON UPDATE NO ACTION)")
	require.NoError(t, err)

	source, err := ParseCreateTable(live)
	require.NoError(t, err)

	// The explicit NO ACTION spelling converges with the live table.
	stmts, err := source.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts, "explicit NO ACTION must converge with live schema")

	// A genuine action change (NO ACTION -> CASCADE) still produces a diff,
	// and applying it converges. The desired FK uses a different constraint
	// name because MySQL rejects a same-name DROP FOREIGN KEY + ADD
	// CONSTRAINT within a single ALTER (Error 1826).
	desiredCascade, err := ParseCreateTable(
		"CREATE TABLE diff_fkna_child (id int primary key, pid int, KEY fk_fkna_pid (pid), " +
			"CONSTRAINT fk_fkna_pid2 FOREIGN KEY (pid) REFERENCES diff_fkna_parent (id) ON DELETE CASCADE)")
	require.NoError(t, err)
	stmts, err = source.Diff(desiredCascade, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "ALTER TABLE `diff_fkna_child` DROP FOREIGN KEY `fk_fkna_pid`, ADD CONSTRAINT `fk_fkna_pid2` FOREIGN KEY (`pid`) REFERENCES `diff_fkna_parent` (`id`) ON DELETE CASCADE", stmts[0].Statement)
	_, err = tt.DB.ExecContext(t.Context(), stmts[0].Statement)
	require.NoError(t, err)
	source, err = ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	stmts, err = source.Diff(desiredCascade, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)
}

// TestDiffIntegrationForeignKeyRestrict is the regression guard for the
// NO ACTION normalization: RESTRICT has identical semantics in InnoDB but IS
// printed by SHOW CREATE TABLE, so it must round-trip verbatim — neither
// normalized away nor producing a spurious diff.
func TestDiffIntegrationForeignKeyRestrict(t *testing.T) {
	_ = testutils.NewTestTable(t, "diff_fkr_parent",
		"CREATE TABLE diff_fkr_parent (id int primary key)")
	tt := testutils.NewTestTable(t, "diff_fkr_child",
		"CREATE TABLE diff_fkr_child (id int primary key, pid int, KEY fk_fkr_pid (pid), "+
			"CONSTRAINT fk_fkr_pid FOREIGN KEY (pid) REFERENCES diff_fkr_parent (id) ON DELETE RESTRICT ON UPDATE RESTRICT)")

	// Document the server behavior: RESTRICT is printed (unlike NO ACTION).
	live := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, live, "ON DELETE RESTRICT ON UPDATE RESTRICT")

	desired, err := ParseCreateTable(
		"CREATE TABLE diff_fkr_child (id int primary key, pid int, KEY fk_fkr_pid (pid), " +
			"CONSTRAINT fk_fkr_pid FOREIGN KEY (pid) REFERENCES diff_fkr_parent (id) ON DELETE RESTRICT ON UPDATE RESTRICT)")
	require.NoError(t, err)

	source, err := ParseCreateTable(live)
	require.NoError(t, err)
	stmts, err := source.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts, "RESTRICT must round-trip unchanged")
}

// TestDiffIntegrationCheckEnforcement verifies against a real MySQL server
// that the [NOT] ENFORCED state of CHECK constraints round-trips through
// SHOW CREATE TABLE (which renders it as /*!80016 NOT ENFORCED */), that
// enforcement flips are applied in place with ALTER CHECK, and that a
// NOT ENFORCED check re-added for an expression change stays NOT ENFORCED
// instead of silently re-enabling enforcement.
func TestDiffIntegrationCheckEnforcement(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_chk_enf",
		"CREATE TABLE diff_chk_enf (id int primary key, age int, "+
			"CONSTRAINT chk_dce_age CHECK (age >= 0) NOT ENFORCED)")

	// Document the server's canonical rendering.
	live := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, live, "/*!80016 NOT ENFORCED */")

	// Desired NOT ENFORCED vs live NOT ENFORCED converges.
	desired, err := ParseCreateTable(
		"CREATE TABLE diff_chk_enf (id int primary key, age int, " +
			"CONSTRAINT chk_dce_age CHECK (age >= 0) NOT ENFORCED)")
	require.NoError(t, err)
	source, err := ParseCreateTable(live)
	require.NoError(t, err)
	stmts, err := source.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts, "NOT ENFORCED on both sides must converge")

	// A row violating the (unenforced) check: flipping enforcement ON must
	// surface MySQL's validation error rather than silently passing, which
	// also proves ALTER CHECK ... ENFORCED validates existing rows just as
	// an enforced ADD CONSTRAINT would.
	_, err = tt.DB.ExecContext(t.Context(), "INSERT INTO diff_chk_enf VALUES (1, -5)")
	require.NoError(t, err)

	desiredEnforced, err := ParseCreateTable(
		"CREATE TABLE diff_chk_enf (id int primary key, age int, " +
			"CONSTRAINT chk_dce_age CHECK (age >= 0))")
	require.NoError(t, err)
	stmts, err = source.Diff(desiredEnforced, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "ALTER TABLE `diff_chk_enf` ALTER CHECK `chk_dce_age` ENFORCED", stmts[0].Statement)
	_, err = tt.DB.ExecContext(t.Context(), stmts[0].Statement)
	require.ErrorContains(t, err, "chk_dce_age", "enforcing over violating rows must fail")

	// Remove the violating row; the same ALTER now applies and converges.
	_, err = tt.DB.ExecContext(t.Context(), "DELETE FROM diff_chk_enf")
	require.NoError(t, err)
	_, err = tt.DB.ExecContext(t.Context(), stmts[0].Statement)
	require.NoError(t, err)
	postAlter := showCreateTable(t, tt.DB, tt.Name)
	require.NotContains(t, postAlter, "NOT ENFORCED")
	source, err = ParseCreateTable(postAlter)
	require.NoError(t, err)
	stmts, err = source.Diff(desiredEnforced, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)

	// Flip back to NOT ENFORCED: exactly one in-place ALTER, then converges.
	stmts, err = source.Diff(desired, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "ALTER TABLE `diff_chk_enf` ALTER CHECK `chk_dce_age` NOT ENFORCED", stmts[0].Statement)
	_, err = tt.DB.ExecContext(t.Context(), stmts[0].Statement)
	require.NoError(t, err)
	postAlter = showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, postAlter, "/*!80016 NOT ENFORCED */")
	source, err = ParseCreateTable(postAlter)
	require.NoError(t, err)
	stmts, err = source.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)

	// Expression change on a NOT ENFORCED check: the re-add must carry
	// NOT ENFORCED through to the server (previously it silently flipped
	// enforcement back on).
	desiredNewExpr, err := ParseCreateTable(
		"CREATE TABLE diff_chk_enf (id int primary key, age int, " +
			"CONSTRAINT chk_dce_age CHECK (age >= 18) NOT ENFORCED)")
	require.NoError(t, err)
	stmts, err = source.Diff(desiredNewExpr, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "ALTER TABLE `diff_chk_enf` DROP CHECK `chk_dce_age`, ADD CONSTRAINT `chk_dce_age` CHECK (`age`>=18) NOT ENFORCED", stmts[0].Statement)
	_, err = tt.DB.ExecContext(t.Context(), stmts[0].Statement)
	require.NoError(t, err)
	postAlter = showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, postAlter, "/*!80016 NOT ENFORCED */")
	source, err = ParseCreateTable(postAlter)
	require.NoError(t, err)
	stmts, err = source.Diff(desiredNewExpr, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)
}

// TestDiffIntegrationDescIndex verifies that changing an index key part from
// ascending to descending (MySQL 8.0+) is detected by Diff(), that the emitted
// combined `DROP INDEX k, ADD INDEX k (a DESC)` really rebuilds the index on a
// real MySQL server (unlike the option-only cases, MySQL does not no-op a
// direction change), and that a re-diff afterwards converges to nil.
//
// This is a regression test: the DESC modifier used to be dropped during
// parsing, so KEY k (a) and KEY k (a DESC) diffed as equal and restored
// descending indexes silently became ascending.
func TestDiffIntegrationDescIndex(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_desc_idx",
		"CREATE TABLE diff_desc_idx (id int primary key, a int, b int, KEY k (a, b))")

	target, err := ParseCreateTable("CREATE TABLE diff_desc_idx (id int primary key, a int, b int, KEY k (a DESC, b))")
	require.NoError(t, err)

	source, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)

	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "ALTER TABLE `diff_desc_idx` DROP INDEX `k`, ADD INDEX `k` (`a` DESC, `b`)", stmts[0].Statement)

	// Execute the emitted statement exactly as the Runner would, and verify
	// the index really became descending.
	_, err = tt.DB.ExecContext(t.Context(), stmts[0].Statement)
	require.NoError(t, err)
	postAlter := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, postAlter, "KEY `k` (`a` DESC,`b`)")

	// Re-diff: the schemas now converge.
	source, err = ParseCreateTable(postAlter)
	require.NoError(t, err)
	stmts, err = source.Diff(target, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)
}

// A table created with an fsp of 0 in its timestamp defaults is stored without
// it, so it must diff clean against the declaration it was created from.
// Without timestampFspZeroNormalizer the diff emits `MODIFY ... DEFAULT
// current_timestamp(0)` on every run.
func TestDiffIntegrationTimestampFspZeroCreatedAsDeclared(t *testing.T) {
	const declaredSQL = "CREATE TABLE diff_fsp_zero (" +
		"id int NOT NULL, " +
		"a datetime(0) DEFAULT CURRENT_TIMESTAMP(0), " +
		"b timestamp(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0), " +
		"c datetime DEFAULT NOW(0) ON UPDATE LOCALTIMESTAMP(0), " +
		"d datetime DEFAULT (CURRENT_TIMESTAMP(0)), " +
		"e datetime DEFAULT (NOW(0) + INTERVAL 1 DAY), " +
		"f time DEFAULT (CURTIME(0)), " +
		"g datetime(3) DEFAULT (IFNULL(NOW(3), NOW(0))), " +
		"h datetime DEFAULT (COALESCE(NOW(), NOW(0))), " +
		"i datetime DEFAULT (LOCALTIME(0)), " +
		"PRIMARY KEY (id))"

	tt := testutils.NewTestTable(t, "diff_fsp_zero", declaredSQL)

	live := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, live, "`a` datetime DEFAULT CURRENT_TIMESTAMP,")
	require.Contains(t, live, "`b` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,")
	require.Contains(t, live, "`d` datetime DEFAULT (now()),")
	require.Contains(t, live, "`g` datetime(3) DEFAULT (ifnull(now(3),now())),")
	require.Contains(t, live, "`h` datetime DEFAULT (coalesce(now(),now())),")

	stmts := diffLiveTable(t, tt.DB, tt.Name, declaredSQL)
	require.Nil(t, stmts)
}

// Adding a zero-fsp default to an existing column is a real change: the diff
// is emitted, MySQL stores the call without the fsp, and a re-diff is clean.
func TestDiffIntegrationTimestampFspZeroConverges(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_fsp_zero_change",
		"CREATE TABLE diff_fsp_zero_change (id int NOT NULL, a datetime, b datetime, PRIMARY KEY (id))")

	const targetSQL = "CREATE TABLE diff_fsp_zero_change (id int NOT NULL, " +
		"a datetime(0) DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0), " +
		"b datetime DEFAULT (NOW(0) + INTERVAL 1 DAY), " +
		"PRIMARY KEY (id))"

	stmts := diffLiveTable(t, tt.DB, tt.Name, targetSQL)
	require.Len(t, stmts, 1)

	execStatements(t, tt.DB, stmts)
	live := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, live, "`a` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,")
	require.Contains(t, live, "`b` datetime DEFAULT ((now() + interval 1 day)),")

	requireConverged(t, tt.DB, tt.Name, targetSQL)
}

// TestDiffIntegrationSubpartitionNoSpuriousDiff verifies that a subpartitioned
// table does not diff against the definition it was created from. The live
// definition differs cosmetically in two ways Diff has to absorb: the partition
// expression comes back lowercased and backtick-quoted, and every partition
// carries an `ENGINE = InnoDB` clause the authored SQL never wrote.
//
// This is a regression test. Comparing the per-partition ENGINE made every
// partitioned table repartition itself on every run — and because the emitted
// PARTITION BY had no SUBPARTITION BY clause, applying that "no-op" silently
// dropped the table's subpartitioning.
func TestDiffIntegrationSubpartitionNoSpuriousDiff(t *testing.T) {
	const authoredSQL = "CREATE TABLE diff_subpart (dt date NOT NULL, PRIMARY KEY (dt)) " +
		"PARTITION BY RANGE (YEAR(dt)) SUBPARTITION BY HASH (dayofmonth(dt)) SUBPARTITIONS 2 " +
		"(PARTITION p0 VALUES LESS THAN (2020), PARTITION p1 VALUES LESS THAN MAXVALUE)"
	tt := testutils.NewTestTable(t, "diff_subpart", authoredSQL)

	target, err := ParseCreateTable(authoredSQL)
	require.NoError(t, err)

	live := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, live, "SUBPARTITION BY HASH", "precondition: the table is subpartitioned")
	require.Contains(t, live, "ENGINE = InnoDB", "precondition: MySQL prints per-partition ENGINE")

	source, err := ParseCreateTable(live)
	require.NoError(t, err)
	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	require.Nil(t, stmts, "live definition must not diff against the SQL it was created from")

	requireNoSelfDiff(t, tt.DB, tt.Name)
}

// TestDiffIntegrationSubpartitionChange verifies that a genuine subpartitioning
// change is emitted in full and actually applies: the PARTITION BY must carry
// the SUBPARTITION BY clause, or the table comes back partitioned but no
// longer subpartitioned. The re-diff then converges.
func TestDiffIntegrationSubpartitionChange(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_subpart_chg",
		"CREATE TABLE diff_subpart_chg (dt date NOT NULL, PRIMARY KEY (dt)) "+
			"PARTITION BY RANGE (YEAR(dt)) SUBPARTITION BY HASH (dayofmonth(dt)) SUBPARTITIONS 2 "+
			"(PARTITION p0 VALUES LESS THAN (2020), PARTITION p1 VALUES LESS THAN MAXVALUE)")

	const targetSQL = "CREATE TABLE diff_subpart_chg (dt date NOT NULL, PRIMARY KEY (dt)) " +
		"PARTITION BY RANGE (YEAR(dt)) SUBPARTITION BY HASH (dayofmonth(dt)) SUBPARTITIONS 4 " +
		"(PARTITION p0 VALUES LESS THAN (2020), PARTITION p1 VALUES LESS THAN MAXVALUE)"
	target, err := ParseCreateTable(targetSQL)
	require.NoError(t, err)

	stmts := diffLiveTable(t, tt.DB, tt.Name, targetSQL)
	require.Len(t, stmts, 1, "a repartition needs no REMOVE PARTITIONING first")
	require.Equal(t,
		"ALTER TABLE `diff_subpart_chg` PARTITION BY RANGE (YEAR(`dt`)) "+
			"SUBPARTITION BY HASH (DAYOFMONTH(`dt`)) SUBPARTITIONS 4 "+
			"(PARTITION `p0` VALUES LESS THAN (2020), PARTITION `p1` VALUES LESS THAN MAXVALUE)",
		stmts[0].Statement)

	// Execute exactly what Diff emitted, as the Runner would.
	for _, stmt := range stmts {
		_, err = tt.DB.ExecContext(t.Context(), stmt.Statement)
		require.NoError(t, err)
	}
	postAlter := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, postAlter, "SUBPARTITION BY HASH (dayofmonth(`dt`))", "subpartitioning must survive")
	require.Contains(t, postAlter, "SUBPARTITIONS 4")

	// Re-diff: the schemas now converge.
	source, err := ParseCreateTable(postAlter)
	require.NoError(t, err)
	stmts, err = source.Diff(target, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)
}

// TestDiffIntegrationSubpartitionNamesAndComments verifies the high-fidelity
// shape: explicitly named subpartitions plus partition/subpartition comments.
// MySQL echoes explicit subpartition names back from SHOW CREATE TABLE, and
// pushes a partition-level comment down onto the subpartitions that lack one,
// so both sides have to model that to converge — and a repartition has to
// re-emit the names and comments rather than silently reset them.
func TestDiffIntegrationSubpartitionNamesAndComments(t *testing.T) {
	const authoredSQL = "CREATE TABLE diff_subpart_named (dt date NOT NULL, PRIMARY KEY (dt)) " +
		"PARTITION BY RANGE (YEAR(dt)) SUBPARTITION BY KEY (dt) " +
		"(PARTITION p0 VALUES LESS THAN (2020) COMMENT 'pc0' (SUBPARTITION s0 COMMENT 'sc0', SUBPARTITION s1), " +
		"PARTITION p1 VALUES LESS THAN MAXVALUE (SUBPARTITION s2, SUBPARTITION s3))"
	tt := testutils.NewTestTable(t, "diff_subpart_named", authoredSQL)

	target, err := ParseCreateTable(authoredSQL)
	require.NoError(t, err)
	source, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	require.Nil(t, stmts, "named subpartitions and comments must not diff against themselves")

	// Now move p0's boundary. The REORGANIZE has to carry every subpartition
	// name and comment through, or they are silently lost.
	const movedSQL = "CREATE TABLE diff_subpart_named (dt date NOT NULL, PRIMARY KEY (dt)) " +
		"PARTITION BY RANGE (YEAR(dt)) SUBPARTITION BY KEY (dt) " +
		"(PARTITION p0 VALUES LESS THAN (2030) COMMENT 'pc0' (SUBPARTITION s0 COMMENT 'sc0', SUBPARTITION s1), " +
		"PARTITION p1 VALUES LESS THAN MAXVALUE (SUBPARTITION s2, SUBPARTITION s3))"
	moved, err := ParseCreateTable(movedSQL)
	require.NoError(t, err)

	stmts = diffLiveTable(t, tt.DB, tt.Name, movedSQL)
	require.Len(t, stmts, 1)
	require.Contains(t, stmts[0].Statement, "REORGANIZE PARTITION `p0`, `p1` INTO")
	for _, stmt := range stmts {
		_, err = tt.DB.ExecContext(t.Context(), stmt.Statement)
		require.NoError(t, err)
	}
	postAlter := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, postAlter, "SUBPARTITION BY KEY")
	require.Contains(t, postAlter, "SUBPARTITION s0 COMMENT = 'sc0'")
	require.Contains(t, postAlter, "SUBPARTITION s1 COMMENT = 'pc0'")
	require.Contains(t, postAlter, "VALUES LESS THAN (2030)")

	// Re-diff: the schemas now converge.
	source, err = ParseCreateTable(postAlter)
	require.NoError(t, err)
	stmts, err = source.Diff(moved, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)
}

// TestDiffIntegrationPartitionChanges applies each kind of partition change
// Diff emits to a table holding rows, and checks that MySQL accepts it, that
// the table converges on the target, and that no row is lost.
func TestDiffIntegrationPartitionChanges(t *testing.T) {
	const rangeSource = "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) " +
		"PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20), PARTITION pmax VALUES LESS THAN MAXVALUE)"
	const listSource = "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) " +
		"PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1, 5), PARTITION p1 VALUES IN (15), PARTITION p2 VALUES IN (25))"
	const hashSource = "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) PARTITION BY HASH (id) PARTITIONS 4"
	const dateSource = "CREATE TABLE diff_part_chg (id int NOT NULL, d date NOT NULL, PRIMARY KEY (id, d)) " +
		"PARTITION BY RANGE COLUMNS (d) (PARTITION p202610 VALUES LESS THAN ('2026-11-01'), PARTITION pmax VALUES LESS THAN (MAXVALUE))"
	tests := []struct {
		name   string
		source string
		insert string
		target string
		// prefix of each emitted statement, after "ALTER TABLE `diff_part_chg` "
		expected []string
		// after, if set, must succeed once the table has converged
		after string
	}{
		{
			name:     "ChangeTypeWithColumn",
			source:   hashSource,
			target:   "CREATE TABLE diff_part_chg (id int NOT NULL, b int, c int, PRIMARY KEY (id)) PARTITION BY KEY (id) PARTITIONS 3",
			expected: []string{"ADD COLUMN `c` int NULL PARTITION BY KEY"},
		},
		{
			name:     "CoalesceWithColumn",
			source:   hashSource,
			target:   "CREATE TABLE diff_part_chg (id int NOT NULL, b int, c int, PRIMARY KEY (id)) PARTITION BY HASH (id) PARTITIONS 2",
			expected: []string{"ADD COLUMN `c` int NULL PARTITION BY HASH"},
		},
		{
			name:     "AddPartitioningWithColumn",
			source:   "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id))",
			target:   "CREATE TABLE diff_part_chg (id int NOT NULL, b int, c int, PRIMARY KEY (id)) PARTITION BY HASH (id) PARTITIONS 2",
			expected: []string{"ADD COLUMN `c` int NULL PARTITION BY HASH"},
		},
		{
			name:     "RemovePartitioningWithColumn",
			source:   hashSource,
			target:   "CREATE TABLE diff_part_chg (id int NOT NULL, b int, c int, PRIMARY KEY (id))",
			expected: []string{"ADD COLUMN `c` int NULL REMOVE PARTITIONING"},
		},
		{
			name:     "RangeToList",
			source:   rangeSource,
			target:   listSource,
			expected: []string{"PARTITION BY LIST"},
		},
		{
			name:   "AppendRangePartitionWithColumn",
			source: "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (30))",
			target: "CREATE TABLE diff_part_chg (id int NOT NULL, b int, c int, PRIMARY KEY (id)) " +
				"PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (30), PARTITION p2 VALUES LESS THAN (40))",
			expected: []string{"ADD COLUMN `c` int NULL", "ADD PARTITION"},
		},
		{
			// MODIFY rounds 9.996 to 10.00, past p0. As a separate statement
			// after the MODIFY, the ADD PARTITION would come too late (MySQL
			// error 1526); folded into one PARTITION BY it applies.
			name:   "AppendRangePartitionWithPartitionKeyChange",
			source: "CREATE TABLE diff_part_chg (id int NOT NULL, d decimal(10,3) NOT NULL, PRIMARY KEY (id, d)) PARTITION BY RANGE (FLOOR(d)) (PARTITION p0 VALUES LESS THAN (10))",
			insert: "INSERT INTO diff_part_chg VALUES (1, 9.996)",
			target: "CREATE TABLE diff_part_chg (id int NOT NULL, d decimal(10,2) NOT NULL, PRIMARY KEY (id, d)) " +
				"PARTITION BY RANGE (FLOOR(d)) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
			expected: []string{"MODIFY COLUMN `d` decimal(10,2) NOT NULL PARTITION BY RANGE"},
		},
		{
			// The same, with the partitioning reading d through a generated
			// column: g's definition is unchanged, but its value moves.
			name:   "AppendRangePartitionWithGeneratedPartitionKeyChange",
			source: "CREATE TABLE diff_part_chg (d decimal(10,3), g int AS (FLOOR(d)) STORED) PARTITION BY RANGE (g) (PARTITION p0 VALUES LESS THAN (10))",
			insert: "INSERT INTO diff_part_chg (d) VALUES (9.996)",
			target: "CREATE TABLE diff_part_chg (d decimal(10,2), g int AS (FLOOR(d)) STORED) " +
				"PARTITION BY RANGE (g) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
			expected: []string{"MODIFY COLUMN `d` decimal(10,2) NULL PARTITION BY RANGE"},
		},
		{
			// Two generated columns deep.
			name:   "AppendRangePartitionWithTransitivePartitionKeyChange",
			source: "CREATE TABLE diff_part_chg (d decimal(10,3), g int AS (FLOOR(d)) STORED, h int AS (g + 1) STORED) PARTITION BY RANGE (h) (PARTITION p0 VALUES LESS THAN (11))",
			insert: "INSERT INTO diff_part_chg (d) VALUES (9.996)",
			target: "CREATE TABLE diff_part_chg (d decimal(10,2), g int AS (FLOOR(d)) STORED, h int AS (g + 1) STORED) " +
				"PARTITION BY RANGE (h) (PARTITION p0 VALUES LESS THAN (11), PARTITION p1 VALUES LESS THAN (21))",
			expected: []string{"MODIFY COLUMN `d` decimal(10,2) NULL PARTITION BY RANGE"},
		},
		{
			// MySQL folds each bound when it stores it: 20, 30, 40, 300, 405.
			name:   "AppendExpressionBounds",
			source: "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10))",
			insert: "INSERT INTO diff_part_chg (id) VALUES (1), (5)",
			target: "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), " +
				"PARTITION p1 VALUES LESS THAN (10+10), PARTITION p2 VALUES LESS THAN (+30), PARTITION p3 VALUES LESS THAN ((40)), " +
				"PARTITION p4 VALUES LESS THAN (7 DIV 2 * 100), PARTITION p5 VALUES LESS THAN (MOD(1000, 600) - -5))",
			expected: []string{"ADD PARTITION"},
			after:    "INSERT INTO diff_part_chg (id) VALUES (394)",
		},
		{
			// MOD takes the dividend's type, so this is a signed -1, not an
			// out-of-range unsigned one.
			name:   "AppendModuloOfUnsignedBound",
			source: "CREATE TABLE diff_part_chg (id bigint NOT NULL, b int, PRIMARY KEY (id)) PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1))",
			insert: "INSERT INTO diff_part_chg (id) VALUES (1)",
			target: "CREATE TABLE diff_part_chg (id bigint NOT NULL, b int, PRIMARY KEY (id)) " +
				"PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1), PARTITION p1 VALUES IN (MOD(7, 9223372036854775808) - 8))",
			expected: []string{"ADD PARTITION"},
			after:    "INSERT INTO diff_part_chg (id) VALUES (-1)",
		},
		{
			name:   "AppendExpressionListValues",
			source: listSource,
			target: "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) " +
				"PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1, 5), PARTITION p1 VALUES IN (15), PARTITION p2 VALUES IN (25), PARTITION p3 VALUES IN (30+5, 3*15))",
			expected: []string{"ADD PARTITION"},
			after:    "INSERT INTO diff_part_chg (id) VALUES (35), (45)",
		},
		{
			name:   "AppendRangeColumnsExpressionBound",
			source: "CREATE TABLE diff_part_chg (id int NOT NULL, d date NOT NULL, PRIMARY KEY (id, d)) PARTITION BY RANGE COLUMNS (id, d) (PARTITION p0 VALUES LESS THAN (10, '2020-01-01'))",
			insert: "INSERT INTO diff_part_chg VALUES (1, '2019-01-01')",
			target: "CREATE TABLE diff_part_chg (id int NOT NULL, d date NOT NULL, PRIMARY KEY (id, d)) " +
				"PARTITION BY RANGE COLUMNS (id, d) (PARTITION p0 VALUES LESS THAN (10, '2020-01-01'), PARTITION p1 VALUES LESS THAN (10+10, '2020-01-01'))",
			expected: []string{"ADD PARTITION"},
		},
		{
			name:   "AppendToDaysBound",
			source: "CREATE TABLE diff_part_chg (id int NOT NULL, d date NOT NULL, PRIMARY KEY (id, d)) PARTITION BY RANGE (TO_DAYS(d)) (PARTITION p0 VALUES LESS THAN (TO_DAYS('2026-01-01')))",
			insert: "INSERT INTO diff_part_chg VALUES (1, '2025-06-01')",
			target: "CREATE TABLE diff_part_chg (id int NOT NULL, d date NOT NULL, PRIMARY KEY (id, d)) PARTITION BY RANGE (TO_DAYS(d)) " +
				"(PARTITION p0 VALUES LESS THAN (TO_DAYS('2026-01-01')), PARTITION p1 VALUES LESS THAN (TO_DAYS('2027-01-01 00:00:00')))",
			expected: []string{"ADD PARTITION"},
			after:    "INSERT INTO diff_part_chg VALUES (2, '2026-12-31')",
		},
		{
			name:   "AppendToSecondsBound",
			source: "CREATE TABLE diff_part_chg (id int NOT NULL, d datetime NOT NULL, PRIMARY KEY (id, d)) PARTITION BY RANGE (TO_SECONDS(d)) (PARTITION p0 VALUES LESS THAN (TO_SECONDS('1969-07-20 20:17:40')))",
			insert: "INSERT INTO diff_part_chg VALUES (1, '1969-07-20 20:17:39')",
			target: "CREATE TABLE diff_part_chg (id int NOT NULL, d datetime NOT NULL, PRIMARY KEY (id, d)) PARTITION BY RANGE (TO_SECONDS(d)) " +
				"(PARTITION p0 VALUES LESS THAN (TO_SECONDS('1969-07-20 20:17:40')), PARTITION p1 VALUES LESS THAN (TO_SECONDS('2026-10-01 12:00:01')))",
			expected: []string{"ADD PARTITION"},
		},
		{
			name:   "AppendYearBound",
			source: "CREATE TABLE diff_part_chg (id int NOT NULL, d date NOT NULL, PRIMARY KEY (id, d)) PARTITION BY RANGE (YEAR(d)) (PARTITION p0 VALUES LESS THAN (YEAR('2026-06-01')))",
			insert: "INSERT INTO diff_part_chg VALUES (1, '2025-06-01')",
			target: "CREATE TABLE diff_part_chg (id int NOT NULL, d date NOT NULL, PRIMARY KEY (id, d)) PARTITION BY RANGE (YEAR(d)) " +
				"(PARTITION p0 VALUES LESS THAN (YEAR('2026-06-01')), PARTITION p1 VALUES LESS THAN (YEAR('2027-06-01')))",
			expected: []string{"ADD PARTITION"},
		},
		{
			name: "AppendParenthesizedTupleLiteral",
			source: "CREATE TABLE diff_part_chg (id int NOT NULL, b varchar(10) NOT NULL, PRIMARY KEY (id, b)) " +
				"PARTITION BY LIST COLUMNS (id, b) (PARTITION p0 VALUES IN ((1, 'x')))",
			insert: "INSERT INTO diff_part_chg VALUES (1, 'x')",
			target: "CREATE TABLE diff_part_chg (id int NOT NULL, b varchar(10) NOT NULL, PRIMARY KEY (id, b)) " +
				"PARTITION BY LIST COLUMNS (id, b) (PARTITION p0 VALUES IN ((1, 'x')), PARTITION p1 VALUES IN ((2, ('y'))))",
			expected: []string{"ADD PARTITION"},
			after:    "INSERT INTO diff_part_chg VALUES (2, 'y')",
		},
		{
			name:   "AppendParenthesizedTupleNull",
			source: "CREATE TABLE diff_part_chg (id int, b varchar(10)) PARTITION BY LIST COLUMNS (id, b) (PARTITION p0 VALUES IN ((1, 'x')))",
			insert: "INSERT INTO diff_part_chg VALUES (1, 'x')",
			target: "CREATE TABLE diff_part_chg (id int, b varchar(10)) " +
				"PARTITION BY LIST COLUMNS (id, b) (PARTITION p0 VALUES IN ((1, 'x')), PARTITION p1 VALUES IN ((2, (NULL))))",
			expected: []string{"ADD PARTITION"},
			after:    "INSERT INTO diff_part_chg VALUES (2, NULL)",
		},
		{
			// MySQL stores RANGE (a + b) as RANGE ((`a` + `b`)); the two must
			// compare equal, or this would be a full PARTITION BY.
			name:   "AppendWithCompoundExpression",
			source: "CREATE TABLE diff_part_chg (a int NOT NULL, b int NOT NULL, PRIMARY KEY (a, b)) PARTITION BY RANGE (a + b) (PARTITION p0 VALUES LESS THAN (10))",
			insert: "INSERT INTO diff_part_chg VALUES (1, 2)",
			target: "CREATE TABLE diff_part_chg (a int NOT NULL, b int NOT NULL, PRIMARY KEY (a, b)) " +
				"PARTITION BY RANGE (a + b) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
			expected: []string{"ADD PARTITION"},
		},
		{
			name:   "AppendWithParenthesizedColumnExpression",
			source: "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) PARTITION BY RANGE ((id)) (PARTITION p0 VALUES LESS THAN (10))",
			insert: "INSERT INTO diff_part_chg (id) VALUES (1)",
			target: "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) " +
				"PARTITION BY RANGE ((id)) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
			expected: []string{"ADD PARTITION"},
		},
		{
			name: "AppendWithCompoundSubpartitionExpression",
			source: "CREATE TABLE diff_part_chg (a int NOT NULL, b int NOT NULL, PRIMARY KEY (a, b)) " +
				"PARTITION BY RANGE (a) SUBPARTITION BY HASH (a + b * 2) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN (10))",
			insert: "INSERT INTO diff_part_chg VALUES (1, 2)",
			target: "CREATE TABLE diff_part_chg (a int NOT NULL, b int NOT NULL, PRIMARY KEY (a, b)) " +
				"PARTITION BY RANGE (a) SUBPARTITION BY HASH (a + b * 2) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (20))",
			expected: []string{"ADD PARTITION"},
		},
		{
			// MySQL stores MOD(id, 2) as (`id` % 2).
			name:   "AppendWithModExpression",
			source: "CREATE TABLE diff_part_chg (id int NOT NULL, PRIMARY KEY (id)) PARTITION BY LIST (MOD(id, 3)) (PARTITION p0 VALUES IN (0))",
			insert: "INSERT INTO diff_part_chg VALUES (3)",
			target: "CREATE TABLE diff_part_chg (id int NOT NULL, PRIMARY KEY (id)) " +
				"PARTITION BY LIST (MOD(id, 3)) (PARTITION p0 VALUES IN (0), PARTITION p1 VALUES IN (1, 2))",
			expected: []string{"ADD PARTITION"},
		},
		{
			// MySQL stores NULL first in a LIST (expr) value list.
			name:   "AppendWithNullNotFirst",
			source: "CREATE TABLE diff_part_chg (id int) PARTITION BY LIST (id) (PARTITION p0 VALUES IN (2, NULL), PARTITION p1 VALUES IN (3))",
			insert: "INSERT INTO diff_part_chg VALUES (NULL), (2), (3)",
			target: "CREATE TABLE diff_part_chg (id int) PARTITION BY LIST (id) " +
				"(PARTITION p0 VALUES IN (2, NULL), PARTITION p1 VALUES IN (3), PARTITION p2 VALUES IN (4))",
			expected: []string{"ADD PARTITION"},
		},
		{
			name:     "KeyAlgorithmToDefault",
			source:   "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) PARTITION BY KEY ALGORITHM=1 (id) PARTITIONS 2",
			target:   "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) PARTITION BY KEY (id) PARTITIONS 2",
			expected: []string{"PARTITION BY KEY (`id`) PARTITIONS 2"},
		},
		{
			name:     "KeyAlgorithmFromDefault",
			source:   "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) PARTITION BY KEY (id) PARTITIONS 2",
			target:   "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) PARTITION BY LINEAR KEY ALGORITHM=1 (id) PARTITIONS 2",
			expected: []string{"PARTITION BY LINEAR KEY ALGORITHM=1 (`id`) PARTITIONS 2"},
		},
		{
			// ADD PARTITION PARTITIONS 1 would keep algorithm 1.
			name:     "KeyAlgorithmAndCountChange",
			source:   "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) PARTITION BY KEY ALGORITHM=1 (id) PARTITIONS 2",
			target:   "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) PARTITION BY KEY (id) PARTITIONS 3",
			expected: []string{"PARTITION BY KEY (`id`) PARTITIONS 3"},
		},
		{
			// ALGORITHM=2 is the default, which MySQL does not print.
			name:   "KeyAlgorithmExplicitDefault",
			source: "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) PARTITION BY KEY (id) PARTITIONS 2",
			target: "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) PARTITION BY KEY ALGORITHM=2 (id) PARTITIONS 2",
		},
		{
			// ADD PARTITION would keep the subpartitions' algorithm 1.
			name:   "AppendWithSubpartitionKeyAlgorithmChange",
			source: "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) PARTITION BY RANGE (id) SUBPARTITION BY KEY ALGORITHM=1 (id) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN (30))",
			target: "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) PARTITION BY RANGE (id) SUBPARTITION BY KEY (id) SUBPARTITIONS 2 " +
				"(PARTITION p0 VALUES LESS THAN (30), PARTITION p1 VALUES LESS THAN (40))",
			expected: []string{"PARTITION BY RANGE (`id`) SUBPARTITION BY KEY (`id`) SUBPARTITIONS 2"},
		},
		{
			name:   "AppendListPartition",
			source: listSource,
			target: "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) " +
				"PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1, 5), PARTITION p1 VALUES IN (15), PARTITION p2 VALUES IN (25), PARTITION p3 VALUES IN (35))",
			expected: []string{"ADD PARTITION"},
		},
		{
			name:   "SplitMaxvaluePartition",
			source: dateSource,
			insert: "INSERT INTO diff_part_chg VALUES (1, '2026-10-05'), (2, '2026-11-05'), (3, '2027-01-01')",
			target: "CREATE TABLE diff_part_chg (id int NOT NULL, d date NOT NULL, PRIMARY KEY (id, d)) " +
				"PARTITION BY RANGE COLUMNS (d) (PARTITION p202610 VALUES LESS THAN ('2026-11-01'), PARTITION p202611 VALUES LESS THAN ('2026-12-01'), PARTITION pmax VALUES LESS THAN (MAXVALUE))",
			expected: []string{"REORGANIZE PARTITION `pmax` INTO"},
		},
		{
			name:   "MergeRangePartitions",
			source: rangeSource,
			target: "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) " +
				"PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION pmax VALUES LESS THAN MAXVALUE)",
			expected: []string{"REORGANIZE PARTITION `p1`, `pmax` INTO"},
		},
		{
			name:   "MoveRangeBoundary",
			source: rangeSource,
			target: "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) " +
				"PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (12), PARTITION pmax VALUES LESS THAN MAXVALUE)",
			expected: []string{"REORGANIZE PARTITION `p1`, `pmax` INTO"},
		},
		{
			name:   "MoveListValue",
			source: listSource,
			target: "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) " +
				"PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1), PARTITION p1 VALUES IN (5, 15), PARTITION p2 VALUES IN (25))",
			expected: []string{"REORGANIZE PARTITION `p0`, `p1` INTO"},
		},
		{
			name: "MoveMultiColumnListTuple",
			source: "CREATE TABLE diff_part_chg (id int NOT NULL, b varchar(10) NOT NULL, PRIMARY KEY (id, b)) " +
				"PARTITION BY LIST COLUMNS (id, b) (PARTITION p0 VALUES IN ((1, 'x'), (3, 'y')), PARTITION p1 VALUES IN ((5, 'z')))",
			insert: "INSERT INTO diff_part_chg VALUES (1, 'x'), (3, 'y'), (5, 'z')",
			target: "CREATE TABLE diff_part_chg (id int NOT NULL, b varchar(10) NOT NULL, PRIMARY KEY (id, b)) " +
				"PARTITION BY LIST COLUMNS (id, b) (PARTITION p0 VALUES IN ((1, 'x')), PARTITION p1 VALUES IN ((3, 'y'), (5, 'z')))",
			expected: []string{"REORGANIZE PARTITION `p0`, `p1` INTO"},
		},
		{
			name:   "AddMultiColumnListPartitioning",
			source: "CREATE TABLE diff_part_chg (id int NOT NULL, b varchar(10) NOT NULL, PRIMARY KEY (id, b))",
			insert: "INSERT INTO diff_part_chg VALUES (1, 'x'), (3, 'y')",
			target: "CREATE TABLE diff_part_chg (id int NOT NULL, b varchar(10) NOT NULL, PRIMARY KEY (id, b)) " +
				"PARTITION BY LIST COLUMNS (id, b) (PARTITION p0 VALUES IN ((1, 'x'), (3, 'y')), PARTITION p1 VALUES IN ((5, 'z')))",
			expected: []string{"PARTITION BY LIST COLUMNS"},
		},
		{
			name:   "SubpartitionedAppend",
			source: "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) PARTITION BY RANGE (id) SUBPARTITION BY HASH (id) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (30))",
			target: "CREATE TABLE diff_part_chg (id int NOT NULL, b int, PRIMARY KEY (id)) PARTITION BY RANGE (id) SUBPARTITION BY HASH (id) SUBPARTITIONS 2 " +
				"(PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN (30), PARTITION p2 VALUES LESS THAN (40))",
			expected: []string{"ADD PARTITION"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// The target must itself be a table MySQL accepts.
			testutils.NewTestTable(t, "diff_part_chg_target",
				strings.Replace(tc.target, "CREATE TABLE diff_part_chg ", "CREATE TABLE diff_part_chg_target ", 1))
			tt := testutils.NewTestTable(t, "diff_part_chg", tc.source)
			insert := tc.insert
			if insert == "" {
				insert = "INSERT INTO diff_part_chg (id) VALUES (1), (5), (15), (25)"
			}
			testutils.RunSQL(t, insert)
			var before int
			require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM diff_part_chg").Scan(&before))

			stmts := diffLiveTable(t, tt.DB, tt.Name, tc.target)
			require.Len(t, stmts, len(tc.expected))
			for i, stmt := range stmts {
				require.True(t, strings.HasPrefix(stmt.Statement, "ALTER TABLE `diff_part_chg` "+tc.expected[i]),
					"statement %d: %s", i, stmt.Statement)
			}
			execStatements(t, tt.DB, stmts)
			requireConverged(t, tt.DB, tt.Name, tc.target)

			var after int
			require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM diff_part_chg").Scan(&after))
			require.Equal(t, before, after, "no row may be lost")
			if tc.after != "" {
				testutils.RunSQL(t, tc.after)
			}
		})
	}
}

// TestDiffIntegrationListNullValueKeepsRows verifies that a LIST partition
// holding NULL keeps its NULL rows through a partition change. Emitted as the
// string 'NULL', a REORGANIZE would move them into no partition, and MySQL
// would delete them without an error.
func TestDiffIntegrationListNullValueKeepsRows(t *testing.T) {
	const create = "CREATE TABLE diff_list_null (id int NOT NULL, s varchar(10)) " +
		"PARTITION BY LIST COLUMNS (s) (PARTITION p0 VALUES IN (NULL, 'a'), PARTITION p1 VALUES IN ('b'))"
	tests := []struct {
		name     string
		target   string
		expected string
	}{
		{
			name: "CommentChange",
			target: "CREATE TABLE diff_list_null (id int NOT NULL, s varchar(10)) " +
				"PARTITION BY LIST COLUMNS (s) (PARTITION p0 VALUES IN (NULL, 'a') COMMENT 'x', PARTITION p1 VALUES IN ('b'))",
			expected: "VALUES IN (NULL, 'a')",
		},
		{
			name: "MoveNull",
			target: "CREATE TABLE diff_list_null (id int NOT NULL, s varchar(10)) " +
				"PARTITION BY LIST COLUMNS (s) (PARTITION p0 VALUES IN ('a'), PARTITION p1 VALUES IN (NULL, 'b'))",
			expected: "VALUES IN (NULL, 'b')",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tt := testutils.NewTestTable(t, "diff_list_null", create)
			testutils.RunSQL(t, "INSERT INTO diff_list_null VALUES (1, NULL), (2, 'a'), (3, 'b')")
			requireNoSelfDiff(t, tt.DB, tt.Name)

			stmts := diffLiveTable(t, tt.DB, tt.Name, tc.target)
			require.Len(t, stmts, 1)
			require.Contains(t, stmts[0].Statement, "REORGANIZE PARTITION")
			require.Contains(t, stmts[0].Statement, tc.expected)
			execStatements(t, tt.DB, stmts)

			var count int
			require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM diff_list_null").Scan(&count))
			require.Equal(t, 3, count, "the NULL row must survive: %s", stmts[0].Statement)
			requireConverged(t, tt.DB, tt.Name, tc.target)
		})
	}

	// On an integer column NULL is re-emitted in a PARTITION BY (the split
	// can't share an ALTER with the column change). Quoted, it didn't apply.
	t.Run("IntegerRepartition", func(t *testing.T) {
		tt := testutils.NewTestTable(t, "diff_list_null",
			"CREATE TABLE diff_list_null (id int) PARTITION BY LIST (id) (PARTITION p0 VALUES IN (NULL, 1))")
		testutils.RunSQL(t, "INSERT INTO diff_list_null VALUES (NULL), (1)")
		const target = "CREATE TABLE diff_list_null (id int, c int) PARTITION BY LIST (id) " +
			"(PARTITION p0 VALUES IN (NULL), PARTITION p1 VALUES IN (1))"
		stmts := diffLiveTable(t, tt.DB, tt.Name, target)
		require.Len(t, stmts, 1)
		require.Contains(t, stmts[0].Statement, "PARTITION BY LIST (`id`) (PARTITION `p0` VALUES IN (NULL)")
		execStatements(t, tt.DB, stmts)
		requireConverged(t, tt.DB, tt.Name, target)
		var count int
		require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM diff_list_null").Scan(&count))
		require.Equal(t, 2, count)
	})
}

// TestDiffIntegrationFractionalDatetimeBoundKeepsRows verifies that a bound
// spirit cannot evaluate offline is emitted as written, for MySQL to
// evaluate. MySQL rounds the fractional second first, so
// YEAR('2030-12-31 23:59:59.9999999') is 2031; evaluated as 2030, a
// comment-only REORGANIZE would move the 2031 row into no partition, and MySQL
// would delete it without an error.
func TestDiffIntegrationFractionalDatetimeBoundKeepsRows(t *testing.T) {
	const bound = "YEAR('2030-12-31 23:59:59.9999999')"
	t.Run("ReorganizeBetweenAuthoredSchemas", func(t *testing.T) {
		const create = "CREATE TABLE diff_fraction (id int NOT NULL, PRIMARY KEY (id)) PARTITION BY LIST (id) " +
			"(PARTITION p0 VALUES IN (" + bound + ") COMMENT 'old')"
		tt := testutils.NewTestTable(t, "diff_fraction", create)
		testutils.RunSQL(t, "INSERT INTO diff_fraction VALUES (2031)")
		source, err := ParseCreateTable(create)
		require.NoError(t, err)
		target, err := ParseCreateTable(strings.Replace(create, "COMMENT 'old'", "COMMENT 'new'", 1))
		require.NoError(t, err)
		stmts, err := source.Diff(target, nil)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		require.Contains(t, stmts[0].Statement, "VALUES IN ("+bound+")")
		execStatements(t, tt.DB, stmts)
		var count int
		require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM diff_fraction").Scan(&count))
		require.Equal(t, 1, count, "the 2031 row must survive: %s", stmts[0].Statement)
	})
	// Against the live table the bound applies with the value MySQL gives
	// it. It does not converge: the live table reads 2031.
	t.Run("AppendToLiveTable", func(t *testing.T) {
		tt := testutils.NewTestTable(t, "diff_fraction",
			"CREATE TABLE diff_fraction (id int NOT NULL, PRIMARY KEY (id)) PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1))")
		stmts := diffLiveTable(t, tt.DB, tt.Name, "CREATE TABLE diff_fraction (id int NOT NULL, PRIMARY KEY (id)) "+
			"PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1), PARTITION p1 VALUES IN ("+bound+"))")
		require.Len(t, stmts, 1)
		execStatements(t, tt.DB, stmts)
		testutils.RunSQL(t, "INSERT INTO diff_fraction VALUES (2031)")
	})
}

// TestDiffIntegrationMultiColumnListNoSelfDiff verifies that a multi-column
// LIST COLUMNS table, read back from SHOW CREATE TABLE, does not diff against
// the SQL it was created from.
func TestDiffIntegrationMultiColumnListNoSelfDiff(t *testing.T) {
	const authoredSQL = "CREATE TABLE diff_list_tuples (a int NOT NULL, b varchar(10) NOT NULL, PRIMARY KEY (a, b)) " +
		"PARTITION BY LIST COLUMNS (a, b) (PARTITION p0 VALUES IN ((1, 'x'), (2, 'y')), PARTITION p1 VALUES IN ((3, 'z')))"
	tt := testutils.NewTestTable(t, "diff_list_tuples", authoredSQL)

	live := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, live, "VALUES IN ((1,'x'),(2,'y'))", "precondition: MySQL prints the tuples")
	source, err := ParseCreateTable(live)
	require.NoError(t, err)
	target, err := ParseCreateTable(authoredSQL)
	require.NoError(t, err)
	stmts, err := source.Diff(target, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)

	requireNoSelfDiff(t, tt.DB, tt.Name)
}

// TestDiffIntegrationPartitionChangeKeepsRows verifies that a partition
// change that would leave rows without a partition fails, rather than
// deleting them. A LIST REORGANIZE PARTITION would delete them silently, so
// Diff must emit a PARTITION BY.
func TestDiffIntegrationPartitionChangeKeepsRows(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_part_keep",
		"CREATE TABLE diff_part_keep (id int NOT NULL, PRIMARY KEY (id)) "+
			"PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1, 2), PARTITION p1 VALUES IN (3))")
	testutils.RunSQL(t, "INSERT INTO diff_part_keep VALUES (1), (2), (3)")

	stmts := diffLiveTable(t, tt.DB, tt.Name,
		"CREATE TABLE diff_part_keep (id int NOT NULL, PRIMARY KEY (id)) "+
			"PARTITION BY LIST (id) (PARTITION p0 VALUES IN (1), PARTITION p1 VALUES IN (3))")
	require.Len(t, stmts, 1)
	require.Contains(t, stmts[0].Statement, "PARTITION BY LIST")
	_, err := tt.DB.ExecContext(t.Context(), stmts[0].Statement)
	require.ErrorContains(t, err, "1526")

	var count int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM diff_part_keep").Scan(&count))
	require.Equal(t, 3, count)
}

// TestDiffIntegrationTableCollationChangeConverges verifies that changing a
// table's default collation converges in a single apply when a column
// inherits the live table's default. MySQL's table-level DEFAULT COLLATE
// clause only affects columns added later, so the diff must include a MODIFY
// COLUMN for the inheriting column alongside the table-option change — a diff
// that emits only the table option leaves the column on the old collation and
// the same drift resurfaces on the next plan.
func TestDiffIntegrationTableCollationChangeConverges(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_collation_converge",
		"CREATE TABLE diff_collation_converge (id varchar(512) NOT NULL, PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")

	const targetSQL = "CREATE TABLE diff_collation_converge (id varchar(512) COLLATE utf8mb4_general_ci NOT NULL, PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"

	stmts := diffLiveTable(t, tt.DB, tt.Name, targetSQL)
	require.Len(t, stmts, 1)
	require.Equal(t, "ALTER TABLE `diff_collation_converge` MODIFY COLUMN `id` varchar(512) COLLATE utf8mb4_general_ci NOT NULL, COLLATE=utf8mb4_general_ci", stmts[0].Statement)

	execStatements(t, tt.DB, stmts)
	var collation string
	err := tt.DB.QueryRowContext(t.Context(),
		"SELECT COLLATION_NAME FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = 'id'",
		tt.Name).Scan(&collation)
	require.NoError(t, err)
	require.Equal(t, "utf8mb4_general_ci", collation)

	// Re-diff: converged in one apply — nothing left over.
	stmts = diffLiveTable(t, tt.DB, tt.Name, targetSQL)
	require.Nil(t, stmts)
}

// TestDiffIntegrationInheritedColumnFollowsNewTableCollation verifies the
// same convergence when the target column also inherits its table default:
// the emitted MODIFY carries no explicit COLLATE, and MySQL resolves it
// against the new table default set by the table-option clause in the same
// ALTER, so the column lands on the target collation in one apply.
func TestDiffIntegrationInheritedColumnFollowsNewTableCollation(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_collation_inherit",
		"CREATE TABLE diff_collation_inherit (id int NOT NULL, name varchar(100), PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")

	const targetSQL = "CREATE TABLE diff_collation_inherit (id int NOT NULL, name varchar(100), PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"

	stmts := diffLiveTable(t, tt.DB, tt.Name, targetSQL)
	require.Len(t, stmts, 1)
	require.Equal(t, "ALTER TABLE `diff_collation_inherit` MODIFY COLUMN `name` varchar(100) NULL, COLLATE=utf8mb4_general_ci", stmts[0].Statement)

	execStatements(t, tt.DB, stmts)
	var collation string
	err := tt.DB.QueryRowContext(t.Context(),
		"SELECT COLLATION_NAME FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = 'name'",
		tt.Name).Scan(&collation)
	require.NoError(t, err)
	require.Equal(t, "utf8mb4_general_ci", collation)

	// Re-diff: converged in one apply — nothing left over.
	stmts = diffLiveTable(t, tt.DB, tt.Name, targetSQL)
	require.Nil(t, stmts)
}

// A schema file declaring `active BOOLEAN NOT NULL DEFAULT FALSE` and the table
// MySQL creates from it are the same table, so a diff between them must be
// empty. MySQL stores the keyword as the integer and reports `tinyint(1) NOT
// NULL DEFAULT '0'`; a diff that read those two forms as different would emit a
// MODIFY COLUMN that stores the same '0' and then diff again on the next run,
// with no apply able to end it.
func TestDiffIntegrationBooleanKeywordDefaultCreatedAsDeclared(t *testing.T) {
	const declaredSQL = "CREATE TABLE diff_bool_keyword_default (" +
		"id bigint unsigned NOT NULL AUTO_INCREMENT, " +
		"cancel_requested boolean NOT NULL DEFAULT FALSE, " +
		"is_enabled boolean NOT NULL DEFAULT TRUE, " +
		"retries int NOT NULL DEFAULT FALSE, " +
		"PRIMARY KEY (id))"

	tt := testutils.NewTestTable(t, "diff_bool_keyword_default", declaredSQL)

	// MySQL really does report the integer, which is what makes the fold
	// necessary rather than cosmetic.
	live := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, live, "`cancel_requested` tinyint(1) NOT NULL DEFAULT '0'")
	require.Contains(t, live, "`is_enabled` tinyint(1) NOT NULL DEFAULT '1'")
	require.Contains(t, live, "`retries` int NOT NULL DEFAULT '0'")

	// The table was created from this exact declaration, so there is nothing
	// left to apply.
	stmts := diffLiveTable(t, tt.DB, tt.Name, declaredSQL)
	require.Nil(t, stmts)
}

// Changing a keyword default is a real change, and it converges in one apply:
// the diff is emitted, MySQL stores the new value, and a re-diff is clean.
func TestDiffIntegrationBooleanKeywordDefaultChange(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_bool_keyword_change",
		"CREATE TABLE diff_bool_keyword_change (id int NOT NULL, active boolean NOT NULL DEFAULT FALSE, PRIMARY KEY (id))")

	const targetSQL = "CREATE TABLE diff_bool_keyword_change (id int NOT NULL, active boolean NOT NULL DEFAULT TRUE, PRIMARY KEY (id))"

	stmts := diffLiveTable(t, tt.DB, tt.Name, targetSQL)
	require.Len(t, stmts, 1)

	execStatements(t, tt.DB, stmts)
	var columnDefault string
	err := tt.DB.QueryRowContext(t.Context(),
		"SELECT COLUMN_DEFAULT FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = 'active'",
		tt.Name).Scan(&columnDefault)
	require.NoError(t, err)
	require.Equal(t, "1", columnDefault)

	requireConverged(t, tt.DB, tt.Name, targetSQL)
}

// Every type that stores the keyword as exactly 1/0 folds. MySQL quotes the
// stored value on all of them except bit, which reports a bit literal, so what
// the fold records is the form Spirit emits rather than the one the server
// prints. Each column here is created from the declaration under test, so the
// reading is the server's own and the table has nothing left to apply.
func TestDiffIntegrationBooleanKeywordDefaultAcrossFoldingTypes(t *testing.T) {
	const declaredSQL = "CREATE TABLE diff_bool_keyword_folding_types (" +
		"unscaled decimal(4,0) NOT NULL DEFAULT TRUE, " +
		"dbl double NOT NULL DEFAULT TRUE, " +
		"flt float NOT NULL DEFAULT FALSE, " +
		"txt varchar(8) NOT NULL DEFAULT FALSE, " +
		"fixed char(8) NOT NULL DEFAULT TRUE, " +
		"vbin varbinary(8) NOT NULL DEFAULT FALSE, " +
		"bits bit(1) NOT NULL DEFAULT TRUE, " +
		"widebits bit(8) NOT NULL DEFAULT FALSE)"
	tt := testutils.NewTestTable(t, "diff_bool_keyword_folding_types", declaredSQL)

	live := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, live, "`unscaled` decimal(4,0) NOT NULL DEFAULT '1'")
	require.Contains(t, live, "`dbl` double NOT NULL DEFAULT '1'")
	require.Contains(t, live, "`flt` float NOT NULL DEFAULT '0'")
	require.Contains(t, live, "`txt` varchar(8) NOT NULL DEFAULT '0'")
	require.Contains(t, live, "`fixed` char(8) NOT NULL DEFAULT '1'")
	require.Contains(t, live, "`vbin` varbinary(8) NOT NULL DEFAULT '0'")
	// A bit column reports the value as a bit literal in its minimal form,
	// independent of the column's width.
	require.Contains(t, live, "`bits` bit(1) NOT NULL DEFAULT b'1'")
	require.Contains(t, live, "`widebits` bit(8) NOT NULL DEFAULT b'0'")

	require.Nil(t, diffLiveTable(t, tt.DB, tt.Name, declaredSQL))
}

// The types that store the keyword as something other than 1/0, with the
// reading that puts each out of scope and the diff it still emits as a result.
// Asserting the leftover diff alongside the reading is deliberate: a reading on
// its own does not say whether the exclusion it justifies is the right one, and
// scaled decimal is excluded for a reason this layer cannot fix — scale padding
// belongs to numeric canonicalization. binary is excluded here too, because it
// pads the keyword to the column width; binaryDefaultBytesNormalizer folds it
// instead, and TestDiffIntegrationBinaryDefaultBytes covers it. year reads the
// keyword as a year (TRUE stores '2001'); yearDefaultNormalizer folds it, and
// TestDiffIntegrationYearDefaultCreatedAsDeclared covers it.
//
// enum and set are excluded too but are deliberately not fixtures here. They
// have no single reading to record: through 8.4 the keyword resolves to a
// member index and from 9.7 to a member value, so enum('0','1') DEFAULT TRUE
// stores '0' on one and '1' on the other. A fixture would have to assert one
// of the two and fail on the rest of the supported matrix. That the fold
// skips them is pinned without a server in
// TestBooleanKeywordDefaultLeavesOtherTypesAlone.
func TestDiffIntegrationBooleanKeywordDefaultOnExcludedTypes(t *testing.T) {
	const declaredSQL = "CREATE TABLE diff_bool_keyword_excluded_types (" +
		"scaled decimal(4,2) NOT NULL DEFAULT TRUE)"
	tt := testutils.NewTestTable(t, "diff_bool_keyword_excluded_types", declaredSQL)

	live := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, live, "`scaled` decimal(4,2) NOT NULL DEFAULT '1.00'")

	// The table was created from this very declaration, so the statement here
	// re-stores a value the column already holds.
	stmts := diffLiveTable(t, tt.DB, tt.Name, declaredSQL)
	require.Len(t, stmts, 1)
	require.Contains(t, stmts[0].Statement, "MODIFY COLUMN `scaled`")
}

// A ZEROFILL integer's default is stored padded to the display width, so a
// table created from those declarations must diff clean against them. Without
// zerofillDefaultNormalizer the diff emits `MODIFY ... DEFAULT 5` against the
// live '0000000005' on every run.
//
// A width that is unwritten or 0 is stored as the type's unsigned default
// width, and the default is padded to it.
func TestDiffIntegrationZerofillDefaultCreatedAsDeclared(t *testing.T) {
	const declaredSQL = "CREATE TABLE diff_zerofill_default (" +
		"id int NOT NULL, " +
		"a int(10) zerofill DEFAULT 5, " +
		"a0 int(0) zerofill DEFAULT 5, " +
		"b int(3) zerofill DEFAULT 12345, " +
		"c tinyint(2) zerofill DEFAULT '7', " +
		"d bigint(20) zerofill NOT NULL DEFAULT 0, " +
		"e int(4) zerofill DEFAULT 0x10, " +
		"f int(4) zerofill DEFAULT b'101', " +
		"g int(4) zerofill DEFAULT TRUE, " +
		"h int(4) zerofill DEFAULT 2.5, " +
		"i int(4) zerofill DEFAULT 2.5e0, " +
		"j int(4) zerofill DEFAULT '2.5e0', " +
		"k int(4) zerofill DEFAULT ' 5 ', " +
		"l int(4) zerofill DEFAULT '\\t5\\n', " +
		"m int(4) zerofill DEFAULT -0.0, " +
		"n int(4) zerofill DEFAULT -0.5e0, " +
		"o int(4) zerofill DEFAULT '-0.49', " +
		"p int(4) zerofill DEFAULT '5e-65', " +
		"q bigint(20) zerofill DEFAULT 1234567890123456789e0, " +
		"w1 int zerofill DEFAULT 5, " +
		"w2 tinyint zerofill DEFAULT 5, " +
		"w3 smallint zerofill DEFAULT 5, " +
		"w4 mediumint zerofill DEFAULT 5, " +
		"w5 bigint zerofill DEFAULT 5, " +
		"w6 int unsigned zerofill DEFAULT 5, " +
		"PRIMARY KEY (id))"

	tt := testutils.NewTestTable(t, "diff_zerofill_default", declaredSQL)

	live := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, live, "`a` int(10) unsigned zerofill DEFAULT '0000000005'")
	require.Contains(t, live, "`a0` int(10) unsigned zerofill DEFAULT '0000000005'")
	require.Contains(t, live, "`b` int(3) unsigned zerofill DEFAULT '12345'")
	require.Contains(t, live, "`h` int(4) unsigned zerofill DEFAULT '0003'")
	require.Contains(t, live, "`i` int(4) unsigned zerofill DEFAULT '0002'")
	require.Contains(t, live, "`j` int(4) unsigned zerofill DEFAULT '0003'")
	require.Contains(t, live, "`m` int(4) unsigned zerofill DEFAULT '0000'")
	require.Contains(t, live, "`q` bigint(20) unsigned zerofill DEFAULT '01234567890123456768'")
	require.Contains(t, live, "`w1` int(10) unsigned zerofill DEFAULT '0000000005'")
	require.Contains(t, live, "`w5` bigint(20) unsigned zerofill DEFAULT '00000000000000000005'")

	stmts := diffLiveTable(t, tt.DB, tt.Name, declaredSQL)
	require.Nil(t, stmts)
}

// Changing a ZEROFILL default is a real change: the diff is emitted, MySQL
// stores the padded value, and a re-diff is clean.
func TestDiffIntegrationZerofillDefaultConverges(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_zerofill_default_change",
		"CREATE TABLE diff_zerofill_default_change (id int NOT NULL, a int(10) zerofill DEFAULT 5, b int(4) zerofill, PRIMARY KEY (id))")

	const targetSQL = "CREATE TABLE diff_zerofill_default_change (id int NOT NULL, " +
		"a int(10) zerofill DEFAULT 6, b int(4) zerofill DEFAULT 42, PRIMARY KEY (id))"

	stmts := diffLiveTable(t, tt.DB, tt.Name, targetSQL)
	require.Len(t, stmts, 1)

	execStatements(t, tt.DB, stmts)
	live := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, live, "`a` int(10) unsigned zerofill DEFAULT '0000000006'")
	require.Contains(t, live, "`b` int(4) unsigned zerofill DEFAULT '0042'")

	requireConverged(t, tt.DB, tt.Name, targetSQL)
}

// TestDiffIntegrationColumnLeavesPrimaryKeyAndRelaxes verifies that a column
// leaving the primary key and declared nullable by the target actually becomes
// nullable on a real MySQL server. Adding a PRIMARY KEY implicitly makes its
// columns NOT NULL, but DROP PRIMARY KEY does not revert that, so the diff must
// carry its own MODIFY for the column. The applied table is compared against a
// reference table created directly from the target, and a re-diff converges.
func TestDiffIntegrationColumnLeavesPrimaryKeyAndRelaxes(t *testing.T) {
	tests := []struct {
		name   string
		source string
		target string
	}{
		{
			name:   "PrimaryKeyMoves",
			source: "CREATE TABLE %s (a varchar(10) NOT NULL, b varchar(10) DEFAULT NULL, PRIMARY KEY (a))",
			target: "CREATE TABLE %s (a varchar(10) DEFAULT NULL, b varchar(10) NOT NULL, PRIMARY KEY (b))",
		},
		{
			name:   "PrimaryKeyMovesToColumnOmittingNotNull",
			source: "CREATE TABLE %s (a varchar(10) NOT NULL, b varchar(10) DEFAULT NULL, PRIMARY KEY (a))",
			target: "CREATE TABLE %s (a varchar(10) DEFAULT NULL, b varchar(10), PRIMARY KEY (b))",
		},
		{
			name:   "PrimaryKeyDropped",
			source: "CREATE TABLE %s (a varchar(10) NOT NULL, b varchar(10) DEFAULT NULL, PRIMARY KEY (a))",
			target: "CREATE TABLE %s (a varchar(10) DEFAULT NULL, b varchar(10) DEFAULT NULL)",
		},
	}
	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			name := fmt.Sprintf("diff_pk_leave_relax_%d", i)
			refName := name + "_ref"
			tt := testutils.NewTestTable(t, name, fmt.Sprintf(tc.source, name))
			ref := testutils.NewTestTable(t, refName, fmt.Sprintf(tc.target, refName))

			target, err := ParseCreateTable(fmt.Sprintf(tc.target, name))
			require.NoError(t, err)
			source, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
			require.NoError(t, err)

			stmts, err := source.Diff(target, nil)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			require.Contains(t, stmts[0].Statement, "MODIFY COLUMN `a` varchar(10) NULL")

			_, err = tt.DB.ExecContext(t.Context(), stmts[0].Statement)
			require.NoError(t, err)

			// Check nullability on the parsed table rather than the rendered
			// text, whose column clauses vary across server builds.
			postAlter := showCreateTable(t, tt.DB, tt.Name)
			source, err = ParseCreateTable(postAlter)
			require.NoError(t, err)
			require.Equal(t, "a", source.Columns[0].Name)
			require.True(t, source.Columns[0].Nullable, "`a` must be nullable after the ALTER")
			want := strings.Replace(showCreateTable(t, ref.DB, ref.Name), "`"+refName+"`", "`"+name+"`", 1)
			require.Equal(t, want, postAlter)

			// Re-diff: the schemas now converge.
			stmts, err = source.Diff(target, nil)
			require.NoError(t, err)
			require.Nil(t, stmts)
		})
	}
}

// TestDiffIntegrationPrimaryKeyImplicitNotNull verifies that a primary key
// column authored without NOT NULL matches what MySQL stores. MySQL makes every
// primary key column NOT NULL, so diffing the live table against the authored
// one must not emit a MODIFY COLUMN ... NULL, which MySQL rejects with error
// 1171. Adding a primary key to such a column must converge in one round.
func TestDiffIntegrationPrimaryKeyImplicitNotNull(t *testing.T) {
	t.Run("PrimaryKeyUnchanged", func(t *testing.T) {
		const authored = "CREATE TABLE diff_pk_implicit_nn (a int, b int, PRIMARY KEY (a))"
		tt := testutils.NewTestTable(t, "diff_pk_implicit_nn", authored)
		require.Contains(t, showCreateTable(t, tt.DB, tt.Name), "`a` int NOT NULL")
		require.Nil(t, diffLiveTable(t, tt.DB, tt.Name, authored))
	})

	t.Run("AddPrimaryKey", func(t *testing.T) {
		tt := testutils.NewTestTable(t, "diff_pk_implicit_nn_add", "CREATE TABLE diff_pk_implicit_nn_add (a int, b int)")
		ref := testutils.NewTestTable(t, "diff_pk_implicit_nn_add_ref", "CREATE TABLE diff_pk_implicit_nn_add_ref (a int, b int, PRIMARY KEY (a))")
		const authored = "CREATE TABLE diff_pk_implicit_nn_add (a int, b int, PRIMARY KEY (a))"

		stmts := diffLiveTable(t, tt.DB, tt.Name, authored)
		require.Len(t, stmts, 1)
		_, err := tt.DB.ExecContext(t.Context(), stmts[0].Statement)
		require.NoError(t, err)

		want := strings.Replace(showCreateTable(t, ref.DB, ref.Name), "`"+ref.Name+"`", "`"+tt.Name+"`", 1)
		require.Equal(t, want, showCreateTable(t, tt.DB, tt.Name))
		require.Nil(t, diffLiveTable(t, tt.DB, tt.Name, authored))
	})

	// The expression default (NULL) is not a NULL declaration: MySQL accepts
	// it on a key column and stores the column NOT NULL.
	t.Run("ExpressionDefaultNull", func(t *testing.T) {
		const authored = "CREATE TABLE diff_pk_expr_default_null (a int DEFAULT (NULL), b int, PRIMARY KEY (a))"
		tt := testutils.NewTestTable(t, "diff_pk_expr_default_null", authored)
		require.Contains(t, showCreateTable(t, tt.DB, tt.Name), "`a` int NOT NULL DEFAULT (NULL)")
		require.Nil(t, diffLiveTable(t, tt.DB, tt.Name, authored))
	})

	// An explicit NULL or DEFAULT NULL on a key column is not implicit: MySQL
	// refuses to create the table, even when NOT NULL follows the NULL, and
	// DeclarativeToImperative rejects it as a desired schema rather than
	// planning toward it.
	for name, desired := range map[string]string{
		"ExplicitNullRejected":            "CREATE TABLE diff_pk_explicit_null (a int NULL, b int, PRIMARY KEY (a))",
		"DefaultNullRejected":             "CREATE TABLE diff_pk_explicit_null (a int DEFAULT NULL, b int, PRIMARY KEY (a))",
		"ExplicitNullInCompositeRejected": "CREATE TABLE diff_pk_explicit_null (a int, b int NULL, PRIMARY KEY (a, b))",
		"ExplicitNullThenNotNullRejected": "CREATE TABLE diff_pk_explicit_null (a int NULL NOT NULL, b int, PRIMARY KEY (a))",
	} {
		t.Run(name, func(t *testing.T) {
			testutils.RunSQL(t, "DROP TABLE IF EXISTS diff_pk_explicit_null")
			db, err := sql.Open("block-mysql", testutils.DSN())
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			_, err = db.ExecContext(t.Context(), desired)
			require.ErrorContains(t, err, "Error 1171")

			_, err = DeclarativeToImperative(nil, []table.TableSchema{{Name: "diff_pk_explicit_null", Schema: desired}}, nil)
			require.ErrorContains(t, err, "is part of the PRIMARY KEY but declares NULL")
		})
	}
}

// TestDiffIntegrationNationalCharset verifies that NCHAR/NVARCHAR columns,
// which always use the national character set (utf8mb3), diff to nothing
// against the table MySQL creates from the same definition, in both a
// utf8mb4 and a utf8mb3 table. A utf8mb3 table default and an explicit
// utf8mb3 column must also still match a live column that inherits it.
func TestDiffIntegrationNationalCharset(t *testing.T) {
	for _, tc := range []struct{ name, ddl string }{
		{"diff_nchar_mb4", "CREATE TABLE diff_nchar_mb4 (id int NOT NULL, a NVARCHAR(10), b NCHAR(3), c NCHAR, d NATIONAL VARCHAR(5) BINARY, e NCHAR VARYING(4) COLLATE utf8mb3_bin, PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"},
		{"diff_nchar_mb3", "CREATE TABLE diff_nchar_mb3 (id int NOT NULL, a NVARCHAR(10), b NCHAR(3) BINARY, c varchar(3) CHARACTER SET utf8mb3, d varchar(3), PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb3"},
		{"diff_nchar_latin1", "CREATE TABLE diff_nchar_latin1 (id int NOT NULL, a NCHAR(5) BINARY COLLATE utf8mb3_unicode_ci, b varchar(10) CHARACTER SET latin1 BINARY COLLATE latin1_general_ci, c varchar(10) BINARY, PRIMARY KEY (id)) DEFAULT CHARSET=latin1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tt := testutils.NewTestTable(t, tc.name, tc.ddl)
			desired, err := ParseCreateTable(tc.ddl)
			require.NoError(t, err)
			live, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
			require.NoError(t, err)
			stmts, err := live.Diff(desired, nil)
			require.NoError(t, err)
			require.Nil(t, stmts, "national charset columns must match their live form")
		})
	}
}

// TestDiffIntegrationNationalCharsetConverges verifies that converting a
// utf8mb4 column to NVARCHAR emits a MODIFY that MySQL applies, after which a
// re-diff converges to nil.
func TestDiffIntegrationNationalCharsetConverges(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_nchar_converge",
		"CREATE TABLE diff_nchar_converge (id int NOT NULL, a varchar(10), PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	desired, err := ParseCreateTable(
		"CREATE TABLE diff_nchar_converge (id int NOT NULL, a NVARCHAR(10), PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)

	live, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	stmts, err := live.Diff(desired, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	testutils.RunSQL(t, stmts[0].Statement)

	live, err = ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	stmts, err = live.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts, "re-diff after applying the MODIFY must converge")
}

// TestDiffIntegrationDefaultCollation verifies, for every charset the server
// knows other than utf8mb4 and binary, that a column or table declaring the
// charset without a COLLATE matches its live form, which writes the default
// collation out. The charsets come from the server rather than the parser, so a
// default the parser's registry gets wrong fails here.
func TestDiffIntegrationDefaultCollation(t *testing.T) {
	db, err := sql.Open("block-mysql", testutils.DSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	rows, err := db.QueryContext(t.Context(),
		"SELECT character_set_name FROM information_schema.character_sets WHERE character_set_name NOT IN ('utf8mb4', 'binary') ORDER BY 1")
	require.NoError(t, err)
	var charsets []string
	for rows.Next() {
		var cs string
		require.NoError(t, rows.Scan(&cs))
		charsets = append(charsets, cs)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.NotEmpty(t, charsets)

	for _, cs := range charsets {
		t.Run(cs, func(t *testing.T) {
			for _, ddl := range []string{
				// A column that declares the charset in a utf8mb4 table.
				fmt.Sprintf("CREATE TABLE diff_default_collation (id int NOT NULL, a varchar(3) CHARACTER SET %s, b text CHARACTER SET %s, PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", cs, cs),
				// A table default, with one column inheriting it and one
				// declaring it.
				fmt.Sprintf("CREATE TABLE diff_default_collation (id int NOT NULL, a varchar(3), b varchar(3) CHARACTER SET %s, PRIMARY KEY (id)) DEFAULT CHARSET=%s", cs, cs),
			} {
				tt := testutils.NewTestTable(t, "diff_default_collation", ddl)
				desired, err := ParseCreateTable(ddl)
				require.NoError(t, err)
				live, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
				require.NoError(t, err)
				stmts, err := live.Diff(desired, nil)
				require.NoError(t, err)
				require.Nil(t, stmts, "a charset without a COLLATE must match its live form: %s", ddl)
			}
		})
	}
}

// TestDiffIntegrationUtf8mb4ColumnWithoutCollation verifies that a column
// declaring CHARACTER SET utf8mb4 without a COLLATE matches its live form in a
// table whose default is another charset or another utf8mb4 collation, in both
// diff directions. The column takes the server's utf8mb4 default rather than
// the table's collation, so SHOW CREATE TABLE writes that collation out on it.
// Before the fix the diff emitted a MODIFY restating the bare charset on every
// plan, which MySQL applies without changing SHOW CREATE TABLE.
func TestDiffIntegrationUtf8mb4ColumnWithoutCollation(t *testing.T) {
	for _, ddl := range []string{
		"CREATE TABLE diff_utf8mb4_no_collate (id int NOT NULL, b char(4) CHARACTER SET utf8mb4 DEFAULT 'a', PRIMARY KEY (id)) DEFAULT CHARSET=latin1",
		"CREATE TABLE diff_utf8mb4_no_collate (id int NOT NULL, b char(4) CHARACTER SET utf8mb4 DEFAULT 'a', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	} {
		tt := testutils.NewTestTable(t, "diff_utf8mb4_no_collate", ddl)

		// The column must not have inherited the table collation, or SHOW
		// CREATE TABLE would omit its clauses and this test would not
		// exercise the spelled-out form.
		var tableCollation, columnCollation string
		require.NoError(t, tt.DB.QueryRowContext(t.Context(),
			"SELECT table_collation FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?", tt.Name).Scan(&tableCollation))
		require.NoError(t, tt.DB.QueryRowContext(t.Context(),
			"SELECT collation_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = 'b'", tt.Name).Scan(&columnCollation))
		require.NotEqual(t, tableCollation, columnCollation, ddl)
		liveDDL := showCreateTable(t, tt.DB, tt.Name)
		require.Contains(t, liveDDL, "COLLATE "+columnCollation, ddl)

		desired, err := ParseCreateTable(ddl)
		require.NoError(t, err)
		live, err := ParseCreateTable(liveDDL)
		require.NoError(t, err)
		stmts, err := live.Diff(desired, nil)
		require.NoError(t, err)
		require.Nil(t, stmts, "live.Diff(desired) must converge: %s", ddl)
		stmts, err = desired.Diff(live, nil)
		require.NoError(t, err)
		require.Nil(t, stmts, "desired.Diff(live) must converge: %s", ddl)
	}
}

// TestDiffIntegrationUtf8mb4ColumnWithoutCollationDetectsDrift verifies that
// a live column on a utf8mb4 collation default_collation_for_utf8mb4 cannot
// hold is MODIFYed onto what the bare CHARACTER SET utf8mb4 declaration
// creates, and that the MODIFY converges. That variable accepts only
// utf8mb4_0900_ai_ci and utf8mb4_general_ci, so utf8mb4_bin is never what the
// declaration creates, on any server.
func TestDiffIntegrationUtf8mb4ColumnWithoutCollationDetectsDrift(t *testing.T) {
	for _, tableOpts := range []string{
		"DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		"DEFAULT CHARSET=latin1",
		// The live column inherits the table's utf8mb4_bin, so SHOW CREATE
		// TABLE writes no COLLATE on it.
		"DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	} {
		t.Run(tableOpts, func(t *testing.T) {
			tt := testutils.NewTestTable(t, "diff_utf8mb4_drift",
				"CREATE TABLE diff_utf8mb4_drift (id int NOT NULL, b varchar(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin, PRIMARY KEY (id)) "+tableOpts)
			desired, err := ParseCreateTable(
				"CREATE TABLE diff_utf8mb4_drift (id int NOT NULL, b varchar(4) CHARACTER SET utf8mb4, PRIMARY KEY (id)) " + tableOpts)
			require.NoError(t, err)

			live, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
			require.NoError(t, err)
			stmts, err := live.Diff(desired, nil)
			require.NoError(t, err)
			require.Len(t, stmts, 1, "a utf8mb4_bin column is not what CHARACTER SET utf8mb4 creates")
			testutils.RunSQL(t, stmts[0].Statement)

			var collation string
			require.NoError(t, tt.DB.QueryRowContext(t.Context(),
				"SELECT collation_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = 'b'", tt.Name).Scan(&collation))
			require.True(t, utf8mb4ServerDefaultCollations[collation], "column collation %s", collation)

			live, err = ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
			require.NoError(t, err)
			stmts, err = live.Diff(desired, nil)
			require.NoError(t, err)
			require.Nil(t, stmts, "the MODIFY must converge")
		})
	}
}

// TestDiffIntegrationUtf8mb4TableWithoutCollation verifies that a desired
// DEFAULT CHARSET=utf8mb4 without a COLLATE converges a table MySQL could not
// have created from it, including the column that inherits the table default,
// in one ALTER, whichever value default_collation_for_utf8mb4 has. That
// variable is what CREATE TABLE gives the declared table, and it accepts only
// the two server-default collations, so a utf8mb4_bin table is drift.
func TestDiffIntegrationUtf8mb4TableWithoutCollation(t *testing.T) {
	const declared = "CREATE TABLE %s (id int NOT NULL, b varchar(3), PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4"
	for _, serverDefault := range []string{"utf8mb4_0900_ai_ci", "utf8mb4_general_ci"} {
		for _, liveOpts := range []string{
			"DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
			"DEFAULT CHARSET=latin1",
		} {
			t.Run(serverDefault+"/"+liveOpts, func(t *testing.T) {
				tt := testutils.NewTestTable(t, "diff_utf8mb4_table_no_collate",
					"CREATE TABLE diff_utf8mb4_table_no_collate (id int NOT NULL, b varchar(3), PRIMARY KEY (id)) "+liveOpts)
				conn, err := tt.DB.Conn(t.Context())
				require.NoError(t, err)
				t.Cleanup(func() { _ = conn.Close() })
				_, err = conn.ExecContext(t.Context(), "SET SESSION default_collation_for_utf8mb4 = "+serverDefault)
				require.NoError(t, err)
				collations := func(table string) (tableCollation, columnCollation string) {
					require.NoError(t, conn.QueryRowContext(t.Context(),
						"SELECT table_collation FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?", table).Scan(&tableCollation))
					require.NoError(t, conn.QueryRowContext(t.Context(),
						"SELECT collation_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = 'b'", table).Scan(&columnCollation))
					return tableCollation, columnCollation
				}

				// The premise: CREATE TABLE gives the declared table and its
				// inheriting column the variable's value.
				const createdName = "diff_utf8mb4_table_no_collate_created"
				t.Cleanup(func() { testutils.RunSQL(t, "DROP TABLE IF EXISTS "+createdName) })
				_, err = conn.ExecContext(t.Context(), "DROP TABLE IF EXISTS "+createdName)
				require.NoError(t, err)
				_, err = conn.ExecContext(t.Context(), fmt.Sprintf(declared, createdName))
				require.NoError(t, err)
				tableCollation, columnCollation := collations(createdName)
				require.Equal(t, serverDefault, tableCollation)
				require.Equal(t, serverDefault, columnCollation)

				desired, err := ParseCreateTable(fmt.Sprintf(declared, tt.Name))
				require.NoError(t, err)
				live, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
				require.NoError(t, err)
				stmts, err := live.Diff(desired, nil)
				require.NoError(t, err)
				require.Len(t, stmts, 1)
				_, err = conn.ExecContext(t.Context(), stmts[0].Statement)
				require.NoError(t, err)

				tableCollation, columnCollation = collations(tt.Name)
				require.Equal(t, serverDefault, tableCollation, stmts[0].Statement)
				require.Equal(t, serverDefault, columnCollation, stmts[0].Statement)

				live, err = ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
				require.NoError(t, err)
				stmts, err = live.Diff(desired, nil)
				require.NoError(t, err)
				require.Nil(t, stmts, "re-diff after applying the ALTER must converge")
			})
		}
	}
}

// TestDiffIntegrationNoTableCharsetStaysUnderdetermined verifies the case the
// rule above must not cover: a table with no charset clause inherits the
// schema default, which can be utf8mb4_bin, so a utf8mb4_bin table matches it.
func TestDiffIntegrationNoTableCharsetStaysUnderdetermined(t *testing.T) {
	// A database of its own, so that changing its default does not affect
	// other tests.
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, "ALTER DATABASE DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	ddl := "CREATE TABLE diff_no_table_charset (id int NOT NULL, b varchar(3), PRIMARY KEY (id))"
	testutils.RunSQLInDatabase(t, dbName, ddl)

	desired, err := ParseCreateTable(ddl)
	require.NoError(t, err)
	liveDDL := showCreateTable(t, db, "diff_no_table_charset")
	require.Contains(t, liveDDL, "COLLATE=utf8mb4_bin")
	live, err := ParseCreateTable(liveDDL)
	require.NoError(t, err)
	stmts, err := live.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts)
}

// TestDiffIntegrationTableCharsetSelectsDefaultCollation verifies that a
// desired DEFAULT CHARSET=latin1 converges a latin1_bin table, including the
// column that inherits the table default, onto latin1_swedish_ci in one ALTER.
func TestDiffIntegrationTableCharsetSelectsDefaultCollation(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_table_default_collation",
		"CREATE TABLE diff_table_default_collation (id int NOT NULL, a varchar(3), PRIMARY KEY (id)) DEFAULT CHARSET=latin1 COLLATE=latin1_bin")
	desired, err := ParseCreateTable(
		"CREATE TABLE diff_table_default_collation (id int NOT NULL, a varchar(3), PRIMARY KEY (id)) DEFAULT CHARSET=latin1")
	require.NoError(t, err)

	live, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	stmts, err := live.Diff(desired, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	testutils.RunSQL(t, stmts[0].Statement)

	var tableCollation, columnCollation string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT table_collation FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?", tt.Name).Scan(&tableCollation))
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT collation_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = 'a'", tt.Name).Scan(&columnCollation))
	require.Equal(t, "latin1_swedish_ci", tableCollation)
	require.Equal(t, "latin1_swedish_ci", columnCollation)

	live, err = ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	stmts, err = live.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts, "re-diff after applying the ALTER must converge")
}

// TestDiffIntegrationColumnCharsetConverges verifies that converting a utf8mb4
// column to CHARACTER SET latin1 emits a MODIFY that MySQL applies, after which
// a re-diff converges to nil.
func TestDiffIntegrationColumnCharsetConverges(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_column_charset_converge",
		"CREATE TABLE diff_column_charset_converge (id int NOT NULL, a varchar(3), PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	desired, err := ParseCreateTable(
		"CREATE TABLE diff_column_charset_converge (id int NOT NULL, a varchar(3) CHARACTER SET latin1, PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)

	live, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	stmts, err := live.Diff(desired, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	testutils.RunSQL(t, stmts[0].Statement)

	live, err = ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	stmts, err = live.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts, "re-diff after applying the MODIFY must converge")
}

// TestDiffIntegrationBinaryCharset verifies that a character column whose
// charset resolves to binary, through the table default or its own COLLATE
// binary, matches its live form, which MySQL stores as the binary type
// (varchar -> varbinary, char -> binary, text -> blob). enum and set keep their
// type, and a column that declares its own charset or collation is not
// rewritten. Without the rewrite the diff emits a MODIFY back to the written
// type on every run.
func TestDiffIntegrationBinaryCharset(t *testing.T) {
	for _, tc := range []struct{ name, ddl string }{
		{"diff_binary_default", "CREATE TABLE diff_binary_default (id int NOT NULL, a varchar(3), PRIMARY KEY (id)) DEFAULT CHARSET=binary"},
		{"diff_binary_default_all", "CREATE TABLE diff_binary_default_all (id int NOT NULL, a varchar(3), b char(3), c text, d tinytext, e mediumtext, f longtext, g enum('x','y'), h set('x'), i varchar(3) CHARACTER SET latin1, j varchar(3) BINARY, k varchar(3) COLLATE utf8mb4_bin, l varchar(3) DEFAULT 'ab', m NVARCHAR(3), n char, o varchar(3) CHARACTER SET binary, p varchar(6) GENERATED ALWAYS AS (concat(a,a)) VIRTUAL, PRIMARY KEY (id)) DEFAULT CHARSET=binary"},
		{"diff_binary_default_collate", "CREATE TABLE diff_binary_default_collate (id int NOT NULL, a varchar(3), c text, PRIMARY KEY (id)) DEFAULT COLLATE=binary"},
		{"diff_binary_default_both", "CREATE TABLE diff_binary_default_both (id int NOT NULL, a varchar(3), PRIMARY KEY (id)) DEFAULT CHARSET=BINARY COLLATE=BINARY"},
		{"diff_binary_column_collate", "CREATE TABLE diff_binary_column_collate (id int NOT NULL, a varchar(3) COLLATE binary, b text COLLATE binary, PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tt := testutils.NewTestTable(t, tc.name, tc.ddl)
			desired, err := ParseCreateTable(tc.ddl)
			require.NoError(t, err)
			live, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
			require.NoError(t, err)
			stmts, err := live.Diff(desired, nil)
			require.NoError(t, err)
			require.Nil(t, stmts, "a column with the binary charset must match its live form")
			stmts, err = desired.Diff(live, nil)
			require.NoError(t, err)
			require.Nil(t, stmts, "the live form must match a column with the binary charset")
		})
	}
}

// TestDiffIntegrationBinaryCharsetConverges verifies that converting a utf8mb4
// table to DEFAULT CHARSET=binary emits an ALTER that MySQL applies, which
// converts the column inheriting the default to its binary type, after which a
// re-diff converges to nil.
func TestDiffIntegrationBinaryCharsetConverges(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_binary_converge",
		"CREATE TABLE diff_binary_converge (id int NOT NULL, a varchar(3), b text, PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	desired, err := ParseCreateTable(
		"CREATE TABLE diff_binary_converge (id int NOT NULL, a varchar(3), b text, PRIMARY KEY (id)) DEFAULT CHARSET=binary")
	require.NoError(t, err)

	live, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	stmts, err := live.Diff(desired, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	testutils.RunSQL(t, stmts[0].Statement)

	var columnType string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT column_type FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = 'a'", tt.Name).Scan(&columnType))
	require.Equal(t, "varbinary(3)", columnType)

	live, err = ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	stmts, err = live.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts, "re-diff after applying the ALTER must converge")
}

// TestIntegrationEnumSetCharsetRestoredAlter runs the restored alter of an
// ENUM or SET column with an explicit charset against MySQL, and checks the
// column has that charset rather than the table's utf8mb4 default. The
// migration runner executes this restored text (for the INSTANT and INPLACE
// attempts and for the shadow table), so a dropped charset silently changes
// the schema.
//
// information_schema.columns reports CHARACTER_SET_NAME and COLLATION_NAME as
// NULL for an ENUM or SET in the binary charset, so the binary cases expect
// NULL. The bug being guarded against yields 'utf8mb4'.
func TestIntegrationEnumSetCharsetRestoredAlter(t *testing.T) {
	for _, tc := range []struct {
		name, alter string
		charset     sql.NullString
		collation   sql.NullString
	}{
		{"add_enum_binary", "ALTER TABLE t ADD COLUMN c enum('a','b') CHARACTER SET binary", sql.NullString{}, sql.NullString{}},
		{"add_enum_byte", "ALTER TABLE t ADD COLUMN c enum('a','b') BYTE", sql.NullString{}, sql.NullString{}},
		{"add_enum_latin1", "ALTER TABLE t ADD COLUMN c enum('a','b') CHARACTER SET latin1", sql.NullString{String: "latin1", Valid: true}, sql.NullString{String: "latin1_swedish_ci", Valid: true}},
		{"add_enum_ascii", "ALTER TABLE t ADD COLUMN c enum('a','b') ASCII", sql.NullString{String: "latin1", Valid: true}, sql.NullString{String: "latin1_swedish_ci", Valid: true}},
		{"add_enum_latin1_binary_attr", "ALTER TABLE t ADD COLUMN c enum('a','b') CHARACTER SET latin1 BINARY", sql.NullString{String: "latin1", Valid: true}, sql.NullString{String: "latin1_bin", Valid: true}},
		{"modify_set_binary", "ALTER TABLE t MODIFY c set('a','b') CHARACTER SET binary DEFAULT 'a'", sql.NullString{}, sql.NullString{}},
		{"modify_set_latin1", "ALTER TABLE t MODIFY c set('a','b') CHARACTER SET latin1", sql.NullString{String: "latin1", Valid: true}, sql.NullString{String: "latin1_swedish_ci", Valid: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl := "enum_set_charset_" + tc.name
			ddl := "CREATE TABLE " + tbl + " (id int NOT NULL PRIMARY KEY) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
			if strings.Contains(tc.alter, "MODIFY") {
				ddl = "CREATE TABLE " + tbl + " (id int NOT NULL PRIMARY KEY, c set('a','b')) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
			}
			tt := testutils.NewTestTable(t, tbl, ddl)

			stmts, err := New(tc.alter)
			require.NoError(t, err)
			testutils.RunSQL(t, fmt.Sprintf("ALTER TABLE `%s` %s", tt.Name, stmts[0].Alter))

			var cs, coll sql.NullString
			require.NoError(t, tt.DB.QueryRowContext(t.Context(),
				"SELECT character_set_name, collation_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = 'c'",
				tt.Name).Scan(&cs, &coll))
			require.Equal(t, tc.charset, cs, "restored alter: %s", stmts[0].Alter)
			require.Equal(t, tc.collation, coll, "restored alter: %s", stmts[0].Alter)
			if !tc.charset.Valid {
				require.Contains(t, showCreateTable(t, tt.DB, tt.Name), "CHARACTER SET binary COLLATE binary")
			}
		})
	}
}

// TestDiffIntegrationEnumSetBinaryCharset verifies that an ENUM or SET column
// declared with the binary charset (CHARACTER SET binary or BYTE) matches its
// live form, which MySQL reports as CHARACTER SET binary COLLATE binary.
// Without the binary collation on the declared side, the diff emits a MODIFY
// that MySQL stores as the same column again, on every run.
func TestDiffIntegrationEnumSetBinaryCharset(t *testing.T) {
	ddl := "CREATE TABLE diff_enum_set_binary (id int NOT NULL, a enum('x','y') CHARACTER SET binary, b set('x','y') CHARACTER SET binary DEFAULT 'x', c enum('x','y') BYTE, d set('x','y') BYTE, e enum('x','y') CHARACTER SET binary COLLATE binary, PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	tt := testutils.NewTestTable(t, "diff_enum_set_binary", ddl)
	desired, err := ParseCreateTable(ddl)
	require.NoError(t, err)
	live, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	stmts, err := live.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts, "an enum/set column with the binary charset must match its live form")
	stmts, err = desired.Diff(live, nil)
	require.NoError(t, err)
	require.Nil(t, stmts, "the live form must match an enum/set column with the binary charset")
}

// TestDiffIntegrationEnumSetBareUTF8MB4ConvergesThroughRestore applies the
// diff's MODIFY through the parser-restored alter the runner executes, and
// requires the re-diff to be empty. A declared ENUM/SET naming CHARACTER SET
// utf8mb4 without a COLLATE takes the server default, so against a column
// inheriting a utf8mb4_bin or latin1 table default the diff emits a MODIFY. If
// the restore drops the charset, the MODIFY is a no-op and the plan never
// converges.
func TestDiffIntegrationEnumSetBareUTF8MB4ConvergesThroughRestore(t *testing.T) {
	for i, tc := range []struct{ tableDefault, colType string }{
		{"DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin", "enum('x','y')"},
		{"DEFAULT CHARSET=latin1", "set('x','y')"},
	} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			tbl := fmt.Sprintf("enum_set_bare_utf8mb4_%d", i)
			tt := testutils.NewTestTable(t, tbl, fmt.Sprintf("CREATE TABLE %s (id int NOT NULL PRIMARY KEY, c %s) %s", tbl, tc.colType, tc.tableDefault))
			desired, err := ParseCreateTable(fmt.Sprintf("CREATE TABLE %s (id int NOT NULL PRIMARY KEY, c %s CHARACTER SET utf8mb4) %s", tbl, tc.colType, tc.tableDefault))
			require.NoError(t, err)

			live, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
			require.NoError(t, err)
			plan, err := live.Diff(desired, nil)
			require.NoError(t, err)
			require.Len(t, plan, 1)
			stmts, err := New(plan[0].Statement)
			require.NoError(t, err)
			testutils.RunSQL(t, fmt.Sprintf("ALTER TABLE `%s` %s", tt.Name, stmts[0].Alter))

			live, err = ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
			require.NoError(t, err)
			plan, err = live.Diff(desired, nil)
			require.NoError(t, err)
			require.Nil(t, plan, "restored alter %q did not converge", stmts[0].Alter)
		})
	}
}

// A TEXT(M) or BLOB(M) column is stored as the smallest type that holds M
// bytes, so a table created from those declarations must diff clean against
// them. Without textBlobLengthNormalizer the diff emits `MODIFY COLUMN ...
// text` (or blob) against the live tinytext: a real type change on a table
// created exactly as declared.
func TestDiffIntegrationTextBlobLengthCreatedAsDeclared(t *testing.T) {
	const declaredSQL = "CREATE TABLE diff_text_blob_length (" +
		"id int NOT NULL, " +
		"a text(0), " +
		"b blob(0), " +
		"c blob(100), " +
		"d text(63), " +
		"e text(100) CHARACTER SET latin1, " +
		"f text(16384), " +
		"g blob(70000), " +
		"PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

	tt := testutils.NewTestTable(t, "diff_text_blob_length", declaredSQL)

	live := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, live, "`a` tinytext,")
	require.Contains(t, live, "`b` tinyblob,")
	require.Contains(t, live, "`c` tinyblob,")
	require.Contains(t, live, "`d` tinytext,")
	require.Contains(t, live, "`e` tinytext CHARACTER SET latin1")
	require.Contains(t, live, "`f` mediumtext,")
	require.Contains(t, live, "`g` mediumblob,")

	stmts := diffLiveTable(t, tt.DB, tt.Name, declaredSQL)
	require.Nil(t, stmts)
}

// Changing a column to TEXT(M) or BLOB(M) is a real change: the diff is
// emitted with the resolved type, MySQL stores it, and a re-diff is clean.
func TestDiffIntegrationTextBlobLengthConverges(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_text_blob_length_change",
		"CREATE TABLE diff_text_blob_length_change (id int NOT NULL, a varchar(10), b varbinary(10), c text, "+
			"PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")

	const targetSQL = "CREATE TABLE diff_text_blob_length_change (id int NOT NULL, a text(0), b blob(100), c text(20000), " +
		"PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

	stmts := diffLiveTable(t, tt.DB, tt.Name, targetSQL)
	require.Len(t, stmts, 1)

	execStatements(t, tt.DB, stmts)
	live := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, live, "`a` tinytext,")
	require.Contains(t, live, "`b` tinyblob,")
	require.Contains(t, live, "`c` mediumtext,")

	requireConverged(t, tt.DB, tt.Name, targetSQL)
}

// A TEXT(M) in a table that names no charset takes its size from the charset
// the column gets, which only the live table determines. The diff compares it
// at the live column's charset and, where the types differ, emits text(M) so
// MySQL resolves the size: a plain `text` would narrow the utf8mb4 column `d`
// (text(20000) is mediumtext there) and widen the latin1 column `b` (text(64)
// is tinytext there).
func TestDiffIntegrationTextBlobLengthUndeterminedCharset(t *testing.T) {
	t.Run("utf8mb4", func(t *testing.T) {
		tt := testutils.NewTestTable(t, "diff_text_length_utf8mb4",
			"CREATE TABLE diff_text_length_utf8mb4 (id int NOT NULL, a varchar(10), b text, c mediumtext, d text, "+
				"PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")

		const targetSQL = "CREATE TABLE diff_text_length_utf8mb4 (id int NOT NULL, " +
			"a text(20000), b text(100), c text(20000), d text(20000), PRIMARY KEY (id))"

		stmts := diffLiveTable(t, tt.DB, tt.Name, targetSQL)
		require.Len(t, stmts, 1)
		require.Contains(t, stmts[0].Statement, "MODIFY COLUMN `a` text(20000) NULL")
		require.Contains(t, stmts[0].Statement, "MODIFY COLUMN `d` text(20000) NULL")
		require.NotContains(t, stmts[0].Statement, "`b`")
		require.NotContains(t, stmts[0].Statement, "`c`")

		execStatements(t, tt.DB, stmts)
		live := showCreateTable(t, tt.DB, tt.Name)
		require.Contains(t, live, "`a` mediumtext,")
		require.Contains(t, live, "`b` text,")
		require.Contains(t, live, "`c` mediumtext,")
		require.Contains(t, live, "`d` mediumtext,")

		requireConverged(t, tt.DB, tt.Name, targetSQL)
	})
	t.Run("latin1", func(t *testing.T) {
		tt := testutils.NewTestTable(t, "diff_text_length_latin1",
			"CREATE TABLE diff_text_length_latin1 (id int NOT NULL, a varchar(10), b text, c tinytext, "+
				"PRIMARY KEY (id)) DEFAULT CHARSET=latin1")

		const targetSQL = "CREATE TABLE diff_text_length_latin1 (id int NOT NULL, " +
			"a text(64), b text(64), c text(64), PRIMARY KEY (id))"

		stmts := diffLiveTable(t, tt.DB, tt.Name, targetSQL)
		require.Len(t, stmts, 1)
		require.Contains(t, stmts[0].Statement, "MODIFY COLUMN `a` text(64) NULL")
		require.Contains(t, stmts[0].Statement, "MODIFY COLUMN `b` text(64) NULL")
		require.NotContains(t, stmts[0].Statement, "`c`")

		execStatements(t, tt.DB, stmts)
		live := showCreateTable(t, tt.DB, tt.Name)
		require.Contains(t, live, "`a` tinytext,")
		require.Contains(t, live, "`b` tinytext,")
		require.Contains(t, live, "`c` tinytext,")

		requireConverged(t, tt.DB, tt.Name, targetSQL)
	})
	// IgnoreCharsetCollation does not change the charset a MODIFY stores the
	// column at, so the column is still compared at the live table's charset:
	// text(20000) is text on latin1, whether or not a comment also differs.
	t.Run("latin1 IgnoreCharsetCollation", func(t *testing.T) {
		tt := testutils.NewTestTable(t, "diff_text_length_ignore_cs",
			"CREATE TABLE diff_text_length_ignore_cs (id int NOT NULL, a mediumtext, b text, c mediumtext COMMENT 'x', "+
				"PRIMARY KEY (id)) DEFAULT CHARSET=latin1")

		const targetSQL = "CREATE TABLE diff_text_length_ignore_cs (id int NOT NULL, " +
			"a text(20000), b text(20000), c text(20000) COMMENT 'y', PRIMARY KEY (id))"
		opts := NewDiffOptions()
		opts.IgnoreCharsetCollation = true
		diff := func() []*AbstractStatement {
			source, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
			require.NoError(t, err)
			target, err := ParseCreateTable(targetSQL)
			require.NoError(t, err)
			stmts, err := source.Diff(target, opts)
			require.NoError(t, err)
			return stmts
		}

		stmts := diff()
		require.Len(t, stmts, 1)
		require.Contains(t, stmts[0].Statement, "MODIFY COLUMN `a` text(20000) NULL")
		require.Contains(t, stmts[0].Statement, "MODIFY COLUMN `c` text(20000) NULL COMMENT 'y'")
		require.NotContains(t, stmts[0].Statement, "`b`")

		execStatements(t, tt.DB, stmts)
		live := showCreateTable(t, tt.DB, tt.Name)
		require.Contains(t, live, "`a` text,")
		require.Contains(t, live, "`b` text,")
		require.Contains(t, live, "`c` text COMMENT 'y',")

		require.Nil(t, diff())
	})
}

// binaryDefaultHexReason is why the binary and utf8mb4 default tests that read
// back a non-utf8mb3 default skip before MySQL 8.0.33: earlier servers' SHOW
// CREATE TABLE replaces each such byte of a binary default, or character of a
// utf8mb4 one, with '?' instead of reporting the default as a hex literal
// (MySQL Bug #104840).
const binaryDefaultHexReason = "SHOW CREATE TABLE reports a non-utf8mb3 default as '?' rather than hex"

// TestDiffIntegrationBinaryDefaultBytes verifies that a literal default on a
// binary(N) column matches its live form, which MySQL stores NUL-padded to the
// column width (binary(3) DEFAULT 'a' is reported as DEFAULT 'a\0\0'), or as a
// hex literal when the padded bytes are not valid utf8mb3. A char(N) column
// that the binary charset makes binary(N) is padded the same way. A varbinary
// default is stored as the same bytes, unpadded (varbinary(4) DEFAULT x'61' is
// reported as DEFAULT 'a'). Without the conversion the diff emits a MODIFY that
// MySQL rewrites to its own form again, on every run.
//
// The hex form is only reported from MySQL 8.0.33. Before that, SHOW CREATE
// TABLE replaces each byte that is not valid utf8mb3 with '?', so the stored
// default cannot be read back and the column cannot converge. The cases in
// needsHexReporting skip on those servers.
func TestDiffIntegrationBinaryDefaultBytes(t *testing.T) {
	needsHexReporting := map[string]bool{
		"diff_binpad_hex_not_utf8": true,
		"diff_binpad_4byte_char":   true,
		"diff_varbin_hex_not_utf8": true,
	}
	for _, tc := range []struct{ name, ddl, live string }{
		{"diff_binpad_string", "CREATE TABLE diff_binpad_string (id int NOT NULL, b binary(3) DEFAULT 'a', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` binary(3) DEFAULT 'a\\0\\0'"},
		{"diff_binpad_empty", "CREATE TABLE diff_binpad_empty (id int NOT NULL, b binary(3) NOT NULL DEFAULT '', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` binary(3) NOT NULL DEFAULT '\\0\\0\\0'"},
		{"diff_binpad_no_width", "CREATE TABLE diff_binpad_no_width (id int NOT NULL, b binary DEFAULT '', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` binary(1) DEFAULT '\\0'"},
		{"diff_binpad_zero_width", "CREATE TABLE diff_binpad_zero_width (id int NOT NULL, b binary(0) DEFAULT '', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` binary(0) DEFAULT ''"},
		{"diff_binpad_full", "CREATE TABLE diff_binpad_full (id int NOT NULL, b binary(3) DEFAULT 'abc', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` binary(3) DEFAULT 'abc'"},
		{"diff_binpad_nul", "CREATE TABLE diff_binpad_nul (id int NOT NULL, b binary(3) DEFAULT 'a\\0', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` binary(3) DEFAULT 'a\\0\\0'"},
		{"diff_binpad_multibyte", "CREATE TABLE diff_binpad_multibyte (id int NOT NULL, b binary(3) DEFAULT 'é', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` binary(3) DEFAULT 'é\\0'"},
		{"diff_binpad_true", "CREATE TABLE diff_binpad_true (id int NOT NULL, b binary(4) NOT NULL DEFAULT TRUE, PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` binary(4) NOT NULL DEFAULT '1\\0\\0\\0'"},
		{"diff_binpad_false", "CREATE TABLE diff_binpad_false (id int NOT NULL, b binary(4) DEFAULT FALSE, PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` binary(4) DEFAULT '0\\0\\0\\0'"},
		{"diff_binpad_int", "CREATE TABLE diff_binpad_int (id int NOT NULL, b binary(3) DEFAULT -1, PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` binary(3) DEFAULT '-1\\0'"},
		{"diff_binpad_big_int", "CREATE TABLE diff_binpad_big_int (id int NOT NULL, b binary(22) DEFAULT 18446744073709551616, PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` binary(22) DEFAULT '18446744073709551616\\0\\0'"},
		{"diff_binpad_hex", "CREATE TABLE diff_binpad_hex (id int NOT NULL, b binary(3) DEFAULT 0x61, PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` binary(3) DEFAULT 'a\\0\\0'"},
		{"diff_binpad_bit", "CREATE TABLE diff_binpad_bit (id int NOT NULL, b binary(3) DEFAULT b'0000000001100001', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` binary(3) DEFAULT '\\0a\\0'"},
		{"diff_binpad_repeated_default", "CREATE TABLE diff_binpad_repeated_default (id int NOT NULL, b binary(3) DEFAULT b'01100010' DEFAULT b'01100001', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` binary(3) DEFAULT 'a\\0\\0'"},
		{"diff_binpad_hex_not_utf8", "CREATE TABLE diff_binpad_hex_not_utf8 (id int NOT NULL, b binary(3) DEFAULT x'ff', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` binary(3) DEFAULT 0xFF0000"},
		{"diff_binpad_4byte_char", "CREATE TABLE diff_binpad_4byte_char (id int NOT NULL, b binary(5) DEFAULT '😀', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` binary(5) DEFAULT 0xF09F988000"},
		{"diff_binpad_char_binary_table", "CREATE TABLE diff_binpad_char_binary_table (id int NOT NULL, b char(3) DEFAULT 'a', t char(3) DEFAULT TRUE, PRIMARY KEY (id)) DEFAULT CHARSET=binary", "`b` binary(3) DEFAULT 'a\\0\\0'"},
		{"diff_binpad_char_collate_binary", "CREATE TABLE diff_binpad_char_collate_binary (id int NOT NULL, b char(3) COLLATE binary DEFAULT 'a', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` binary(3) DEFAULT 'a\\0\\0'"},
		{"diff_varbin_hex", "CREATE TABLE diff_varbin_hex (id int NOT NULL, b varbinary(4) DEFAULT x'61', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` varbinary(4) DEFAULT 'a'"},
		{"diff_varbin_bit", "CREATE TABLE diff_varbin_bit (id int NOT NULL, b varbinary(4) DEFAULT b'0000000001100001', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` varbinary(4) DEFAULT '\\0a'"},
		{"diff_varbin_int", "CREATE TABLE diff_varbin_int (id int NOT NULL, b varbinary(4) DEFAULT 007, PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` varbinary(4) DEFAULT '7'"},
		{"diff_varbin_hex_not_utf8", "CREATE TABLE diff_varbin_hex_not_utf8 (id int NOT NULL, b varbinary(4) DEFAULT x'ff', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` varbinary(4) DEFAULT 0xFF"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if needsHexReporting[tc.name] {
				testutils.SkipBeforeMySQLVersion(t, "8.0.33", binaryDefaultHexReason)
			}
			tt := testutils.NewTestTable(t, tc.name, tc.ddl)
			liveSQL := showCreateTable(t, tt.DB, tt.Name)
			require.Contains(t, liveSQL, tc.live, "the reading this case pins")
			desired, err := ParseCreateTable(tc.ddl)
			require.NoError(t, err)
			live, err := ParseCreateTable(liveSQL)
			require.NoError(t, err)
			stmts, err := live.Diff(desired, nil)
			require.NoError(t, err)
			require.Nil(t, stmts, "a binary default must match its live form")
			stmts, err = desired.Diff(live, nil)
			require.NoError(t, err)
			require.Nil(t, stmts, "the live form must match the binary default")
		})
	}
}

// TestDiffIntegrationBinaryDefaultBytesConverges verifies that the MODIFY
// emitted for a binary(N) or varbinary(N) default round-trips: MySQL applies it
// and stores the value it carries (NULs escaped inside a string, or a bare hex literal),
// after which a re-diff converges to nil.
func TestDiffIntegrationBinaryDefaultBytesConverges(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_binpad_converge",
		"CREATE TABLE diff_binpad_converge (id int NOT NULL, a binary(3), k binary(4) NOT NULL, w binary(3) DEFAULT 'a', v varbinary(4), PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	desired, err := ParseCreateTable(
		"CREATE TABLE diff_binpad_converge (id int NOT NULL, a binary(3) DEFAULT 'a', k binary(4) NOT NULL DEFAULT TRUE, w binary(4) DEFAULT 'a', v varbinary(4) DEFAULT x'61', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)

	live, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	stmts, err := live.Diff(desired, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Contains(t, stmts[0].Statement, "DEFAULT 'a\\0\\0'")
	testutils.RunSQL(t, stmts[0].Statement)

	liveSQL := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, liveSQL, "`a` binary(3) DEFAULT 'a\\0\\0'")
	require.Contains(t, liveSQL, "`k` binary(4) NOT NULL DEFAULT '1\\0\\0\\0'")
	require.Contains(t, liveSQL, "`w` binary(4) DEFAULT 'a\\0\\0\\0'")
	require.Contains(t, liveSQL, "`v` varbinary(4) DEFAULT 'a'")

	live, err = ParseCreateTable(liveSQL)
	require.NoError(t, err)
	stmts, err = live.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts, "re-diff after applying the ALTER must converge")
}

// TestDiffIntegrationBinaryDefaultBytesHexConverges verifies that a binary or
// varbinary default that is not valid utf8mb3 is emitted as a bare hex literal MySQL
// stores unchanged, after which a re-diff converges to nil. It needs MySQL
// 8.0.33+, which reports such a default as hex (see binaryDefaultHexReason).
func TestDiffIntegrationBinaryDefaultBytesHexConverges(t *testing.T) {
	testutils.SkipBeforeMySQLVersion(t, "8.0.33", binaryDefaultHexReason)
	tt := testutils.NewTestTable(t, "diff_binpad_hex_converge",
		"CREATE TABLE diff_binpad_hex_converge (id int NOT NULL, h binary(3), v varbinary(4), PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	desired, err := ParseCreateTable(
		"CREATE TABLE diff_binpad_hex_converge (id int NOT NULL, h binary(3) DEFAULT x'ff', v varbinary(4) DEFAULT x'ff', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)

	live, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	stmts, err := live.Diff(desired, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Contains(t, stmts[0].Statement, "`h` binary(3) NULL DEFAULT x'ff0000'")
	require.Contains(t, stmts[0].Statement, "`v` varbinary(4) NULL DEFAULT x'ff'")
	testutils.RunSQL(t, stmts[0].Statement)

	var stored string
	testutils.RunSQL(t, "INSERT INTO diff_binpad_hex_converge (id) VALUES (1)")
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT CONCAT(HEX(h), '/', HEX(v)) FROM diff_binpad_hex_converge WHERE id = 1").Scan(&stored))
	require.Equal(t, "FF0000/FF", stored)

	liveSQL := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, liveSQL, "`h` binary(3) DEFAULT 0xFF0000")
	require.Contains(t, liveSQL, "`v` varbinary(4) DEFAULT 0xFF")
	live, err = ParseCreateTable(liveSQL)
	require.NoError(t, err)
	stmts, err = live.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts, "re-diff after applying the ALTER must converge")
}

// TestDiffIntegrationZeroWidthConverges verifies that the ALTER Diff emits for
// zero-width columns applies on a real MySQL server and converges. varchar(0)
// and varbinary(0) have no width-less spelling (a bare varchar is invalid SQL),
// char(0) and binary(0) must not be emitted as char/binary (which are width 1),
// and a change to or from a zero width must still be reported. The int(0)
// zerofill and decimal(0) columns are rewritten by MySQL to their default
// widths, so they must not diff against the live table at all.
func TestDiffIntegrationZeroWidthConverges(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_zero_width",
		"CREATE TABLE diff_zero_width (id int NOT NULL, v varchar(0), w varchar(0), c char(0), bn binary(1), vb varbinary(1), z int(10) unsigned zerofill, d decimal(10,0), PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	const declaredSQL = "CREATE TABLE diff_zero_width (id int NOT NULL, v varchar(0) DEFAULT '', w varchar(1), c char(0) DEFAULT '', bn binary(0), vb varbinary(0) DEFAULT '', z int(0) zerofill, d decimal(0), PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

	stmts := diffLiveTable(t, tt.DB, tt.Name, declaredSQL)
	require.Len(t, stmts, 1)
	for _, want := range []string{
		"MODIFY COLUMN `v` varchar(0) NULL DEFAULT ''",
		"MODIFY COLUMN `w` varchar(1) NULL",
		"MODIFY COLUMN `c` char(0) NULL DEFAULT ''",
		"MODIFY COLUMN `bn` binary(0) NULL",
		"MODIFY COLUMN `vb` varbinary(0) NULL DEFAULT ''",
	} {
		require.Contains(t, stmts[0].Statement, want)
	}
	require.NotContains(t, stmts[0].Statement, "`z`")
	require.NotContains(t, stmts[0].Statement, "`d`")
	execStatements(t, tt.DB, stmts)

	liveSQL := showCreateTable(t, tt.DB, tt.Name)
	for _, want := range []string{
		"`v` varchar(0) DEFAULT ''",
		"`w` varchar(1) DEFAULT NULL",
		"`c` char(0) DEFAULT ''",
		"`bn` binary(0) DEFAULT NULL",
		"`vb` varbinary(0) DEFAULT ''",
	} {
		require.Contains(t, liveSQL, want)
	}
	requireConverged(t, tt.DB, tt.Name, declaredSQL)
}

// TestDiffIntegrationZeroWidthCreatedAsDeclared verifies that a table created
// from a schema with zero-width columns diffs clean against that schema: each
// column is parsed to the same width MySQL stores for it.
func TestDiffIntegrationZeroWidthCreatedAsDeclared(t *testing.T) {
	const declaredSQL = "CREATE TABLE diff_zero_width_created (id int NOT NULL, v varchar(0), c char(0), bn binary(0), vb varbinary(0), z int(0) zerofill, t tinyint(0) zerofill, i int(0), d decimal(0), u decimal(0,0) unsigned, PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	tt := testutils.NewTestTable(t, "diff_zero_width_created", declaredSQL)

	liveSQL := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, liveSQL, "`z` int(10) unsigned zerofill")
	require.Contains(t, liveSQL, "`t` tinyint(3) unsigned zerofill")
	require.Contains(t, liveSQL, "`d` decimal(10,0)")
	requireConverged(t, tt.DB, tt.Name, declaredSQL)
	requireNoSelfDiff(t, tt.DB, tt.Name)
}

// TestDiffIntegrationBinaryLiteralDefaults verifies that a hex or bit literal
// default on an integer, bit, char or varchar column matches its live form.
// MySQL converts the literal to the column's own type and reports it in that
// form: int DEFAULT 0x1A as DEFAULT '26', bit(8) DEFAULT x'61' as
// DEFAULT b'1100001', and varchar(4) DEFAULT 0x61 as DEFAULT 'a'. An integer
// default on a bit column is converted the same way (bit(1) DEFAULT 0 is
// reported as DEFAULT b'0'). Without the conversion the diff emits a MODIFY
// that MySQL rewrites to its own form again, on every run.
//
// A char default that is not valid utf8mb3 is reported as a hex literal only
// from MySQL 8.0.33 (see binaryDefaultHexReason), so that case skips before it.
func TestDiffIntegrationBinaryLiteralDefaults(t *testing.T) {
	needsHexReporting := map[string]bool{"diff_binlit_char_4byte": true}
	for _, tc := range []struct{ name, column, live string }{
		{"diff_binlit_int_hex", "b int DEFAULT 0x1A", "`b` int DEFAULT '26'"},
		{"diff_binlit_int_bit", "b int DEFAULT b'1010'", "`b` int DEFAULT '10'"},
		{"diff_binlit_decimal_hex", "b decimal(5) DEFAULT 0x1A", "`b` decimal(5,0) DEFAULT '26'"},
		{"diff_binlit_decimal_max", "b decimal(20,0) DEFAULT 0x7FFFFFFFFFFFFFFF", "`b` decimal(20,0) DEFAULT '9223372036854775807'"},
		{"diff_binlit_int_max", "b bigint unsigned NOT NULL DEFAULT 0xFFFFFFFFFFFFFFFF", "`b` bigint unsigned NOT NULL DEFAULT '18446744073709551615'"},
		{"diff_binlit_bit_hex", "b bit(8) DEFAULT x'61'", "`b` bit(8) DEFAULT b'1100001'"},
		{"diff_binlit_bit_hex_zero", "b bit(8) DEFAULT x'0000'", "`b` bit(8) DEFAULT b'0'"},
		{"diff_binlit_bit_int", "b bit(1) NOT NULL DEFAULT 0", "`b` bit(1) NOT NULL DEFAULT b'0'"},
		{"diff_binlit_bit_no_width", "b bit DEFAULT 1", "`b` bit(1) DEFAULT b'1'"},
		{"diff_binlit_bit_string", "b bit(8) DEFAULT '0'", "`b` bit(8) DEFAULT b'110000'"},
		{"diff_binlit_char_hex", "b char(4) DEFAULT x'61'", "`b` char(4) DEFAULT 'a'"},
		{"diff_binlit_varchar_hex", "b varchar(4) DEFAULT 0x61", "`b` varchar(4) DEFAULT 'a'"},
		{"diff_binlit_varchar_bit", "b varchar(4) DEFAULT b'01100001'", "`b` varchar(4) DEFAULT 'a'"},
		{"diff_binlit_char_multibyte", "b char(4) DEFAULT x'c3a9'", "`b` char(4) DEFAULT 'é'"},
		{"diff_binlit_char_quote", "b char(4) DEFAULT x'27'", "`b` char(4) DEFAULT ''''"},
		{"diff_binlit_char_ascii", "b char(4) CHARACTER SET ascii DEFAULT x'61'", "`b` char(4) CHARACTER SET ascii COLLATE ascii_general_ci DEFAULT 'a'"},
		{"diff_binlit_char_4byte", "b char(4) DEFAULT x'f09f9880'", "`b` char(4) DEFAULT 0xF09F9880"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if needsHexReporting[tc.name] {
				testutils.SkipBeforeMySQLVersion(t, "8.0.33", binaryDefaultHexReason)
			}
			ddl := "CREATE TABLE " + tc.name + " (id int NOT NULL, " + tc.column + ", PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
			tt := testutils.NewTestTable(t, tc.name, ddl)
			liveSQL := showCreateTable(t, tt.DB, tt.Name)
			require.Contains(t, liveSQL, tc.live, "the reading this case pins")
			desired, err := ParseCreateTable(ddl)
			require.NoError(t, err)
			live, err := ParseCreateTable(liveSQL)
			require.NoError(t, err)
			stmts, err := live.Diff(desired, nil)
			require.NoError(t, err)
			require.Nil(t, stmts, "a literal default must match its live form")
			stmts, err = desired.Diff(live, nil)
			require.NoError(t, err)
			require.Nil(t, stmts, "the live form must match the literal default")
		})
	}
}

// TestDiffIntegrationBinaryLiteralDefaultsConverge verifies that the MODIFY
// emitted for a hex or bit literal default on an integer, bit or varchar column
// carries the value in the column's own form, which MySQL applies unchanged,
// after which a re-diff converges to nil.
func TestDiffIntegrationBinaryLiteralDefaultsConverge(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_binlit_converge",
		"CREATE TABLE diff_binlit_converge (id int NOT NULL, i int, b bit(8), f bit(1) NOT NULL, v varchar(4), PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	desired, err := ParseCreateTable(
		"CREATE TABLE diff_binlit_converge (id int NOT NULL, i int DEFAULT 0x1A, b bit(8) DEFAULT x'61', f bit(1) NOT NULL DEFAULT 0, v varchar(4) DEFAULT 0x27, PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)

	live, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	stmts, err := live.Diff(desired, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Contains(t, stmts[0].Statement, "`i` int NULL DEFAULT 26")
	require.Contains(t, stmts[0].Statement, "`b` bit(8) NULL DEFAULT b'1100001'")
	require.Contains(t, stmts[0].Statement, "`f` bit(1) NOT NULL DEFAULT b'0'")
	require.Contains(t, stmts[0].Statement, "`v` varchar(4) NULL DEFAULT '\\''")
	testutils.RunSQL(t, stmts[0].Statement)

	liveSQL := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, liveSQL, "`i` int DEFAULT '26'")
	require.Contains(t, liveSQL, "`b` bit(8) DEFAULT b'1100001'")
	require.Contains(t, liveSQL, "`f` bit(1) NOT NULL DEFAULT b'0'")
	require.Contains(t, liveSQL, "`v` varchar(4) DEFAULT ''''")

	live, err = ParseCreateTable(liveSQL)
	require.NoError(t, err)
	stmts, err = live.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts, "re-diff after applying the ALTER must converge")
}

// TestDiffIntegrationZerofillDefaultWidthConverges verifies that a ZEROFILL
// integer declared without a width resolves to the unsigned default width MySQL
// stores for it (int zerofill is int(10) unsigned zerofill, not the parser's
// signed int(11)). The live table starts on widths other than the unsigned
// default — the signed defaults for tinyint through int, where the two differ,
// and bigint(21) for bigint, whose signed and unsigned defaults are both 20 and
// so converged before this rule — so the diff must move every column to the
// unsigned default, and after the ALTER is applied a re-diff must converge.
func TestDiffIntegrationZerofillDefaultWidthConverges(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_zerofill_width",
		"CREATE TABLE diff_zerofill_width (id int NOT NULL, a int(11) zerofill, b tinyint(4) zerofill, c smallint(6) zerofill, d mediumint(9) zerofill, e bigint(21) zerofill, PRIMARY KEY (id))")
	const declaredSQL = "CREATE TABLE diff_zerofill_width (id int NOT NULL, a int zerofill, b tinyint zerofill, c smallint unsigned zerofill, d mediumint zerofill, e bigint zerofill, PRIMARY KEY (id))"

	stmts := diffLiveTable(t, tt.DB, tt.Name, declaredSQL)
	require.Len(t, stmts, 1)
	for _, want := range []string{
		"`a` int(10) unsigned zerofill",
		"`b` tinyint(3) unsigned zerofill",
		"`c` smallint(5) unsigned zerofill",
		"`d` mediumint(8) unsigned zerofill",
		"`e` bigint(20) unsigned zerofill",
	} {
		require.Contains(t, stmts[0].Statement, want)
	}
	execStatements(t, tt.DB, stmts)

	liveSQL := showCreateTable(t, tt.DB, tt.Name)
	for _, want := range []string{
		"`a` int(10) unsigned zerofill",
		"`b` tinyint(3) unsigned zerofill",
		"`c` smallint(5) unsigned zerofill",
		"`d` mediumint(8) unsigned zerofill",
		"`e` bigint(20) unsigned zerofill",
	} {
		require.Contains(t, liveSQL, want)
	}
	requireConverged(t, tt.DB, tt.Name, declaredSQL)
	requireNoSelfDiff(t, tt.DB, tt.Name)
}

// TestDiffIntegrationZerofillExplicitWidthKept verifies that an explicit
// ZEROFILL width equal to the signed default (int(11) zerofill) is kept: MySQL
// stores it as written, so a table created from that schema diffs clean
// against it, and against the width-less spelling it does not.
func TestDiffIntegrationZerofillExplicitWidthKept(t *testing.T) {
	const declaredSQL = "CREATE TABLE diff_zerofill_explicit (id int NOT NULL, a int(11) zerofill, b tinyint(4) zerofill, PRIMARY KEY (id))"
	tt := testutils.NewTestTable(t, "diff_zerofill_explicit", declaredSQL)

	liveSQL := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, liveSQL, "`a` int(11) unsigned zerofill")
	require.Contains(t, liveSQL, "`b` tinyint(4) unsigned zerofill")
	requireConverged(t, tt.DB, tt.Name, declaredSQL)
	requireNoSelfDiff(t, tt.DB, tt.Name)

	stmts := diffLiveTable(t, tt.DB, tt.Name, "CREATE TABLE diff_zerofill_explicit (id int NOT NULL, a int zerofill, b tinyint zerofill, PRIMARY KEY (id))")
	require.Len(t, stmts, 1)
	require.Contains(t, stmts[0].Statement, "`a` int(10) unsigned zerofill")
	require.Contains(t, stmts[0].Statement, "`b` tinyint(3) unsigned zerofill")
}

// TestDiffIntegrationCharUTF8MB4Default verifies that a string default on a
// utf8mb4 char or varchar column that is not valid utf8mb3 matches its live
// form, which SHOW CREATE TABLE reports as a hex literal (char(4) DEFAULT '😀'
// is reported as DEFAULT 0xF09F9880). Without the conversion the diff emits a
// MODIFY that MySQL reports as hex again, on every run. It needs MySQL
// 8.0.33+, which reports such a default as hex (see binaryDefaultHexReason).
func TestDiffIntegrationCharUTF8MB4Default(t *testing.T) {
	testutils.SkipBeforeMySQLVersion(t, "8.0.33", binaryDefaultHexReason)
	for _, tc := range []struct{ name, ddl, live string }{
		{"diff_mb4def_char", "CREATE TABLE diff_mb4def_char (id int NOT NULL, b char(4) DEFAULT '😀', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` char(4) DEFAULT 0xF09F9880"},
		{"diff_mb4def_varchar", "CREATE TABLE diff_mb4def_varchar (id int NOT NULL, b varchar(4) NOT NULL DEFAULT 'a😀', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` varchar(4) NOT NULL DEFAULT 0x61F09F9880"},
		{"diff_mb4def_char_spaces", "CREATE TABLE diff_mb4def_char_spaces (id int NOT NULL, b char(4) DEFAULT '😀 ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` char(4) DEFAULT 0xF09F9880"},
		{"diff_mb4def_varchar_spaces", "CREATE TABLE diff_mb4def_varchar_spaces (id int NOT NULL, b varchar(4) DEFAULT '😀 ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` varchar(4) DEFAULT 0xF09F988020"},
		{"diff_mb4def_escapes", "CREATE TABLE diff_mb4def_escapes (id int NOT NULL, b varchar(4) DEFAULT '''\\\\😀', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` varchar(4) DEFAULT 0x275CF09F9880"},
		{"diff_mb4def_introducer", "CREATE TABLE diff_mb4def_introducer (id int NOT NULL, b char(4) DEFAULT _utf8mb4'😀', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` char(4) DEFAULT 0xF09F9880"},
		{"diff_mb4def_binary_introducer", "CREATE TABLE diff_mb4def_binary_introducer (id int NOT NULL, b char(4) DEFAULT _binary'😀', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` char(4) DEFAULT 0xF09F9880"},
		{"diff_mb4def_hex_spaces", "CREATE TABLE diff_mb4def_hex_spaces (id int NOT NULL, b char(4) DEFAULT x'f09f988020', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` char(4) DEFAULT 0xF09F9880"},
		{"diff_mb4def_bit", "CREATE TABLE diff_mb4def_bit (id int NOT NULL, b char(4) DEFAULT b'11110000100111111001100010000000', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` char(4) DEFAULT 0xF09F9880"},
		{"diff_mb4def_varchar_overflow_spaces", "CREATE TABLE diff_mb4def_varchar_overflow_spaces (id int NOT NULL, b varchar(1) DEFAULT '😀 ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` varchar(1) DEFAULT 0xF09F9880"},
		{"diff_mb4def_column_charset", "CREATE TABLE diff_mb4def_column_charset (id int NOT NULL, b char(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT '😀', PRIMARY KEY (id)) DEFAULT CHARSET=latin1", "`b` char(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT 0xF09F9880"},
		{"diff_mb4def_utf8mb3_string", "CREATE TABLE diff_mb4def_utf8mb3_string (id int NOT NULL, b char(4) DEFAULT 'é', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` char(4) DEFAULT 'é'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tt := testutils.NewTestTable(t, tc.name, tc.ddl)
			liveSQL := showCreateTable(t, tt.DB, tt.Name)
			require.Contains(t, liveSQL, tc.live, "the reading this case pins")
			desired, err := ParseCreateTable(tc.ddl)
			require.NoError(t, err)
			live, err := ParseCreateTable(liveSQL)
			require.NoError(t, err)
			stmts, err := live.Diff(desired, nil)
			require.NoError(t, err)
			require.Nil(t, stmts, "a utf8mb4 default must match its live form")
			stmts, err = desired.Diff(live, nil)
			require.NoError(t, err)
			require.Nil(t, stmts, "the live form must match the utf8mb4 default")
		})
	}
}

// TestDiffIntegrationCharUTF8MB4DefaultConverges verifies that the MODIFY
// emitted for a utf8mb4 default that is not valid utf8mb3 carries a hex
// literal with a _utf8mb4 introducer, that MySQL applies it and stores the
// intended character, and that a re-diff then converges to nil.
func TestDiffIntegrationCharUTF8MB4DefaultConverges(t *testing.T) {
	testutils.SkipBeforeMySQLVersion(t, "8.0.33", binaryDefaultHexReason)
	tt := testutils.NewTestTable(t, "diff_mb4def_converge",
		"CREATE TABLE diff_mb4def_converge (id int NOT NULL, c char(4), v varchar(4) DEFAULT 'a', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	desired, err := ParseCreateTable(
		"CREATE TABLE diff_mb4def_converge (id int NOT NULL, c char(4) DEFAULT '😀', v varchar(4) DEFAULT 'a😀', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)

	live, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	stmts, err := live.Diff(desired, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Contains(t, stmts[0].Statement, "`c` char(4) NULL DEFAULT _utf8mb4 x'f09f9880'")
	require.Contains(t, stmts[0].Statement, "`v` varchar(4) NULL DEFAULT _utf8mb4 x'61f09f9880'")
	testutils.RunSQL(t, stmts[0].Statement)

	var matches bool
	testutils.RunSQL(t, "INSERT INTO diff_mb4def_converge (id) VALUES (1)")
	require.NoError(t, tt.DB.QueryRowContext(t.Context(),
		"SELECT c = _utf8mb4 X'F09F9880' AND HEX(c) = 'F09F9880' AND CHAR_LENGTH(c) = 1 AND HEX(v) = '61F09F9880' AND CHAR_LENGTH(v) = 2 FROM diff_mb4def_converge WHERE id = 1").Scan(&matches))
	require.True(t, matches, "the hex literal must store the character, not its bytes as characters")

	liveSQL := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, liveSQL, "`c` char(4) DEFAULT 0xF09F9880")
	require.Contains(t, liveSQL, "`v` varchar(4) DEFAULT 0x61F09F9880")
	live, err = ParseCreateTable(liveSQL)
	require.NoError(t, err)
	stmts, err = live.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts, "re-diff after applying the ALTER must converge")
}

// TestDiffIntegrationCharUTF8MB4DefaultIgnoreCharsetCollation verifies that
// the MODIFY emitted for a declared utf8mb4 default stores the intended
// character on a live column of another charset, which happens when
// IgnoreCharsetCollation leaves the table charset alone. A bare
// x'f09f9880' would be read as utf16 there, storing U+F09F U+9880; the
// _utf8mb4 introducer makes MySQL convert the character instead.
func TestDiffIntegrationCharUTF8MB4DefaultIgnoreCharsetCollation(t *testing.T) {
	testutils.SkipBeforeMySQLVersion(t, "8.0.33", binaryDefaultHexReason)
	tt := testutils.NewTestTable(t, "diff_mb4def_ignore_charset",
		"CREATE TABLE diff_mb4def_ignore_charset (id int NOT NULL, b char(4), PRIMARY KEY (id)) DEFAULT CHARSET=utf16")
	desired, err := ParseCreateTable(
		"CREATE TABLE diff_mb4def_ignore_charset (id int NOT NULL, b char(4) DEFAULT '😀', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)
	live, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	opts := NewDiffOptions()
	opts.IgnoreCharsetCollation = true
	stmts, err := live.Diff(desired, opts)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	testutils.RunSQL(t, stmts[0].Statement)

	var stored string
	testutils.RunSQL(t, "INSERT INTO diff_mb4def_ignore_charset (id) VALUES (1)")
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT HEX(b) FROM diff_mb4def_ignore_charset WHERE id = 1").Scan(&stored))
	require.Equal(t, "D83DDE00", stored, "the utf16 encoding of the character")
}

// TestDiffIntegrationTinyint1UnsignedConverges verifies that a table created
// from tinyint(1) unsigned diffs clean against that schema: MySQL keeps the
// width only on the signed tinyint(1) and stores tinyint unsigned. Under
// ZEROFILL the width is kept.
func TestDiffIntegrationTinyint1UnsignedConverges(t *testing.T) {
	const declaredSQL = "CREATE TABLE diff_tinyint1_unsigned (id int NOT NULL, a tinyint(1) unsigned, b tinyint(1), c tinyint(1) unsigned zerofill, PRIMARY KEY (id))"
	tt := testutils.NewTestTable(t, "diff_tinyint1_unsigned", declaredSQL)

	liveSQL := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, liveSQL, "`a` tinyint unsigned DEFAULT NULL")
	require.Contains(t, liveSQL, "`b` tinyint(1) DEFAULT NULL")
	require.Contains(t, liveSQL, "`c` tinyint(1) unsigned zerofill DEFAULT NULL")
	requireConverged(t, tt.DB, tt.Name, declaredSQL)
	requireNoSelfDiff(t, tt.DB, tt.Name)
}

// TestDiffIntegrationCharDefaultSpaces verifies that a string default on a
// char(N) column matches its live form, which MySQL reports with every
// trailing space stripped (char(4) DEFAULT 'a  ' is reported as DEFAULT 'a'),
// in every charset and collation, NO PAD collations included. A varchar(N)
// default keeps its trailing spaces, except those past the column's width,
// which MySQL drops (varchar(4) DEFAULT 'ab      ' is reported as 'ab  ').
// A hex or bit literal default is reported as the string its bytes form, with
// the same spaces handling (char(4) DEFAULT x'612020' is reported as 'a').
// Without the conversion the diff emits a MODIFY that MySQL rewrites to its
// own form again, on every run.
func TestDiffIntegrationCharDefaultSpaces(t *testing.T) {
	for _, tc := range []struct{ name, ddl, live string }{
		{"diff_charpad_trailing", "CREATE TABLE diff_charpad_trailing (id int NOT NULL, b char(4) DEFAULT 'a  ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` char(4) DEFAULT 'a'"},
		{"diff_charpad_only_spaces", "CREATE TABLE diff_charpad_only_spaces (id int NOT NULL, b char(4) NOT NULL DEFAULT '    ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` char(4) NOT NULL DEFAULT ''"},
		{"diff_charpad_no_width", "CREATE TABLE diff_charpad_no_width (id int NOT NULL, b char DEFAULT ' ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` char(1) DEFAULT ''"},
		{"diff_charpad_leading", "CREATE TABLE diff_charpad_leading (id int NOT NULL, b char(4) DEFAULT ' a ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` char(4) DEFAULT ' a'"},
		{"diff_charpad_past_width", "CREATE TABLE diff_charpad_past_width (id int NOT NULL, b char(4) DEFAULT 'abcd  ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` char(4) DEFAULT 'abcd'"},
		{"diff_charpad_tab", "CREATE TABLE diff_charpad_tab (id int NOT NULL, b char(4) DEFAULT 'ab\\t  ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` char(4) DEFAULT 'ab\t'"},
		{"diff_charpad_nul", "CREATE TABLE diff_charpad_nul (id int NOT NULL, b char(4) DEFAULT 'a\\0 ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` char(4) DEFAULT 'a\\0'"},
		{"diff_charpad_introducer", "CREATE TABLE diff_charpad_introducer (id int NOT NULL, b char(4) DEFAULT _latin1'a  ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` char(4) DEFAULT 'a'"},
		{"diff_charpad_no_pad", "CREATE TABLE diff_charpad_no_pad (id int NOT NULL, b char(4) COLLATE utf8mb4_0900_bin DEFAULT 'a  ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` char(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin DEFAULT 'a'"},
		{"diff_charpad_utf16", "CREATE TABLE diff_charpad_utf16 (id int NOT NULL, b char(4) CHARACTER SET utf16 DEFAULT 'abcd  ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` char(4) CHARACTER SET utf16 COLLATE utf16_general_ci DEFAULT 'abcd'"},
		{"diff_charpad_latin1_table", "CREATE TABLE diff_charpad_latin1_table (id int NOT NULL, b char(4) DEFAULT 'a  ', PRIMARY KEY (id)) DEFAULT CHARSET=latin1", "`b` char(4) DEFAULT 'a'"},
		{"diff_charpad_hex", "CREATE TABLE diff_charpad_hex (id int NOT NULL, b char(4) DEFAULT x'612020', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` char(4) DEFAULT 'a'"},
		{"diff_charpad_hex_past_width", "CREATE TABLE diff_charpad_hex_past_width (id int NOT NULL, b char(4) DEFAULT 0x61202020202020, PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` char(4) DEFAULT 'a'"},
		{"diff_charpad_bit", "CREATE TABLE diff_charpad_bit (id int NOT NULL, b char(4) DEFAULT b'011000010010000000100000', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` char(4) DEFAULT 'a'"},
		{"diff_varcharpad_hex_past_width", "CREATE TABLE diff_varcharpad_hex_past_width (id int NOT NULL, b varchar(4) DEFAULT x'61202020202020', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` varchar(4) DEFAULT 'a   '"},
		{"diff_varcharpad_keeps", "CREATE TABLE diff_varcharpad_keeps (id int NOT NULL, b varchar(4) DEFAULT 'a  ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` varchar(4) DEFAULT 'a  '"},
		{"diff_varcharpad_past_width", "CREATE TABLE diff_varcharpad_past_width (id int NOT NULL, b varchar(4) DEFAULT 'ab      ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` varchar(4) DEFAULT 'ab  '"},
		{"diff_varcharpad_multibyte", "CREATE TABLE diff_varcharpad_multibyte (id int NOT NULL, b varchar(2) DEFAULT 'é   ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` varchar(2) DEFAULT 'é '"},
		{"diff_varcharpad_latin1", "CREATE TABLE diff_varcharpad_latin1 (id int NOT NULL, b varchar(4) CHARACTER SET latin1 NOT NULL DEFAULT 'ab    ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` varchar(4) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL DEFAULT 'ab  '"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tt := testutils.NewTestTable(t, tc.name, tc.ddl)
			liveSQL := showCreateTable(t, tt.DB, tt.Name)
			require.Contains(t, liveSQL, tc.live, "the reading this case pins")
			desired, err := ParseCreateTable(tc.ddl)
			require.NoError(t, err)
			live, err := ParseCreateTable(liveSQL)
			require.NoError(t, err)
			stmts, err := live.Diff(desired, nil)
			require.NoError(t, err)
			require.Nil(t, stmts, "a char default must match its live form")
			stmts, err = desired.Diff(live, nil)
			require.NoError(t, err)
			require.Nil(t, stmts, "the live form must match the char default")
		})
	}
}

// TestDiffIntegrationCharDefaultSpacesConverges verifies that the MODIFY
// emitted for a char or varchar default with trailing spaces round-trips:
// MySQL applies it and reports the value it carries, after which a re-diff
// converges to nil.
func TestDiffIntegrationCharDefaultSpacesConverges(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_charpad_converge",
		"CREATE TABLE diff_charpad_converge (id int NOT NULL, c char(4), v varchar(4), PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	desired, err := ParseCreateTable(
		"CREATE TABLE diff_charpad_converge (id int NOT NULL, c char(4) DEFAULT 'a  ', v varchar(4) DEFAULT 'ab      ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)

	live, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	stmts, err := live.Diff(desired, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Contains(t, stmts[0].Statement, "`c` char(4) NULL DEFAULT 'a'")
	require.Contains(t, stmts[0].Statement, "`v` varchar(4) NULL DEFAULT 'ab  '")
	testutils.RunSQL(t, stmts[0].Statement)

	var stored string
	testutils.RunSQL(t, "INSERT INTO diff_charpad_converge (id) VALUES (1)")
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT CONCAT('[', c, '][', v, ']') FROM diff_charpad_converge WHERE id = 1").Scan(&stored))
	require.Equal(t, "[a][ab  ]", stored)

	liveSQL := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, liveSQL, "`c` char(4) DEFAULT 'a'")
	require.Contains(t, liveSQL, "`v` varchar(4) DEFAULT 'ab  '")
	live, err = ParseCreateTable(liveSQL)
	require.NoError(t, err)
	stmts, err = live.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts, "re-diff after applying the ALTER must converge")
}

// TestDiffIntegrationCharDefaultSpacesPadCharToFullLength verifies that a char
// default read by a session with the PAD_CHAR_TO_FULL_LENGTH sql_mode, whose
// SHOW CREATE TABLE reports it padded to the column's width (DEFAULT 'a   '),
// still matches the declared default. Spirit's own connections never set that
// mode, but a caller of Diff may read the live table through its own.
func TestDiffIntegrationCharDefaultSpacesPadCharToFullLength(t *testing.T) {
	ddl := "CREATE TABLE diff_charpad_full_length (id int NOT NULL, b char(4) DEFAULT 'a  ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	tt := testutils.NewTestTable(t, "diff_charpad_full_length", ddl)

	conn, err := tt.DB.Conn(t.Context())
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	_, err = conn.ExecContext(t.Context(), "SET SESSION sql_mode = CONCAT(@@sql_mode, ',PAD_CHAR_TO_FULL_LENGTH')")
	require.NoError(t, err)
	var name, liveSQL string
	require.NoError(t, conn.QueryRowContext(t.Context(), "SHOW CREATE TABLE diff_charpad_full_length").Scan(&name, &liveSQL))
	require.Contains(t, liveSQL, "`b` char(4) DEFAULT 'a   '", "the reading this test pins")

	desired, err := ParseCreateTable(ddl)
	require.NoError(t, err)
	live, err := ParseCreateTable(liveSQL)
	require.NoError(t, err)
	stmts, err := live.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts, "a padded reading must match the char default")
}

// TestDiffIntegrationEnumSetDefault verifies that a string default on an enum
// or set column matches its live form, which MySQL reports as the member text
// the default names: trailing spaces stripped (enum('a','b') DEFAULT 'b ' is
// reported as DEFAULT 'b'), the member's own case on a _ci collation, and a set
// default's members once each in definition order. Without the conversion the
// diff emits a MODIFY that MySQL rewrites to the member again, on every run.
func TestDiffIntegrationEnumSetDefault(t *testing.T) {
	for _, tc := range []struct{ name, ddl, live string }{
		{"diff_enumdef_trailing", "CREATE TABLE diff_enumdef_trailing (id int NOT NULL, b enum('a','b') DEFAULT 'b ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` enum('a','b') DEFAULT 'b'"},
		{"diff_enumdef_not_null", "CREATE TABLE diff_enumdef_not_null (id int NOT NULL, b enum('a','b') NOT NULL DEFAULT 'b   ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` enum('a','b') NOT NULL DEFAULT 'b'"},
		{"diff_enumdef_member_spaces", "CREATE TABLE diff_enumdef_member_spaces (id int NOT NULL, b enum('a','b ') DEFAULT 'b  ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` enum('a','b') DEFAULT 'b'"},
		{"diff_enumdef_empty_member", "CREATE TABLE diff_enumdef_empty_member (id int NOT NULL, b enum('','b') DEFAULT ' ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` enum('','b') DEFAULT ''"},
		{"diff_enumdef_introducer", "CREATE TABLE diff_enumdef_introducer (id int NOT NULL, b enum('a','b') DEFAULT _latin1'b ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` enum('a','b') DEFAULT 'b'"},
		{"diff_enumdef_no_pad", "CREATE TABLE diff_enumdef_no_pad (id int NOT NULL, b enum('a','b') COLLATE utf8mb4_0900_bin DEFAULT 'b ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` enum('a','b') CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin DEFAULT 'b'"},
		{"diff_enumdef_utf16", "CREATE TABLE diff_enumdef_utf16 (id int NOT NULL, b enum('a','b') CHARACTER SET utf16 DEFAULT 'b ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` enum('a','b') CHARACTER SET utf16 COLLATE utf16_general_ci DEFAULT 'b'"},
		{"diff_enumdef_case", "CREATE TABLE diff_enumdef_case (id int NOT NULL, b enum('a','B') DEFAULT 'b ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` enum('a','B') DEFAULT 'B'"},
		{"diff_enumdef_case_general_ci", "CREATE TABLE diff_enumdef_case_general_ci (id int NOT NULL, b enum('a','B') COLLATE utf8mb4_general_ci DEFAULT 'b', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` enum('a','B') CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT 'B'"},
		{"diff_enumdef_case_latin1", "CREATE TABLE diff_enumdef_case_latin1 (id int NOT NULL, b enum('a','B') DEFAULT 'b', PRIMARY KEY (id)) DEFAULT CHARSET=latin1", "`b` enum('a','B') DEFAULT 'B'"},
		{"diff_enumdef_case_non_ascii", "CREATE TABLE diff_enumdef_case_non_ascii (id int NOT NULL, b enum('a','Bé') DEFAULT 'bé ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` enum('a','Bé') DEFAULT 'Bé'"},
		{"diff_setdef_trailing", "CREATE TABLE diff_setdef_trailing (id int NOT NULL, b set('a','b') DEFAULT 'b ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` set('a','b') DEFAULT 'b'"},
		{"diff_setdef_several", "CREATE TABLE diff_setdef_several (id int NOT NULL, b set('a','b') DEFAULT 'a,b ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` set('a','b') DEFAULT 'a,b'"},
		{"diff_setdef_order", "CREATE TABLE diff_setdef_order (id int NOT NULL, b set('a','b','c') DEFAULT 'c,A,b,a ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` set('a','b','c') DEFAULT 'a,b,c'"},
		{"diff_enumdef_binary_attr", "CREATE TABLE diff_enumdef_binary_attr (id int NOT NULL, b enum('a','B') BINARY DEFAULT 'B ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` enum('a','B') CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT 'B'"},
		{"diff_setdef_duplicate", "CREATE TABLE diff_setdef_duplicate (id int NOT NULL, b set('a','b') DEFAULT 'a,a', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci", "`b` set('a','b') DEFAULT 'a'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tt := testutils.NewTestTable(t, tc.name, tc.ddl)
			liveSQL := showCreateTable(t, tt.DB, tt.Name)
			require.Contains(t, liveSQL, tc.live, "the reading this case pins")
			desired, err := ParseCreateTable(tc.ddl)
			require.NoError(t, err)
			live, err := ParseCreateTable(liveSQL)
			require.NoError(t, err)
			stmts, err := live.Diff(desired, nil)
			require.NoError(t, err)
			require.Nil(t, stmts, "an enum or set default must match its live form")
			stmts, err = desired.Diff(live, nil)
			require.NoError(t, err)
			require.Nil(t, stmts, "the live form must match the enum or set default")
		})
	}
}

// TestDiffIntegrationEnumSetDefaultConverges verifies that the MODIFY emitted
// for an enum or set default written with trailing spaces, another case or out
// of order round-trips: MySQL applies it and reports the member text it
// carries, after which a re-diff converges to nil.
func TestDiffIntegrationEnumSetDefaultConverges(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_enumdef_converge",
		"CREATE TABLE diff_enumdef_converge (id int NOT NULL, e enum('a','B'), s set('a','b','c'), PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	desired, err := ParseCreateTable(
		"CREATE TABLE diff_enumdef_converge (id int NOT NULL, e enum('a','B') DEFAULT 'b ', s set('a','b','c') DEFAULT 'c,a ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)

	live, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	stmts, err := live.Diff(desired, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Contains(t, stmts[0].Statement, "`e` enum('a','B') NULL DEFAULT 'B'")
	require.Contains(t, stmts[0].Statement, "`s` set('a','b','c') NULL DEFAULT 'a,c'")
	testutils.RunSQL(t, stmts[0].Statement)

	var stored string
	testutils.RunSQL(t, "INSERT INTO diff_enumdef_converge (id) VALUES (1)")
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT CONCAT(e, '/', s) FROM diff_enumdef_converge WHERE id = 1").Scan(&stored))
	require.Equal(t, "B/a,c", stored)

	liveSQL := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, liveSQL, "`e` enum('a','B') DEFAULT 'B'")
	require.Contains(t, liveSQL, "`s` set('a','b','c') DEFAULT 'a,c'")
	live, err = ParseCreateTable(liveSQL)
	require.NoError(t, err)
	stmts, err = live.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts, "re-diff after applying the ALTER must converge")
}

// TestDiffIntegrationEnumDefaultContractionKeepsStoredValue verifies that a
// default the rule leaves alone because the collation does not fold ASCII case
// keeps the value MySQL stores for the declared DDL. utf8mb4_da_0900_ai_ci
// reads 'AA' as the contraction 'aa' and not as 'aA', so the table stores
// 'aa'; folding the default to the first case-insensitive match would emit a
// MODIFY to 'aA' that MySQL accepts, changing the stored default silently.
func TestDiffIntegrationEnumDefaultContractionKeepsStoredValue(t *testing.T) {
	ddl := "CREATE TABLE diff_enumdef_contraction (id int NOT NULL, b enum('aA','aa') COLLATE utf8mb4_da_0900_ai_ci DEFAULT 'AA', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	tt := testutils.NewTestTable(t, "diff_enumdef_contraction", ddl)
	liveSQL := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, liveSQL, "DEFAULT 'aa'", "the reading this test pins")
	desired, err := ParseCreateTable(ddl)
	require.NoError(t, err)
	live, err := ParseCreateTable(liveSQL)
	require.NoError(t, err)
	stmts, err := live.Diff(desired, nil)
	require.NoError(t, err)
	for _, s := range stmts {
		testutils.RunSQL(t, s.Statement)
	}
	testutils.RunSQL(t, "INSERT INTO diff_enumdef_contraction (id) VALUES (1)")
	var stored string
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT CAST(b AS BINARY) FROM diff_enumdef_contraction WHERE id = 1").Scan(&stored))
	require.Equal(t, "aa", stored, "the diff must not change the default MySQL stores for the declared DDL")
}

// TestDiffIntegrationEnumSetDefaultFoldAllowlist checks collationFoldsASCIICase
// against every collation on the server: each one it accepts must compare
// every ASCII letter, and every two-letter string, equal to its case variants
// (the two-letter strings catch contractions such as Danish aa or Czech ch).
// Tailored collations that break the equivalence are checked too, so the
// comparison is shown to catch it.
func TestDiffIntegrationEnumSetDefaultFoldAllowlist(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_enumdef_fold_pairs",
		"CREATE TABLE diff_enumdef_fold_pairs (id int NOT NULL AUTO_INCREMENT, l varchar(3) NOT NULL, v varchar(3) NOT NULL, PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	var rows []string
	for x := 'a'; x <= 'z'; x++ {
		rows = append(rows, fmt.Sprintf("('%c','%c')", x, unicode.ToUpper(x)))
		for y := 'a'; y <= 'z'; y++ {
			for _, v := range []string{string([]rune{unicode.ToUpper(x), y}), string([]rune{x, unicode.ToUpper(y)}), string([]rune{unicode.ToUpper(x), unicode.ToUpper(y)})} {
				rows = append(rows, fmt.Sprintf("('%c%c','%s')", x, y, v))
			}
		}
	}
	testutils.RunSQL(t, "INSERT INTO diff_enumdef_fold_pairs (l, v) VALUES "+strings.Join(rows, ","))

	unequal := func(cs, collation string) string {
		var examples sql.NullString
		query := fmt.Sprintf("SELECT GROUP_CONCAT(CONCAT(l, '/', v) ORDER BY id SEPARATOR ' ') FROM diff_enumdef_fold_pairs WHERE CONVERT(l USING %s) COLLATE %s <> CONVERT(v USING %s) COLLATE %s", cs, collation, cs, collation)
		require.NoError(t, tt.DB.QueryRowContext(t.Context(), query).Scan(&examples))
		return examples.String
	}

	collations, err := tt.DB.QueryContext(t.Context(), "SELECT COLLATION_NAME, CHARACTER_SET_NAME FROM information_schema.COLLATIONS WHERE CHARACTER_SET_NAME <> 'binary'")
	require.NoError(t, err)
	defer func() { require.NoError(t, collations.Close()) }()
	var folding int
	for collations.Next() {
		var collation, cs string
		require.NoError(t, collations.Scan(&collation, &cs))
		if !collationFoldsASCIICase(cs, collation) {
			continue
		}
		folding++
		assert.Empty(t, unequal(cs, collation), "%s is taken to fold ASCII case but compares these unequal", collation)
	}
	require.NoError(t, collations.Err())
	require.GreaterOrEqual(t, folding, 30, "the allowlist must match the server's collations")

	for _, c := range []struct{ cs, collation string }{
		{"utf8mb4", "utf8mb4_da_0900_ai_ci"},
		{"utf8mb4", "utf8mb4_czech_ci"},
		{"utf8mb4", "utf8mb4_tr_0900_ai_ci"},
		{"cp866", "cp866_general_ci"},
		{"latin7", "latin7_general_ci"},
	} {
		assert.NotEmpty(t, unequal(c.cs, c.collation), "%s must be caught not folding ASCII case", c.collation)
		assert.False(t, collationFoldsASCIICase(c.cs, c.collation), c.collation)
	}
}

// TestDiffIntegrationEnumSetMemberSpaces verifies that an enum or set member
// written with trailing spaces matches its live form: MySQL keeps the spaces
// when the column's charset is binary, through its own CHARACTER SET or
// COLLATE or the table default, and strips them otherwise.
func TestDiffIntegrationEnumSetMemberSpaces(t *testing.T) {
	for _, tc := range []struct{ name, ddl, live string }{
		{"diff_enumsp_charset", "CREATE TABLE diff_enumsp_charset (id int NOT NULL, b enum('a','b ') CHARACTER SET binary DEFAULT 'b ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			"`b` enum('a','b ') CHARACTER SET binary COLLATE binary DEFAULT 'b '"},
		{"diff_enumsp_collate", "CREATE TABLE diff_enumsp_collate (id int NOT NULL, b enum('a','b ') COLLATE binary DEFAULT 'b ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			"`b` enum('a','b ') CHARACTER SET binary COLLATE binary DEFAULT 'b '"},
		{"diff_enumsp_tabledefault", "CREATE TABLE diff_enumsp_tabledefault (id int NOT NULL, b enum('a','b ') DEFAULT 'b ', PRIMARY KEY (id)) DEFAULT CHARSET=binary",
			"`b` enum('a','b ') DEFAULT 'b '"},
		{"diff_enumsp_set", "CREATE TABLE diff_enumsp_set (id int NOT NULL, b set('a','b ') CHARACTER SET binary DEFAULT 'a,b ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			"`b` set('a','b ') CHARACTER SET binary COLLATE binary DEFAULT 'a,b '"},
		{"diff_enumsp_utf8mb4", "CREATE TABLE diff_enumsp_utf8mb4 (id int NOT NULL, b enum('a','b ') DEFAULT 'b', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			"`b` enum('a','b') DEFAULT 'b'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tt := testutils.NewTestTable(t, tc.name, tc.ddl)
			liveSQL := showCreateTable(t, tt.DB, tt.Name)
			require.Contains(t, liveSQL, tc.live, "the reading this case pins")
			desired, err := ParseCreateTable(tc.ddl)
			require.NoError(t, err)
			live, err := ParseCreateTable(liveSQL)
			require.NoError(t, err)
			stmts, err := live.Diff(desired, nil)
			require.NoError(t, err)
			require.Nil(t, stmts, "a member written with trailing spaces must match its live form")
			stmts, err = desired.Diff(live, nil)
			require.NoError(t, err)
			require.Nil(t, stmts, "the live form must match a member written with trailing spaces")
		})
	}
}

// TestDiffIntegrationEnumSetMemberSpacesModify verifies that the MODIFY
// emitted for another change to a binary enum or set column writes each member
// with its trailing spaces. Written without them, MySQL would store a
// different member, and reject the default that no longer names one (error
// 1067). After the ALTER the members and default are unchanged and a re-diff
// converges to nil.
func TestDiffIntegrationEnumSetMemberSpacesModify(t *testing.T) {
	tt := testutils.NewTestTable(t, "diff_enumsp_modify",
		"CREATE TABLE diff_enumsp_modify (id int NOT NULL, b enum('a','b ') CHARACTER SET binary DEFAULT 'b ', s set('x','y ') CHARACTER SET binary DEFAULT 'y ', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	desired, err := ParseCreateTable(
		"CREATE TABLE diff_enumsp_modify (id int NOT NULL, b enum('a','b ') CHARACTER SET binary DEFAULT 'b ' COMMENT 'changed', s set('x','y ') CHARACTER SET binary DEFAULT 'y ' COMMENT 'changed', PRIMARY KEY (id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)

	live, err := ParseCreateTable(showCreateTable(t, tt.DB, tt.Name))
	require.NoError(t, err)
	stmts, err := live.Diff(desired, nil)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	testutils.RunSQL(t, stmts[0].Statement)

	liveSQL := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, liveSQL, "`b` enum('a','b ') CHARACTER SET binary COLLATE binary DEFAULT 'b ' COMMENT 'changed'")
	require.Contains(t, liveSQL, "`s` set('x','y ') CHARACTER SET binary COLLATE binary DEFAULT 'y ' COMMENT 'changed'")

	live, err = ParseCreateTable(liveSQL)
	require.NoError(t, err)
	stmts, err = live.Diff(desired, nil)
	require.NoError(t, err)
	require.Nil(t, stmts, "re-diff after applying the ALTER must converge")
}

// TestDiffIntegrationEnumSetMemberSpacesNoTableDefault: a schema file that
// omits DEFAULT CHARSET is diffed against the live table, which always reports
// one. The members MySQL stores depend on the database default the table
// inherited, so a member written with trailing spaces must match the live form
// in a utf8mb4 database (stripped) and in a binary one (kept). A MODIFY for
// another change must keep the stored members, and a re-diff converge.
func TestDiffIntegrationEnumSetMemberSpacesNoTableDefault(t *testing.T) {
	for _, tc := range []struct{ dbCharset, liveB, liveS string }{
		{"utf8mb4", "`b` enum('a','b')", "`s` set('x','y')"},
		{"binary", "`b` enum('a','b ')", "`s` set('x','y ')"},
	} {
		t.Run(tc.dbCharset, func(t *testing.T) {
			dbName, db := testutils.CreateUniqueTestDatabase(t)
			testutils.RunSQLInDatabase(t, dbName, "ALTER DATABASE `"+dbName+"` CHARACTER SET "+tc.dbCharset)
			ddl := "CREATE TABLE enumsp (id int NOT NULL, b enum('a','b '), s set('x','y '), PRIMARY KEY (id))"
			testutils.RunSQLInDatabase(t, dbName, ddl)
			liveSQL := showCreateTable(t, db, "enumsp")
			require.Contains(t, liveSQL, tc.liveB+" DEFAULT NULL", "the reading this case pins")
			require.Contains(t, liveSQL, tc.liveS+" DEFAULT NULL", "the reading this case pins")

			desired, err := ParseCreateTable(ddl)
			require.NoError(t, err)
			live, err := ParseCreateTable(liveSQL)
			require.NoError(t, err)
			stmts, err := live.Diff(desired, nil)
			require.NoError(t, err)
			require.Nil(t, stmts, "a member written with trailing spaces must match its live form")

			desired, err = ParseCreateTable("CREATE TABLE enumsp (id int NOT NULL, b enum('a','b ') COMMENT 'changed', s set('x','y ') COMMENT 'changed', PRIMARY KEY (id))")
			require.NoError(t, err)
			stmts, err = live.Diff(desired, nil)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			testutils.RunSQLInDatabase(t, dbName, stmts[0].Statement)
			liveSQL = showCreateTable(t, db, "enumsp")
			require.Contains(t, liveSQL, tc.liveB+" DEFAULT NULL COMMENT 'changed'", "the MODIFY must keep the stored members")
			require.Contains(t, liveSQL, tc.liveS+" DEFAULT NULL COMMENT 'changed'", "the MODIFY must keep the stored members")

			live, err = ParseCreateTable(liveSQL)
			require.NoError(t, err)
			stmts, err = live.Diff(desired, nil)
			require.NoError(t, err)
			require.Nil(t, stmts, "re-diff after applying the ALTER must converge")
		})
	}
}

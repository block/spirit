package statement

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
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
// change is emitted in full and actually applies: the REMOVE PARTITIONING +
// PARTITION BY pair must carry the SUBPARTITION BY clause, or the table comes
// back partitioned but no longer subpartitioned. The re-diff then converges.
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
	require.Len(t, stmts, 2, "a subpartitioning change needs REMOVE PARTITIONING first")
	require.Equal(t, "ALTER TABLE `diff_subpart_chg` REMOVE PARTITIONING", stmts[0].Statement)
	require.Equal(t,
		"ALTER TABLE `diff_subpart_chg` PARTITION BY RANGE (YEAR(`dt`)) "+
			"SUBPARTITION BY HASH (dayofmonth(`dt`)) SUBPARTITIONS 4 "+
			"(PARTITION `p0` VALUES LESS THAN (2020), PARTITION `p1` VALUES LESS THAN MAXVALUE)",
		stmts[1].Statement)

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

	// Now move p0's boundary. The repartition has to carry every subpartition
	// name and comment through, or they are silently lost.
	const movedSQL = "CREATE TABLE diff_subpart_named (dt date NOT NULL, PRIMARY KEY (dt)) " +
		"PARTITION BY RANGE (YEAR(dt)) SUBPARTITION BY KEY (dt) " +
		"(PARTITION p0 VALUES LESS THAN (2030) COMMENT 'pc0' (SUBPARTITION s0 COMMENT 'sc0', SUBPARTITION s1), " +
		"PARTITION p1 VALUES LESS THAN MAXVALUE (SUBPARTITION s2, SUBPARTITION s3))"
	moved, err := ParseCreateTable(movedSQL)
	require.NoError(t, err)

	stmts = diffLiveTable(t, tt.DB, tt.Name, movedSQL)
	require.Len(t, stmts, 2)
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
// these two are excluded for reasons this layer cannot fix — scale padding
// belongs to numeric canonicalization. binary is excluded here too, because it
// pads the keyword to the column width; binaryDefaultBytesNormalizer folds it
// instead, and TestDiffIntegrationBinaryDefaultBytes covers it.
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
		"scaled decimal(4,2) NOT NULL DEFAULT TRUE, " +
		"yr year NOT NULL DEFAULT TRUE)"
	tt := testutils.NewTestTable(t, "diff_bool_keyword_excluded_types", declaredSQL)

	live := showCreateTable(t, tt.DB, tt.Name)
	require.Contains(t, live, "`scaled` decimal(4,2) NOT NULL DEFAULT '1.00'")
	require.Contains(t, live, "`yr` year NOT NULL DEFAULT '2001'")

	// The table was created from this very declaration, so every statement here
	// re-stores a value the column already holds.
	stmts := diffLiveTable(t, tt.DB, tt.Name, declaredSQL)
	require.Len(t, stmts, 1)
	for _, col := range []string{"scaled", "yr"} {
		require.Contains(t, stmts[0].Statement, "MODIFY COLUMN `"+col+"`")
	}
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

// binaryDefaultHexReason is why the binary-default tests that read back a
// non-utf8mb3 default skip before MySQL 8.0.33: earlier servers' SHOW CREATE
// TABLE replaces each such byte with '?' instead of reporting the default as a
// hex literal (MySQL Bug #104840).
const binaryDefaultHexReason = "SHOW CREATE TABLE reports a non-utf8mb3 binary default as '?' rather than hex"

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

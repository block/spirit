package lint

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/require"
)

// TestIndexVisibilityMixedLinter covers the cases that used to be enforced by
// the "visibility" preflight check in pkg/migration/check.
func TestIndexVisibilityMixedLinter(t *testing.T) {
	tests := []struct {
		alter    string
		expected bool // expect a violation
		desc     string
	}{
		// Pure metadata operations are safe (no index visibility)
		{"drop index `a`", false, "drop index"},
		{"rename index `a` to `b`", false, "rename index"},
		{"drop index `a`, drop index `b`", false, "multiple drop index"},
		{"drop index `a`, rename index `b` to c", false, "mixed metadata operations"},
		{"drop partition `p1`", false, "drop partition"},
		{"truncate partition `p1`", false, "truncate partition"},
		{"add partition (partition `p1` values less than (100))", false, "add partition"},

		// Pure table-rebuilding operations are safe (no index visibility)
		{"ADD COLUMN `a` INT", false, "add column"},
		{"ADD index (a)", false, "add index"},
		{"drop index `a`, add index `b` (`b`)", false, "drop and add index"},
		{"engine=innodb", false, "change engine"},
		{"add unique(b)", false, "add unique"},
		{"modify `a` int", false, "modify column non-varchar"},
		{"change column `a` `a` int", false, "change column non-varchar"},

		// Pure index visibility operations are safe
		{"ALTER INDEX b INVISIBLE", false, "pure visibility invisible"},
		{"ALTER INDEX b VISIBLE", false, "pure visibility visible"},

		// Index visibility mixed with metadata-only operations is safe
		{"ALTER INDEX b INVISIBLE, drop index `c`", false, "visibility + drop index"},
		{"ALTER INDEX b VISIBLE, rename index `a` to `new_a`", false, "visibility + rename index"},
		{"ALTER INDEX b INVISIBLE, drop partition `p1`", false, "visibility + drop partition"},
		{"ALTER INDEX b VISIBLE, truncate partition `p2`", false, "visibility + truncate partition"},
		{"ALTER INDEX b INVISIBLE, add partition (partition `p3` values less than (200))", false, "visibility + add partition"},
		{"ALTER INDEX b VISIBLE, modify `a` varchar(100)", false, "visibility + varchar modify"},
		{"ALTER INDEX b INVISIBLE, change column `a` `a` varchar(150)", false, "visibility + varchar change"},

		// Complex mixed operations - multiple metadata operations with visibility
		{"ALTER INDEX a INVISIBLE, rename index `b` to `new_b`, modify `col` varchar(100)", false, "visibility + rename + varchar modify"},
		{"ALTER INDEX a INVISIBLE, rename index `b` to `new_b`, modify `col` int", true, "visibility + rename + non-varchar modify"},

		// Index visibility mixed with table-rebuilding operations is flagged
		{"ALTER INDEX b INVISIBLE, ADD COLUMN `c` INT", true, "visibility + add column"},
		{"ALTER INDEX b VISIBLE, ADD index (d)", true, "visibility + add index"},
		{"ALTER INDEX b INVISIBLE, engine=innodb", true, "visibility + engine change"},
		{"ALTER INDEX b VISIBLE, add unique(e)", true, "visibility + add unique"},
		{"ALTER INDEX b INVISIBLE, modify `a` int", true, "visibility + non-varchar modify"},
		{"ALTER INDEX b VISIBLE, change column `a` `a` int", true, "visibility + non-varchar change"},

		// A VARCHAR redeclaration that also reorders the column or declares NOT
		// NULL is accepted by MySQL as INPLACE but rebuilds the table, so it is
		// not a metadata-only carve-out.
		{"ALTER INDEX b INVISIBLE, modify `a` varchar(100) NOT NULL", true, "visibility + varchar NOT NULL"},
		{"ALTER INDEX b INVISIBLE, modify `a` varchar(100) FIRST", true, "visibility + varchar reordered"},
		{"ALTER INDEX b INVISIBLE, change column `a` `a` varchar(100) AFTER `z`", true, "visibility + varchar change reordered"},

		// A COMMENT-only table option is metadata-only; other options rebuild.
		{"ALTER INDEX b INVISIBLE, COMMENT='hello'", false, "visibility + table comment"},
		{"ALTER INDEX b INVISIBLE, COMMENT='hello' engine=innodb", true, "visibility + comment mixed with engine"},

		// Multiple index visibility changes with mixed operations
		{"ALTER INDEX a INVISIBLE, ALTER INDEX b VISIBLE, drop index `c`", false, "multiple visibility + metadata"},
		{"ALTER INDEX a INVISIBLE, ALTER INDEX b VISIBLE, ADD COLUMN `c` INT", true, "multiple visibility + table rebuilding"},
		{"ALTER INDEX a INVISIBLE, ALTER INDEX b VISIBLE, modify `a` varchar(200)", false, "multiple visibility + varchar"},
		{"ALTER INDEX a INVISIBLE, ALTER INDEX b VISIBLE, modify `a` text", true, "multiple visibility + text"},
	}

	linter := &IndexVisibilityMixedLinter{}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			stmts, err := statement.New("ALTER TABLE t1 " + tt.alter)
			require.NoError(t, err)

			violations := linter.Lint(nil, stmts)
			if !tt.expected {
				require.Empty(t, violations)
				return
			}
			require.Len(t, violations, 1)
			// This is intentionally a warning and not an error: declarative
			// workflows generate the ALTER from a diff, so the user does not
			// control which clauses are batched together.
			require.Equal(t, SeverityWarning, violations[0].Severity)
			require.Equal(t, "index_visibility_mixed", violations[0].Linter.Name())
			require.Equal(t, "t1", violations[0].Location.Table)
			require.NotNil(t, violations[0].Suggestion)
		})
	}
}

func TestIndexVisibilityMixedLinter_ViolationDetail(t *testing.T) {
	stmts, err := statement.New("ALTER TABLE t1 ALTER INDEX b INVISIBLE, ADD COLUMN `c` INT")
	require.NoError(t, err)

	linter := &IndexVisibilityMixedLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	require.Contains(t, violations[0].Message, "Index visibility change on \"b\"")
	require.Contains(t, violations[0].Message, "ADD COLUMN")
	require.NotNil(t, violations[0].Location.Index)
	require.Equal(t, "b", *violations[0].Location.Index)
	require.Contains(t, *violations[0].Suggestion, "Apply the index visibility change separately")
}

// TestIndexVisibilityMixedLinter_MultipleIndexes verifies that a statement
// flipping several indexes lists them all in the message, and leaves
// Location.Index unset rather than arbitrarily naming the first one.
func TestIndexVisibilityMixedLinter_MultipleIndexes(t *testing.T) {
	stmts, err := statement.New("ALTER TABLE t1 ALTER INDEX a INVISIBLE, ALTER INDEX b VISIBLE, ADD COLUMN c INT")
	require.NoError(t, err)

	linter := &IndexVisibilityMixedLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 1)
	require.Contains(t, violations[0].Message, "\"a, b\"")
	require.Equal(t, "t1", violations[0].Location.Table)
	require.Nil(t, violations[0].Location.Index)
}

// TestIndexVisibilityMixedLinter_MultipleStatements verifies each offending
// ALTER produces its own violation, and clean statements produce none.
func TestIndexVisibilityMixedLinter_MultipleStatements(t *testing.T) {
	stmts, err := statement.New(`ALTER TABLE t1 ALTER INDEX b INVISIBLE, ADD COLUMN c INT;
		ALTER TABLE t2 ALTER INDEX d VISIBLE;
		ALTER TABLE t3 ALTER INDEX e INVISIBLE, ENGINE=InnoDB;`)
	require.NoError(t, err)

	linter := &IndexVisibilityMixedLinter{}
	violations := linter.Lint(nil, stmts)

	require.Len(t, violations, 2)
	require.Equal(t, "t1", violations[0].Location.Table)
	require.Equal(t, "t3", violations[1].Location.Table)
}

func TestIndexVisibilityMixedLinter_NonAlterStatement(t *testing.T) {
	stmts, err := statement.New("CREATE TABLE t1 (id INT PRIMARY KEY)")
	require.NoError(t, err)

	linter := &IndexVisibilityMixedLinter{}
	require.Empty(t, linter.Lint(nil, stmts))
}

func TestIndexVisibilityMixedLinter_Metadata(t *testing.T) {
	linter := &IndexVisibilityMixedLinter{}
	require.Equal(t, "index_visibility_mixed", linter.Name())
	require.NotEmpty(t, linter.Description())
	require.Contains(t, linter.String(), "index_visibility_mixed")
}

// TestIndexVisibilityMixedLinter_Registered confirms the linter is enabled by
// default via its init() registration.
func TestIndexVisibilityMixedLinter_Registered(t *testing.T) {
	stmts, err := statement.New("ALTER TABLE t1 ALTER INDEX b INVISIBLE, ADD COLUMN c INT")
	require.NoError(t, err)

	violations, err := RunLinters(nil, stmts, Config{})
	require.NoError(t, err)
	require.Len(t, FilterByLinter(violations, "index_visibility_mixed"), 1)
}

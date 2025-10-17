package check

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVisibilityChange(t *testing.T) {
	testCheck := func(stmt string, shouldError bool, expectedError error) {
		r := Resources{Statement: statement.MustNew("ALTER TABLE t1 " + stmt)[0]}
		err := RunChecks(t.Context(), r, nil, ScopePostSetup)
		if shouldError {
			require.Error(t, err)
			if expectedError != nil {
				assert.ErrorIs(t, err, expectedError)
			}
		} else {
			require.NoError(t, err)
		}
	}

	testDirect := func(stmt string, shouldError bool, expectedError error) {
		err := AlterContainsIndexVisibility(statement.MustNew("ALTER TABLE t1 " + stmt)[0])
		if shouldError {
			require.Error(t, err)
			if expectedError != nil {
				assert.ErrorIs(t, err, expectedError)
			}
		} else {
			require.NoError(t, err)
		}
	}

	// Test both check function and direct function with same data
	tests := []struct {
		stmt          string
		shouldError   bool
		expectedError error
		desc          string
	}{
		// Pure metadata operations should be safe (no index visibility)
		{"drop index `a`", false, nil, "drop index"},
		{"rename index `a` to `b`", false, nil, "rename index"},
		{"drop index `a`, drop index `b`", false, nil, "multiple drop index"},
		{"drop index `a`, rename index `b` to c", false, nil, "mixed metadata operations"},
		{"drop partition `p1`", false, nil, "drop partition"},
		{"truncate partition `p1`", false, nil, "truncate partition"},
		{"add partition (partition `p1` values less than (100))", false, nil, "add partition"},

		// Pure table-rebuilding operations should be safe (no index visibility)
		{"ADD COLUMN `a` INT", false, nil, "add column"},
		{"ADD index (a)", false, nil, "add index"},
		{"drop index `a`, add index `b` (`b`)", false, nil, "drop and add index"},
		{"engine=innodb", false, nil, "change engine"},
		{"add unique(b)", false, nil, "add unique"},
		{"modify `a` int", false, nil, "modify column non-varchar"},
		{"change column `a` `a` int", false, nil, "change column non-varchar"},

		// Pure index visibility operations should be safe
		{"ALTER INDEX b INVISIBLE", false, nil, "pure visibility invisible"},
		{"ALTER INDEX b VISIBLE", false, nil, "pure visibility visible"},

		// Index visibility mixed with metadata-only operations should be safe
		{"ALTER INDEX b INVISIBLE, drop index `c`", false, nil, "visibility + drop index"},
		{"ALTER INDEX b VISIBLE, rename index `a` to `new_a`", false, nil, "visibility + rename index"},
		{"ALTER INDEX b INVISIBLE, drop partition `p1`", false, nil, "visibility + drop partition"},
		{"ALTER INDEX b VISIBLE, truncate partition `p2`", false, nil, "visibility + truncate partition"},
		{"ALTER INDEX b INVISIBLE, add partition (partition `p3` values less than (200))", false, nil, "visibility + add partition"},
		{"ALTER INDEX b VISIBLE, modify `a` varchar(100)", false, nil, "visibility + varchar modify"},
		{"ALTER INDEX b INVISIBLE, change column `a` `a` varchar(150)", false, nil, "visibility + varchar change"},

		// Complex mixed operations - multiple metadata operations with visibility
		{"ALTER INDEX a INVISIBLE, rename index `b` to `new_b`, modify `col` varchar(100)", false, nil, "visibility + rename + varchar modify"},
		{"ALTER INDEX a INVISIBLE, rename index `b` to `new_b`, modify `col` int", true, ErrVisibilityMixedWithOtherChanges, "visibility + rename + non-varchar modify"},

		// Index visibility mixed with table-rebuilding operations should fail
		{"ALTER INDEX b INVISIBLE, ADD COLUMN `c` INT", true, ErrVisibilityMixedWithOtherChanges, "visibility + add column"},
		{"ALTER INDEX b VISIBLE, ADD index (d)", true, ErrVisibilityMixedWithOtherChanges, "visibility + add index"},
		{"ALTER INDEX b INVISIBLE, engine=innodb", true, ErrVisibilityMixedWithOtherChanges, "visibility + engine change"},
		{"ALTER INDEX b VISIBLE, add unique(e)", true, ErrVisibilityMixedWithOtherChanges, "visibility + add unique"},
		{"ALTER INDEX b INVISIBLE, modify `a` int", true, ErrVisibilityMixedWithOtherChanges, "visibility + non-varchar modify"},
		{"ALTER INDEX b VISIBLE, change column `a` `a` int", true, ErrVisibilityMixedWithOtherChanges, "visibility + non-varchar change"},

		// Multiple index visibility changes with mixed operations
		{"ALTER INDEX a INVISIBLE, ALTER INDEX b VISIBLE, drop index `c`", false, nil, "multiple visibility + metadata"},
		{"ALTER INDEX a INVISIBLE, ALTER INDEX b VISIBLE, ADD COLUMN `c` INT", true, ErrVisibilityMixedWithOtherChanges, "multiple visibility + table rebuilding"},
		{"ALTER INDEX a INVISIBLE, ALTER INDEX b VISIBLE, modify `a` varchar(200)", false, nil, "multiple visibility + varchar"},
		{"ALTER INDEX a INVISIBLE, ALTER INDEX b VISIBLE, modify `a` text", true, ErrVisibilityMixedWithOtherChanges, "multiple visibility + text"},
	}

	for _, tt := range tests {
		t.Run("check_"+tt.desc, func(t *testing.T) {
			testCheck(tt.stmt, tt.shouldError, tt.expectedError)
		})
		t.Run("direct_"+tt.desc, func(t *testing.T) {
			testDirect(tt.stmt, tt.shouldError, tt.expectedError)
		})
	}

	// Test non-ALTER statements (only for direct function)
	t.Run("non_alter_statement", func(t *testing.T) {
		err := AlterContainsIndexVisibility(statement.MustNew("CREATE TABLE t1 (id INT)")[0])
		assert.ErrorIs(t, err, statement.ErrNotAlterTable)
	})
}

func TestAlterContainsIndexVisibility_ErrorMessages(t *testing.T) {
	// Test specific error message content
	err := AlterContainsIndexVisibility(statement.MustNew("ALTER TABLE t1 ALTER INDEX b INVISIBLE, ADD COLUMN `c` INT")[0])
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrVisibilityMixedWithOtherChanges)
	assert.Contains(t, err.Error(), "the ALTER operation contains a change to index visibility mixed with table-rebuilding operations")
	assert.Contains(t, err.Error(), "Please split the ALTER statement into separate statements")
}

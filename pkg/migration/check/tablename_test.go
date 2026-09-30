package check

import (
	"log/slog"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestCheckTableName(t *testing.T) {
	testTableName := func(name string, skipDropAfterCutover bool) error {
		r := Resources{
			Table: &table.TableInfo{
				TableName: name,
			},
			SkipDropAfterCutover: skipDropAfterCutover,
		}
		return tableNameCheck(t.Context(), r, slog.Default())
	}

	require.NoError(t, testTableName("a", false))
	require.NoError(t, testTableName("a", true))

	require.ErrorContains(t, testTableName("", false), "table name must be at least 1 character")
	require.ErrorContains(t, testTableName("", true), "table name must be at least 1 character")

	// MySQL's hard limit is 64 characters; anything longer is rejected.
	longName := strings.Repeat("a", utils.MaxTableNameLength+1)
	require.ErrorContains(t, testTableName(longName, false), "table name must be 64 characters or fewer")
	require.ErrorContains(t, testTableName(longName, true), "table name must be 64 characters or fewer")

	// A table name at the maximum length should pass regardless of
	// SkipDropAfterCutover. Auxiliary names (_new, _chkpnt, _old, _old_<ts>)
	// are produced via deterministic truncation in utils.AuxTableName.
	exactFitName := strings.Repeat("x", utils.MaxTableNameLength)
	require.NoError(t, testTableName(exactFitName, false))
	require.NoError(t, testTableName(exactFitName, true))
}

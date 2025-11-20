package check

import (
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
)

func TestCheckTableNameConstants(t *testing.T) {
	// Calculated extra chars should always be greater than 0
	assert.Positive(t, NameFormatNormalExtraChars)
	assert.Positive(t, NameFormatTimestampExtraChars)

	// Calculated extra chars should be less than the max table name length
	assert.Less(t, NameFormatNormalExtraChars, maxTableNameLength)
	assert.Less(t, NameFormatTimestampExtraChars, maxTableNameLength)
}

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

	assert.NoError(t, testTableName("a", false))
	assert.NoError(t, testTableName("a", true))

	assert.ErrorContains(t, testTableName("", false), "table name must be at least 1 character")
	assert.ErrorContains(t, testTableName("", true), "table name must be at least 1 character")

	longName := "thisisareallylongtablenamethisisareallylongtablenamethisisareallylongtablename"
	assert.ErrorContains(t, testTableName(longName, false), "table name must be less than")
	assert.ErrorContains(t, testTableName(longName, true), "table name must be less than")
}

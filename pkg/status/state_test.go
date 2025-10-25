package status

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStateString(t *testing.T) {
	assert.Equal(t, "initial", Initial.String())
	assert.Equal(t, "copyRows", CopyRows.String())
	assert.Equal(t, "waitingOnSentinelTable", WaitingOnSentinelTable.String())
	assert.Equal(t, "applyChangeset", ApplyChangeset.String())
	assert.Equal(t, "checksum", Checksum.String())
	assert.Equal(t, "cutOver", CutOver.String())
	assert.Equal(t, "errCleanup", ErrCleanup.String())
	assert.Equal(t, "analyzeTable", AnalyzeTable.String())
	assert.Equal(t, "close", Close.String())
}

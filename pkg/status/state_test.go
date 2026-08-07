package status

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestStateString(t *testing.T) {
	require.Equal(t, "initial", Initial.String())
	require.Equal(t, "copyRows", CopyRows.String())
	require.Equal(t, "waitingOnSentinelTable", WaitingOnSentinelTable.String())
	require.Equal(t, "applyChangeset", ApplyChangeset.String())
	require.Equal(t, "checksum", Checksum.String())
	require.Equal(t, "cutOver", CutOver.String())
	require.Equal(t, "errCleanup", ErrCleanup.String())
	require.Equal(t, "analyzeTable", AnalyzeTable.String())
	require.Equal(t, "close", Close.String())
}

func TestStateSetNotifiesAfterStore(t *testing.T) {
	var state State
	notifications := 0

	state.Set(CopyRows, func() {
		notifications++
		require.Equal(t, CopyRows, state.Get())
	}, nil)

	require.Equal(t, CopyRows, state.Get())
	require.Equal(t, 1, notifications)
}

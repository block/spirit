package copier

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type copierWithoutCompletedWork struct{ Copier }

func TestCompletedWorkCountsAffectedRowsAndZeroRowChunks(t *testing.T) {
	bufferedCopier := &buffered{}
	bufferedCopier.recordCompletedChunk(12)
	bufferedCopier.recordCompletedChunk(0)
	bufferedCopier.recordCompletedChunk(0)
	rows, chunks, available := CompletedWork(bufferedCopier)
	require.True(t, available)
	require.Equal(t, uint64(12), rows)
	require.Equal(t, uint64(3), chunks)

	unbufferedCopier := &Unbuffered{}
	unbufferedCopier.recordCompletedChunk(9)
	unbufferedCopier.recordCompletedChunk(0)
	rows, chunks, available = CompletedWork(unbufferedCopier)
	require.True(t, available)
	require.Equal(t, uint64(9), rows)
	require.Equal(t, uint64(2), chunks)
}

func TestCompletedWorkUnavailableForCopierWithoutCapability(t *testing.T) {
	rows, chunks, available := CompletedWork(&copierWithoutCompletedWork{})
	require.False(t, available)
	require.Zero(t, rows)
	require.Zero(t, chunks)
}

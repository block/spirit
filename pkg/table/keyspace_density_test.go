package table

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeySpaceDensitySparse(t *testing.T) {
	var k keySpaceDensity

	// A short window is never sparse: "not known yet" must not authorize
	// dropping the chunk size to StartingChunkSize.
	for range keySpaceDensityWindow - 1 {
		k.record(MaxDynamicRowSize, 0)
	}
	require.False(t, k.sparse())
	k.record(MaxDynamicRowSize, 0)
	require.True(t, k.sparse(), "a window entirely inside a gap is sparse")

	// A dense key space — every chunk returned as many rows as it was wide —
	// is what a healthy table looks like, and must never read as sparse no
	// matter how long we watch it.
	k.reset()
	for range keySpaceDensityWindow * 3 {
		k.record(MaxDynamicRowSize, MaxDynamicRowSize)
	}
	require.False(t, k.sparse())

	// The threshold is prefetch's own exit test: StartingChunkSize rows must
	// span at least MaxDynamicRowSize keys, i.e. minKeysPerRowForPrefetch keys
	// per row. Just under it does not qualify.
	k.reset()
	for range keySpaceDensityWindow {
		k.record(MaxDynamicRowSize, MaxDynamicRowSize/minKeysPerRowForPrefetch+1)
	}
	require.False(t, k.sparse())
	k.reset()
	for range keySpaceDensityWindow {
		k.record(MaxDynamicRowSize, MaxDynamicRowSize/minKeysPerRowForPrefetch)
	}
	require.True(t, k.sparse())

	// The window slides, so a table that gets denser stops reading as sparse
	// rather than being judged forever on where it started.
	for range keySpaceDensityWindow {
		k.record(MaxDynamicRowSize, MaxDynamicRowSize)
	}
	require.False(t, k.sparse())
	require.Len(t, k.window, keySpaceDensityWindow)
}

func TestKeySpaceDensityTotals(t *testing.T) {
	var k keySpaceDensity
	keys, rows := k.totals()
	require.Zero(t, keys)
	require.Zero(t, rows)

	k.record(100, 7)
	k.record(200, 3)
	keys, rows = k.totals()
	require.Equal(t, uint64(300), keys)
	require.Equal(t, uint64(10), rows)
}

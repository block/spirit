package datasync

import (
	"reflect"
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/stretchr/testify/require"
)

func TestSyncMaxConnections(t *testing.T) {
	r, err := NewRunner(&Sync{})
	require.NoError(t, err)
	require.Equal(t, dbconn.DefaultMaxConnections, r.sync.MaxConnections)
	field, ok := reflect.TypeFor[Sync]().FieldByName("MaxConnections")
	require.True(t, ok)
	require.Equal(t, "128", field.Tag.Get("default"))
	_, err = NewRunner(&Sync{MaxConnections: -1})
	require.ErrorContains(t, err, "--max-connections must be non-negative")
	// Sync has neither cutover nor pinned checksum snapshots. Workers may
	// outnumber its connection budget without requiring snapshot headroom.
	r, err = NewRunner(&Sync{MaxConnections: 2, Threads: 16, WriteThreads: 16})
	require.NoError(t, err)
	require.Equal(t, 2, r.sync.MaxConnections)
	require.Equal(t, 16, r.sync.Threads)
	require.Equal(t, 16, r.sync.WriteThreads)
}

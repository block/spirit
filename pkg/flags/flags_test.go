package flags

import (
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/require"
)

// The Default* constants are what Normalize hands a programmatic caller, so
// they must equal the Kong defaults the CLI gets.
func TestDefaultsMatchKongTags(t *testing.T) {
	typ := reflect.TypeFor[Common]()
	for field, want := range map[string]int{
		"Threads":        DefaultThreads,
		"WriteThreads":   DefaultWriteThreads,
		"MaxConnections": DefaultMaxConnections,
	} {
		f, ok := typ.FieldByName(field)
		require.True(t, ok, field)
		require.Equal(t, strconv.Itoa(want), f.Tag.Get("default"), field)
	}
	f, _ := typ.FieldByName("TargetChunkSize")
	require.Equal(t, strconv.FormatUint(table.DefaultTargetChunkBytes, 10), f.Tag.Get("default"))
	f, _ = typ.FieldByName("LockWaitTimeout")
	lockWait, err := time.ParseDuration(f.Tag.Get("default"))
	require.NoError(t, err)
	require.Equal(t, dbconn.NewDBConfig().LockWaitTimeout, int(lockWait.Seconds()),
		"a programmatic caller's zero keeps dbconn's default, which must equal the CLI's")
}

func TestValidate(t *testing.T) {
	require.NoError(t, (&Common{}).Validate())
	require.ErrorContains(t, (&Common{Threads: -1}).Validate(), "--threads must be non-negative")
	require.ErrorContains(t, (&Common{WriteThreads: -1}).Validate(), "--write-threads must be non-negative")
	require.Error(t, (&Common{ForceKillAfter: -time.Second}).Validate())
	require.Error(t, (&Common{LockWaitTimeout: 10 * time.Second, ForceKillAfter: 10 * time.Second}).Validate())
	require.NoError(t, (&Common{LockWaitTimeout: 10 * time.Second, ForceKillAfter: 9 * time.Second}).Validate())
}

func TestNormalize(t *testing.T) {
	c := &Common{}
	c.Normalize(nil)
	require.Equal(t, DefaultThreads, c.Threads)
	require.Equal(t, DefaultWriteThreads, c.WriteThreads)
	require.Equal(t, DefaultMaxConnections, c.MaxConnections)
	require.Equal(t, uint64(table.DefaultTargetChunkBytes), c.TargetChunkSize)
	require.Zero(t, c.MaxCommitLatency, "zero disables the commit-latency throttler and must survive")

	c = &Common{Threads: 3, WriteThreads: 5, MaxConnections: 37, TargetChunkSize: 8192}
	c.Normalize(nil)
	require.Equal(t, Common{Threads: 3, WriteThreads: 5, MaxConnections: 37, TargetChunkSize: 8192}, *c)
}

func TestApplyTo(t *testing.T) {
	// Zero and empty values keep the config's own.
	config := dbconn.NewDBConfig()
	want := *config
	(&Common{}).ApplyTo(config)
	require.Equal(t, want, *config)

	(&Common{
		MaxConnections:     37,
		LockWaitTimeout:    10 * time.Second,
		ForceKillAfter:     5 * time.Second,
		TLSMode:            "REQUIRED",
		TLSCertificatePath: "/ca.pem",
	}).ApplyTo(config)
	require.Equal(t, 37, config.MaxOpenConnections)
	require.Equal(t, 10, config.LockWaitTimeout)
	require.Equal(t, 5*time.Second, config.ForceKillAfter)
	require.Equal(t, "REQUIRED", config.TLSMode)
	require.Equal(t, "/ca.pem", config.TLSCertificatePath)
}

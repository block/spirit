package flags

import (
	"bytes"
	"log/slog"
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
	f, _ = typ.FieldByName("CheckpointMaxAge")
	maxAge, err := time.ParseDuration(f.Tag.Get("default"))
	require.NoError(t, err)
	require.Equal(t, DefaultCheckpointMaxAge, maxAge)

	f, _ = reflect.TypeFor[Cutover]().FieldByName("LockWaitTimeout")
	lockWait, err := time.ParseDuration(f.Tag.Get("default"))
	require.NoError(t, err)
	require.Equal(t, dbconn.NewDBConfig().LockWaitTimeout, int(lockWait.Seconds()),
		"a programmatic caller's zero keeps dbconn's default, which must equal the CLI's")
}

func TestValidate(t *testing.T) {
	require.NoError(t, (&Common{}).Validate())
	require.ErrorContains(t, (&Common{Threads: -1}).Validate(), "--threads must be non-negative")
	require.ErrorContains(t, (&Common{WriteThreads: -1}).Validate(), "--write-threads must be non-negative")
	require.ErrorContains(t, (&Common{MaxCommitLatency: -time.Millisecond}).Validate(), "--max-commit-latency must be non-negative")
	require.ErrorContains(t, (&Common{CheckpointMaxAge: -time.Hour}).Validate(), "--checkpoint-max-age must be non-negative")
}

func TestCutoverValidate(t *testing.T) {
	require.NoError(t, (&Cutover{}).Validate())
	require.Error(t, (&Cutover{ForceKillAfter: -time.Second}).Validate())
	require.ErrorContains(t, (&Cutover{LockWaitTimeout: -time.Second}).Validate(), "--lock-wait-timeout must be non-negative")
	require.Error(t, (&Cutover{LockWaitTimeout: 10 * time.Second, ForceKillAfter: 10 * time.Second}).Validate())
	require.NoError(t, (&Cutover{LockWaitTimeout: 10 * time.Second, ForceKillAfter: 9 * time.Second}).Validate())
}

func TestWaitsOnSentinel(t *testing.T) {
	require.False(t, (&Cutover{}).WaitsOnSentinel())
	require.True(t, (&Cutover{RespectSentinel: true}).WaitsOnSentinel())
	require.True(t, (&Cutover{DeferCutOver: true}).WaitsOnSentinel(),
		"a run that created a sentinel must not cut over past it")
}

func TestNormalize(t *testing.T) {
	c := &Common{}
	c.Normalize()
	require.Equal(t, DefaultThreads, c.Threads)
	require.Equal(t, DefaultWriteThreads, c.WriteThreads)
	require.Equal(t, DefaultMaxConnections, c.MaxConnections)
	require.Equal(t, uint64(table.DefaultTargetChunkBytes), c.TargetChunkSize)
	require.Equal(t, DefaultCheckpointMaxAge, c.CheckpointMaxAge)
	require.Zero(t, c.MaxCommitLatency, "zero disables the commit-latency throttler and must survive")

	c = &Common{Threads: 3, WriteThreads: 5, MaxConnections: 37, TargetChunkSize: 8192, CheckpointMaxAge: time.Hour}
	c.Normalize()
	require.Equal(t, Common{Threads: 3, WriteThreads: 5, MaxConnections: 37, TargetChunkSize: 8192, CheckpointMaxAge: time.Hour}, *c)
}

func TestApplyTo(t *testing.T) {
	// Zero and empty values keep the config's own.
	config := dbconn.NewDBConfig()
	want := *config
	(&Common{}).ApplyTo(config)
	require.Equal(t, want, *config)

	(&Cutover{}).ApplyTo(config)
	require.Equal(t, want, *config)

	(&Common{
		MaxConnections:     37,
		InterpolateParams:  true,
		TLSMode:            "REQUIRED",
		TLSCertificatePath: "/ca.pem",
	}).ApplyTo(config)
	(&Cutover{LockWaitTimeout: 10 * time.Second, ForceKillAfter: 5 * time.Second}).ApplyTo(config)
	require.Equal(t, 37, config.MaxOpenConnections)
	require.True(t, config.InterpolateParams)
	require.Equal(t, 10, config.LockWaitTimeout)
	require.Equal(t, 5*time.Second, config.ForceKillAfter)
	require.Equal(t, "REQUIRED", config.TLSMode)
	require.Equal(t, "/ca.pem", config.TLSCertificatePath)
}

func TestWarnZeroWriteThreads(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	(&Common{WriteThreads: 3}).WarnZeroWriteThreads(logger)
	require.Empty(t, buf.String())
	(&Common{}).WarnZeroWriteThreads(logger)
	require.Contains(t, buf.String(), "--write-threads 0 no longer means auto-size")
}

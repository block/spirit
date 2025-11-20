package check

import (
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
)

func TestSettings(t *testing.T) {
	r := Resources{
		Table:           &table.TableInfo{TableName: "test", SchemaName: "test"},
		Threads:         2,
		TargetChunkTime: time.Second * 5,
		ReplicaMaxLag:   time.Hour,
	}

	// 0 means "use default". This is a bit of a hack,
	// but the test suite depends on it.
	if r.TargetChunkTime == 0 {
		r.TargetChunkTime = time.Millisecond * 500
	}
	if r.ReplicaMaxLag == 0 {
		r.ReplicaMaxLag = time.Second * 120
	}

	err := settingsCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err) // all looks good

	r.Threads = 0
	err = settingsCheck(t.Context(), r, slog.Default())
	assert.Error(t, err)

	r.Threads = 65
	err = settingsCheck(t.Context(), r, slog.Default())
	assert.Error(t, err)

	r.Threads = 2
	err = settingsCheck(t.Context(), r, slog.Default())
	assert.NoError(t, err) // all looks good

	r.TargetChunkTime = time.Second * 6
	err = settingsCheck(t.Context(), r, slog.Default())
	assert.Error(t, err)

	r.TargetChunkTime = time.Second * 4
	r.ReplicaMaxLag = time.Second * 5
	err = settingsCheck(t.Context(), r, slog.Default())
	assert.Error(t, err)
}

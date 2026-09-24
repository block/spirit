package migration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMigrationForceKillAfter(t *testing.T) {
	for _, tc := range []struct {
		name           string
		timeout, delay time.Duration
		valid          bool
	}{
		{"default", 0, 0, true},
		{"explicit", 10 * time.Second, 5 * time.Second, true},
		{"default timeout", 0, 5 * time.Second, true},
		{"negative", 10 * time.Second, -time.Second, false},
		{"equal", 10 * time.Second, 10 * time.Second, false},
		{"longer", 10 * time.Second, 11 * time.Second, false},
		{"truncated timeout", 1500 * time.Millisecond, 1200 * time.Millisecond, false},
		{"subsecond timeout", 500 * time.Millisecond, 100 * time.Millisecond, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &Migration{Statement: "ALTER TABLE t ADD COLUMN c INT", LockWaitTimeout: tc.timeout, ForceKillAfter: tc.delay}
			err := m.Validate()
			_, runnerErr := NewRunner(m)
			if tc.valid {
				require.NoError(t, err)
				require.NoError(t, runnerErr)
			} else {
				require.ErrorContains(t, err, "force-kill-after")
				require.ErrorContains(t, runnerErr, "force-kill-after")
			}
		})
	}
}

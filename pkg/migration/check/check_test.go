package check

import (
	"context"
	"log/slog"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckAPI(t *testing.T) {
	testVal := "test"
	myfunc := func(
		_ context.Context,
		_ Resources,
		_ *slog.Logger,
	) error {
		testVal = "newval"
		return nil
	}
	checkLen := len(checks)
	registerCheck("mycheck", myfunc, ScopeTesting)
	require.Len(t, checks, checkLen+1)

	// Can't be duplicate registered because of a map
	registerCheck("mycheck", myfunc, ScopeTesting)
	require.Len(t, checks, checkLen+1)

	require.Equal(t, "test", testVal)
	err := RunChecks(t.Context(), Resources{}, slog.Default(), ScopeTesting)
	require.NoError(t, err)
	require.Equal(t, "newval", testVal)
}

// TestChecksInScope covers reporting a scope's membership, which a caller
// outside this package pins so a check added to a scope it relies on has to be
// judged rather than picked up silently. A name only belongs in the answer if
// the check behind it really is registered for the scope asked about, in the
// order RunChecks would run it.
func TestChecksInScope(t *testing.T) {
	assert.Empty(t, ChecksInScope(ScopeNone), "no check registers under the empty scope")
	assert.Equal(t, []string{"version"}, ChecksInScope(ScopePreRun))

	statementScope := ChecksInScope(ScopeStatement)
	require.NotEmpty(t, statementScope)
	assert.True(t, slices.IsSorted(statementScope), "names must come back in the order RunChecks runs them")
	for _, name := range statementScope {
		lock.Lock()
		registered, ok := checks[name]
		lock.Unlock()
		require.True(t, ok, "reported %q, which is not a registered check", name)
		assert.NotZero(t, registered.scope&ScopeStatement, "reported %q, which is not in the scope asked about", name)
	}
}

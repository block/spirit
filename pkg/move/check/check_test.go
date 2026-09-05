package check

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunChecksIsOrderedAndSupportsExclusions(t *testing.T) {
	const (
		first  = "test_order_a"
		second = "test_order_b"
	)
	t.Cleanup(func() {
		lock.Lock()
		defer lock.Unlock()
		delete(checks, first)
		delete(checks, second)
	})

	var ran []string
	registerCheck(second, func(context.Context, Resources, *slog.Logger) error {
		ran = append(ran, second)
		return errors.New("second failed")
	}, ScopeNone)
	registerCheck(first, func(context.Context, Resources, *slog.Logger) error {
		ran = append(ran, first)
		return nil
	}, ScopeNone)

	err := RunChecks(t.Context(), Resources{}, slog.Default(), ScopeNone)
	require.EqualError(t, err, "second failed")
	require.Equal(t, []string{first, second}, ran)

	ran = nil
	require.NoError(t, RunChecks(t.Context(), Resources{}, slog.Default(), ScopeNone, second))
	require.Equal(t, []string{first}, ran)
}

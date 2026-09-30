package utils

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWithCancelGraceOutlivesParentCancel(t *testing.T) {
	parent, cancelParent := context.WithCancel(t.Context())
	ctx, cancel := WithCancelGrace(parent, 100*time.Millisecond)
	defer cancel()
	cancelParent()
	require.Never(t, func() bool { return ctx.Err() != nil }, 50*time.Millisecond, 5*time.Millisecond,
		"derived context must outlive the parent's cancel for the grace period")
	require.Eventually(t, func() bool { return ctx.Err() != nil }, time.Second, 5*time.Millisecond,
		"derived context must end once the grace has elapsed")
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestWithCancelGraceKeepsParentDeadline(t *testing.T) {
	parent, cancelParent := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancelParent()
	ctx, cancel := WithCancelGrace(parent, time.Hour)
	defer cancel()
	want, _ := parent.Deadline()
	got, ok := ctx.Deadline()
	require.True(t, ok)
	require.Equal(t, want, got)
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("derived context did not end at the parent's deadline")
	}
	require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
}

func TestWithCancelGraceNoCancelWhileParentLive(t *testing.T) {
	ctx, cancel := WithCancelGrace(t.Context(), time.Millisecond)
	require.Never(t, func() bool { return ctx.Err() != nil }, 50*time.Millisecond, 5*time.Millisecond)
	cancel()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

// TestWithCancelGraceCancelReleasesGraceGoroutine checks that the returned
// cancel ends the derived context at once and does not leave the grace
// goroutine waiting out its timer.
func TestWithCancelGraceCancelReleasesGraceGoroutine(t *testing.T) {
	parent, cancelParent := context.WithCancel(t.Context())
	ctx, cancel := WithCancelGrace(parent, time.Hour)
	cancelParent()
	require.Eventually(t, func() bool { return graceGoroutines() == 1 }, time.Second, 5*time.Millisecond)
	cancel()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Eventually(t, func() bool { return graceGoroutines() == 0 }, time.Second, 5*time.Millisecond,
		"grace goroutine still waiting on its timer after cancel")
}

// graceGoroutines counts goroutines running WithCancelGrace's AfterFunc.
func graceGoroutines() int {
	buf := make([]byte, 1<<20)
	return strings.Count(string(buf[:runtime.Stack(buf, true)]), "utils.WithCancelGrace.func1(")
}

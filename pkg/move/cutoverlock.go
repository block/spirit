package move

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/utils"
)

// DefaultCutoverLockTimeout is how long a cutover attempt may hold its table
// locks before it reaches the traffic switch, when Move.CutoverLockTimeout is
// zero.
const DefaultCutoverLockTimeout = time.Minute

// errCutoverLockTimeout marks a cutover attempt that held its table locks for
// the cutover lock timeout without reaching the traffic switch. Nothing has
// switched yet, so the attempt releases the locks and may be retried.
var errCutoverLockTimeout = errors.New("cutover lock timeout")

// lockAll takes the table locks lock returns for indexes 0 to n-1 at the same
// time, and returns them indexed the same way. Each LOCK TABLES blocks new
// reads and writes of its tables from the moment it is sent, so taking them
// one after another would block the first server for as long as every later
// one waits. On any failure it releases the locks it took and returns the
// first error; the other attempts are cancelled.
func lockAll(ctx context.Context, n int, lock func(ctx context.Context, i int) (*dbconn.TableLock, error)) ([]*dbconn.TableLock, error) {
	locks := make([]*dbconn.TableLock, n)
	g, gctx := errgroup.WithContext(ctx)
	for i := range n {
		g.Go(func() error {
			l, err := lock(gctx, i)
			if err != nil {
				return err
			}
			locks[i] = l
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		closeLocks(ctx, locks)
		return nil, err
	}
	return locks, nil
}

// closeLocks releases every non-nil lock in locks.
func closeLocks(ctx context.Context, locks []*dbconn.TableLock) {
	for _, l := range locks {
		if l != nil {
			utils.CloseAndLogWithContext(ctx, l)
		}
	}
}

// lockTiming measures one cutover attempt: how long it waited for its table
// locks, how long each step under them took, and how long the tables were
// blocked before traffic switched. log reports it once the locks are released.
type lockTiming struct {
	logger    *slog.Logger
	operation string
	requested time.Time // the first LOCK TABLES was sent
	acquired  time.Time // every lock is held
	last      time.Time // the previous step ended
	switched  time.Time // the traffic switch returned
	steps     []any
}

func newLockTiming(logger *slog.Logger, operation string) *lockTiming {
	now := time.Now()
	return &lockTiming{logger: logger, operation: operation, requested: now, last: now}
}

// locked records that every lock is held.
func (t *lockTiming) locked() {
	t.acquired = time.Now()
	t.last = t.acquired
}

// step records the time since the previous step (or since the locks were
// taken) as name.
func (t *lockTiming) step(name string) {
	now := time.Now()
	t.steps = append(t.steps, slog.Duration(name, now.Sub(t.last)))
	t.last = now
}

// switchedTraffic records the traffic switch as a step and logs how long the
// tables were blocked before it.
func (t *lockTiming) switchedTraffic() {
	t.step("switch")
	t.switched = t.last
	t.logger.Warn(fmt.Sprintf("%s: traffic switched", t.operation),
		"blocked_before_switch", t.switched.Sub(t.requested))
}

// log reports the attempt once its locks are released. err is the attempt's
// result.
func (t *lockTiming) log(err error) {
	now := time.Now()
	attrs := []any{slog.Duration("blocked", now.Sub(t.requested))}
	if t.acquired.IsZero() {
		attrs = append(attrs, slog.Duration("lock_wait", now.Sub(t.requested)))
	} else {
		attrs = append(attrs,
			slog.Duration("lock_wait", t.acquired.Sub(t.requested)),
			slog.Duration("lock_held", now.Sub(t.acquired)))
	}
	if !t.switched.IsZero() {
		attrs = append(attrs, slog.Duration("blocked_before_switch", t.switched.Sub(t.requested)))
	}
	attrs = append(attrs, t.steps...)
	if err != nil {
		attrs = append(attrs, "error", err.Error())
		t.logger.Warn(fmt.Sprintf("%s attempt failed; table locks released", t.operation), attrs...)
		return
	}
	t.logger.Warn(fmt.Sprintf("%s complete; table locks released", t.operation), attrs...)
}

// lockTimeoutError marks err with errCutoverLockTimeout when lockedCtx, the
// context bounded by the cutover lock timeout, has expired while ctx, its
// parent, has not.
func lockTimeoutError(ctx, lockedCtx context.Context, timeout time.Duration, err error) error {
	if ctx.Err() == nil && errors.Is(lockedCtx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%w: held the table locks for %s (--cutover-lock-timeout) without reaching the traffic switch: %w", errCutoverLockTimeout, timeout, err)
	}
	return err
}

# Migration

The `migration` package is used to orchestrate schema changes to one or more tables. Each `change` is tracked in the runner, with the copier and checker largely agnostic that they may be copying multiple tables at once.

There is one change source (`change.Source`) for all changes, and a subscription is added for each table:

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                    SPIRIT MIGRATION                                     │
│                                                                                         │
│ ┌─────────────────────────────────────────────────────────────────────────────────────┐ │
│ │                   RUNNER (Orchestrator) — pkg/migration/runner.go                   │ │
│ │                                                                                     │ │
│ │          Owns and coordinates every component below, across the lifecycle:          │ │
│ │            1. Setup (_new + checkpoint tables)   2. Start change source             │ │
│ │           3. Run copier   4. Disable watermark opt.   5. Initial checksum           │ │
│ │           6. Wait on defer-cutover sentinel    7. Cutover (RENAME TABLE)            │ │
│ └─────────────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                         │
│   ┌────────────────────┐        ┌────────────────────┐        ┌────────────────────┐    │
│   │       COPIER       │        │   CHANGE SOURCE    │        │      CHECKER       │    │
│   │                    │        │   (binlogClient)   │        │     (Checksum)     │    │
│   │  reads the source  │        │   reads binlog;    │        │   verifies that    │    │
│   │  table in chunks   │        │ one sub. per table │        │   source == _new   │    │
│   └──────────┬─────────┘        └──────────┬─────────┘        └────────────────────┘    │
│              │ row images                  │ row images                                 │
│              │ (buffered copier)           │ (always)                                   │
│              └──────────────┬──────────────┘                                            │
│                             ▼                                                           │
│                     ┌───────────────┐                                                   │
│                     │    APPLIER    │ ── REPLACE INTO _new VALUES (…) ──►  _new table   │
│                     │  pkg/applier  │                                                   │
│                     └───────────────┘                                                   │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

The copier and [change source](../change/README.md) are running in parallel during step 3. The only hard requirement is that the change source starts before the copier so we can ensure that all changes are tracked.

The copier is essentially a dirty-copy, in that each chunk does not correctly keep track of changes being made. This is reconciled via the change source, which detects any changes that have been made and applies them to the new table consistently.

Once copying is complete, a [checksum process](../checksum/README.md) is started. This ensures that all data has safely made it to the new table, and it is safe to cutover.

## What parts of the process are locking?

To answer this question, we need to understand that there are two types of locks:

1. **Data locks**, aka InnoDB row-level locks. These are configurable via `innodb_lock_wait_timeout`: the server default is 50s, but spirit overwrites this to 3s.
2. **Metadata locks** (MDL). These are configurable via `lock_wait_timeout`: the server default is 1 year(!), but spirit overwrites this to 30s (configurable by `--lock-wait-timeout`).

**Spirit takes no data locks on the source table.** The copier reads rows into Spirit and writes them to `_new` through the applier (`REPLACE INTO _new VALUES (...)`), and the change source is likewise buffered — it applies binlog row images and never runs `SELECT FROM original`. The checksum's chunk repair works the same way (see [pkg/checksum/README.md](../checksum/README.md#chunk-repair)): it reads the chunk into Spirit and rewrites it through the applier, rather than the `REPLACE INTO _new ... SELECT FROM original` it used historically, whose `SELECT` half did take shared row locks on the source. None of these paths hold shared row locks on the source, so there is no contention with production workloads on hot rows. (The applier's writes do take locks, but only on `_new`, which nothing else touches.) See [pkg/change/README.md](../change/README.md) for the subscription design.

So the locking that *does* apply is **metadata locks**. When we describe Spirit as a "non-blocking schema change tool" that is a bit of a white lie: we don't hold an MDL for the entire 10h schema change, as built-in MySQL DDL often does, but we do need a brief exclusive MDL at a few points:

* Spirit initially attempts INSTANT/INPLACE DDL. If this is compatible, it requires an exclusive metadata lock on the table.
* Starting a checksum requires an initial exclusive metadata lock to ensure that all data is synchronized between the checksum threads.
* The cutover operation requires an exclusive metadata lock.

What causes all metadata lock issues? (hint: it's not spirit)

Any open transactions will have shared metadata locks on any of the tables that you are modifying. If you have long transactions that have not yet committed/rolled back, Spirit's exclusive lock will be queued waiting for them to finish. This then looks like a Spirit problem because any shared lock requests that arrive after Spirit's exclusive lock request will then be queued behind Spirit. So the solution is to keep your transactions as short as possible.

The _fix_ for Spirit is that it will `force-kill` the specific connections that are blocking it from acquiring an exclusive lock. This is always enabled: retrying MDL acquisition over and over while blocked is more dangerous to production systems than targeted killing of the blockers.

## Using Spirit `migration` as a Go package

I will assume that similar to our use-case, you are probably wrapping some sort of automation around Spirit. If this automation is written in Go, I would encourage you to use the Spirit API and not the CLI executable.

The following is a simplification of what we use ourselves:

```go
func (sm *Spirit) Execute(ctx context.Context, m *ExecutableTask) error {
	startTime := time.Now()
	runner, err := migration.NewRunner(&migration.Migration{
		Host:              m.Cluster.Host,
		Username:          m.Cluster.Username,
		Password:          &m.Cluster.Password,
		Database:          m.Cluster.DatabaseName,
		Statement:         m.Statement,
		Threads:           m.Concurrency,
		LockWaitTimeout:   m.LockWaitTimeout,
		InterpolateParams: true,
	})
	if err != nil {
		return errors.Wrap(err, "failed to create spirit migration runner")
	}
	defer runner.Close()
	if m.Metrics != nil {
		runner.SetMetricsSink(m.Metrics)
	}
	sm.Lock()
	sm.progressCallback = func() string {
		return runner.Progress().Summary
	}
	sm.Unlock()
	runner.SetLogger(m.Logger)
	if err = runner.Run(ctx); err != nil {
		return errors.Wrap(err, "failed to run spirit migration")
	}
	m.Logger.Infof("spirit migration completed in %s", time.Since(startTime))
	return nil
}
```

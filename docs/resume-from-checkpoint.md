# Resume from Checkpoint

Spirit supports resuming migrations from a checkpoint. This document explains the checkpoint mechanism, potential failure modes, and how they're handled.

## Checkpoint Data

When Spirit checkpoints (every 50 seconds or on stop), it saves:

```sql
CREATE TABLE _tablename_chkpnt (
    id INT AUTO_INCREMENT PRIMARY KEY,
    copier_watermark TEXT,      -- JSON: where row copy was
    checksum_watermark TEXT,    -- JSON: where checksum was (if applicable)
    binlog_name VARCHAR(255),   -- e.g., "mysql-bin.000042"
    binlog_pos INT,             -- e.g., 4567
    statement TEXT              -- The DDL statement
);
```

## Resume Flow

```
Runner.Run()
    └── setup()
            └── resumeFromCheckpoint()
                    ├── 1. Check _tablename_new table exists
                    ├── 2. Read checkpoint table
                    ├── 3. Validate DDL statement matches
                    ├── 4. setupCopierCheckerAndReplClient()
                    │       ├── Create replClient
                    │       └── AddSubscription() for each table
                    └── 5. replClient.Run() ← Start binlog streaming
```

## Failure Modes

### Binlog Position Too Old

MySQL automatically deletes old binary log files based on `expire_logs_days` or `binlog_expire_logs_seconds`. If the checkpoint's binlog position references a deleted file, `replClient.Run()` fails.

```
Checkpoint saved:
├── binlog_name: "mysql-bin.000042"
└── binlog_pos: 4567

After some time, MySQL expires old logs:
├── mysql-bin.000042 ← DELETED
├── mysql-bin.000043
└── mysql-bin.000044 ← Current

Resume attempt:
└── replClient.Run() fails: "could not find first log file in binary log index"
```

### Volume Change Scenario

Volume changes (adjusting migration speed) trigger a stop/resume cycle:

```
1. User requests volume change
       ↓
2. Spirit Engine: Volume()
       ├── Stop()
       │     ├── DumpCheckpoint() - saves current progress
       │     ├── Cancel context
       │     └── Wait for goroutine to finish
       ├── Update engine settings (threads, chunk time)
       └── Start() - spawns new goroutine
       ↓
3. New Runner created
       ↓
4. Runner.Run() → setup() → resumeFromCheckpoint()
       ↓
5. resumeFromCheckpoint():
       ├── Check new table exists ✓
       ├── Read checkpoint table ✓
       ├── Validate statement matches ✓
       ├── setupCopierCheckerAndReplClient()
       │     ├── Creates replClient
       │     └── Adds subscriptions for all tables
       └── replClient.Run() ← May fail if binlog stale
       ↓
6. If resumeFromCheckpoint() fails:
       ├── Close replClient (cleanup subscriptions) ← IMPORTANT
       └── Call newMigration() to start fresh
       ↓
7. newMigration() → setupCopierCheckerAndReplClient()
       └── AddSubscription() ← Works because old subscriptions cleaned up
```

## Best Practices

1. **Short binlog expiry risk**: If your MySQL has aggressive binlog expiration (e.g., 1 day), avoid stopping migrations for extended periods.

2. **Strict mode**: Use `Strict: true` if you want to prevent silent restarts when checkpoints become invalid.

3. **Monitor checkpoint age**: If resuming an old migration, be aware the binlog position may be stale.

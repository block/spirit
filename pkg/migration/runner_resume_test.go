package migration

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/copier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/status"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

// Test int to bigint primary key while resuming from checkpoint.
func TestChangeIntToBigIntPKResumeFromChkPt(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testChangeIntToBigIntPKResumeFromChkPt(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		if isMinimalRBRTestRunner(t) {
			t.Skip("Skipping buffered copy test because binlog_row_image is not FULL or binlog_row_value_options is not empty")
		}
		testChangeIntToBigIntPKResumeFromChkPt(t, true)
	})
}

func testChangeIntToBigIntPKResumeFromChkPt(t *testing.T, enableBuffered bool) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS bigintpk, _bigintpk_chkpnt, _bigintpk_new`)
	table := `CREATE TABLE bigintpk (
			pk int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
			name varchar(255) NOT NULL,
			b varchar(10) NOT NULL,
            version bigint unsigned NOT NULL DEFAULT '1' COMMENT 'Used for optimistic concurrency.'
		)`
	testutils.RunSQL(t, table)
	// Insert initial data, there needs to be enough that it doesn't just finish
	// the full copy before the first checkpoint can be written.
	testutils.RunSQL(t, "INSERT INTO bigintpk (name, b) VALUES ('a', 'a')")
	testutils.RunSQL(t, `INSERT INTO bigintpk (name, b) SELECT a.name, a.b FROM bigintpk a JOIN bigintpk b JOIN bigintpk c`)
	testutils.RunSQL(t, `INSERT INTO bigintpk (name, b) SELECT a.name, a.b FROM bigintpk a JOIN bigintpk b JOIN bigintpk c`)
	testutils.RunSQL(t, `INSERT INTO bigintpk (name, b) SELECT a.name, a.b FROM bigintpk a JOIN bigintpk b JOIN bigintpk c`)
	testutils.RunSQL(t, `INSERT INTO bigintpk (name, b) SELECT a.name, a.b FROM bigintpk a JOIN bigintpk b JOIN bigintpk c LIMIT 100000`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	m, err := NewRunner(&Migration{
		Host:                           cfg.Addr,
		Username:                       cfg.User,
		Password:                       &cfg.Passwd,
		Database:                       cfg.DBName,
		Threads:                        1,
		EnableExperimentalBufferedCopy: enableBuffered,
		TargetChunkTime:                100 * time.Millisecond,
		Table:                          "bigintpk",
		Alter:                          "modify column pk bigint unsigned not null auto_increment",
	})
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		err := m.Run(ctx)
		assert.Error(t, err) // it gets interrupted as soon as there is a checkpoint saved.
	}()
	// Wait until we are at least copying rows
	// before we dump a checkpoint.
	for m.status.Get() != status.CopyRows {
		time.Sleep(10 * time.Millisecond)
	}
	// The checkpoint may fail initially because of watermark
	// not ready, wait for the first successful one.
	for {
		err = m.DumpCheckpoint(t.Context())
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Between cancel and Close() every resource is freed.
	assert.NoError(t, m.Close())
	cancel()

	// Insert some more dummy data
	testutils.RunSQL(t, "INSERT INTO bigintpk (name,b) VALUES('t', 't')")
	// Start a new migration with the same parameters.
	// Let it complete.
	m2, err := NewRunner(&Migration{
		Host:                           cfg.Addr,
		Username:                       cfg.User,
		Password:                       &cfg.Passwd,
		Database:                       cfg.DBName,
		Threads:                        2,
		EnableExperimentalBufferedCopy: enableBuffered,
		Table:                          "bigintpk",
		Alter:                          "modify column pk bigint unsigned not null auto_increment",
	})
	assert.NoError(t, err)
	assert.NotNil(t, m2)

	err = m2.Run(t.Context())
	assert.NoError(t, err)
	assert.True(t, m2.usedResumeFromCheckpoint)
	assert.NoError(t, m2.Close())
}

func TestCheckpoint(t *testing.T) {
	t.Run("unbuffered", func(t *testing.T) {
		testCheckpoint(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		// This test currently asserts for the buffered copier.
		t.Skip("needs modification to work with buffered copier")
	})
}

func testCheckpoint(t *testing.T, enableBuffered bool) {
	tbl := `CREATE TABLE cpt1 (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		id2 INT NOT NULL,
		pad VARCHAR(100) NOT NULL default 0)`
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS cpt1, _cpt1_new, _cpt1_chkpnt`)
	testutils.RunSQL(t, tbl)
	testutils.RunSQL(t, `insert into cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM dual`)
	testutils.RunSQL(t, `insert into cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1`)
	testutils.RunSQL(t, `insert into cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1 a JOIN cpt1 b JOIN cpt1 c`)
	testutils.RunSQL(t, `insert into cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1 a JOIN cpt1 b JOIN cpt1 c`)
	testutils.RunSQL(t, `insert into cpt1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1 a JOIN cpt1 LIMIT 100000`) // ~100k rows

	preSetup := func() *Runner {
		r, err := NewRunner(&Migration{
			Host:                           cfg.Addr,
			Username:                       cfg.User,
			Password:                       &cfg.Passwd,
			Database:                       cfg.DBName,
			Threads:                        2,
			Table:                          "cpt1",
			EnableExperimentalBufferedCopy: enableBuffered,
			Alter:                          "ENGINE=InnoDB",
		})
		assert.NoError(t, err)
		assert.Equal(t, "initial", r.status.Get().String())
		// Usually we would call r.Run() but we want to step through
		// the migration process manually.
		// Set up both connections: poolDB for worker operations and db for management
		r.poolDB, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
		assert.NoError(t, err)
		r.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
		assert.NoError(t, err)
		r.dbConfig = dbconn.NewDBConfig()

		// Get Table Info
		r.changes[0].table = table.NewTableInfo(r.db, r.migration.Database, r.migration.Table)
		err = r.changes[0].table.SetInfo(t.Context())
		assert.NoError(t, err)
		assert.NoError(t, r.changes[0].dropOldTable(t.Context()))
		return r
	}
	r := preSetup()
	// migrationRunner.Run usually calls r.Setup() here.
	// Which first checks if the table can be restored from checkpoint.
	// Because this is the first run, it can't.
	assert.Error(t, r.resumeFromCheckpoint(t.Context()))
	// So we proceed with the initial steps.
	assert.NoError(t, r.newMigration(t.Context()))

	// Now we are ready to start copying rows.
	// Instead of calling r.copyRows() we will step through it manually.
	// Since we want to checkpoint after a few chunks.

	r.status.Set(status.CopyRows)
	assert.Equal(t, "copyRows", r.status.Get().String())

	assert.Contains(t, r.Status(), `migration status: state=copyRows copy-progress=0/101040 0.00% binlog-deltas=0`)

	// first chunk.
	chunk1, err := r.copyChunker.Next()
	assert.NoError(t, err)

	chunk2, err := r.copyChunker.Next()
	assert.NoError(t, err)

	chunk3, err := r.copyChunker.Next()
	assert.NoError(t, err)

	// Assert there is no watermark yet, because we've not finished
	// copying any of the chunks.
	_, err = r.copyChunker.GetLowWatermark()
	assert.Error(t, err)
	// Dump checkpoint also returns an error for the same reason.
	assert.Error(t, r.DumpCheckpoint(t.Context()))

	ccopier, ok := r.copier.(*copier.Unbuffered)
	assert.True(t, ok)

	// Because it's multi-threaded, we can't guarantee the order of the chunks.
	// Let's complete them in the order of 2, 1, 3. When 2 phones home first
	// it should be queued. Then when 1 phones home it should apply and de-queue 2.
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk2))
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk1))
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk3))

	time.Sleep(time.Second) // wait for status to be updated.
	assert.Contains(t, r.Status(), `migration status: state=copyRows copy-progress=3000/101040 2.97% binlog-deltas=0`)

	// The watermark should exist now, because migrateChunk()
	// gives feedback back to table.
	watermark, err := r.copyChunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"1001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2001\"],\"Inclusive\":false}}", watermark)
	// Dump a checkpoint
	assert.NoError(t, r.DumpCheckpoint(t.Context()))

	// Clean up first runner
	assert.NoError(t, r.Close())

	// Now lets imagine that everything fails and we need to start
	// from checkpoint again.

	r = preSetup()
	defer r.Close()
	// Start the binary log feed just before copy rows starts.
	// replClient.Run() is already called in resumeFromCheckpoint.
	assert.NoError(t, r.resumeFromCheckpoint(t.Context()))
	// This opens the table at the checkpoint (table.OpenAtWatermark())
	// which sets the chunkPtr at the LowerBound. It also has to position
	// the watermark to this point so new watermarks "align" correctly.
	// So lets now call NextChunk to verify.

	ccopier, ok = r.copier.(*copier.Unbuffered)
	assert.True(t, ok)

	chunk, err := r.copyChunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "1001", chunk.LowerBound.Value[0].String())
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk))

	// It's ideally not typical but you can still dump checkpoint from
	// a restored checkpoint state. We won't have advanced anywhere from
	// the last checkpoint because on restore, the LowerBound is taken.
	watermark, err = r.copyChunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"1001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2001\"],\"Inclusive\":false}}", watermark)
	// Dump a checkpoint
	assert.NoError(t, r.DumpCheckpoint(t.Context()))

	// Let's confirm we do advance the watermark.
	for range 10 {
		chunk, err = r.copyChunker.Next()
		assert.NoError(t, err)
		assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk))
	}

	watermark, err = r.copyChunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"11001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"12001\"],\"Inclusive\":false}}", watermark)
}

func TestCheckpointRestore(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testCheckpointRestore(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		if isMinimalRBRTestRunner(t) {
			t.Skip("Skipping buffered copy test because binlog_row_image is not FULL or binlog_row_value_options is not empty")
		}
		testCheckpointRestore(t, true)
	})
}

func testCheckpointRestore(t *testing.T, enableBuffered bool) {
	tbl := `CREATE TABLE cpt2 (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		id2 INT NOT NULL,
		pad VARCHAR(100) NOT NULL default 0)`
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS cpt2, _cpt2_new, _cpt2_chkpnt`)
	testutils.RunSQL(t, tbl)

	r, err := NewRunner(&Migration{
		Host:                           cfg.Addr,
		Username:                       cfg.User,
		Password:                       &cfg.Passwd,
		Database:                       cfg.DBName,
		Threads:                        2,
		EnableExperimentalBufferedCopy: enableBuffered,
		Table:                          "cpt2",
		Alter:                          "ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	assert.Equal(t, "initial", r.status.Get().String())
	// Usually we would call r.Run() but we want to step through
	// the migration process manually.
	// Set up both connections: poolDB for worker operations and db for management
	r.poolDB, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	r.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	r.dbConfig = dbconn.NewDBConfig()
	// Get Table Info
	r.changes[0].table = table.NewTableInfo(r.db, r.migration.Database, r.migration.Table)
	err = r.changes[0].table.SetInfo(t.Context())
	assert.NoError(t, err)
	assert.NoError(t, r.changes[0].dropOldTable(t.Context()))

	// Proceed with the initial steps.
	assert.NoError(t, r.newMigration(t.Context()))

	// Now insert a fake checkpoint, this uses a known bad value
	// from issue #125
	watermark := "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\":[\"53926425\"],\"Inclusive\":true},\"UpperBound\":{\"Value\":[\"53926425\"],\"Inclusive\":false}}"
	binlog := r.replClient.GetBinlogApplyPosition()
	err = dbconn.Exec(t.Context(), r.db, `INSERT INTO %n.%n
	(copier_watermark, checksum_watermark, binlog_name, binlog_pos, statement)
	VALUES
	(%?,  %?, %?, %?, %?)`,
		r.checkpointTable.SchemaName,
		r.checkpointTable.TableName,
		watermark,
		"",
		binlog.Name,
		binlog.Pos,
		r.migration.Statement,
	)
	assert.NoError(t, err)
	assert.NoError(t, r.Close())

	r2, err := NewRunner(&Migration{
		Host:                           cfg.Addr,
		Username:                       cfg.User,
		Password:                       &cfg.Passwd,
		Database:                       cfg.DBName,
		Threads:                        2,
		EnableExperimentalBufferedCopy: enableBuffered,
		Table:                          "cpt2",
		Alter:                          "ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	err = r2.Run(t.Context())
	assert.NoError(t, err)
	assert.True(t, r2.usedResumeFromCheckpoint)
	assert.NoError(t, r2.Close())
}

// https://github.com/block/spirit/issues/381
func TestCheckpointRestoreBinaryPK(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testCheckpointRestoreBinaryPK(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		// This test currently asserts for the buffered copier.
		t.Skip("needs modification to work with buffered copier")
	})
}

func testCheckpointRestoreBinaryPK(t *testing.T, enableBuffered bool) {
	ctx := t.Context()
	tbl := `CREATE TABLE binarypk (
 main_id varbinary(16) NOT NULL,
 sub_id varchar(36) CHARACTER SET latin1 COLLATE latin1_swedish_ci GENERATED ALWAYS AS (jsonbody->>'$._id') STORED NOT NULL,
 jsonbody json NOT NULL,
 PRIMARY KEY (main_id,sub_id)
);`
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	testutils.RunSQL(t, `DROP TABLE IF EXISTS binarypk, _binarypk_new, _binarypk_chkpnt`)
	testutils.RunSQL(t, tbl)
	testutils.RunSQL(t, `INSERT INTO binarypk (main_id, jsonbody) SELECT RANDOM_BYTES(16), JSON_OBJECT('_id', "0xabc", 'name', 'bbb', 'randombytes', HEX(RANDOM_BYTES(1024))) from dual`)
	testutils.RunSQL(t, `INSERT INTO binarypk (main_id, jsonbody) SELECT RANDOM_BYTES(16), JSON_OBJECT('_id', "0xabc", 'name', 'bbb', 'randombytes', HEX(RANDOM_BYTES(1024))) from binarypk a JOIN binarypk b JOIN binarypk c LIMIT 10000;`)
	testutils.RunSQL(t, `INSERT INTO binarypk (main_id, jsonbody) SELECT RANDOM_BYTES(16), JSON_OBJECT('_id', "0xabc", 'name', 'bbb', 'randombytes', HEX(RANDOM_BYTES(1024))) from binarypk a JOIN binarypk b JOIN binarypk c LIMIT 10000;`)
	testutils.RunSQL(t, `INSERT INTO binarypk (main_id, jsonbody) SELECT RANDOM_BYTES(16), JSON_OBJECT('_id', "0xabc", 'name', 'bbb', 'randombytes', HEX(RANDOM_BYTES(1024))) from binarypk a JOIN binarypk b JOIN binarypk c LIMIT 10000;`)
	testutils.RunSQL(t, `INSERT INTO binarypk (main_id, jsonbody) SELECT RANDOM_BYTES(16), JSON_OBJECT('_id', "0xabc", 'name', 'bbb', 'randombytes', HEX(RANDOM_BYTES(1024))) from binarypk a JOIN binarypk b JOIN binarypk c LIMIT 10000;`)

	r, err := NewRunner(&Migration{
		Host:                           cfg.Addr,
		Username:                       cfg.User,
		Password:                       &cfg.Passwd,
		Database:                       cfg.DBName,
		Threads:                        1,
		EnableExperimentalBufferedCopy: enableBuffered,
		Table:                          "binarypk",
		Alter:                          "ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	assert.Equal(t, "initial", r.status.Get().String())
	// Usually we would call r.Run() but we want to step through
	// the migration process manually.
	r.poolDB, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	r.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	r.dbConfig = dbconn.NewDBConfig()
	// Get Table Info
	r.changes[0].table = table.NewTableInfo(r.db, r.migration.Database, r.migration.Table)
	err = r.changes[0].table.SetInfo(ctx)
	assert.NoError(t, err)
	assert.NoError(t, r.changes[0].dropOldTable(ctx))

	assert.NoError(t, r.newMigration(t.Context()))

	ccopier, ok := r.copier.(*copier.Unbuffered)
	assert.True(t, ok)

	for range 3 {
		chunk, err := r.copyChunker.Next()
		assert.NoError(t, err)
		assert.NoError(t, ccopier.CopyChunk(ctx, chunk))
	}
	// Dump checkpoint and close runner.
	assert.NoError(t, r.DumpCheckpoint(t.Context()))
	assert.NoError(t, r.Close())
	// Try and resume and then check if we used a checkpoint
	// for resuming.
	r2, err := NewRunner(&Migration{
		Host:                           cfg.Addr,
		Username:                       cfg.User,
		Password:                       &cfg.Passwd,
		Database:                       cfg.DBName,
		Threads:                        2,
		EnableExperimentalBufferedCopy: enableBuffered,
		Table:                          "binarypk",
		Alter:                          "ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	err = r2.Run(t.Context())
	assert.NoError(t, err)
	assert.True(t, r2.usedResumeFromCheckpoint) // managed to resume.
	assert.NoError(t, r2.Close())
}

func TestCheckpointResumeDuringChecksum(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testCheckpointResumeDuringChecksum(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		if isMinimalRBRTestRunner(t) {
			t.Skip("Skipping buffered copy test because binlog_row_image is not FULL or binlog_row_value_options is not empty")
		}
		testCheckpointResumeDuringChecksum(t, true)
	})
}

func testCheckpointResumeDuringChecksum(t *testing.T, enableBuffered bool) {
	// Create unique database for this test
	dbName := testutils.CreateUniqueTestDatabase(t)

	tbl := `CREATE TABLE cptresume (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		id2 INT NOT NULL,
		pad VARCHAR(100) NOT NULL default 0)`
	cfg, err := mysql.ParseDSN(testutils.DSNForDatabase(dbName))
	assert.NoError(t, err)
	testutils.RunSQLInDatabase(t, dbName, `DROP TABLE IF EXISTS cptresume, _cptresume_new, _cptresume_chkpnt`)
	testutils.RunSQLInDatabase(t, dbName, tbl)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE _spirit_sentinel (id INT NOT NULL PRIMARY KEY)`)
	testutils.RunSQLInDatabase(t, dbName, `insert into cptresume (id2,pad) SELECT 1, REPEAT('a', 100) FROM dual`)
	testutils.RunSQLInDatabase(t, dbName, `insert into cptresume (id2,pad) SELECT 1, REPEAT('a', 100) FROM cptresume`)
	testutils.RunSQLInDatabase(t, dbName, `insert into cptresume (id2,pad) SELECT 1, REPEAT('a', 100) FROM cptresume a JOIN cptresume b JOIN cptresume c`)

	r, err := NewRunner(&Migration{
		Host:                           cfg.Addr,
		Username:                       cfg.User,
		Password:                       &cfg.Passwd,
		Database:                       cfg.DBName,
		Threads:                        4,
		EnableExperimentalBufferedCopy: enableBuffered,
		TargetChunkTime:                100 * time.Millisecond,
		Table:                          "cptresume",
		Alter:                          "ENGINE=InnoDB",
		RespectSentinel:                true,
	})
	assert.NoError(t, err)

	// Call r.Run() with our context in a go-routine.
	// When we see that we are waiting on the sentinel table,
	// we then manually start the first bits of checksum, and then close()
	// We should be able to resume from the checkpoint into the checksum state.
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		err := r.Run(ctx)
		assert.Error(t, err) // context cancelled
	}()
	for r.status.Get() < status.WaitingOnSentinelTable {
		// Wait for the sentinel table.
		time.Sleep(time.Millisecond)
	}

	assert.NoError(t, r.checksum(t.Context()))       // run the checksum, the original Run is blocked on sentinel.
	assert.NoError(t, r.DumpCheckpoint(t.Context())) // dump a checkpoint with the watermark.
	assert.NoError(t, r.Close())                     // close the run first to avoid race conditions.
	cancel()                                         // unblock the original waiting on sentinel.

	// drop the sentinel table.
	testutils.RunSQLInDatabase(t, dbName, `DROP TABLE _spirit_sentinel`)

	// insert a couple more rows (should not change anything)
	testutils.RunSQLInDatabase(t, dbName, `insert into cptresume (id2,pad) SELECT 1, REPEAT('b', 100) FROM dual`)
	testutils.RunSQLInDatabase(t, dbName, `insert into cptresume (id2,pad) SELECT 1, REPEAT('c', 100) FROM dual`)

	// Start again as a new runner,
	r2, err := NewRunner(&Migration{
		Host:                           cfg.Addr,
		Username:                       cfg.User,
		Password:                       &cfg.Passwd,
		Database:                       cfg.DBName,
		Threads:                        4,
		EnableExperimentalBufferedCopy: enableBuffered,
		TargetChunkTime:                100 * time.Millisecond,
		Table:                          "cptresume",
		Alter:                          "ENGINE=InnoDB",
	})
	assert.NoError(t, err)
	err = r2.Run(t.Context())
	assert.NoError(t, err)
	defer r2.Close()
	assert.True(t, r2.usedResumeFromCheckpoint)
}

func TestCheckpointDifferentRestoreOptions(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testCheckpointDifferentRestoreOptions(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		// This test currently asserts for the buffered copier.
		t.Skip("needs modification to work with buffered copier")
	})
}

func testCheckpointDifferentRestoreOptions(t *testing.T, enableBuffered bool) {
	tbl := `CREATE TABLE cpt1difft1 (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		id2 INT NOT NULL,
		pad VARCHAR(100) NOT NULL default 0)`

	testutils.RunSQL(t, `DROP TABLE IF EXISTS cpt1difft1, cpt1difft1_new, _cpt1difft1_chkpnt`)
	testutils.RunSQL(t, tbl)
	testutils.RunSQL(t, `insert into cpt1difft1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM dual`)
	testutils.RunSQL(t, `insert into cpt1difft1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1difft1`)
	testutils.RunSQL(t, `insert into cpt1difft1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1difft1 a JOIN cpt1difft1 b JOIN cpt1difft1 c`)
	testutils.RunSQL(t, `insert into cpt1difft1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1difft1 a JOIN cpt1difft1 b JOIN cpt1difft1 c`)
	testutils.RunSQL(t, `insert into cpt1difft1 (id2,pad) SELECT 1, REPEAT('a', 100) FROM cpt1difft1 a JOIN cpt1difft1 LIMIT 100000`) // ~100k rows
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	preSetup := func(alter string) *Runner {
		m, err := NewRunner(&Migration{
			Host:                           cfg.Addr,
			Username:                       cfg.User,
			Password:                       &cfg.Passwd,
			Database:                       cfg.DBName,
			Threads:                        2,
			EnableExperimentalBufferedCopy: enableBuffered,
			Table:                          "cpt1difft1",
			Alter:                          alter,
		})
		assert.NoError(t, err)
		assert.Equal(t, "initial", m.status.Get().String())
		// Usually we would call m.Run() but we want to step through
		// the migration process manually.
		// Set up both connections: poolDB for worker operations and db for management
		m.poolDB, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
		assert.NoError(t, err)
		m.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
		assert.NoError(t, err)
		m.dbConfig = dbconn.NewDBConfig()
		// Get Table Info
		m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.migration.Table)
		err = m.changes[0].table.SetInfo(t.Context())
		assert.NoError(t, err)
		assert.NoError(t, m.changes[0].dropOldTable(t.Context()))
		return m
	}

	m := preSetup("ADD COLUMN id3 INT NOT NULL DEFAULT 0, ADD INDEX(id2)")
	// migrationRunner.Run usually calls m.Setup() here.
	// Which first checks if the table can be restored from checkpoint.
	// Because this is the first run, it can't.

	assert.Error(t, m.resumeFromCheckpoint(t.Context()))

	assert.NoError(t, m.newMigration(t.Context()))

	// Now we are ready to start copying rows.
	// Instead of calling m.copyRows() we will step through it manually.
	// Since we want to checkpoint after a few chunks.

	// m.copier.StartTime = time.Now()
	m.status.Set(status.CopyRows)
	assert.Equal(t, "copyRows", m.status.Get().String())

	// first chunk.
	chunk1, err := m.copyChunker.Next()
	assert.NoError(t, err)

	chunk2, err := m.copyChunker.Next()
	assert.NoError(t, err)

	chunk3, err := m.copyChunker.Next()
	assert.NoError(t, err)

	// There is no watermark yet.
	_, err = m.copyChunker.GetLowWatermark()
	assert.Error(t, err)
	// Dump checkpoint also returns an error for the same reason.
	assert.Error(t, m.DumpCheckpoint(t.Context()))

	ccopier, ok := m.copier.(*copier.Unbuffered)
	assert.True(t, ok)

	// Because it's multi-threaded, we can't guarantee the order of the chunks.
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk2))
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk1))
	assert.NoError(t, ccopier.CopyChunk(t.Context(), chunk3))

	// The watermark should exist now, because migrateChunk()
	// gives feedback back to table.

	watermark, err := m.copyChunker.GetLowWatermark()
	assert.NoError(t, err)
	assert.JSONEq(t, "{\"Key\":[\"id\"],\"ChunkSize\":1000,\"LowerBound\":{\"Value\": [\"1001\"],\"Inclusive\":true},\"UpperBound\":{\"Value\": [\"2001\"],\"Inclusive\":false}}", watermark)
	// Dump a checkpoint
	assert.NoError(t, m.DumpCheckpoint(t.Context()))

	// Close m
	assert.NoError(t, m.Close())

	// Now lets imagine that everything fails and we need to start
	// from checkpoint again.

	m = preSetup("ADD COLUMN id4 INT NOT NULL DEFAULT 0, ADD INDEX(id2)")
	assert.Error(t, m.resumeFromCheckpoint(t.Context())) // it should error because the ALTER does not match.
	assert.NoError(t, m.Close())
}

func TestResumeFromCheckpointE2E(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testResumeFromCheckpointE2E(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		if isMinimalRBRTestRunner(t) {
			t.Skip("Skipping buffered copy test because binlog_row_image is not FULL or binlog_row_value_options is not empty")
		}
		testResumeFromCheckpointE2E(t, true)
	})
}

func testResumeFromCheckpointE2E(t *testing.T, enableBuffered bool) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS chkpresumetest, _chkpresumetest_old, _chkpresumetest_chkpnt`)
	table := `CREATE TABLE chkpresumetest (
		id int(11) NOT NULL AUTO_INCREMENT,
		pad varbinary(1024) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	// Insert dummy data.
	testutils.RunSQL(t, "INSERT INTO chkpresumetest (pad) SELECT RANDOM_BYTES(1024) FROM dual")
	testutils.RunSQL(t, "INSERT INTO chkpresumetest (pad) SELECT RANDOM_BYTES(1024) FROM chkpresumetest a, chkpresumetest b, chkpresumetest c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO chkpresumetest (pad) SELECT RANDOM_BYTES(1024) FROM chkpresumetest a, chkpresumetest b, chkpresumetest c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO chkpresumetest (pad) SELECT RANDOM_BYTES(1024) FROM chkpresumetest a, chkpresumetest b, chkpresumetest c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO chkpresumetest (pad) SELECT RANDOM_BYTES(1024) FROM chkpresumetest a, chkpresumetest b, chkpresumetest c LIMIT 100000")
	alterSQL := "ADD INDEX(pad);"
	// use as slow as possible here: we want the copy to be still running
	// when we kill it once we have a checkpoint saved.
	migration := &Migration{
		Host:                           cfg.Addr,
		Username:                       cfg.User,
		Password:                       &cfg.Passwd,
		Database:                       cfg.DBName,
		Threads:                        1,
		EnableExperimentalBufferedCopy: enableBuffered,
		Table:                          "chkpresumetest",
		Alter:                          alterSQL,
		TargetChunkTime:                100 * time.Millisecond,
	}

	runner, err := NewRunner(migration)
	assert.NoError(t, err)

	go func() {
		err := runner.Run(t.Context())
		assert.Error(t, err) // it gets interrupted as soon as there is a checkpoint saved.
	}()

	// wait until a checkpoint is saved (which means copy is in progress)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()
	for {
		var rowCount int
		err = db.QueryRowContext(t.Context(), `SELECT count(*) from _chkpresumetest_chkpnt`).Scan(&rowCount)
		if err != nil {
			continue // table does not exist yet
		}
		if rowCount > 0 {
			break
		}
	}
	// Close() before cancelFunc() to avoid race conditions.
	assert.NoError(t, runner.Close())
	runner.cancelFunc()

	// Insert some more dummy data
	testutils.RunSQL(t, "INSERT INTO chkpresumetest (pad) SELECT RANDOM_BYTES(1024) FROM chkpresumetest LIMIT 1000")
	// Start a new migration with the same parameters.
	// Let it complete.
	newmigration := &Migration{
		Host:                           cfg.Addr,
		Username:                       cfg.User,
		Password:                       &cfg.Passwd,
		Database:                       cfg.DBName,
		Threads:                        4,
		EnableExperimentalBufferedCopy: enableBuffered,
		Table:                          "chkpresumetest",
		Alter:                          alterSQL,
		TargetChunkTime:                5 * time.Second,
	}
	m, err := NewRunner(newmigration)
	assert.NoError(t, err)
	assert.NotNil(t, m)

	err = m.Run(t.Context())
	assert.NoError(t, err)
	assert.True(t, m.usedResumeFromCheckpoint)
	assert.NoError(t, m.Close())
}

func TestResumeFromCheckpointE2ECompositeVarcharPK(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testResumeFromCheckpointE2ECompositeVarcharPK(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		if isMinimalRBRTestRunner(t) {
			t.Skip("Skipping buffered copy test because binlog_row_image is not FULL or binlog_row_value_options is not empty")
		}
		testResumeFromCheckpointE2ECompositeVarcharPK(t, true)
	})
}

func testResumeFromCheckpointE2ECompositeVarcharPK(t *testing.T, enableBuffered bool) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS compositevarcharpk, _compositevarcharpk_chkpnt`)
	testutils.RunSQL(t, `CREATE TABLE compositevarcharpk (
  token varchar(128) NOT NULL,
  version varchar(255) NOT NULL,
  state varchar(255) NOT NULL,
  source varchar(128) NOT NULL,
  created_at datetime(3) NOT NULL,
  updated_at datetime(3) NOT NULL,
  PRIMARY KEY (token,version)
	);`)
	testutils.RunSQL(t, `INSERT INTO compositevarcharpk VALUES
 (HEX(RANDOM_BYTES(60)), '1', 'active', 'test', NOW(3), NOW(3))`)
	testutils.RunSQL(t, `INSERT INTO compositevarcharpk SELECT
 HEX(RANDOM_BYTES(60)), '1', 'active', 'test', NOW(3), NOW(3)
FROM compositevarcharpk a JOIN compositevarcharpk b JOIN compositevarcharpk c LIMIT 10000`)
	testutils.RunSQL(t, `INSERT INTO compositevarcharpk SELECT
 HEX(RANDOM_BYTES(60)), '1', 'active', 'test', NOW(3), NOW(3)
FROM compositevarcharpk a JOIN compositevarcharpk b JOIN compositevarcharpk c LIMIT 10000`)
	testutils.RunSQL(t, `INSERT INTO compositevarcharpk SELECT
 HEX(RANDOM_BYTES(60)), '1', 'active', 'test', NOW(3), NOW(3)
FROM compositevarcharpk a JOIN compositevarcharpk b JOIN compositevarcharpk c LIMIT 10000`)
	testutils.RunSQL(t, `INSERT INTO compositevarcharpk SELECT
 HEX(RANDOM_BYTES(60)), '1', 'active', 'test', NOW(3), NOW(3)
FROM compositevarcharpk a JOIN compositevarcharpk b JOIN compositevarcharpk c LIMIT 10000`)
	testutils.RunSQL(t, `INSERT INTO compositevarcharpk SELECT
 a.token, '2', 'active', 'test', NOW(3), NOW(3)
FROM compositevarcharpk a WHERE version='1'`)

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	migration := &Migration{
		Host:                           cfg.Addr,
		Username:                       cfg.User,
		Password:                       &cfg.Passwd,
		Database:                       cfg.DBName,
		Threads:                        1,
		EnableExperimentalBufferedCopy: enableBuffered,
		Table:                          "compositevarcharpk",
		Alter:                          "ENGINE=InnoDB",
		TargetChunkTime:                100 * time.Millisecond,
	}
	runner, err := NewRunner(migration)
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		err := runner.Run(ctx)
		assert.Error(t, err) // it gets interrupted as soon as there is a checkpoint saved.
	}()

	// wait until a checkpoint is saved (which means copy is in progress)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()
	for {
		var rowCount int
		err = db.QueryRowContext(t.Context(), `SELECT count(*) from _compositevarcharpk_chkpnt`).Scan(&rowCount)
		if err != nil {
			continue // table does not exist yet
		}
		if rowCount > 0 {
			break
		}
	}
	// Close() before cancel() to avoid race conditions.
	assert.NoError(t, runner.Close())
	cancel()

	newmigration := &Migration{
		Host:                           cfg.Addr,
		Username:                       cfg.User,
		Password:                       &cfg.Passwd,
		Database:                       cfg.DBName,
		Threads:                        2,
		EnableExperimentalBufferedCopy: enableBuffered,
		Table:                          "compositevarcharpk",
		Alter:                          "ENGINE=InnoDB",
		TargetChunkTime:                5 * time.Second,
	}
	m2, err := NewRunner(newmigration)
	assert.NoError(t, err)
	assert.NotNil(t, m2)

	err = m2.Run(t.Context())
	assert.NoError(t, err)
	assert.True(t, m2.usedResumeFromCheckpoint)
	assert.NoError(t, m2.Close())
}

func TestResumeFromCheckpointStrict(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testResumeFromCheckpointStrict(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		if isMinimalRBRTestRunner(t) {
			t.Skip("Skipping buffered copy test because binlog_row_image is not FULL or binlog_row_value_options is not empty")
		}
		testResumeFromCheckpointStrict(t, true)
	})
}

func testResumeFromCheckpointStrict(t *testing.T, enableBuffered bool) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS resumestricttest, _resumestricttest_old, _resumestricttest_chkpnt`)
	table := `CREATE TABLE resumestricttest (
		id int(11) NOT NULL AUTO_INCREMENT,
		pad varbinary(1024) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)

	// Insert dummy data.
	testutils.RunSQL(t, "INSERT INTO resumestricttest (pad) SELECT RANDOM_BYTES(1024) FROM dual")
	testutils.RunSQL(t, "INSERT INTO resumestricttest (pad) SELECT RANDOM_BYTES(1024) FROM resumestricttest a, resumestricttest b, resumestricttest c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO resumestricttest (pad) SELECT RANDOM_BYTES(1024) FROM resumestricttest a, resumestricttest b, resumestricttest c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO resumestricttest (pad) SELECT RANDOM_BYTES(1024) FROM resumestricttest a, resumestricttest b, resumestricttest c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO resumestricttest (pad) SELECT RANDOM_BYTES(1024) FROM resumestricttest a, resumestricttest b, resumestricttest c LIMIT 100000")

	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	alterSQL := "ADD INDEX(pad);"

	migration := &Migration{
		Host:                           cfg.Addr,
		Username:                       cfg.User,
		Password:                       &cfg.Passwd,
		Database:                       cfg.DBName,
		Threads:                        1,
		EnableExperimentalBufferedCopy: enableBuffered,
		Table:                          "resumestricttest",
		Alter:                          alterSQL,
		TargetChunkTime:                100 * time.Millisecond,
		Strict:                         true,
	}

	// Kick off a migration with --strict enabled and let it run until the first checkpoint is available

	ctx, cancel := context.WithCancel(t.Context())

	runner, err := NewRunner(migration)
	assert.NoError(t, err)

	go func() {
		err := runner.Run(ctx)
		assert.Error(t, err) // it gets interrupted as soon as there is a checkpoint saved.
	}()

	// wait until a checkpoint is saved (which means copy is in progress)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()
	for {
		var rowCount int
		err = db.QueryRowContext(t.Context(), `SELECT count(*) from _resumestricttest_chkpnt`).Scan(&rowCount)
		if err != nil {
			continue // table does not exist yet
		}
		if rowCount > 0 {
			break
		}
	}
	// Close() before cancel() to avoid race conditions.
	assert.NoError(t, runner.Close())
	cancel()

	// Insert some more dummy data
	testutils.RunSQL(t, "INSERT INTO resumestricttest (pad) SELECT RANDOM_BYTES(1024) FROM resumestricttest LIMIT 1000")

	// Start a _different_ migration on the same table. We don't expect this to work when --strict is enabled
	// since the --alter doesn't match what is recorded in the checkpoint table

	migrationB := &Migration{
		Host:                           migration.Host,
		Username:                       migration.Username,
		Password:                       migration.Password,
		Database:                       migration.Database,
		Threads:                        migration.Threads,
		EnableExperimentalBufferedCopy: enableBuffered,
		Table:                          migration.Table,
		Alter:                          "ENGINE=INNODB",
		TargetChunkTime:                migration.TargetChunkTime,
		Strict:                         migration.Strict,
	}

	runner, err = NewRunner(migrationB)
	assert.NoError(t, err)

	err = runner.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorIs(t, err, status.ErrMismatchedAlter)

	assert.NoError(t, runner.Close())

	// We should be able to force the migration to run even though there's a mismatched --alter
	// by disabling --strict

	migrationB.Strict = false
	migrationB.Threads = 4    // to make the test run faster
	migrationB.Statement = "" // reset for validation

	runner, err = NewRunner(migrationB)
	assert.NoError(t, err)

	err = runner.Run(t.Context())
	assert.NoError(t, err)
	assert.False(t, runner.usedResumeFromCheckpoint)

	assert.NoError(t, runner.Close())
}

// TestResumeFromCheckpointPhantom tests that there is not a phantom row issue
// when resuming from checkpoint. i.e. consider the following scenario:
// 1) A new row is inserted at the end of the table, and the copier copies it.. but the low watermark never advances past this point
// 2) The row is then deleted after it’s been copied (but the binary log doesn't get to this point)
// 3) A resume occurs
// 4) The insert and delete tracking ignore the row because it’s above the high watermark.
// 5) The INSERT..SELECT only inserts new rows, it doesn't delete non-conflicting existing rows.
// This leaves a broken state because the _new table has a row that should have been deleted.
//
// The fix for this is simple:
// - When resuming from checkpoint, we need to initialize the high watermark from a SELECT MAX(key) FROM the _new table.
// - If this is done correctly, then on resume the DELETE will no longer be ignored.
func TestResumeFromCheckpointPhantom(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testResumeFromCheckpointPhantom(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		// This test currently asserts for the buffered copier.
		t.Skip("needs modification to work with buffered copier")
	})
}

func testResumeFromCheckpointPhantom(t *testing.T, enableBuffered bool) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS phantomtest, _phantomtest_old, _phantomtest_chkpnt`)
	tbl := `CREATE TABLE phantomtest (
		id int(11) NOT NULL AUTO_INCREMENT,
		pad varbinary(1024) NOT NULL,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, tbl)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	// Insert dummy data.
	testutils.RunSQL(t, "INSERT INTO phantomtest (pad) SELECT RANDOM_BYTES(1024) FROM dual")
	testutils.RunSQL(t, "INSERT INTO phantomtest (pad) SELECT RANDOM_BYTES(1024) FROM phantomtest a, phantomtest b, phantomtest c LIMIT 100000")
	testutils.RunSQL(t, "INSERT INTO phantomtest (pad) SELECT RANDOM_BYTES(1024) FROM phantomtest a, phantomtest b, phantomtest c LIMIT 100000")

	m, err := NewRunner(&Migration{
		Host:                           cfg.Addr,
		Username:                       cfg.User,
		Password:                       &cfg.Passwd,
		Database:                       cfg.DBName,
		Threads:                        2,
		EnableExperimentalBufferedCopy: enableBuffered,
		Table:                          "phantomtest",
		Alter:                          "ENGINE=InnoDB",
		TargetChunkTime:                100 * time.Millisecond,
	})
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())

	// Do the initial setup.
	// Set up both connections: poolDB for worker operations and db for management
	m.poolDB, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	m.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	m.dbConfig = dbconn.NewDBConfig()
	m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.migration.Table)
	assert.NoError(t, m.changes[0].table.SetInfo(ctx))

	assert.NoError(t, m.newMigration(t.Context()))

	// Now we are ready to start copying rows.
	// We step through this manually using the unbuffered copier, since we want
	// to checkpoint after a few chunks.

	copier, ok := m.copier.(*copier.Unbuffered)
	assert.True(t, ok)

	m.status.Set(status.CopyRows)
	assert.Equal(t, "copyRows", m.status.Get().String())

	// first chunk.
	chunk, err := m.copyChunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "`id` < 1", chunk.String())
	err = copier.CopyChunk(ctx, chunk)
	assert.NoError(t, err)

	// second chunk
	chunk, err = m.copyChunker.Next()
	assert.NoError(t, err)
	assert.Equal(t, "`id` >= 1 AND `id` < 1001", chunk.String())
	err = copier.CopyChunk(ctx, chunk)
	assert.NoError(t, err)

	// now we insert a row in the range of the third chunk
	testutils.RunSQL(t, "INSERT INTO phantomtest (id, pad) VALUES (1002, RANDOM_BYTES(1024))")

	// we copy it but we don't feedback it (a hack)
	testutils.RunSQL(t, "INSERT INTO _phantomtest_new (id, pad) SELECT * FROM phantomtest WHERE id = 1002")

	// delete the row (but not from the _new table)
	// when it gets to recopy it will not be there.
	testutils.RunSQL(t, "DELETE FROM phantomtest WHERE id = 1002")

	// then we save the checkpoint without the feedback.
	assert.NoError(t, m.DumpCheckpoint(ctx))
	// assert there is a checkpoint
	var rowCount int
	err = m.db.QueryRowContext(ctx, `SELECT count(*) from _phantomtest_chkpnt`).Scan(&rowCount)
	assert.NoError(t, err)
	assert.Equal(t, 1, rowCount)

	// kill it.
	cancel()
	assert.NoError(t, m.Close())

	// Resume the migration using and apply all of the replication
	// changes before starting the copier.
	ctx = t.Context()
	m, err = NewRunner(&Migration{
		Host:                           cfg.Addr,
		Username:                       cfg.User,
		Password:                       &cfg.Passwd,
		Database:                       cfg.DBName,
		Threads:                        2,
		EnableExperimentalBufferedCopy: enableBuffered,
		Table:                          "phantomtest",
		Alter:                          "ENGINE=InnoDB",
		TargetChunkTime:                100 * time.Millisecond,
	})
	assert.NoError(t, err)
	// Set up both connections: poolDB for worker operations and db for management
	m.poolDB, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	m.db, err = dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	m.dbConfig = dbconn.NewDBConfig()
	m.changes[0].table = table.NewTableInfo(m.db, m.migration.Database, m.migration.Table)
	assert.NoError(t, m.changes[0].table.SetInfo(ctx))
	// check we can resume from checkpoint
	// this is normally done in m.setup() but we want to call it in isolation.
	assert.NoError(t, m.resumeFromCheckpoint(ctx))
	// This is normally done in m.setup()
	m.replClient.SetWatermarkOptimization(true)
	// doublecheck that the highPtr is 1002 in the _new table and not in the original table.
	assert.Equal(t, "10", m.changes[0].table.MaxValue().String())
	assert.Equal(t, "1002", m.changes[0].newTable.MaxValue().String())

	// flush the replication changes
	// if the bug exists, this would cause the breakage.
	assert.NoError(t, m.replClient.Flush(ctx))
	// start the copier.
	assert.NoError(t, m.copier.Run(ctx))
	// the checksum runs in prepare for cutover.
	// previously it would fail, but it should work as long as the resumeFromCheckpoint()
	// correctly finds the high watermark.
	err = m.checksum(ctx)
	assert.NoError(t, err)
	assert.NoError(t, m.Close())
}

func TestResumeFromCheckpointE2EWithManualSentinel(t *testing.T) {
	t.Parallel()
	t.Run("unbuffered", func(t *testing.T) {
		testResumeFromCheckpointE2EWithManualSentinel(t, false)
	})
	t.Run("buffered", func(t *testing.T) {
		if isMinimalRBRTestRunner(t) {
			t.Skip("Skipping buffered copy test because binlog_row_image is not FULL or binlog_row_value_options is not empty")
		}
		testResumeFromCheckpointE2EWithManualSentinel(t, true)
	})
}

func testResumeFromCheckpointE2EWithManualSentinel(t *testing.T, enableBuffered bool) {
	// This test is similar to TestResumeFromCheckpointE2E but it adds a sentinel table
	// created after the migration begins and is interrupted.
	// The migration itself runs with DeferCutOver=false
	// so we test to make sure a sentinel table created manually by the operator
	// blocks cutover.

	dbName := testutils.CreateUniqueTestDatabase(t)
	tableName := `resume_checkpoint_e2e_w_sentinel`
	tableInfo := table.TableInfo{SchemaName: dbName, TableName: tableName}
	lockTables := []*table.TableInfo{&tableInfo}

	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf(`DROP TABLE IF EXISTS %s, _%s_old, _%s_chkpnt`, tableName, tableName, tableName))
	table := fmt.Sprintf(`CREATE TABLE %s (
		id int(11) NOT NULL AUTO_INCREMENT,
		pad varbinary(1024) NOT NULL,
		PRIMARY KEY (id)
	)`, tableName)
	testutils.RunSQLInDatabase(t, dbName, table)
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)

	// Insert dummy data.
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (pad) SELECT RANDOM_BYTES(1024) FROM dual", tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (pad) SELECT RANDOM_BYTES(1024) FROM %s a, %s b, %s c LIMIT 100000", tableName, tableName, tableName, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (pad) SELECT RANDOM_BYTES(1024) FROM %s a, %s b, %s c LIMIT 100000", tableName, tableName, tableName, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (pad) SELECT RANDOM_BYTES(1024) FROM %s a, %s b, %s c LIMIT 100000", tableName, tableName, tableName, tableName))
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (pad) SELECT RANDOM_BYTES(1024) FROM %s a, %s b, %s c LIMIT 100000", tableName, tableName, tableName, tableName))
	alterSQL := "ADD INDEX(pad);"
	// use as slow as possible here: we want the copy to be still running
	// when we kill it once we have a checkpoint saved.
	migration := &Migration{
		Host:                           cfg.Addr,
		Username:                       cfg.User,
		Password:                       &cfg.Passwd,
		Database:                       dbName,
		Threads:                        1,
		EnableExperimentalBufferedCopy: enableBuffered,
		Table:                          tableName,
		Alter:                          alterSQL,
		TargetChunkTime:                100 * time.Millisecond,
		DeferCutOver:                   false,
		RespectSentinel:                true,
	}
	runner, err := NewRunner(migration)
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())

	go func() {
		err := runner.Run(ctx)
		assert.Error(t, err) // it gets interrupted as soon as there is a checkpoint saved.
	}()

	// wait until a checkpoint is saved (which means copy is in progress)
	db, err := dbconn.New(testutils.DSNForDatabase(dbName), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()
	for {
		var rowCount int
		err = db.QueryRowContext(t.Context(), fmt.Sprintf(`SELECT count(*) from _%s_chkpnt`, tableName)).Scan(&rowCount)
		if err != nil {
			continue // table does not exist yet
		}
		if rowCount > 0 {
			// Test that it's not possible to acquire metadata lock with name
			// as tablename while the migration is running.
			lock, err := dbconn.NewMetadataLock(ctx, testutils.DSN(),
				lockTables, dbconn.NewDBConfig(), slog.Default())
			assert.Error(t, err)
			if lock != nil {
				assert.ErrorContains(t, err, fmt.Sprintf("could not acquire metadata lock for %s, lock is held by another connection", lock.GetLockName()))
			}
			assert.Nil(t, lock)
			break
		}
	}
	// Close() before cancel() to avoid race conditions.
	assert.NoError(t, runner.Close())
	cancel()

	// Manually create the sentinel table.
	testutils.RunSQLInDatabase(t, dbName, "CREATE TABLE _spirit_sentinel (id int unsigned primary key)")

	// Insert some more dummy data
	testutils.RunSQLInDatabase(t, dbName, fmt.Sprintf("INSERT INTO %s (pad) SELECT RANDOM_BYTES(1024) FROM %s LIMIT 1000", tableName, tableName))
	// Start a new migration with the same parameters.
	// Let it complete.
	newmigration := &Migration{
		Host:                           cfg.Addr,
		Username:                       cfg.User,
		Password:                       &cfg.Passwd,
		Database:                       dbName,
		Threads:                        4,
		EnableExperimentalBufferedCopy: enableBuffered,
		Table:                          tableName,
		Alter:                          alterSQL,
		TargetChunkTime:                5 * time.Second,
		DeferCutOver:                   false,
		RespectSentinel:                true,
	}
	m, err := NewRunner(newmigration)
	assert.NoError(t, err)
	assert.NotNil(t, m)

	err = m.Run(t.Context())
	assert.Error(t, err)
	assert.ErrorContains(t, err, "timed out waiting for sentinel table to be dropped")
	assert.True(t, m.usedResumeFromCheckpoint)
	assert.NoError(t, m.Close())
}

package copier

import (
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBufferedCopier(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS bufferedt1, bufferedt2")
	testutils.RunSQL(t, "CREATE TABLE bufferedt1 (a INT NOT NULL, b INT, c VARCHAR(255), d VARBINARY(255), e JSON, f DATETIME, g TIMESTAMP, PRIMARY KEY (a))")
	testutils.RunSQL(t, "CREATE TABLE bufferedt2 (a INT NOT NULL, b INT, c VARCHAR(255), d VARBINARY(255), e JSON, f DATETIME, g TIMESTAMP, PRIMARY KEY (a))")

	// Insert all sorts of evil data.
	testutils.RunSQL(t, "INSERT INTO bufferedt1 VALUES (1, NULL, 'normal'' string', RANDOM_BYTES(10), JSON_ARRAY(1,2,3), NOW(), NOW())")
	testutils.RunSQL(t, `INSERT INTO bufferedt1 VALUES (2, 42, 'string with \\ backslash', RANDOM_BYTES(10), JSON_OBJECT('key', 'value\\ \''), NOW(), NOW())`)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer db.Close()
	require.Equal(t, 0, db.Stats().InUse) // no connections in use.

	t1 := table.NewTableInfo(db, "test", "bufferedt1")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(db, "test", "bufferedt2")
	assert.NoError(t, t2.SetInfo(t.Context()))

	cfg := NewCopierDefaultConfig()
	cfg.UseExperimentalBufferedCopier = true
	chunker, err := table.NewChunker(t1, t2, cfg.TargetChunkTime, cfg.Logger)
	assert.NoError(t, err)

	assert.NoError(t, chunker.Open())

	copier, err := NewCopier(db, chunker, cfg)
	assert.NoError(t, err)
	assert.NoError(t, copier.Run(t.Context())) // works.

	// We should expect to have the same number of rows
	// and a basic checksum confirms a match.
	var checksumSrc, checksumDst string
	err = db.QueryRow("SELECT BIT_XOR(CRC32(CONCAT(a, IFNULL(b, ''), c, d, e, f, g))) as checksum FROM bufferedt1").Scan(&checksumSrc)
	assert.NoError(t, err)

	err = db.QueryRow("SELECT BIT_XOR(CRC32(CONCAT(a, IFNULL(b, ''), c, d, e, f, g))) as checksum FROM bufferedt2").Scan(&checksumDst)
	assert.NoError(t, err)
	assert.Equal(t, checksumSrc, checksumDst, "Checksums do not match between source and destination tables")

	require.Equal(t, 0, db.Stats().InUse) // no connections in use.
}

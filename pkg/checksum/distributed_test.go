package checksum

import (
	"log/slog"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFixCorruptWithApplier(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	assert.NoError(t, err)
	newDBName := testutils.CreateUniqueTestDatabase(t)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS corruptt1")
	testutils.RunSQL(t, "CREATE TABLE corruptt1 (a INT NOT NULL , b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO corruptt1 VALUES (1, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO corruptt1 VALUES (2, 2, 3)")
	testutils.RunSQL(t, "INSERT INTO corruptt1 VALUES (3, 2, 3)")

	testutils.RunSQL(t, "CREATE TABLE "+newDBName+".corruptt1 (a INT NOT NULL , b INT, c INT, PRIMARY KEY (a))")
	testutils.RunSQL(t, "INSERT INTO "+newDBName+".corruptt1 VALUES (1, 2, 3)")
	// row 2 is missing
	testutils.RunSQL(t, "INSERT INTO "+newDBName+".corruptt1 VALUES (3, 9, 9)")

	destDB := cfg.Clone()
	destDB.DBName = newDBName

	src, err := dbconn.New(cfg.FormatDSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer src.Close()
	dest, err := dbconn.New(destDB.FormatDSN(), dbconn.NewDBConfig())
	assert.NoError(t, err)
	defer dest.Close()

	t1 := table.NewTableInfo(src, "test", "corruptt1")
	assert.NoError(t, t1.SetInfo(t.Context()))
	t2 := table.NewTableInfo(dest, newDBName, "corruptt1")
	assert.NoError(t, t2.SetInfo(t.Context()))
	logger := slog.Default()
	target := applier.Target{
		DB:       dest,
		KeyRange: "0",
		Config:   destDB,
	}
	applier, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)

	// Start the applier so its workers can process async Apply calls
	feed := repl.NewClient(src, src, cfg.Addr, cfg.User, cfg.Passwd, &repl.ClientConfig{
		Logger:                     logger,
		Concurrency:                4,
		TargetBatchTime:            time.Second,
		ServerID:                   repl.NewServerID(),
		UseExperimentalBufferedMap: true,
		Applier:                    applier,
	})
	defer feed.Close()
	assert.NoError(t, feed.AddSubscription(t1, t2, nil))
	assert.NoError(t, feed.Run(t.Context()))

	chunker, err := table.NewChunker(t1, t2, 0, slog.Default())
	assert.NoError(t, err)
	assert.NoError(t, chunker.Open())

	config := NewCheckerDefaultConfig()
	config.FixDifferences = true
	config.Applier = applier

	checker, err := NewChecker(src, chunker, feed, config)
	assert.Equal(t, "0/3 0.00%", checker.GetProgress())
	assert.NoError(t, err)
	assert.NoError(t, checker.Run(t.Context())) // should be fixed!
	assert.Equal(t, "3/3 100.00%", checker.GetProgress())
}

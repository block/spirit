package change

import (
	"testing"

	mysql2 "github.com/block/mysql"
	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
)

// processRowsEvent is shared by both clients through feedCore, so its early
// exits must behave identically whichever client embeds it.
func TestFeedCoreProcessRowsEventGuards(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	testutils.RunSQL(t, "DROP TABLE IF EXISTS fcguard_t1")
	testutils.RunSQL(t, "CREATE TABLE fcguard_t1 (id INT NOT NULL, a INT, PRIMARY KEY (id))")
	t1 := table.NewTableInfo(db, "test", "fcguard_t1")
	require.NoError(t, t1.SetInfo(t.Context()))

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	appl := applier.NewSingleTargetForTest(t, db)
	binlogC := NewBinlogClient(db, cfg.Addr, cfg.User, cfg.Passwd, NewClientDefaultConfig()).(*binlogClient)
	gtidC := NewGTIDClient(db, cfg.Addr, cfg.User, cfg.Passwd, NewClientDefaultConfig()).(*gtidClient)
	for _, c := range []Source{binlogC, gtidC} {
		chunker, err := table.NewChunker(t1, table.ChunkerConfig{})
		require.NoError(t, err)
		require.NoError(t, c.AddSubscription(BufferedSubscriptionFactory(t1, nil, chunker, appl)))
	}
	clients := map[string]struct {
		process func(*replication.BinlogEvent, *replication.RowsEvent) error
		core    *feedCore
	}{
		"binlog": {binlogC.processRowsEvent, &binlogC.feedCore},
		"gtid":   {gtidC.processRowsEvent, &gtidC.feedCore},
	}
	newEvent := func(rows [][]any, skipped [][]int) (*replication.BinlogEvent, *replication.RowsEvent) {
		return &replication.BinlogEvent{Header: &replication.EventHeader{EventType: replication.WRITE_ROWS_EVENTv2}},
			&replication.RowsEvent{
				Table:          &replication.TableMapEvent{Schema: []byte("test"), Table: []byte("fcguard_t1")},
				Rows:           rows,
				SkippedColumns: skipped,
			}
	}
	for name, cl := range clients {
		t.Run(name, func(t *testing.T) {
			// Undecoded rows on a subscribed table is a lifecycle violation.
			ev, e := newEvent(nil, nil)
			require.ErrorContains(t, cl.process(ev, e), "undecoded rows")

			// A minimal row image is refused.
			ev, e = newEvent([][]any{{1, 2}}, [][]int{{1}})
			require.ErrorContains(t, cl.process(ev, e), "binlog_row_image=FULL")

			// After Stop the event is dropped before any of the checks run.
			cl.core.stopped.Store(true)
			defer cl.core.stopped.Store(false)
			ev, e = newEvent(nil, nil)
			require.NoError(t, cl.process(ev, e))
			ev, e = newEvent([][]any{{1, 2}}, [][]int{{1}})
			require.NoError(t, cl.process(ev, e))
		})
	}
}

// prepareSyncerConfig is the single place the host:port split and the TLS
// config for the binlog connection are built for both clients.
func TestFeedCorePrepareSyncerConfig(t *testing.T) {
	for name, ctor := range map[string]func(*ClientConfig) *feedCore{
		"binlog": func(c *ClientConfig) *feedCore {
			return &NewBinlogClient(nil, "db.example.test:3307", "u", "p", c).(*binlogClient).feedCore
		},
		"gtid": func(c *ClientConfig) *feedCore {
			return &NewGTIDClient(nil, "db.example.test:3307", "u", "p", c).(*gtidClient).feedCore
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.NotNil(t, ctor(NewClientDefaultConfig()).dbConfig, "a nil DBConfig defaults at construction")
			conf := NewClientDefaultConfig()
			conf.DBConfig = dbconn.NewDBConfig()
			conf.DBConfig.TLSMode = "PREFERRED"
			core := ctor(conf)
			require.NoError(t, core.prepareSyncerConfig())
			require.Equal(t, "db.example.test", core.cfg.Host)
			require.EqualValues(t, 3307, core.cfg.Port)
			require.NotNil(t, core.cfg.TLSConfig)
			require.Equal(t, "db.example.test", core.cfg.TLSConfig.ServerName)

			conf = NewClientDefaultConfig()
			conf.DBConfig = dbconn.NewDBConfig()
			conf.DBConfig.TLSMode = "DISABLED"
			core = ctor(conf)
			require.NoError(t, core.prepareSyncerConfig())
			require.Nil(t, core.cfg.TLSConfig)
			core.host = "no-port"
			require.ErrorContains(t, core.prepareSyncerConfig(), "failed to parse host")
			core.host = "h:notaport"
			require.ErrorContains(t, core.prepareSyncerConfig(), "failed to parse port")
		})
	}
}

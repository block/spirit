package change

import (
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestIsGTIDPosition(t *testing.T) {
	// GTID-set encodings, including the multi-UUID and whitespace/newline
	// shapes MySQL emits for gtid_executed with several source servers.
	for _, pos := range []string{
		"3e11fa47-71ca-11e1-9e33-c80aa9429562:5",
		"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
		"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5:11-18",
		"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5,2c256447-3f0d-431b-95c4-8a25a2a86227:1-11",
		"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5,\n2c256447-3f0d-431b-95c4-8a25a2a86227:1-11",
	} {
		require.True(t, IsGTIDPosition(pos), "expected %q to classify as a GTID position", pos)
	}
	// Binlog file:offset encodings (formatBinlogPosition output), the empty
	// "no position yet" string, and garbage must all classify as non-GTID.
	for _, pos := range []string{
		"",
		"binlog.000001:4",
		"mysql-bin.000123:456789",
		// A basename that looks like a UUID still has the ".NNNNNN" index
		// suffix, which can never parse as part of a GTID set.
		"3e11fa47-71ca-11e1-9e33-c80aa9429562.000001:4",
		"not-a-position",
	} {
		require.False(t, IsGTIDPosition(pos), "expected %q to classify as a non-GTID position", pos)
	}
}

func TestUseGTIDForResume(t *testing.T) {
	gtidSet := "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5"

	// A binlog position resumes through the binlog client regardless of
	// whether the server could serve GTIDs.
	useGTID, err := useGTIDForResume("binlog.000001:4", true)
	require.NoError(t, err)
	require.False(t, useGTID)
	useGTID, err = useGTIDForResume("binlog.000001:4", false)
	require.NoError(t, err)
	require.False(t, useGTID)

	// A GTID position requires the server to still have GTIDs enabled.
	useGTID, err = useGTIDForResume(gtidSet, true)
	require.NoError(t, err)
	require.True(t, useGTID)
	_, err = useGTIDForResume(gtidSet, false)
	require.ErrorContains(t, err, "no longer has GTIDs enabled")
}

// TestNewAutoClient exercises the selection against the test server, whatever
// its GTID configuration: a fresh client must match the server's capability,
// a binlog-format resume position must always select the binlog client, and a
// GTID-format resume position must select the GTID client exactly when the
// server can serve it (and error otherwise). CI runs this both with and
// without GTIDs enabled, covering every branch between the two modes.
func TestNewAutoClient(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	gtidEnabled, err := GTIDEnabled(t.Context(), db)
	require.NoError(t, err)

	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	newClient := func(resumePosition string) (Source, error) {
		return NewAutoClient(t.Context(), db, cfg.Addr, cfg.User, cfg.Passwd, nil, NewClientDefaultConfig(), resumePosition)
	}

	// Fresh run: the probe decides.
	client, err := newClient("")
	require.NoError(t, err)
	defer client.Close()
	if gtidEnabled {
		require.IsType(t, &gtidClient{}, client)
	} else {
		require.IsType(t, &binlogClient{}, client)
	}

	// Resuming a binlog file:offset checkpoint keeps the binlog client even
	// when the server has GTIDs enabled (e.g. a checkpoint written by an
	// older spirit).
	client, err = newClient("binlog.000001:4")
	require.NoError(t, err)
	defer client.Close()
	require.IsType(t, &binlogClient{}, client)

	// Resuming a GTID checkpoint requires GTIDs.
	client, err = newClient("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	if gtidEnabled {
		require.NoError(t, err)
		defer client.Close()
		require.IsType(t, &gtidClient{}, client)
	} else {
		require.ErrorContains(t, err, "no longer has GTIDs enabled")
	}
}

// TestGTIDEnabled pins the probe against the server's actual settings, so a
// probe regression (e.g. a typo'd variable name) fails here rather than
// silently flipping every fresh run to the binlog client.
func TestGTIDEnabled(t *testing.T) {
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	var gtidMode, enforce string
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT @@global.gtid_mode, @@global.enforce_gtid_consistency").Scan(&gtidMode, &enforce))

	enabled, err := GTIDEnabled(t.Context(), db)
	require.NoError(t, err)
	require.Equal(t, gtidMode == "ON" && enforce == "ON", enabled)
}

// skipUnlessGTIDEnabled skips tests that start a live GTID replication stream,
// which the server rejects unless gtid_mode=ON. Tests that merely construct a
// gtidClient or drive its internals without starting the stream do not need
// this guard. The GTID-enabled CI configurations provide the coverage that a
// no-GTID run skips here.
func skipUnlessGTIDEnabled(t *testing.T) {
	t.Helper()
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	enabled, err := GTIDEnabled(t.Context(), db)
	require.NoError(t, err)
	if !enabled {
		t.Skip("server does not have GTIDs enabled (needs gtid_mode=ON and enforce_gtid_consistency=ON)")
	}
}

package dbconn

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

func assertDSNConfig(t *testing.T, dsnStr string, user, password, addr, dbName, tlsConfig string, interpolateParams bool) {
	t.Helper()
	cfg, err := mysql.ParseDSN(dsnStr)
	require.NoError(t, err)
	if cfg == nil {
		return
	}
	require.Equal(t, user, cfg.User)
	require.Equal(t, password, cfg.Passwd)
	require.Equal(t, addr, cfg.Addr)
	require.Equal(t, dbName, cfg.DBName)
	require.Equal(t, tlsConfig, cfg.TLSConfig)
	require.True(t, cfg.AllowNativePasswords)
	// Nothing to assert about rejectReadOnly: the driver applies it
	// unconditionally and no longer carries the option (see DBConfig).
	require.Equal(t, interpolateParams, cfg.InterpolateParams)
	require.Equal(t, "utf8mb4_bin", cfg.Collation)
	require.Equal(t, `"NO_AUTO_VALUE_ON_ZERO"`, cfg.Params["sql_mode"])
	require.Equal(t, `"+00:00"`, cfg.Params["time_zone"])
	require.Equal(t, `"read-committed"`, cfg.Params["transaction_isolation"])
}

func TestNewDSN(t *testing.T) {
	// Start with a basic example
	dsn := "root:password@tcp(127.0.0.1:3306)/test"
	resp, err := newDSN(dsn, NewDBConfig())
	require.NoError(t, err)
	assertDSNConfig(t, resp, "root", "password", "127.0.0.1:3306", "test", "custom", false)

	// With interpolate on.
	config := NewDBConfig()
	config.InterpolateParams = true
	resp, err = newDSN(dsn, config)
	require.NoError(t, err)
	assertDSNConfig(t, resp, "root", "password", "127.0.0.1:3306", "test", "custom", true)

	// Also with TLS for non-RDS hosts (now includes tls=custom)
	dsn = "root:password@tcp(mydbhost.internal:3306)/test"
	resp, err = newDSN(dsn, NewDBConfig())
	require.NoError(t, err)
	assertDSNConfig(t, resp, "root", "password", "mydbhost.internal:3306", "test", "custom", false)

	// However, if it is RDS - it will be changed to use rds bundle.
	dsn = "root:password@tcp(tern-001.cluster-ro-ckxxxxxxvm.us-west-2.rds.amazonaws.com)/test"
	resp, err = newDSN(dsn, NewDBConfig())
	require.NoError(t, err)
	assertDSNConfig(t, resp, "root", "password", "tern-001.cluster-ro-ckxxxxxxvm.us-west-2.rds.amazonaws.com:3306", "test", "rds", false)

	// This is with optional port too
	dsn = "root:password@tcp(tern-001.cluster-ro-ckxxxxxxvm.us-west-2.rds.amazonaws.com:12345)/test"
	resp, err = newDSN(dsn, NewDBConfig())
	require.NoError(t, err)
	assertDSNConfig(t, resp, "root", "password", "tern-001.cluster-ro-ckxxxxxxvm.us-west-2.rds.amazonaws.com:12345", "test", "rds", false)

	// Password with special characters (e.g. AWS IAM auth token with ?, @, &)
	iamToken := "dbhost.rds.amazonaws.com:3306/?Action=connect&DBUser=iam_user&X-Amz-Signature=abc123"
	dsn = fmt.Sprintf("iam_user:%s@tcp(host.docker.internal:8410)/mydb", iamToken)
	resp, err = newDSN(dsn, NewDBConfig())
	require.NoError(t, err)
	assertDSNConfig(t, resp, "iam_user", iamToken, "host.docker.internal:8410", "mydb", "custom", false)

	// DSN with explicit tls parameter — TLS config preserved, but session vars still applied
	dsn = "root:password@tcp(127.0.0.1:3306)/test?tls=skip-verify"
	resp, err = newDSN(dsn, NewDBConfig())
	require.NoError(t, err)
	assertDSNConfig(t, resp, "root", "password", "127.0.0.1:3306", "test", "skip-verify", false)

	// Invalid DSN, can't parse.
	dsn = "invalid"
	resp, err = newDSN(dsn, NewDBConfig())
	require.Error(t, err)
	require.Empty(t, resp)
}

func TestNewDSNAllowNativePasswords(t *testing.T) {
	// Verify AllowNativePasswords is true for both TLS-enabled and TLS-disabled DSNs.
	// This is important because Spirit's PREFERRED TLS mode falls back to a DISABLED
	// DSN when TLS is unavailable, and both paths must support mysql_native_password.
	dsn := "root:password@tcp(127.0.0.1:3306)/test"

	// Default (PREFERRED) mode — TLS enabled
	resp, err := newDSN(dsn, NewDBConfig())
	require.NoError(t, err)
	cfg, err := mysql.ParseDSN(resp)
	require.NoError(t, err)
	require.True(t, cfg.AllowNativePasswords, "AllowNativePasswords must be true with TLS enabled")

	// DISABLED mode — the fallback path used when TLS is unavailable
	config := NewDBConfig()
	config.TLSMode = "DISABLED"
	resp, err = newDSN(dsn, config)
	require.NoError(t, err)
	cfg, err = mysql.ParseDSN(resp)
	require.NoError(t, err)
	require.True(t, cfg.AllowNativePasswords, "AllowNativePasswords must be true with TLS disabled (fallback path)")
	require.NotContains(t, resp, "allowNativePasswords=false",
		"DSN must not contain allowNativePasswords=false")
}

func TestNewDSNDisablesTinyInt1IsBool(t *testing.T) {
	// The driver maps a signed tinyint(1) to a Go bool by default, which
	// collapses every non-zero value to true. The copier reads rows back into
	// Go to build its INSERT, so spirit must always ask for integers: a stored
	// 2 would otherwise be written as 1, and the value is unrecoverable by the
	// time spirit sees it.
	//
	// Assert on the presence of tinyInt1IsBool=false rather than the absence of
	// =true: the driver's default is true, so FormatDSN only writes the
	// parameter when it is false. An untouched DSN carries no parameter at all,
	// which reads as "bool mapping on".
	inputs := []struct {
		name string
		dsn  string
	}{
		{"unset", "root:password@tcp(127.0.0.1:3306)/test"},
		// Spirit requires exact values, so it overrides a caller who asked for
		// the bool mapping rather than deferring to them.
		{"caller_asked_for_bool", "root:password@tcp(127.0.0.1:3306)/test?tinyInt1IsBool=true"},
	}

	for _, tlsMode := range []string{"PREFERRED", "DISABLED"} {
		for _, input := range inputs {
			t.Run(tlsMode+"/"+input.name, func(t *testing.T) {
				config := NewDBConfig()
				config.TLSMode = tlsMode
				resp, err := newDSN(input.dsn, config)
				require.NoError(t, err)
				require.Contains(t, resp, "tinyInt1IsBool=false",
					"DSN must disable the driver's tinyint(1)-to-bool mapping")

				// Round-trip it: the flag has to survive parsing, since that is
				// how the driver actually receives it.
				cfg, err := mysql.ParseDSN(resp)
				require.NoError(t, err)
				require.Contains(t, cfg.FormatDSN(), "tinyInt1IsBool=false")
			})
		}
	}
}

// TestConnectionReadsTinyInt1AsInteger proves the setting changes what the
// driver returns, not just what the DSN says. The assertions above check the
// DSN text; this checks the behavior every connection spirit opens gets, since
// New is the single door onto newDSN.
func TestConnectionReadsTinyInt1AsInteger(t *testing.T) {
	testutils.RunSQL(t, "DROP TABLE IF EXISTS conn_tinyint1_scan")
	testutils.RunSQL(t, "CREATE TABLE conn_tinyint1_scan (id INT NOT NULL PRIMARY KEY, flags TINYINT(1) NOT NULL)")
	testutils.RunSQL(t, "INSERT INTO conn_tinyint1_scan VALUES (1, 0), (2, 1), (3, 2), (4, 127), (5, -128)")

	db, err := New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	rows, err := db.QueryContext(t.Context(), "SELECT id, flags FROM conn_tinyint1_scan ORDER BY id")
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)

	// Scan into any: a bool here means the driver collapsed the value, which is
	// exactly what the copier cannot tolerate.
	want := map[int]int64{1: 0, 2: 1, 3: 2, 4: 127, 5: -128}
	seen := 0
	for rows.Next() {
		var id int
		var flags any
		require.NoError(t, rows.Scan(&id, &flags))
		require.IsType(t, int64(0), flags, "tinyint(1) came back as %T, not an integer", flags)
		require.Equal(t, want[id], flags, "tinyint(1) value changed for id=%d", id)
		seen++
	}
	require.NoError(t, rows.Err())
	require.Len(t, want, seen, "not every seeded row was read back")
}

func TestNewDSNAllowCleartextPasswords(t *testing.T) {
	// With TLS enabled (default PREFERRED mode), AllowCleartextPasswords should be true
	dsn := "root:password@tcp(127.0.0.1:3306)/test"
	resp, err := newDSN(dsn, NewDBConfig())
	require.NoError(t, err)
	cfg, err := mysql.ParseDSN(resp)
	require.NoError(t, err)
	require.NotEmpty(t, cfg.TLSConfig, "TLS should be configured in default mode")
	require.True(t, cfg.AllowCleartextPasswords, "AllowCleartextPasswords should be true when TLS is enabled")

	// With TLS disabled, AllowCleartextPasswords should be false
	config := NewDBConfig()
	config.TLSMode = "DISABLED"
	resp, err = newDSN(dsn, config)
	require.NoError(t, err)
	cfg, err = mysql.ParseDSN(resp)
	require.NoError(t, err)
	require.False(t, cfg.AllowCleartextPasswords, "AllowCleartextPasswords should be false when TLS is disabled")
}

// TestNewDSNDisabledMode pins what --tls-mode=DISABLED produces, on an RDS
// address as well as a local one.
//
// The RDS row is the whole point. [DriverName] gives an RDS address verified
// TLS whenever the DSN asks for nothing, so writing an empty tls= for DISABLED
// — which is what this code did before, and what the sibling test above
// asserted — hands the user TLS on exactly the connection where they asked for
// none. It fails silently: the connection works, it is just not the one that
// was requested, and no local-MySQL test can see it because the driver's
// auto-TLS only fires on an RDS hostname.
//
// So both assertions here are load-bearing, and each fails on its own
// mutation:
//
//   - assert on cfg.TLS (the effective setting) rather than on cfg.TLSConfig
//     being empty; restoring `cfg.TLSConfig = ""` makes the RDS row fail
//   - AllowCleartextPasswords must stay false; a bare `cfg.TLSConfig != ""`
//     reads "false" as "TLS is on" and sends the password in the clear over a
//     plaintext connection
func TestNewDSNDisabledMode(t *testing.T) {
	for _, host := range []string{
		"db.cxyz.us-east-1.rds.amazonaws.com:3306",
		"127.0.0.1:3306",
	} {
		t.Run(host, func(t *testing.T) {
			config := NewDBConfig()
			config.TLSMode = "DISABLED"
			resp, err := newDSN("root:password@tcp("+host+")/test", config)
			require.NoError(t, err)

			cfg, err := mysql.ParseDSN(resp)
			require.NoError(t, err)
			require.Nil(t, cfg.TLS, "DISABLED produced a TLS connection")
			require.False(t, cfg.AllowCleartextPasswords,
				"cleartext passwords allowed on a connection with no TLS")
		})
	}
}

func TestNewConn(t *testing.T) {
	db, err := New("invalid", NewDBConfig())
	require.Error(t, err)
	require.Nil(t, db)

	db, err = New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)
	require.NotNil(t, db)
	defer utils.CloseAndLog(db)
	for range 10 {
		var resp int
		err = db.QueryRowContext(t.Context(), "SELECT 1").Scan(&resp)
		require.NoError(t, err)
		require.Equal(t, 1, resp)
	}
	// New on syntactically valid but won't respond to ping.
	db, err = New("root:wrongpassword@tcp(127.0.0.1)/doesnotexist", NewDBConfig())
	require.Error(t, err)
	require.Nil(t, db)
}

// TestSetPoolSizeKeepsIdleWithOpen pins the reason SetPoolSize exists: an idle
// limit below the open limit makes database/sql close connections on release
// and redial on the next acquire, which during the copy phase is continuous
// churn against the target. The two limits must move together at every call
// site, including the ones that ratchet a pool after it was created.
func TestSetPoolSizeKeepsIdleWithOpen(t *testing.T) {
	db, err := New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	// New() itself must not leave the pool churning: the default open limit is
	// 32, so a pool that retains only the old hardcoded 10 would fail here.
	require.Equal(t, 32, db.Stats().MaxOpenConnections)
	require.Equal(t, 16, idleAfterCycling(t, db, 16))

	SetPoolSize(db, 20)
	require.Equal(t, 20, db.Stats().MaxOpenConnections)
	require.Equal(t, 20, idleAfterCycling(t, db, 20),
		"a ratcheted pool must retain everything it is allowed to open")

	// Shrinking (the cutover path) applies to both, so a smaller pool does not
	// retain more idle connections than it may open.
	SetPoolSize(db, 5)
	require.Equal(t, 5, db.Stats().MaxOpenConnections)
	require.Equal(t, 5, idleAfterCycling(t, db, 5))

	// Non-positive means unlimited to SetMaxOpenConns. The idle limit is left
	// alone rather than set to a nonsense value, since database/sql has no
	// "unlimited idle" setting.
	SetPoolSize(db, 0)
	require.Equal(t, 0, db.Stats().MaxOpenConnections)
	require.Equal(t, 5, idleAfterCycling(t, db, 8),
		"a non-positive size must leave the idle limit untouched")
}

// idleAfterCycling opens n connections, returns them all, and reports how many
// the pool kept. database/sql does not expose MaxIdleConns, but it discards
// anything returned beyond that limit, so the retained count is min(n,
// MaxIdleConns) — which is exactly the churn behaviour under test.
func idleAfterCycling(t *testing.T, db *sql.DB, n int) int {
	t.Helper()
	conns := make([]*sql.Conn, 0, n)
	for range n {
		c, err := db.Conn(t.Context())
		require.NoError(t, err)
		conns = append(conns, c)
	}
	for _, c := range conns {
		require.NoError(t, c.Close())
	}
	return db.Stats().Idle
}

func TestNewConnRejectsReadOnlyConnections(t *testing.T) {
	// Database connection check
	db, err := New(testutils.DSN(), NewDBConfig())
	require.NoError(t, err)
	if db != nil {
		utils.CloseAndLog(db)
	}

	testutils.RunSQL(t, "DROP TABLE IF EXISTS conn_read_only")
	testutils.RunSQL(t, "CREATE TABLE conn_read_only (a INT NOT NULL, b INT, c INT, PRIMARY KEY (a))")

	config := NewDBConfig()
	// Setting the connection pool size = 1 && transaction_read_only = 1 for the session.
	// This ensures that if the test passes, the connection was definitely recycled by rejectReadOnly=true.
	config.MaxOpenConnections = 1
	db, err = New(testutils.DSN(), config)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	_, err = db.ExecContext(context.Background(), "set session transaction_read_only = 1")
	require.NoError(t, err)

	// This would error, but `database/sql` automatically retries on a
	// new connection which is not read-only, and eventually succeed.
	// See also: rejectReadOnly test in `go-sql-driver/mysql`: https://github.com/go-sql-driver/mysql/blob/52c1917d99904701db2b0e4f14baffa948009cd7/driver_test.go#L2270-L2301
	_, err = db.ExecContext(context.Background(), "insert into conn_read_only values (1, 2, 3)")
	require.NoError(t, err)

	var count int
	err = db.QueryRowContext(context.Background(), "select count(*) from conn_read_only where a = 1").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

// TestValidCertificateBundle used to parse spirit's own embedded
// global-bundle.pem and assert it held at least one certificate. The bundle now
// lives in the driver, which has its own parse test, so what is left to check
// here is the delegation: that spirit's RDS TLS config actually arrives with
// roots in it. An empty pool is the failure that matters — it does not error,
// it just fails every RDS handshake at connect time with an x509 message that
// names nothing in this repository.
func TestValidCertificateBundle(t *testing.T) {
	cfg := NewTLSConfig()
	require.NotNil(t, cfg)
	require.NotNil(t, cfg.RootCAs, "RDS TLS config has no root pool")
	require.NotEmpty(t, cfg.RootCAs.Subjects(), "RDS root pool is empty") //nolint:staticcheck // SA1019: Subjects is fine for a pool we built, and there is no other way to count roots
	require.False(t, cfg.InsecureSkipVerify, "RDS TLS must verify the server")

	// The pool must be per-call. GetTLSConfigForBinlog mutates what it gets
	// back (it sets ServerName), and callers may append roots; a shared
	// *x509.CertPool would widen trust process-wide and race with handshakes.
	other := NewTLSConfig()
	require.NotSame(t, cfg.RootCAs, other.RootCAs, "RDS TLS configs share one root pool")

	// An empty certData is the documented "use the RDS roots" fallback, and it
	// must reach the same place rather than silently building an empty pool.
	custom := NewCustomTLSConfig(nil, "VERIFY_IDENTITY")
	require.NotNil(t, custom)
	require.NotNil(t, custom.RootCAs, "empty certData produced no root pool")
	require.NotEmpty(t, custom.RootCAs.Subjects(), "empty certData produced an empty root pool") //nolint:staticcheck // SA1019: see above
}

// stalledServer accepts TCP connections and never sends the MySQL handshake,
// the way a server too starved to service a new connection behaves. It
// returns the listener's address.
func stalledServer(t *testing.T) string {
	t.Helper()
	return stalledServerWithGreeting(t, nil)
}

// noTLSGreeting is a HandshakeV10 greeting packet whose capability flags omit
// CLIENT_SSL: protocol 10, server version, connection id, scramble part 1,
// filler, capabilities (lower) = LONG_PASSWORD|PROTOCOL_41|SECURE_CONNECTION,
// charset, status, capabilities (upper) = PLUGIN_AUTH, scramble length,
// reserved, scramble part 2, auth plugin name.
func noTLSGreeting() []byte {
	payload := []byte{0x0a}
	payload = append(payload, "8.0.0\x00"...)
	payload = append(payload, 1, 0, 0, 0)
	payload = append(payload, "abcdefgh"...)
	payload = append(payload, 0x00, 0x01, 0x82, 0xff, 0x02, 0x00, 0x08, 0x00, 21)
	payload = append(payload, make([]byte, 10)...)
	payload = append(payload, "ijklmnopqrst\x00"...)
	payload = append(payload, "mysql_native_password\x00"...)
	return append([]byte{byte(len(payload)), 0, 0, 0}, payload...)
}

// stalledServerWithGreeting is stalledServer, except that it writes greeting
// (if non-nil) to each connection before going silent.
func stalledServerWithGreeting(t *testing.T, greeting []byte) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var mu sync.Mutex
	var conns []net.Conn
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			if greeting != nil {
				_, _ = conn.Write(greeting)
			}
		}
	})
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range conns {
			_ = conn.Close()
		}
	})
	return ln.Addr().String()
}

// TestNewStalledHandshakeTimesOut checks that New gives up on a server that
// accepts the connection but never completes the handshake. The ping used to
// have no deadline, so New blocked forever; the driver's dial timeout does not
// help here because the dial itself succeeds.
func TestNewStalledHandshakeTimesOut(t *testing.T) {
	addr := stalledServer(t)
	dsn := fmt.Sprintf("spirit:spirit@tcp(%s)/test", addr)
	for _, tlsMode := range []string{"DISABLED", "PREFERRED", "REQUIRED"} {
		t.Run(tlsMode, func(t *testing.T) {
			cfg := NewDBConfig()
			cfg.TLSMode = tlsMode
			const timeout = 200 * time.Millisecond
			start := time.Now()
			db, err := newWithConnectTimeout(t.Context(), dsn, cfg, "main database", timeout)
			elapsed := time.Since(start)
			require.Error(t, err)
			require.Nil(t, db)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.GreaterOrEqual(t, elapsed, timeout)
			require.Less(t, elapsed, 10*time.Second, "the ping deadline did not bound the handshake")
		})
	}
}

// TestNewPreferredFallbackStalledHandshakeTimesOut covers the PREFERRED
// plaintext fallback. The server greets without CLIENT_SSL, so the TLS attempt
// fails with ErrNoTLS and New falls back to plaintext; the server then never
// answers the handshake response, so the fallback ping stalls in auth. That
// second ping must be bounded by the same deadline, and a stall after the dial
// shows the deadline covers more than the dial.
func TestNewPreferredFallbackStalledHandshakeTimesOut(t *testing.T) {
	addr := stalledServerWithGreeting(t, noTLSGreeting())
	cfg := NewDBConfig()
	cfg.TLSMode = "PREFERRED"
	const timeout = 200 * time.Millisecond
	start := time.Now()
	db, err := newWithConnectTimeout(t.Context(), fmt.Sprintf("spirit:spirit@tcp(%s)/test", addr), cfg, "main database", timeout)
	elapsed := time.Since(start)
	require.Nil(t, db)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, "[MAIN-DATABASE-CONNECTION-FALLBACK] ping failed")
	require.Less(t, elapsed, 10*time.Second, "the fallback ping deadline did not bound the handshake")
}

// TestNewAddsDSNTimeoutToDeadline checks that a DSN timeout= is added to the
// ping deadline. The dial runs under the ping's context, so taking only the
// larger of the two would let a slow dial use up the handshake's budget.
func TestNewAddsDSNTimeoutToDeadline(t *testing.T) {
	addr := stalledServer(t)
	cfg := NewDBConfig()
	cfg.TLSMode = "DISABLED"
	const dsnTimeout = time.Second
	const timeout = 500 * time.Millisecond
	start := time.Now()
	db, err := newWithConnectTimeout(t.Context(), fmt.Sprintf("spirit:spirit@tcp(%s)/test?timeout=%s", addr, dsnTimeout), cfg, "main database", timeout)
	elapsed := time.Since(start)
	require.Nil(t, db)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.GreaterOrEqual(t, elapsed, dsnTimeout+timeout, "the DSN timeout= was not added to the handshake budget")
	require.Less(t, elapsed, 10*time.Second)
}

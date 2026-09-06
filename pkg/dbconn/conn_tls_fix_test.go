package dbconn

import (
	"errors"
	"fmt"
	"testing"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// TestIsTLSUnsupportedByServer pins the error-classification used by the
// PREFERRED fallback. Only the go-sql-driver sentinel mysql.ErrNoTLS (returned
// when the server's handshake advertises no TLS capability) is allowed to
// trigger a silent downgrade to plaintext. Every other error — timeouts, auth
// failures, certificate-verification failures — must NOT classify as
// "TLS unsupported" so that PREFERRED propagates them instead of papering over
// them with an unencrypted connection.
//
// The local MySQL used by the integration suite has TLS enabled (8.0 default),
// so the genuine no-TLS-server path cannot be exercised against a live server;
// we therefore exercise the classifier directly here.
func TestIsTLSUnsupportedByServer(t *testing.T) {
	// The exact sentinel from go-sql-driver.
	require.True(t, isTLSUnsupportedByServer(mysql.ErrNoTLS))
	// Wrapped sentinel must still match (database/sql / fmt.Errorf wrapping).
	require.True(t, isTLSUnsupportedByServer(fmt.Errorf("ping failed: %w", mysql.ErrNoTLS)))

	// Everything that is NOT the no-TLS sentinel must be treated as a real
	// error and propagate rather than downgrade the pool.
	require.False(t, isTLSUnsupportedByServer(nil))
	require.False(t, isTLSUnsupportedByServer(errors.New("dial tcp: i/o timeout")))
	require.False(t, isTLSUnsupportedByServer(&mysql.MySQLError{Number: 1045, Message: "Access denied for user"}))
	require.False(t, isTLSUnsupportedByServer(&mysql.MySQLError{Number: 1040, Message: "Too many connections"}))
	require.False(t, isTLSUnsupportedByServer(errors.New("x509: certificate signed by unknown authority")))
	// A *different* sentinel must not match.
	require.False(t, isTLSUnsupportedByServer(mysql.ErrInvalidConn))
}

// TestGetTLSConfigForBinlogCaseInsensitive verifies that GetTLSConfigForBinlog
// treats the documented mode strings case-insensitively, in particular that a
// lowercase "disabled" returns a nil TLS config (no TLS) exactly like the
// uppercase "DISABLED" does.
//
// The case-sensitivity bug was masked for non-RDS hosts (NewCustomTLSConfig
// itself returns nil for DISABLED, so the default branch coincidentally yielded
// nil), but surfaced for RDS hosts: a lowercase "disabled" fell through the
// exact "DISABLED" check, hit the default branch (nil config), then the
// post-switch `if tlsConfig == nil && IsRDSHost(host)` block silently turned it
// into a non-nil RDS TLS config. We assert nil for BOTH host kinds.
func TestGetTLSConfigForBinlogCaseInsensitive(t *testing.T) {
	const (
		nonRDSHost = "mysql.internal.example"
		rdsHost    = "mydb.us-west-2.rds.amazonaws.com"
	)

	// DISABLED in any case must produce no TLS config for the binlog conn,
	// regardless of whether the host is an RDS host.
	for _, host := range []string{nonRDSHost, rdsHost} {
		for _, mode := range []string{"DISABLED", "disabled", "Disabled", "DiSaBlEd"} {
			t.Run(fmt.Sprintf("disabled_%s_%s", mode, host), func(t *testing.T) {
				cfg := &DBConfig{TLSMode: mode}
				tlsConfig, err := GetTLSConfigForBinlog(cfg, host)
				require.NoError(t, err)
				require.Nil(t, tlsConfig, "DISABLED (case %q, host %q) must yield a nil binlog TLS config", mode, host)
			})
		}
	}

	// PREFERRED/REQUIRED in any case must produce a non-nil TLS config, and the
	// ServerName must be set to the host. We assert lowercase behaves like
	// uppercase.
	for _, mode := range []string{"PREFERRED", "preferred", "Preferred", "REQUIRED", "required", "Required"} {
		t.Run("enabled_"+mode, func(t *testing.T) {
			cfg := &DBConfig{TLSMode: mode}
			tlsConfig, err := GetTLSConfigForBinlog(cfg, nonRDSHost)
			require.NoError(t, err)
			require.NotNil(t, tlsConfig, "mode %q should yield a TLS config", mode)
			require.Equal(t, nonRDSHost, tlsConfig.ServerName)
		})
	}
}

// TestGetTLSConfigForBinlogDISABLEDReturnsNil is the explicit regression test
// called for in the task: it pins that the binlog TLS config for a lowercase
// "disabled" mode is nil. It uses an RDS host because that is where the
// case-sensitivity bug actually surfaces. On the UNFIXED code this FAILS:
// lowercase "disabled" misses the exact "DISABLED" early return, falls into the
// default branch (nil), and the post-switch RDS block promotes it to a non-nil
// *tls.Config — exactly the confusing TLS-on-a-DISABLED-binlog-connection the
// fix prevents.
func TestGetTLSConfigForBinlogDISABLEDReturnsNil(t *testing.T) {
	cfg := &DBConfig{TLSMode: "disabled"}
	tlsConfig, err := GetTLSConfigForBinlog(cfg, "mydb.us-west-2.rds.amazonaws.com")
	require.NoError(t, err)
	require.Nil(t, tlsConfig, "lowercase 'disabled' must return a nil binlog TLS config (no TLS), even for RDS hosts")
}

// TestNewWithConnectionTypeCaseInsensitivePool verifies that lowercase and
// mixed-case TLS mode strings drive the same DSN/TLS decision in the pool
// constructor as their uppercase forms. We don't need a live server for this:
// newDSN is the chokepoint, and we assert the generated TLS config name.
func TestNewWithConnectionTypeCaseInsensitivePool(t *testing.T) {
	const nonRDSHost = "root:password@tcp(mysql.internal.example:3306)/test"

	cases := []struct {
		mode        string
		expectTLS   string // substring expected in DSN, "" means no tls= param
		description string
	}{
		{"DISABLED", "", "uppercase disabled"},
		{"disabled", "", "lowercase disabled"},
		{"Disabled", "", "mixedcase disabled"},
		{"PREFERRED", "tls=custom", "uppercase preferred"},
		{"preferred", "tls=custom", "lowercase preferred"},
		{"Preferred", "tls=custom", "mixedcase preferred"},
		{"REQUIRED", "tls=required", "uppercase required"},
		{"required", "tls=required", "lowercase required"},
		{"Required", "tls=required", "mixedcase required"},
	}
	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			cfg := NewDBConfig()
			cfg.TLSMode = tc.mode
			// newDSN upper-cases internally, so this already proves the
			// chokepoint is case-insensitive for DSN generation.
			dsn, err := newDSN(nonRDSHost, cfg)
			require.NoError(t, err)
			if tc.expectTLS == "" {
				require.NotContains(t, dsn, "tls=", tc.description)
			} else {
				require.Contains(t, dsn, tc.expectTLS, tc.description)
			}
		})
	}
}

// TestNewWithConnectionTypePreferredLowercaseLive proves that lowercase
// "preferred" connects to the live local MySQL exactly like uppercase
// "PREFERRED" (which is the default). Before the fix, config.TLSMode was
// compared with == "PREFERRED" (case-sensitive), so a lowercase "preferred"
// skipped the PREFERRED branch entirely and used the standard connection path;
// the connection still worked but did not exercise the documented
// case-insensitive contract. This test pins that both succeed.
func TestNewWithConnectionTypePreferredLowercaseLive(t *testing.T) {
	for _, mode := range []string{"PREFERRED", "preferred", "Preferred"} {
		t.Run(mode, func(t *testing.T) {
			cfg := NewDBConfig()
			cfg.TLSMode = mode
			db, err := New(testutils.DSN(), cfg)
			require.NoError(t, err)
			require.NotNil(t, db)
			defer utils.CloseAndLog(db)

			var resp int
			err = db.QueryRowContext(t.Context(), "SELECT 1").Scan(&resp)
			require.NoError(t, err)
			require.Equal(t, 1, resp)
		})
	}
}

// TestPreferredNonTLSErrorPropagates verifies that under PREFERRED mode a ping
// failure that is NOT "server does not support TLS" propagates as an error
// instead of silently downgrading the whole pool to plaintext. We use a
// deterministic non-TLS failure: a syntactically valid DSN pointing at the
// live server but with wrong credentials. The TLS attempt fails with an auth
// error (1045), which is not mysql.ErrNoTLS, so New must return an error
// rather than retrying plaintext and connecting.
//
// (Against the local TLS-enabled server, the genuine no-TLS-server fallback
// path is unreachable; the error-classification itself is covered directly by
// TestIsTLSUnsupportedByServer above.)
func TestPreferredNonTLSErrorPropagates(t *testing.T) {
	// Reuse the live host/port but with bad credentials so the failure is a
	// deterministic auth error, not a network or TLS error.
	liveCfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	badDSN := fmt.Sprintf("nosuchuser:wrongpassword@tcp(%s)/%s", liveCfg.Addr, liveCfg.DBName)

	cfg := NewDBConfig()
	cfg.TLSMode = "PREFERRED"
	db, err := New(badDSN, cfg)
	require.Error(t, err, "PREFERRED must propagate a non-TLS ping failure instead of downgrading to plaintext")
	require.Nil(t, db)
	// The error must reflect the original (auth) failure, not a fallback ping.
	require.NotContains(t, err.Error(), "FALLBACK",
		"a non-TLS failure must not reach the plaintext fallback path")
}

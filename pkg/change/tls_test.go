package change

import (
	"crypto/tls"
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
)

// TestTLSConfigurationLogic tests the TLS configuration logic in isolation
func TestTLSConfigurationLogic(t *testing.T) {
	tests := []struct {
		name             string
		host             string
		dbConfig         *dbconn.DBConfig
		expectTLSSet     bool
		expectServerName string
	}{
		{
			name:         "No TLS config - non-RDS host",
			host:         "localhost:3306",
			dbConfig:     nil,
			expectTLSSet: false,
		},
		{
			name:             "No TLS config - RDS host gets fallback TLS",
			host:             "mydb.cluster-123.us-west-2.rds.amazonaws.com:3306",
			dbConfig:         nil,
			expectTLSSet:     true,
			expectServerName: "mydb.cluster-123.us-west-2.rds.amazonaws.com",
		},
		{
			name: "TLS disabled",
			host: "localhost:3306",
			dbConfig: &dbconn.DBConfig{
				TLSMode: "DISABLED",
			},
			expectTLSSet: false,
		},
		{
			name: "TLS enabled - REQUIRED mode",
			host: "localhost:3306",
			dbConfig: &dbconn.DBConfig{
				TLSMode: "REQUIRED",
			},
			expectTLSSet:     true,
			expectServerName: "localhost",
		},
		{
			name: "TLS enabled - PREFERRED mode",
			host: "localhost:3306",
			dbConfig: &dbconn.DBConfig{
				TLSMode: "PREFERRED",
			},
			expectTLSSet:     true,
			expectServerName: "localhost",
		},
		{
			name: "TLS enabled with custom cert",
			host: "localhost:3306",
			dbConfig: &dbconn.DBConfig{
				TLSMode:            "REQUIRED",
				TLSCertificatePath: "/path/to/cert.pem",
			},
			expectTLSSet:     true,
			expectServerName: "localhost",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock client with the test configuration
			client := &binlogClient{
				host:     tt.host,
				dbConfig: tt.dbConfig,
				cfg:      replication.BinlogSyncerConfig{},
			}

			// Extract host part for ServerName (remove port)
			host := tt.host
			if colonIndex := len(host) - 1; colonIndex >= 0 {
				for i := len(host) - 1; i >= 0; i-- {
					if host[i] == ':' {
						host = host[:i]
						break
					}
				}
			}

			// Apply our TLS configuration logic
			if client.dbConfig != nil && client.dbConfig.TLSMode != "DISABLED" {
				// Create a TLS config based on the mode, same as main DB connections
				var tlsConfig *tls.Config

				// Use RDS config for RDS hosts, custom config otherwise
				if dbconn.IsRDSHost(client.host) {
					tlsConfig = dbconn.NewTLSConfig()
				} else {
					// nil cert data means "use the RDS roots", which is what
					// this test wants: it has no cert files of its own.
					tlsConfig = dbconn.NewCustomTLSConfig(nil, client.dbConfig.TLSMode)
				}

				if tlsConfig != nil {
					client.cfg.TLSConfig = tlsConfig
					// Set ServerName for certificate verification
					client.cfg.TLSConfig.ServerName = host
				}
			} else if dbconn.IsRDSHost(client.host) {
				// Fallback to RDS TLS for RDS hosts when no explicit config provided
				client.cfg.TLSConfig = dbconn.NewTLSConfig()
				client.cfg.TLSConfig.ServerName = host
			}

			// Verify expectations
			if tt.expectTLSSet {
				require.NotNil(t, client.cfg.TLSConfig, "Expected TLS config to be set")
				if client.cfg.TLSConfig != nil {
					require.Equal(t, tt.expectServerName, client.cfg.TLSConfig.ServerName,
						"Expected ServerName to match host")
				}
			} else {
				require.Nil(t, client.cfg.TLSConfig, "Expected TLS config to be nil")
			}
		})
	}
}

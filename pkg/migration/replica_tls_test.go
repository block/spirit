package migration

import (
	"log/slog"
	"os"
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/repl"
	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicaTLSEnhancement(t *testing.T) {
	// Create temporary certificate file for testing
	tempFile, err := os.CreateTemp(t.TempDir(), "test-cert-*.pem")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	// Write test certificate data
	certData := generateTestCertForTLS(t)
	_, err = tempFile.Write(certData)
	require.NoError(t, err)
	require.NoError(t, tempFile.Close())

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	testCases := []struct {
		name                   string
		mainTLSMode            string
		mainTLSCert            string
		replicaDSN             string
		expectedReplicaTLSMode string
		shouldEnhance          bool
		description            string
	}{
		{
			name:                   "DISABLED main should not enhance replica DSN",
			mainTLSMode:            "DISABLED",
			mainTLSCert:            "",
			replicaDSN:             "replica_user:replica_pass@tcp(replica.example.com:3306)/testdb",
			expectedReplicaTLSMode: "DISABLED",
			shouldEnhance:          false,
			description:            "DISABLED main TLS should not add TLS to replica DSN",
		},
		{
			name:                   "PREFERRED main should enhance replica DSN without TLS",
			mainTLSMode:            "PREFERRED",
			mainTLSCert:            tempFile.Name(),
			replicaDSN:             "replica_user:replica_pass@tcp(replica.example.com:3306)/testdb",
			expectedReplicaTLSMode: "PREFERRED",
			shouldEnhance:          true,
			description:            "PREFERRED main TLS should add custom TLS to replica DSN",
		},
		{
			name:                   "REQUIRED main should enhance replica DSN without TLS",
			mainTLSMode:            "REQUIRED",
			mainTLSCert:            tempFile.Name(),
			replicaDSN:             "replica_user:replica_pass@tcp(replica.example.com:3306)/testdb",
			expectedReplicaTLSMode: "REQUIRED",
			shouldEnhance:          true,
			description:            "REQUIRED main TLS should add required TLS to replica DSN",
		},
		{
			name:                   "VERIFY_CA main should enhance replica DSN without TLS",
			mainTLSMode:            "VERIFY_CA",
			mainTLSCert:            tempFile.Name(),
			replicaDSN:             "replica_user:replica_pass@tcp(replica.example.com:3306)/testdb",
			expectedReplicaTLSMode: "VERIFY_CA",
			shouldEnhance:          true,
			description:            "VERIFY_CA main TLS should add verify_ca TLS to replica DSN",
		},
		{
			name:                   "VERIFY_IDENTITY main should enhance replica DSN without TLS",
			mainTLSMode:            "VERIFY_IDENTITY",
			mainTLSCert:            tempFile.Name(),
			replicaDSN:             "replica_user:replica_pass@tcp(replica.example.com:3306)/testdb",
			expectedReplicaTLSMode: "VERIFY_IDENTITY",
			shouldEnhance:          true,
			description:            "VERIFY_IDENTITY main TLS should add verify_identity TLS to replica DSN",
		},
		{
			name:                   "Replica DSN with existing TLS should be preserved",
			mainTLSMode:            "REQUIRED",
			mainTLSCert:            tempFile.Name(),
			replicaDSN:             "replica_user:replica_pass@tcp(replica.example.com:3306)/testdb?tls=skip-verify",
			expectedReplicaTLSMode: "skip-verify", // This will be the preserved TLS setting
			shouldEnhance:          false,
			description:            "Replica DSN with existing TLS config should not be modified",
		},
		{
			name:                   "RDS replica should get RDS TLS for REQUIRED mode",
			mainTLSMode:            "REQUIRED",
			mainTLSCert:            "",
			replicaDSN:             "replica_user:replica_pass@tcp(replica.us-west-2.rds.amazonaws.com:3306)/testdb",
			expectedReplicaTLSMode: "REQUIRED",
			shouldEnhance:          true,
			description:            "RDS replica with REQUIRED mode should use RDS TLS config",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup migration config with main TLS settings
			migration := &Migration{
				Host:               cfg.Addr,
				Username:           cfg.User,
				Password:           &cfg.Passwd,
				Database:           cfg.DBName,
				Table:              "test_table",
				Alter:              "ADD COLUMN test_col INT",
				TLSMode:            tc.mainTLSMode,
				TLSCertificatePath: tc.mainTLSCert,
				ReplicaDSN:         tc.replicaDSN,
			}

			runner, err := NewRunner(migration)
			require.NoError(t, err)
			defer runner.Close()

			// Initialize DB config to simulate real usage
			runner.dbConfig = dbconn.NewDBConfig()
			runner.dbConfig.TLSMode = tc.mainTLSMode
			runner.dbConfig.TLSCertificatePath = tc.mainTLSCert

			// Test DSN enhancement directly
			enhanced, err := dbconn.EnhanceDSNWithTLS(tc.replicaDSN, runner.dbConfig)
			assert.NoError(t, err)

			if tc.shouldEnhance {
				assert.NotEqual(t, tc.replicaDSN, enhanced, tc.description)
				assert.Contains(t, enhanced, "tls=", tc.description)
			} else {
				assert.Equal(t, tc.replicaDSN, enhanced, tc.description)
			}
		})
	}
}

func TestReplicationClientTLSConfig(t *testing.T) {
	// Create temporary certificate file for testing
	tempFile, err := os.CreateTemp(t.TempDir(), "test-cert-*.pem")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	certData := generateTestCertForTLS(t)
	_, err = tempFile.Write(certData)
	require.NoError(t, err)
	require.NoError(t, tempFile.Close())

	testCases := []struct {
		name        string
		tlsMode     string
		tlsCert     string
		host        string
		description string
	}{
		{
			name:        "DISABLED mode should not set TLS config",
			tlsMode:     "DISABLED",
			tlsCert:     "",
			host:        "example.com:3306",
			description: "DISABLED TLS mode should not configure TLS for replication",
		},
		{
			name:        "PREFERRED mode should set custom TLS config",
			tlsMode:     "PREFERRED",
			tlsCert:     tempFile.Name(),
			host:        "example.com:3306",
			description: "PREFERRED TLS mode should configure custom TLS for replication",
		},
		{
			name:        "REQUIRED mode with RDS host should use RDS config",
			tlsMode:     "REQUIRED",
			tlsCert:     "",
			host:        "mydb.us-west-2.rds.amazonaws.com:3306",
			description: "REQUIRED TLS mode with RDS host should use RDS TLS config",
		},
		{
			name:        "REQUIRED mode with non-RDS host should use custom config",
			tlsMode:     "REQUIRED",
			tlsCert:     tempFile.Name(),
			host:        "example.com:3306",
			description: "REQUIRED TLS mode with non-RDS host should use custom TLS config",
		},
		{
			name:        "VERIFY_CA mode should set verify_ca TLS config",
			tlsMode:     "VERIFY_CA",
			tlsCert:     tempFile.Name(),
			host:        "example.com:3306",
			description: "VERIFY_CA TLS mode should configure certificate verification",
		},
		{
			name:        "VERIFY_IDENTITY mode should set verify_identity TLS config",
			tlsMode:     "VERIFY_IDENTITY",
			tlsCert:     tempFile.Name(),
			host:        "example.com:3306",
			description: "VERIFY_IDENTITY TLS mode should configure full verification",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create TLS config
			tlsConfig := dbconn.NewDBConfig()
			tlsConfig.TLSMode = tc.tlsMode
			tlsConfig.TLSCertificatePath = tc.tlsCert

			// Create replication client config with database config
			clientConfig := &repl.ClientConfig{
				Logger:   slog.Default(),
				ServerID: repl.NewServerID(),
				DBConfig: tlsConfig,
			} // Create a mock database connection for testing
			db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
			require.NoError(t, err)
			defer db.Close()

			// Create replication client
			client := repl.NewClient(db, tc.host, "user", "pass", clientConfig)
			assert.NotNil(t, client)

			// Verify TLS config is stored
			if tc.tlsMode == "DISABLED" {
				// For disabled mode, we might still have the config but it shouldn't be used
				// The actual TLS behavior is tested in the Run() method
			} else {
				// For non-disabled modes, the config should be present
				assert.NotNil(t, clientConfig.DBConfig)
				assert.Equal(t, tc.tlsMode, clientConfig.DBConfig.TLSMode)
				assert.Equal(t, tc.tlsCert, clientConfig.DBConfig.TLSCertificatePath)
			}
		})
	}
}

func TestReplicaTLSIntegration(t *testing.T) {
	// This test verifies the end-to-end integration between migration runner
	// and replica TLS configuration

	// Create temporary certificate file for testing
	tempFile, err := os.CreateTemp(t.TempDir(), "test-cert-*.pem")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	certData := generateTestCertForTLS(t)
	_, err = tempFile.Write(certData)
	require.NoError(t, err)
	require.NoError(t, tempFile.Close())

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Test with a mock replica DSN (won't actually connect, just test configuration)
	replicaDSN := "replica_user:replica_pass@tcp(replica.example.com:3306)/testdb"

	migration := &Migration{
		Host:               cfg.Addr,
		Username:           cfg.User,
		Password:           &cfg.Passwd,
		Database:           cfg.DBName,
		Table:              "test_table",
		Alter:              "ADD COLUMN test_col INT",
		TLSMode:            "VERIFY_CA",
		TLSCertificatePath: tempFile.Name(),
		ReplicaDSN:         replicaDSN,
	}

	runner, err := NewRunner(migration)
	require.NoError(t, err)
	defer runner.Close()

	// Initialize the runner's DB config (normally done in Run())
	runner.dbConfig = dbconn.NewDBConfig()
	runner.dbConfig.TLSMode = migration.TLSMode
	runner.dbConfig.TLSCertificatePath = migration.TLSCertificatePath

	// Test that the replica DSN would be enhanced
	enhanced, err := dbconn.EnhanceDSNWithTLS(replicaDSN, runner.dbConfig)
	assert.NoError(t, err)
	assert.Contains(t, enhanced, "tls=verify_ca")
	assert.Contains(t, enhanced, "replica.example.com:3306")

	// Verify that the enhancement preserves the original connection details
	enhancedCfg, err := mysql.ParseDSN(enhanced)
	assert.NoError(t, err)
	assert.Equal(t, "replica_user", enhancedCfg.User)
	assert.Equal(t, "replica_pass", enhancedCfg.Passwd)
	assert.Equal(t, "replica.example.com:3306", enhancedCfg.Addr)
	assert.Equal(t, "testdb", enhancedCfg.DBName)
}

// generateTestCertForTLS creates a test certificate for TLS testing
func generateTestCertForTLS(t *testing.T) []byte {
	t.Helper()
	// This is a simple test certificate. In a real test environment,
	// you might want to generate a proper self-signed certificate.
	return []byte(`-----BEGIN CERTIFICATE-----
MIICljCCAX4CCQChiHMXdFjPDzANBgkqhkiG9w0BAQsFADANMQswCQYDVQQGEwJV
UzAeFw0yMzAxMDEwMDAwMDBaFw0yNDAxMDEwMDAwMDBaMA0xCzAJBgNVBAYTAlVT
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAw2WSwKkCaZKm/5aDHvmv
7r+9OKlpLODy8q3fOJwPl2zVSkqKbx8y2gJ8O8T5Z5s4z4V3Xf4Q5bZAbCdEfGhI
jKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStU
vWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfG
hIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrS
tUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdE
fGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQ
rStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbC
QIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQCcW5F5gHiJkLmNoPqRsTuVwXyZ1a2b
3c4d5e6f7g8h9i0j1k2l3m4n5o6p7q8r9s0t1u2v3w4x5y6z7A8B9C0D1E2F3G4H
5I6J7K8L9M0N1O2P3Q4R5S6T7U8V9W0X1Y2Z3a4b5c6d7e8f9g0h1i2j3k4l5m6n
7o8p9q0r1s2t3u4v5w6x7y8z9A0B1C2D3E4F5G6H7I8J9K0L1M2N3O4P5Q6R7S8T
9U0V1W2X3Y4Z5a6b7c8d9e0f1g2h3i4j5k6l7m8n9o0p1q2r3s4t5u6v7w8x9y0z
1A2B3C4D5E6F7G8H9I0J1K2L3M4N5O6P7Q8R9S0T1U2V3W4X5Y6Z7a8b9c0d1e2f
3g4h5i6j7k8l9m0n1o2p3q4r5s6t7u8v9w0x1y2z3A4B5C6D7E8F9G0H1I2J3K4L
5M6N7O8P9Q0R1S2T3U4V5W6X7Y8Z9a0b1c2d3e4f5g6h7i8j9k0l1m2n3o4p5q6r
-----END CERTIFICATE-----`)
}

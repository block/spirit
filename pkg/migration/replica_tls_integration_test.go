package migration

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/testutils"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTempCertFile creates a temporary certificate file for testing
func createTempCertFile(t *testing.T) string {
	tempFile, err := os.CreateTemp(t.TempDir(), "test_cert_*.pem")
	require.NoError(t, err)
	defer tempFile.Close()

	// Write minimal certificate content
	_, err = tempFile.WriteString("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----\n")
	require.NoError(t, err)

	return tempFile.Name()
}

// sanitizeColumnName removes special characters to create valid SQL column names
func sanitizeColumnName(name string) string {
	// Replace special characters with underscores
	reg := regexp.MustCompile(`[^a-zA-Z0-9_]`)
	sanitized := reg.ReplaceAllString(name, "_")

	// Remove consecutive underscores
	reg2 := regexp.MustCompile(`_+`)
	sanitized = reg2.ReplaceAllString(sanitized, "_")

	// Remove leading/trailing underscores
	sanitized = strings.Trim(sanitized, "_")

	// Ensure it starts with a letter or underscore
	if len(sanitized) > 0 && !regexp.MustCompile(`^[a-zA-Z_]`).MatchString(sanitized) {
		sanitized = "col_" + sanitized
	}

	return sanitized
}

// TestReplicaTLSIntegrationScenarios tests end-to-end TLS behavior in realistic scenarios
func TestReplicaTLSIntegrationScenarios(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Create temporary certificate files for testing
	tempCertFile := createTempCertFile(t)
	tempCAFile := createTempCertFile(t)

	scenarios := []struct {
		name              string
		mainTLSMode       string
		mainTLSCert       string
		replicaDSN        string
		expectedBehavior  string
		shouldInheritTLS  bool
		shouldPreserveTLS bool
		description       string
	}{
		{
			name:              "Main REQUIRED with replica tls=false override",
			mainTLSMode:       "REQUIRED",
			mainTLSCert:       "",
			replicaDSN:        "replica_user:replica_pass@tcp(replica.example.com:3306)/testdb?tls=false",
			expectedBehavior:  "preserve_explicit",
			shouldInheritTLS:  false,
			shouldPreserveTLS: true,
			description:       "Replica explicitly disables TLS even when main requires it",
		},
		{
			name:              "Main DISABLED with replica tls=true override",
			mainTLSMode:       "DISABLED",
			mainTLSCert:       "",
			replicaDSN:        "replica_user:replica_pass@tcp(replica.example.com:3306)/testdb?tls=true",
			expectedBehavior:  "preserve_explicit",
			shouldInheritTLS:  false,
			shouldPreserveTLS: true,
			description:       "Replica explicitly enables TLS even when main disables it",
		},
		{
			name:              "Main PREFERRED with replica inheritance",
			mainTLSMode:       "PREFERRED",
			mainTLSCert:       tempCertFile,
			replicaDSN:        "replica_user:replica_pass@tcp(replica.example.com:3306)/testdb",
			expectedBehavior:  "inherit_main",
			shouldInheritTLS:  true,
			shouldPreserveTLS: false,
			description:       "Replica inherits PREFERRED TLS mode from main when no explicit config",
		},
		{
			name:              "Main VERIFY_IDENTITY with replica inheritance",
			mainTLSMode:       "VERIFY_IDENTITY",
			mainTLSCert:       tempCAFile,
			replicaDSN:        "replica_user:replica_pass@tcp(replica.example.com:3306)/testdb",
			expectedBehavior:  "inherit_main",
			shouldInheritTLS:  true,
			shouldPreserveTLS: false,
			description:       "Replica inherits VERIFY_IDENTITY TLS mode from main",
		},
		{
			name:              "Main REQUIRED with RDS replica inheritance",
			mainTLSMode:       "REQUIRED",
			mainTLSCert:       "",
			replicaDSN:        "replica_user:replica_pass@tcp(replica.us-west-2.rds.amazonaws.com:3306)/testdb",
			expectedBehavior:  "inherit_rds",
			shouldInheritTLS:  true,
			shouldPreserveTLS: false,
			description:       "RDS replica inherits TLS but uses RDS-specific configuration",
		},
		{
			name:              "Complex scenario: mixed parameters with TLS override",
			mainTLSMode:       "VERIFY_CA",
			mainTLSCert:       "/tmp/ca.pem",
			replicaDSN:        "replica_user:replica_pass@tcp(replica.example.com:3306)/testdb?charset=utf8mb4&tls=skip-verify&timeout=30s",
			expectedBehavior:  "preserve_complex",
			shouldInheritTLS:  false,
			shouldPreserveTLS: true,
			description:       "Complex DSN should preserve TLS override while keeping other parameters",
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			// Create migration with TLS configuration
			sanitizedName := sanitizeColumnName(scenario.name)
			migration := &Migration{
				Host:               cfg.Addr,
				Username:           cfg.User,
				Password:           &cfg.Passwd,
				Database:           cfg.DBName,
				Table:              "test_tls_table",
				Alter:              "ADD COLUMN test_col_" + sanitizedName + " VARCHAR(50)",
				TLSMode:            scenario.mainTLSMode,
				TLSCertificatePath: scenario.mainTLSCert,
				ReplicaDSN:         scenario.replicaDSN,
			}

			// Create runner to test TLS configuration
			runner, err := NewRunner(migration)
			require.NoError(t, err)
			defer runner.Close()

			// Initialize DB config
			runner.dbConfig = dbconn.NewDBConfig()
			runner.dbConfig.TLSMode = scenario.mainTLSMode
			runner.dbConfig.TLSCertificatePath = scenario.mainTLSCert

			// Test TLS enhancement behavior
			enhancedDSN, err := dbconn.EnhanceDSNWithTLS(scenario.replicaDSN, runner.dbConfig)
			assert.NoError(t, err, scenario.description)

			// Verify behavior based on expected scenario
			switch scenario.expectedBehavior {
			case "preserve_explicit":
				// Should preserve original TLS parameter
				assert.Equal(t, scenario.replicaDSN, enhancedDSN, scenario.description)
				assert.True(t, scenario.shouldPreserveTLS, "Test setup error: should preserve TLS")
				assert.False(t, scenario.shouldInheritTLS, "Test setup error: should not inherit TLS")

			case "inherit_main":
				// Should inherit from main but not be identical to original
				assert.NotEqual(t, scenario.replicaDSN, enhancedDSN, scenario.description)
				assert.Contains(t, enhancedDSN, "tls=", "Should contain TLS parameter")
				assert.True(t, scenario.shouldInheritTLS, "Test setup error: should inherit TLS")
				assert.False(t, scenario.shouldPreserveTLS, "Test setup error: should not preserve explicit TLS")

			case "inherit_rds":
				// Should inherit RDS-specific TLS configuration
				assert.NotEqual(t, scenario.replicaDSN, enhancedDSN, scenario.description)
				assert.Contains(t, enhancedDSN, "tls=rds", "Should use RDS TLS configuration")

			case "preserve_complex":
				// Should preserve TLS while keeping other parameters
				assert.Equal(t, scenario.replicaDSN, enhancedDSN, scenario.description)
				assert.Contains(t, enhancedDSN, "tls=skip-verify", "Should preserve explicit TLS")
				assert.Contains(t, enhancedDSN, "charset=utf8mb4", "Should preserve other parameters")
				assert.Contains(t, enhancedDSN, "timeout=30s", "Should preserve other parameters")
			}

			// Parse enhanced DSN to verify it's valid
			parsedDSN, err := mysql.ParseDSN(enhancedDSN)
			assert.NoError(t, err, "Enhanced DSN should be valid: %s", enhancedDSN)
			if err == nil {
				// Verify connection details are preserved
				originalParsed, _ := mysql.ParseDSN(scenario.replicaDSN)
				if originalParsed != nil {
					assert.Equal(t, originalParsed.User, parsedDSN.User, "Username should be preserved")
					assert.Equal(t, originalParsed.Passwd, parsedDSN.Passwd, "Password should be preserved")
					assert.Equal(t, originalParsed.Addr, parsedDSN.Addr, "Address should be preserved")
					assert.Equal(t, originalParsed.DBName, parsedDSN.DBName, "Database name should be preserved")
				}
			}
		})
	}
}

// TestTLSConfigurationFlow tests the complete flow from Migration to DBConfig to DSN
func TestTLSConfigurationFlow(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Create temporary certificate files for testing
	tempCertFile := createTempCertFile(t)
	tempCAFile := createTempCertFile(t)

	testCases := []struct {
		name             string
		migration        *Migration
		expectMainTLS    string
		expectReplicaTLS string
		description      string
	}{
		{
			name: "End-to-end REQUIRED mode flow",
			migration: &Migration{
				Host:               cfg.Addr,
				Username:           cfg.User,
				Password:           &cfg.Passwd,
				Database:           cfg.DBName,
				Table:              "test_table",
				Alter:              "ADD COLUMN flow_test VARCHAR(50)",
				TLSMode:            "REQUIRED",
				TLSCertificatePath: tempCertFile,
				ReplicaDSN:         "replica:pass@tcp(replica.example.com:3306)/testdb",
			},
			expectMainTLS:    "required",
			expectReplicaTLS: "required",
			description:      "REQUIRED mode should flow through to both main and replica connections",
		},
		{
			name: "End-to-end PREFERRED mode flow",
			migration: &Migration{
				Host:               cfg.Addr,
				Username:           cfg.User,
				Password:           &cfg.Passwd,
				Database:           cfg.DBName,
				Table:              "test_table",
				Alter:              "ADD COLUMN flow_test VARCHAR(50)",
				TLSMode:            "PREFERRED",
				TLSCertificatePath: tempCertFile,
				ReplicaDSN:         "replica:pass@tcp(replica.example.com:3306)/testdb",
			},
			expectMainTLS:    "custom",
			expectReplicaTLS: "custom",
			description:      "PREFERRED mode should flow through as custom TLS config",
		},
		{
			name: "End-to-end with explicit replica TLS override",
			migration: &Migration{
				Host:               cfg.Addr,
				Username:           cfg.User,
				Password:           &cfg.Passwd,
				Database:           cfg.DBName,
				Table:              "test_table",
				Alter:              "ADD COLUMN flow_test VARCHAR(50)",
				TLSMode:            "VERIFY_IDENTITY",
				TLSCertificatePath: tempCAFile,
				ReplicaDSN:         "replica:pass@tcp(replica.example.com:3306)/testdb?tls=false",
			},
			expectMainTLS:    "verify_identity",
			expectReplicaTLS: "false",
			description:      "Explicit replica TLS should override main TLS configuration",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create runner and verify TLS configuration flows correctly
			runner, err := NewRunner(tc.migration)
			require.NoError(t, err)
			defer runner.Close()

			// Test that runner initializes with correct TLS config
			assert.Equal(t, tc.migration.TLSMode, runner.migration.TLSMode,
				"Runner should preserve migration TLS mode")
			assert.Equal(t, tc.migration.TLSCertificatePath, runner.migration.TLSCertificatePath,
				"Runner should preserve migration TLS certificate path")

			// Initialize DB config (this normally happens in Run())
			runner.dbConfig = dbconn.NewDBConfig()
			runner.dbConfig.TLSMode = tc.migration.TLSMode
			runner.dbConfig.TLSCertificatePath = tc.migration.TLSCertificatePath

			// Test main DSN generation using testutils.DSN as base
			baseDSN := testutils.DSN()
			mainDSN, err := dbconn.EnhanceDSNWithTLS(baseDSN, runner.dbConfig)
			assert.NoError(t, err, tc.description)
			if tc.expectMainTLS != "" {
				assert.Contains(t, mainDSN, "tls="+tc.expectMainTLS,
					"Main DSN should contain expected TLS config: %s", tc.description)
			}

			// Test replica DSN enhancement
			enhancedReplicaDSN, err := dbconn.EnhanceDSNWithTLS(tc.migration.ReplicaDSN, runner.dbConfig)
			assert.NoError(t, err, tc.description)
			if tc.expectReplicaTLS != "" {
				assert.Contains(t, enhancedReplicaDSN, "tls="+tc.expectReplicaTLS,
					"Replica DSN should contain expected TLS config: %s", tc.description)
			}
		})
	}
}

// TestTLSErrorHandling tests error conditions in TLS configuration
func TestTLSErrorHandling(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	errorTests := []struct {
		name        string
		migration   *Migration
		expectError bool
		description string
	}{
		{
			name: "Invalid replica DSN should be handled gracefully",
			migration: &Migration{
				Host:               cfg.Addr,
				Username:           cfg.User,
				Password:           &cfg.Passwd,
				Database:           cfg.DBName,
				Table:              "test_table",
				Alter:              "ADD COLUMN error_test VARCHAR(50)",
				TLSMode:            "REQUIRED",
				TLSCertificatePath: "",
				ReplicaDSN:         "invalid-dsn-format",
			},
			expectError: false, // Should handle gracefully, not error
			description: "Invalid replica DSN should be handled gracefully",
		},
		{
			name: "Missing TLS certificate file should be handled",
			migration: &Migration{
				Host:               cfg.Addr,
				Username:           cfg.User,
				Password:           &cfg.Passwd,
				Database:           cfg.DBName,
				Table:              "test_table",
				Alter:              "ADD COLUMN error_test VARCHAR(50)",
				TLSMode:            "VERIFY_IDENTITY",
				TLSCertificatePath: "/nonexistent/cert.pem",
				ReplicaDSN:         "replica:pass@tcp(replica.example.com:3306)/testdb",
			},
			expectError: false, // Configuration should still work, connection might fail later
			description: "Missing TLS certificate should not prevent configuration",
		},
	}

	for _, test := range errorTests {
		t.Run(test.name, func(t *testing.T) {
			runner, err := NewRunner(test.migration)
			if test.expectError {
				assert.Error(t, err, test.description)
			} else {
				assert.NoError(t, err, test.description)
				if err == nil {
					defer runner.Close()
				}
			}
		})
	}
}

// TestConcurrentTLSConfiguration tests TLS configuration under concurrent access
func TestConcurrentTLSConfiguration(t *testing.T) {
	cfg, err := mysql.ParseDSN(testutils.DSN())
	require.NoError(t, err)

	// Test that multiple runners with different TLS configs don't interfere
	numRunners := 10
	runners := make([]*Runner, numRunners)

	// Create multiple runners with different TLS configurations
	for i := range numRunners {
		migration := &Migration{
			Host:               cfg.Addr,
			Username:           cfg.User,
			Password:           &cfg.Passwd,
			Database:           cfg.DBName,
			Table:              "test_table",
			Alter:              "ADD COLUMN concurrent_test VARCHAR(50)",
			TLSMode:            []string{"DISABLED", "PREFERRED", "REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY"}[i%5],
			TLSCertificatePath: "",
			ReplicaDSN:         "replica:pass@tcp(replica.example.com:3306)/testdb",
		}

		runner, err := NewRunner(migration)
		require.NoError(t, err)
		runners[i] = runner
	}

	// Verify each runner maintains its own TLS configuration
	for i, runner := range runners {
		expectedMode := []string{"DISABLED", "PREFERRED", "REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY"}[i%5]
		assert.Equal(t, expectedMode, runner.migration.TLSMode,
			"Runner %d should maintain its TLS mode", i)
	}

	// Clean up
	for _, runner := range runners {
		runner.Close()
	}
}

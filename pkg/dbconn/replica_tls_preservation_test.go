package dbconn

import (
	"os"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/require"
)

// createTempCertFile creates a temporary certificate file for testing
func createTempCertFile(t *testing.T) string {
	tempFile, err := os.CreateTemp(t.TempDir(), "test_cert_*.pem")
	require.NoError(t, err)
	defer utils.CloseAndLog(tempFile)

	// Write minimal certificate content
	_, err = tempFile.WriteString("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----\n")
	require.NoError(t, err)

	return tempFile.Name()
}

// TestTLSParameterPreservation tests the core TLS bug fix - ensuring explicit TLS parameters
// in replica DSN are preserved and not overridden by main connection TLS settings
func TestTLSParameterPreservation(t *testing.T) {
	tests := []struct {
		name           string
		mainTLSMode    string
		replicaDSN     string
		expectedDSN    string
		shouldPreserve bool
		description    string
	}{
		{
			name:           "tls=false should be preserved with REQUIRED main",
			mainTLSMode:    "REQUIRED",
			replicaDSN:     "user:pass@tcp(replica:3306)/db?tls=false",
			expectedDSN:    "user:pass@tcp(replica:3306)/db?tls=false",
			shouldPreserve: true,
			description:    "Explicit tls=false should override main REQUIRED TLS mode",
		},
		{
			name:           "tls=true should be preserved with DISABLED main",
			mainTLSMode:    "DISABLED",
			replicaDSN:     "user:pass@tcp(replica:3306)/db?tls=true",
			expectedDSN:    "user:pass@tcp(replica:3306)/db?tls=true",
			shouldPreserve: true,
			description:    "Explicit tls=true should override main DISABLED TLS mode",
		},
		{
			name:           "tls=skip-verify should be preserved",
			mainTLSMode:    "VERIFY_IDENTITY",
			replicaDSN:     "user:pass@tcp(replica:3306)/db?tls=skip-verify",
			expectedDSN:    "user:pass@tcp(replica:3306)/db?tls=skip-verify",
			shouldPreserve: true,
			description:    "Explicit tls=skip-verify should override main VERIFY_IDENTITY mode",
		},
		{
			name:           "tls=true should be preserved",
			mainTLSMode:    "PREFERRED",
			replicaDSN:     "user:pass@tcp(replica:3306)/db?tls=true",
			expectedDSN:    "user:pass@tcp(replica:3306)/db?tls=true",
			shouldPreserve: true,
			description:    "Explicit tls=true should override main PREFERRED mode",
		},
		{
			name:           "tls=preferred should be preserved",
			mainTLSMode:    "REQUIRED",
			replicaDSN:     "user:pass@tcp(replica:3306)/db?tls=preferred",
			expectedDSN:    "user:pass@tcp(replica:3306)/db?tls=preferred",
			shouldPreserve: true,
			description:    "Explicit tls=preferred should override main REQUIRED mode",
		},
		{
			name:           "tls=verify-ca should be preserved",
			mainTLSMode:    "DISABLED",
			replicaDSN:     "user:pass@tcp(replica:3306)/db?tls=verify-ca",
			expectedDSN:    "user:pass@tcp(replica:3306)/db?tls=verify-ca",
			shouldPreserve: true,
			description:    "Explicit tls=verify-ca should override main DISABLED mode",
		},
		{
			name:           "tls=preferred should be preserved",
			mainTLSMode:    "REQUIRED",
			replicaDSN:     "user:pass@tcp(replica:3306)/db?tls=preferred",
			expectedDSN:    "user:pass@tcp(replica:3306)/db?tls=preferred",
			shouldPreserve: true,
			description:    "Explicit tls=preferred should override main REQUIRED mode",
		},
		{
			name:           "No explicit TLS should inherit main config",
			mainTLSMode:    "REQUIRED",
			replicaDSN:     "user:pass@tcp(replica:3306)/db",
			expectedDSN:    "", // Will be enhanced by main TLS mode
			shouldPreserve: false,
			description:    "DSN without explicit TLS should inherit main TLS configuration",
		},
		{
			name:           "Complex DSN with other params should preserve TLS",
			mainTLSMode:    "VERIFY_CA",
			replicaDSN:     "user:pass@tcp(replica:3306)/db?charset=utf8&tls=false&timeout=30s",
			expectedDSN:    "user:pass@tcp(replica:3306)/db?charset=utf8&tls=false&timeout=30s",
			shouldPreserve: true,
			description:    "Complex DSN should preserve explicit TLS while keeping other parameters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := NewDBConfig()
			config.TLSMode = tt.mainTLSMode

			result, err := EnhanceDSNWithTLS(tt.replicaDSN, config)
			require.NoError(t, err)

			if tt.shouldPreserve {
				require.Equal(t, tt.expectedDSN, result, tt.description)
				require.Contains(t, result, "tls=", "Expected TLS parameter to be preserved")
			} else {
				require.NotEqual(t, tt.replicaDSN, result, tt.description)
				// For inheritance cases, verify TLS was added based on main config
				if tt.mainTLSMode != "DISABLED" {
					require.Contains(t, result, "tls=", "Expected TLS parameter to be inherited from main config")
				}
			}
		})
	}
}

// TestMixedEnvironmentTLSScenarios tests realistic mixed environment scenarios
func TestMixedEnvironmentTLSScenarios(t *testing.T) {
	scenarios := []struct {
		name        string
		mainTLSMode string
		mainHost    string
		replicaDSN  string
		expectedTLS string
		expectError bool
		description string
	}{
		{
			name:        "Main requires TLS, replica explicitly disabled",
			mainTLSMode: "REQUIRED",
			mainHost:    "secure-main.example.com:3306",
			replicaDSN:  "user:pass@tcp(legacy-replica.example.com:3306)/db?tls=false",
			expectedTLS: "tls=false",
			expectError: false,
			description: "Mixed environment where main requires TLS but replica can't support it",
		},
		{
			name:        "Main disabled, replica requires TLS",
			mainTLSMode: "DISABLED",
			mainHost:    "legacy-main.example.com:3306",
			replicaDSN:  "user:pass@tcp(secure-replica.example.com:3306)/db?tls=required",
			expectedTLS: "tls=required",
			expectError: false,
			description: "Mixed environment where main is legacy but replica requires TLS",
		},
		{
			name:        "Main preferred, replica skip-verify",
			mainTLSMode: "PREFERRED",
			mainHost:    "adaptive-main.example.com:3306",
			replicaDSN:  "user:pass@tcp(insecure-replica.example.com:3306)/db?tls=skip-verify",
			expectedTLS: "tls=skip-verify",
			expectError: false,
			description: "Adaptive main with insecure but encrypted replica",
		},
		{
			name:        "RDS main with custom CA replica",
			mainTLSMode: "REQUIRED",
			mainHost:    "main.us-west-2.rds.amazonaws.com:3306",
			replicaDSN:  "user:pass@tcp(on-prem-replica.corp.com:3306)/db?tls=preferred",
			expectedTLS: "tls=preferred",
			expectError: false,
			description: "Cloud main database with on-premise replica using custom certificates",
		},
		{
			name:        "Custom CA main with RDS replica",
			mainTLSMode: "REQUIRED",
			mainHost:    "on-prem-main.corp.com:3306",
			replicaDSN:  "user:pass@tcp(replica.us-east-1.rds.amazonaws.com:3306)/db",
			expectedTLS: "tls=rds", // Should be enhanced for RDS
			expectError: false,
			description: "On-premise main with cloud replica inheriting TLS",
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			config := NewDBConfig()
			config.TLSMode = scenario.mainTLSMode

			result, err := EnhanceDSNWithTLS(scenario.replicaDSN, config)

			if scenario.expectError {
				require.Error(t, err, scenario.description)
			} else {
				require.NoError(t, err, scenario.description)
				require.Contains(t, result, scenario.expectedTLS, scenario.description)
			}
		})
	}
}

// TestTLSInheritanceBehavior tests when replica DSN should inherit main TLS settings
func TestTLSInheritanceBehavior(t *testing.T) {
	// Create temporary certificate files for testing
	tempCertFile := createTempCertFile(t)
	tempCAFile := createTempCertFile(t)

	tests := []struct {
		name           string
		mainTLSMode    string
		mainTLSCert    string
		replicaDSN     string
		expectedConfig string
		expectCustom   bool
		description    string
	}{
		{
			name:           "PREFERRED inheritance",
			mainTLSMode:    "PREFERRED",
			mainTLSCert:    tempCertFile,
			replicaDSN:     "user:pass@tcp(replica.example.com:3306)/db",
			expectedConfig: "tls=custom",
			expectCustom:   true,
			description:    "PREFERRED mode should inherit as custom TLS config",
		},
		{
			name:           "REQUIRED inheritance for non-RDS",
			mainTLSMode:    "REQUIRED",
			mainTLSCert:    tempCertFile,
			replicaDSN:     "user:pass@tcp(replica.example.com:3306)/db",
			expectedConfig: "tls=required",
			expectCustom:   false,
			description:    "REQUIRED mode should inherit as required TLS config",
		},
		{
			name:           "REQUIRED inheritance for RDS",
			mainTLSMode:    "REQUIRED",
			mainTLSCert:    "",
			replicaDSN:     "user:pass@tcp(replica.us-west-2.rds.amazonaws.com:3306)/db",
			expectedConfig: "tls=rds",
			expectCustom:   false,
			description:    "REQUIRED mode for RDS should inherit as RDS TLS config",
		},
		{
			name:           "VERIFY_CA inheritance",
			mainTLSMode:    "VERIFY_CA",
			mainTLSCert:    tempCAFile,
			replicaDSN:     "user:pass@tcp(replica.example.com:3306)/db",
			expectedConfig: "tls=verify_ca",
			expectCustom:   false,
			description:    "VERIFY_CA mode should inherit as verify_ca TLS config",
		},
		{
			name:           "VERIFY_IDENTITY inheritance",
			mainTLSMode:    "VERIFY_IDENTITY",
			mainTLSCert:    tempCAFile,
			replicaDSN:     "user:pass@tcp(replica.example.com:3306)/db",
			expectedConfig: "tls=verify_identity",
			expectCustom:   false,
			description:    "VERIFY_IDENTITY mode should inherit as verify_identity TLS config",
		},
		{
			name:           "DISABLED inheritance",
			mainTLSMode:    "DISABLED",
			mainTLSCert:    "",
			replicaDSN:     "user:pass@tcp(replica.example.com:3306)/db",
			expectedConfig: "", // No TLS should be added
			expectCustom:   false,
			description:    "DISABLED mode should not add any TLS configuration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := NewDBConfig()
			config.TLSMode = tt.mainTLSMode
			config.TLSCertificatePath = tt.mainTLSCert

			result, err := EnhanceDSNWithTLS(tt.replicaDSN, config)
			require.NoError(t, err)

			if tt.expectedConfig == "" {
				// "No TLS" is a property of the resolved connection, not of
				// whether the DSN spells "tls=". DISABLED now writes tls=false
				// precisely so that an RDS replica gets no TLS, which the old
				// NotContains assertion would have called a failure while the
				// silent DSN it preferred was the one that got TLS.
				requireNoEffectiveTLS(t, result, tt.description)
			} else {
				require.Contains(t, result, tt.expectedConfig, tt.description)
			}
		})
	}
}

// TestCaseInsensitiveTLSModes tests that TLS modes are handled case-insensitively
func TestCaseInsensitiveTLSModes(t *testing.T) {
	cases := []string{"required", "REQUIRED", "Required", "rEqUiReD"}

	for _, tlsMode := range cases {
		t.Run("Case_"+tlsMode, func(t *testing.T) {
			config := NewDBConfig()
			config.TLSMode = tlsMode

			dsn := "user:pass@tcp(host:3306)/db"
			result, err := EnhanceDSNWithTLS(dsn, config)

			require.NoError(t, err)
			require.Contains(t, result, "tls=required",
				"TLS mode %s should be handled case-insensitively", tlsMode)
		})
	}
}

// TestTLSParameterEdgeCases tests edge cases in TLS parameter handling
func TestTLSParameterEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		dsn         string
		config      *DBConfig
		expected    string
		description string
	}{
		{
			name:        "Mixed case TLS parameter is ignored by ParseDSN",
			dsn:         "user:pass@tcp(host:3306)/db?TLS=false",
			config:      &DBConfig{TLSMode: "REQUIRED"},
			expected:    "user:pass@tcp(host:3306)/db?tls=required&TLS=false",
			description: "Mixed case 'TLS' (uppercase) is not recognized by go-sql-driver's ParseDSN — it is ignored and passed through as an unknown query parameter, so our TLS config is added alongside it",
		},
		{
			name:        "TLS in middle of query params",
			dsn:         "user:pass@tcp(host:3306)/db?charset=utf8&tls=skip-verify&timeout=30s",
			config:      &DBConfig{TLSMode: "REQUIRED"},
			expected:    "user:pass@tcp(host:3306)/db?charset=utf8&tls=skip-verify&timeout=30s",
			description: "TLS parameter in middle of query string should be preserved",
		},
		{
			name:        "Empty TLS value treated as unset",
			dsn:         "user:pass@tcp(host:3306)/db?tls=",
			config:      &DBConfig{TLSMode: "REQUIRED"},
			expected:    "user:pass@tcp(host:3306)/db?tls=required",
			description: "Empty TLS value is indistinguishable from unset after ParseDSN, so TLS config is applied",
		},
		{
			name:        "URL encoded TLS value",
			dsn:         "user:pass@tcp(host:3306)/db?tls=skip%2Dverify",
			config:      &DBConfig{TLSMode: "REQUIRED"},
			expected:    "user:pass@tcp(host:3306)/db?tls=skip%2Dverify",
			description: "URL encoded TLS value should be preserved",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := EnhanceDSNWithTLS(tt.dsn, tt.config)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result, tt.description)
		})
	}
}

// TestNewDSNTLSPreservationExtensive tests the core newDSN function extensively
func TestNewDSNTLSPreservationExtensive(t *testing.T) {
	tests := []struct {
		name               string
		dsn                string
		config             *DBConfig
		expectTLSPreserved bool
		expectTLSValue     string
		description        string
	}{
		{
			name:               "Preserve tls=false",
			dsn:                "user:pass@tcp(host:3306)/db?tls=false",
			config:             &DBConfig{TLSMode: "REQUIRED"},
			expectTLSPreserved: true,
			expectTLSValue:     "false",
			description:        "tls=false should be preserved regardless of main config",
		},
		{
			name:               "Preserve tls=true",
			dsn:                "user:pass@tcp(host:3306)/db?tls=true",
			config:             &DBConfig{TLSMode: "DISABLED"},
			expectTLSPreserved: true,
			expectTLSValue:     "true",
			description:        "tls=true should be preserved regardless of main config",
		},
		{
			name:               "Preserve registered TLS config name",
			dsn:                "user:pass@tcp(host:3306)/db?tls=preferred",
			config:             &DBConfig{TLSMode: "REQUIRED"},
			expectTLSPreserved: true,
			expectTLSValue:     "preferred",
			description:        "Registered TLS config name should be preserved",
		},
		{
			name:               "Add TLS when missing",
			dsn:                "user:pass@tcp(host:3306)/db",
			config:             &DBConfig{TLSMode: "REQUIRED"},
			expectTLSPreserved: false,
			expectTLSValue:     "required",
			description:        "TLS should be added when missing from DSN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := newDSN(tt.dsn, tt.config)
			require.NoError(t, err)

			// Should contain the expected TLS value (either preserved from original DSN or from config)
			require.Contains(t, result, "tls="+tt.expectTLSValue, tt.description)
		})
	}
}

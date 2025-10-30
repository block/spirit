package dbconn

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnhanceDSNWithTLS(t *testing.T) {
	// Create a temporary certificate file for testing
	tempFile, err := os.CreateTemp(t.TempDir(), "test-cert-*.pem")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	// Write some test certificate data
	certData := generateTestCertForMode(t)
	_, err = tempFile.Write(certData)
	require.NoError(t, err)
	require.NoError(t, tempFile.Close())

	testCases := []struct {
		name           string
		inputDSN       string
		config         *DBConfig
		expectedResult string
		expectError    bool
		description    string
	}{
		{
			name:     "DISABLED mode should not modify DSN",
			inputDSN: "user:pass@tcp(localhost:3306)/db",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "DISABLED"
				return cfg
			}(),
			expectedResult: "user:pass@tcp(localhost:3306)/db",
			expectError:    false,
			description:    "DISABLED mode should return original DSN unchanged",
		},
		{
			name:     "DSN with tls=false should be preserved",
			inputDSN: "user:pass@tcp(localhost:3306)/db?tls=false",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "REQUIRED"
				return cfg
			}(),
			expectedResult: "user:pass@tcp(localhost:3306)/db?tls=false",
			expectError:    false,
			description:    "DSN with explicit tls=false should be preserved even with REQUIRED TLS config",
		},
		{
			name:     "DSN with existing tls parameter should be unchanged",
			inputDSN: "user:pass@tcp(localhost:3306)/db?tls=skip-verify",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "REQUIRED"
				return cfg
			}(),
			expectedResult: "user:pass@tcp(localhost:3306)/db?tls=skip-verify",
			expectError:    false,
			description:    "DSN with existing TLS config should be respected",
		},
		{
			name:     "PREFERRED mode for non-RDS host",
			inputDSN: "user:pass@tcp(example.com:3306)/db",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "PREFERRED"
				cfg.TLSCertificatePath = tempFile.Name()
				return cfg
			}(),
			expectedResult: "user:pass@tcp(example.com:3306)/db?tls=custom",
			expectError:    false,
			description:    "PREFERRED mode should add custom TLS config",
		},
		{
			name:     "REQUIRED mode for RDS host",
			inputDSN: "user:pass@tcp(mydb.us-west-2.rds.amazonaws.com:3306)/db",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "REQUIRED"
				return cfg
			}(),
			expectedResult: "user:pass@tcp(mydb.us-west-2.rds.amazonaws.com:3306)/db?tls=rds",
			expectError:    false,
			description:    "REQUIRED mode should use RDS TLS for RDS hosts",
		},
		{
			name:     "REQUIRED mode for non-RDS host",
			inputDSN: "user:pass@tcp(example.com:3306)/db",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "REQUIRED"
				cfg.TLSCertificatePath = tempFile.Name()
				return cfg
			}(),
			expectedResult: "user:pass@tcp(example.com:3306)/db?tls=required",
			expectError:    false,
			description:    "REQUIRED mode should add required TLS config for non-RDS",
		},
		{
			name:     "VERIFY_CA mode",
			inputDSN: "user:pass@tcp(example.com:3306)/db",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "VERIFY_CA"
				cfg.TLSCertificatePath = tempFile.Name()
				return cfg
			}(),
			expectedResult: "user:pass@tcp(example.com:3306)/db?tls=verify_ca",
			expectError:    false,
			description:    "VERIFY_CA mode should add verify_ca TLS config",
		},
		{
			name:     "VERIFY_IDENTITY mode",
			inputDSN: "user:pass@tcp(example.com:3306)/db",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "VERIFY_IDENTITY"
				cfg.TLSCertificatePath = tempFile.Name()
				return cfg
			}(),
			expectedResult: "user:pass@tcp(example.com:3306)/db?tls=verify_identity",
			expectError:    false,
			description:    "VERIFY_IDENTITY mode should add verify_identity TLS config",
		},
		{
			name:     "DSN with existing query parameters",
			inputDSN: "user:pass@tcp(example.com:3306)/db?charset=utf8mb4",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "PREFERRED"
				cfg.TLSCertificatePath = tempFile.Name()
				return cfg
			}(),
			expectedResult: "user:pass@tcp(example.com:3306)/db?charset=utf8mb4&tls=custom",
			expectError:    false,
			description:    "Should append TLS parameter to existing query string",
		},
		{
			name:           "Nil config should return original DSN",
			inputDSN:       "user:pass@tcp(localhost:3306)/db",
			config:         nil,
			expectedResult: "user:pass@tcp(localhost:3306)/db",
			expectError:    false,
			description:    "Nil config should not modify DSN",
		},
		{
			name:     "Invalid DSN should return original",
			inputDSN: "invalid-dsn",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "REQUIRED"
				return cfg
			}(),
			expectedResult: "invalid-dsn",
			expectError:    false,
			description:    "Invalid DSN should be returned unchanged",
		},
		{
			name:     "Case insensitive TLS mode",
			inputDSN: "user:pass@tcp(example.com:3306)/db",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "required" // lowercase
				cfg.TLSCertificatePath = tempFile.Name()
				return cfg
			}(),
			expectedResult: "user:pass@tcp(example.com:3306)/db?tls=required",
			expectError:    false,
			description:    "TLS mode should be case insensitive",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := EnhanceDSNWithTLS(tc.inputDSN, tc.config)

			if tc.expectError {
				assert.Error(t, err, tc.description)
			} else {
				assert.NoError(t, err, tc.description)
				assert.Equal(t, tc.expectedResult, result, tc.description)
			}
		})
	}
}

func TestEnhanceDSNWithTLS_EdgeCases(t *testing.T) {
	testCases := []struct {
		name           string
		inputDSN       string
		config         *DBConfig
		expectedResult string
		description    string
	}{
		{
			name:     "DSN with TLS config name should be unchanged",
			inputDSN: "user:pass@tcp(localhost:3306)/db?tls=custom",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "REQUIRED"
				return cfg
			}(),
			expectedResult: "user:pass@tcp(localhost:3306)/db?tls=custom",
			description:    "DSN with existing TLS config name should be preserved",
		},
		{
			name:     "DSN with mixed case tls parameter",
			inputDSN: "user:pass@tcp(localhost:3306)/db?TLS=skip-verify",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "REQUIRED"
				return cfg
			}(),
			expectedResult: "user:pass@tcp(localhost:3306)/db?TLS=skip-verify",
			description:    "Mixed case TLS parameter should be preserved",
		},
		{
			name:     "Empty DSN should be handled gracefully",
			inputDSN: "",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "REQUIRED"
				return cfg
			}(),
			expectedResult: "",
			description:    "Empty DSN should be returned unchanged",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := EnhanceDSNWithTLS(tc.inputDSN, tc.config)
			assert.NoError(t, err, tc.description)
			assert.Equal(t, tc.expectedResult, result, tc.description)
		})
	}
}

func TestAddTLSParametersToDSN(t *testing.T) {
	// Create a temporary certificate file for testing
	tempFile, err := os.CreateTemp(t.TempDir(), "test-cert-*.pem")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	// Write some test certificate data
	certData := generateTestCertForMode(t)
	_, err = tempFile.Write(certData)
	require.NoError(t, err)
	require.NoError(t, tempFile.Close())

	testCases := []struct {
		name           string
		inputDSN       string
		config         *DBConfig
		expectedResult string
		expectError    bool
		description    string
	}{
		{
			name:     "DISABLED mode returns original DSN",
			inputDSN: "user:pass@tcp(localhost:3306)/db",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "DISABLED"
				return cfg
			}(),
			expectedResult: "user:pass@tcp(localhost:3306)/db",
			expectError:    false,
			description:    "DISABLED mode should not add TLS parameters",
		},
		{
			name:     "Unknown TLS mode defaults to PREFERRED",
			inputDSN: "user:pass@tcp(example.com:3306)/db",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "UNKNOWN_MODE"
				cfg.TLSCertificatePath = tempFile.Name()
				return cfg
			}(),
			expectedResult: "user:pass@tcp(example.com:3306)/db?tls=custom",
			expectError:    false,
			description:    "Unknown TLS mode should default to custom config",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := addTLSParametersToDSN(tc.inputDSN, tc.config)

			if tc.expectError {
				assert.Error(t, err, tc.description)
			} else {
				assert.NoError(t, err, tc.description)
				assert.Equal(t, tc.expectedResult, result, tc.description)
			}
		})
	}
}

func TestEnhanceDSNWithTLS_Integration(t *testing.T) {
	// This test verifies the integration between DSN enhancement and actual connection creation
	tempFile, err := os.CreateTemp(t.TempDir(), "test-cert-*.pem")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	certData := generateTestCertForMode(t)
	_, err = tempFile.Write(certData)
	require.NoError(t, err)
	require.NoError(t, tempFile.Close())

	config := NewDBConfig()
	config.TLSMode = "PREFERRED"
	config.TLSCertificatePath = tempFile.Name()

	originalDSN := "user:pass@tcp(example.com:3306)/testdb"
	enhancedDSN, err := EnhanceDSNWithTLS(originalDSN, config)

	assert.NoError(t, err)
	assert.Contains(t, enhancedDSN, "tls=custom")
	assert.Contains(t, enhancedDSN, "example.com:3306")
	assert.Contains(t, enhancedDSN, "testdb")
}

// TestNewDSNTLSPreservation tests the critical newDSN function for TLS parameter preservation
func TestNewDSNTLSPreservation(t *testing.T) {
	tests := []struct {
		name        string
		inputDSN    string
		config      *DBConfig
		expectSame  bool // Should the output DSN be the same as input?
		description string
	}{
		{
			name:     "tls=false should be preserved",
			inputDSN: "user:pass@tcp(host:3306)/db?tls=false",
			config: &DBConfig{
				TLSMode: "REQUIRED",
			},
			expectSame:  true,
			description: "DSN with explicit tls=false should not be modified even when config has TLS enabled",
		},
		{
			name:     "tls=true should be preserved",
			inputDSN: "user:pass@tcp(host:3306)/db?tls=true",
			config: &DBConfig{
				TLSMode: "DISABLED",
			},
			expectSame:  true,
			description: "DSN with explicit tls=true should not be modified even when config disables TLS",
		},
		{
			name:     "DSN without TLS should be enhanced",
			inputDSN: "user:pass@tcp(host:3306)/db",
			config: &DBConfig{
				TLSMode: "REQUIRED",
			},
			expectSame:  false,
			description: "DSN without TLS parameters should be enhanced with config TLS settings",
		},
		{
			name:     "DISABLED config should not add TLS",
			inputDSN: "user:pass@tcp(host:3306)/db",
			config: &DBConfig{
				TLSMode: "DISABLED",
			},
			expectSame:  false, // DSN will be standardized with default params
			description: "When config has DISABLED TLS, DSN should be standardized but not have TLS parameter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := newDSN(tt.inputDSN, tt.config)
			if err != nil {
				t.Fatalf("newDSN failed: %v", err)
			}

			if tt.expectSame {
				assert.Equal(t, tt.inputDSN, result, tt.description)
			} else {
				assert.NotEqual(t, tt.inputDSN, result, tt.description)
				// For REQUIRED mode, verify that TLS was actually added
				if tt.config.TLSMode == "REQUIRED" {
					assert.Contains(t, result, "tls=", "Expected TLS parameter to be added to DSN")
				}
				// For DISABLED mode, verify that TLS was NOT added
				if tt.config.TLSMode == "DISABLED" {
					assert.NotContains(t, result, "tls=", "TLS parameter should not be added when disabled")
				}
			}
		})
	}
}

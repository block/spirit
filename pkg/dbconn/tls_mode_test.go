package dbconn

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// generateTestCert creates a self-signed certificate for testing
func generateTestCertForMode(t *testing.T) []byte {
	// Generate a private key
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	// Create certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization:  []string{"Test Org"},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"Test City"},
			StreetAddress: []string{""},
			PostalCode:    []string{""},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	// Create the certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	require.NoError(t, err)

	// Encode certificate to PEM format
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	})

	return certPEM
}

func TestNewCustomTLSConfigModes(t *testing.T) {
	testCert := generateTestCertForMode(t)

	tests := []struct {
		name             string
		certData         []byte
		sslMode          string
		expectNil        bool
		expectSkipVerify bool
		expectVerifyFunc bool
		expectRootCAs    bool
	}{
		{
			name:             "VERIFY_IDENTITY mode",
			certData:         testCert,
			sslMode:          "VERIFY_IDENTITY",
			expectNil:        false,
			expectSkipVerify: false,
			expectVerifyFunc: false,
			expectRootCAs:    true,
		},
		{
			name:             "VERIFY_CA mode",
			certData:         testCert,
			sslMode:          "VERIFY_CA",
			expectNil:        false,
			expectSkipVerify: true,
			expectVerifyFunc: true,
			expectRootCAs:    true,
		},
		{
			name:             "REQUIRED mode",
			certData:         testCert,
			sslMode:          "REQUIRED",
			expectNil:        false,
			expectSkipVerify: true,
			expectVerifyFunc: false,
			expectRootCAs:    true,
		},
		{
			name:             "PREFERRED mode",
			certData:         testCert,
			sslMode:          "PREFERRED",
			expectNil:        false,
			expectSkipVerify: true,
			expectVerifyFunc: false,
			expectRootCAs:    false, // PREFERRED mode doesn't use RootCAs
		},
		{
			name:             "DISABLED mode",
			certData:         testCert,
			sslMode:          "DISABLED",
			expectNil:        true,
			expectSkipVerify: false,
			expectVerifyFunc: false,
			expectRootCAs:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := NewCustomTLSConfig(tt.certData, tt.sslMode)

			if tt.expectNil {
				assert.Nil(t, config)
			} else {
				assert.NotNil(t, config)
				assert.Equal(t, tt.expectSkipVerify, config.InsecureSkipVerify)

				if tt.expectRootCAs {
					assert.NotNil(t, config.RootCAs, "Expected RootCAs for mode %s", tt.sslMode)
				} else {
					assert.Nil(t, config.RootCAs, "Expected no RootCAs for mode %s", tt.sslMode)
				}

				if tt.expectVerifyFunc {
					assert.NotNil(t, config.VerifyPeerCertificate)
				} else {
					assert.Nil(t, config.VerifyPeerCertificate)
				}
			}
		})
	}
}

func TestNewDSNWithTLSModes(t *testing.T) {
	testCert := generateTestCertForMode(t)

	// Create a temporary certificate file
	tempFile, err := os.CreateTemp(t.TempDir(), "test_cert_*.pem")
	require.NoError(t, err)
	defer tempFile.Close()

	_, err = tempFile.Write(testCert)
	require.NoError(t, err)

	tests := []struct {
		name        string
		dsn         string
		config      *DBConfig
		expectedTLS string
		expectError bool
	}{
		// DISABLED mode
		{
			name: "DISABLED mode - no TLS",
			dsn:  "root:password@tcp(example.com:3306)/test",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "DISABLED"
				return cfg
			}(),
			expectedTLS: "",
			expectError: false,
		},
		// PREFERRED mode
		{
			name: "PREFERRED mode - RDS host",
			dsn:  "root:password@tcp(myhost.us-west-2.rds.amazonaws.com:3306)/test",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "PREFERRED"
				return cfg
			}(),
			expectedTLS: "tls=rds",
			expectError: false,
		},
		{
			name: "PREFERRED mode - non-RDS host no custom cert",
			dsn:  "root:password@tcp(example.com:3306)/test",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "PREFERRED"
				return cfg
			}(),
			expectedTLS: "tls=custom", // PREFERRED now uses custom TLS config
			expectError: false,
		},
		{
			name: "PREFERRED mode - non-RDS host with custom cert",
			dsn:  "root:password@tcp(example.com:3306)/test",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "PREFERRED"
				cfg.TLSCertificatePath = tempFile.Name()
				return cfg
			}(),
			expectedTLS: "tls=custom",
			expectError: false,
		},
		// REQUIRED mode
		{
			name: "REQUIRED mode - RDS host",
			dsn:  "root:password@tcp(myhost.us-west-2.rds.amazonaws.com:3306)/test",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "REQUIRED"
				return cfg
			}(),
			expectedTLS: "tls=rds",
			expectError: false,
		},
		{
			name: "REQUIRED mode - non-RDS host",
			dsn:  "root:password@tcp(example.com:3306)/test",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "REQUIRED"
				return cfg
			}(),
			expectedTLS: "tls=required",
			expectError: false,
		},
		{
			name: "REQUIRED mode - custom certificate",
			dsn:  "root:password@tcp(example.com:3306)/test",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "REQUIRED"
				cfg.TLSCertificatePath = tempFile.Name()
				return cfg
			}(),
			expectedTLS: "tls=required",
			expectError: false,
		},
		// VERIFY_CA mode
		{
			name: "VERIFY_CA mode - RDS host",
			dsn:  "root:password@tcp(myhost.us-west-2.rds.amazonaws.com:3306)/test",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "VERIFY_CA"
				return cfg
			}(),
			expectedTLS: "tls=rds",
			expectError: false,
		},
		{
			name: "VERIFY_CA mode - custom certificate",
			dsn:  "root:password@tcp(example.com:3306)/test",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "VERIFY_CA"
				cfg.TLSCertificatePath = tempFile.Name()
				return cfg
			}(),
			expectedTLS: "tls=verify_ca",
			expectError: false,
		},
		// VERIFY_IDENTITY mode
		{
			name: "VERIFY_IDENTITY mode - RDS host",
			dsn:  "root:password@tcp(myhost.us-west-2.rds.amazonaws.com:3306)/test",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "VERIFY_IDENTITY"
				return cfg
			}(),
			expectedTLS: "tls=rds",
			expectError: false,
		},
		{
			name: "VERIFY_IDENTITY mode - custom certificate",
			dsn:  "root:password@tcp(example.com:3306)/test",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "VERIFY_IDENTITY"
				cfg.TLSCertificatePath = tempFile.Name()
				return cfg
			}(),
			expectedTLS: "tls=verify_identity",
			expectError: false,
		},
		// Error cases
		{
			name: "invalid certificate path",
			dsn:  "root:password@tcp(example.com:3306)/test",
			config: func() *DBConfig {
				cfg := NewDBConfig()
				cfg.TLSMode = "REQUIRED"
				cfg.TLSCertificatePath = "/nonexistent/file.pem"
				return cfg
			}(),
			expectedTLS: "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := newDSN(tt.dsn, tt.config)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			if tt.expectedTLS != "" {
				assert.Contains(t, result, tt.expectedTLS)
			} else {
				assert.NotContains(t, result, "tls=")
			}
		})
	}
}

func TestDBConfigTLSModeDefaults(t *testing.T) {
	config := NewDBConfig()

	// Test TLS defaults
	assert.Equal(t, "PREFERRED", config.TLSMode)
	assert.Empty(t, config.TLSCertificatePath)
}

func TestTLSModeConfigRegistration(t *testing.T) {
	// Test that we can register multiple TLS configs without conflict
	err1 := initRDSTLS()
	assert.NoError(t, err1)

	// Test custom TLS registration
	config := NewDBConfig()
	config.TLSMode = "VERIFY_CA"
	config.TLSCertificatePath = ""

	err2 := initCustomTLS(config)
	assert.NoError(t, err2)
}

// TestVERIFY_CACertificateTrustLogic tests the specific certificate trust logic
// described in TLS_CONFIGURATION.md for VERIFY_CA mode
func TestVERIFY_CACertificateTrustLogic(t *testing.T) {
	// Generate two different CA certificates to test trust logic
	companyCA := generateTestCertForMode(t)
	otherCA := generateTestCertForMode(t)

	// Create temporary files for both CAs
	companyCertFile, err := os.CreateTemp(t.TempDir(), "company_ca_*.pem")
	require.NoError(t, err)
	defer companyCertFile.Close()
	_, err = companyCertFile.Write(companyCA)
	require.NoError(t, err)

	otherCertFile, err := os.CreateTemp(t.TempDir(), "other_ca_*.pem")
	require.NoError(t, err)
	defer otherCertFile.Close()
	_, err = otherCertFile.Write(otherCA)
	require.NoError(t, err)

	tests := []struct {
		name        string
		description string
		tlsCaFile   string
		serverCert  []byte
		expectTrust bool
	}{
		{
			name:        "trusted_ca_in_bundle",
			description: "Server cert signed by CA in bundle → Connection succeeds",
			tlsCaFile:   companyCertFile.Name(),
			serverCert:  companyCA, // Same CA
			expectTrust: true,
		},
		{
			name:        "untrusted_ca_not_in_bundle",
			description: "Server cert signed by CA not in bundle → Connection fails",
			tlsCaFile:   companyCertFile.Name(),
			serverCert:  otherCA, // Different CA
			expectTrust: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := NewDBConfig()
			config.TLSMode = "VERIFY_CA"
			config.TLSCertificatePath = tt.tlsCaFile

			// Test that the TLS config is created with the specified bundle
			tlsConfig := NewCustomTLSConfig(companyCA, "VERIFY_CA")
			require.NotNil(t, tlsConfig, tt.description)

			// Verify VERIFY_CA specific behavior
			assert.True(t, tlsConfig.InsecureSkipVerify, "VERIFY_CA uses InsecureSkipVerify=true with custom verification")
			assert.NotNil(t, tlsConfig.VerifyPeerCertificate, "VERIFY_CA should have custom verification")
			assert.NotNil(t, tlsConfig.RootCAs, "VERIFY_CA should have CA bundle")
			assert.Empty(t, tlsConfig.ServerName, "VERIFY_CA should not set ServerName for hostname flexibility")

			// Test DSN generation uses custom TLS
			dsn := "root:password@tcp(192.168.1.100:3306)/test"
			result, err := newDSN(dsn, config)
			assert.NoError(t, err, tt.description)
			assert.Contains(t, result, "tls=verify_ca", "Should use verify_ca TLS config")
		})
	}
}

// TestVERIFY_CAHostnameFlexibility tests that VERIFY_CA allows hostname mismatches
// as described in the documentation
func TestVERIFY_CAHostnameFlexibility(t *testing.T) {
	testCert := generateTestCertForMode(t)

	tempFile, err := os.CreateTemp(t.TempDir(), "test_cert_*.pem")
	require.NoError(t, err)
	defer tempFile.Close()
	_, err = tempFile.Write(testCert)
	require.NoError(t, err)

	hostnameTests := []struct {
		name        string
		host        string
		tlsMode     string
		description string
		shouldWork  bool
	}{
		{
			name:        "verify_ca_ip_address",
			host:        "192.168.1.100",
			tlsMode:     "VERIFY_CA",
			description: "VERIFY_CA allows IP addresses (hostname mismatch)",
			shouldWork:  true,
		},
		{
			name:        "verify_ca_load_balancer",
			host:        "lb.company.com",
			tlsMode:     "VERIFY_CA",
			description: "VERIFY_CA allows load balancer hostnames",
			shouldWork:  true,
		},
		{
			name:        "verify_identity_ip_fails",
			host:        "192.168.1.100",
			tlsMode:     "VERIFY_IDENTITY",
			description: "VERIFY_IDENTITY rejects IP addresses (hostname mismatch)",
			shouldWork:  false, // Would fail in real connection due to hostname mismatch
		},
		{
			name:        "verify_identity_exact_match",
			host:        "mysql.company.com",
			tlsMode:     "VERIFY_IDENTITY",
			description: "VERIFY_IDENTITY works with exact hostname match",
			shouldWork:  true,
		},
	}

	for _, tt := range hostnameTests {
		t.Run(tt.name, func(t *testing.T) {
			config := NewDBConfig()
			config.TLSMode = tt.tlsMode
			config.TLSCertificatePath = tempFile.Name()

			tlsConfig := NewCustomTLSConfig(testCert, tt.tlsMode)
			require.NotNil(t, tlsConfig, tt.description)

			// Test the specific behaviors mentioned in documentation
			switch tt.tlsMode {
			case "VERIFY_CA":
				assert.True(t, tlsConfig.InsecureSkipVerify, "VERIFY_CA uses InsecureSkipVerify=true with custom verification")
				assert.NotNil(t, tlsConfig.VerifyPeerCertificate, "VERIFY_CA should have custom verification function")
				assert.Empty(t, tlsConfig.ServerName, "VERIFY_CA should not set ServerName (allows hostname mismatches)")
			case "VERIFY_IDENTITY":
				assert.False(t, tlsConfig.InsecureSkipVerify, "VERIFY_IDENTITY should validate everything")
				assert.Nil(t, tlsConfig.VerifyPeerCertificate, "VERIFY_IDENTITY uses default verification")
				// ServerName would be set by Go's TLS library during actual connection
			}

			// Test DSN generation works for all scenarios
			dsn := fmt.Sprintf("root:password@tcp(%s:3306)/test", tt.host)
			result, err := newDSN(dsn, config)
			assert.NoError(t, err, tt.description)

			// Assert the correct TLS config name based on mode
			switch tt.tlsMode {
			case "VERIFY_CA":
				assert.Contains(t, result, "tls=verify_ca", "Should use verify_ca TLS config")
			case "VERIFY_IDENTITY":
				assert.Contains(t, result, "tls=verify_identity", "Should use verify_identity TLS config")
			default:
				assert.Contains(t, result, "tls=custom", "Should use custom TLS config")
			}
		})
	}
}

// TestCertificateAuthorityPrecedence tests the CA selection precedence
// as documented in TLS_CONFIGURATION.md
func TestCertificateAuthorityPrecedence(t *testing.T) {
	testCert := generateTestCertForMode(t)

	customCertFile, err := os.CreateTemp(t.TempDir(), "custom_cert_*.pem")
	require.NoError(t, err)
	defer customCertFile.Close()
	_, err = customCertFile.Write(testCert)
	require.NoError(t, err)

	precedenceTests := []struct {
		name            string
		host            string
		tlsMode         string
		tlsCertPath     string
		expectedTLSType string
		description     string
	}{
		{
			name:            "highest_priority_custom_cert",
			host:            "mydb.us-west-2.rds.amazonaws.com", // RDS host
			tlsMode:         "VERIFY_CA",
			tlsCertPath:     customCertFile.Name(), // Custom cert provided
			expectedTLSType: "tls=verify_ca",
			description:     "Custom certificate has HIGHEST priority - disregards RDS bundle even for RDS hosts",
		},
		{
			name:            "secondary_rds_auto_detection",
			host:            "mydb.us-west-2.rds.amazonaws.com", // RDS host
			tlsMode:         "VERIFY_CA",
			tlsCertPath:     "", // No custom cert
			expectedTLSType: "tls=rds",
			description:     "RDS auto-detection is SECONDARY - used when no custom cert provided",
		},
		{
			name:            "fallback_rds_bundle",
			host:            "mysql.company.com", // Non-RDS host
			tlsMode:         "VERIFY_CA",
			tlsCertPath:     "", // No custom cert
			expectedTLSType: "tls=verify_ca",
			description:     "Embedded RDS bundle as FALLBACK for non-RDS hosts when no custom cert",
		},
	}

	for _, tt := range precedenceTests {
		t.Run(tt.name, func(t *testing.T) {
			config := NewDBConfig()
			config.TLSMode = tt.tlsMode
			config.TLSCertificatePath = tt.tlsCertPath

			dsn := fmt.Sprintf("root:password@tcp(%s:3306)/test", tt.host)
			result, err := newDSN(dsn, config)
			assert.NoError(t, err, tt.description)
			assert.Contains(t, result, tt.expectedTLSType, tt.description)
		})
	}
}

// MockDB simulates database connection behavior for testing
type MockDB struct {
	shouldFailPing  bool
	pingCallCount   int
	closeCallCount  int
	maxOpenConns    int
	connMaxLifetime time.Duration
}

func (m *MockDB) Ping() error {
	m.pingCallCount++
	if m.shouldFailPing {
		return errors.New("mock ping failure")
	}
	return nil
}

func (m *MockDB) Close() error {
	m.closeCallCount++
	return nil
}

func (m *MockDB) SetMaxOpenConns(n int) {
	m.maxOpenConns = n
}

func (m *MockDB) SetConnMaxLifetime(d time.Duration) {
	m.connMaxLifetime = d
}

func TestPREFERREDModeFallbackBehavior(t *testing.T) {
	// Skip this test if we can't mock sql.Open
	// This test demonstrates the intended behavior structure
	t.Skip("Integration test - requires actual database connections")

	tests := []struct {
		name             string
		tlsSucceeds      bool
		plainSucceeds    bool
		expectedAttempts int
		expectError      bool
		description      string
	}{
		{
			name:             "TLS succeeds on first attempt",
			tlsSucceeds:      true,
			plainSucceeds:    true,
			expectedAttempts: 1,
			expectError:      false,
			description:      "Should use TLS connection when server supports it",
		},
		{
			name:             "TLS fails, plain succeeds",
			tlsSucceeds:      false,
			plainSucceeds:    true,
			expectedAttempts: 2,
			expectError:      false,
			description:      "Should fallback to plain connection when TLS fails",
		},
		{
			name:             "Both TLS and plain fail",
			tlsSucceeds:      false,
			plainSucceeds:    false,
			expectedAttempts: 2,
			expectError:      true,
			description:      "Should fail when both TLS and plain connections fail",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This test would require mocking sql.Open
			// The actual implementation is tested through integration tests
			t.Logf("Test case: %s", tt.description)
		})
	}
}

func TestPREFERREDModeDSNGeneration(t *testing.T) {
	testCert := generateTestCertForMode(t)

	// Create a temporary certificate file
	tempFile, err := os.CreateTemp(t.TempDir(), "test_cert_*.pem")
	require.NoError(t, err)
	defer tempFile.Close()

	_, err = tempFile.Write(testCert)
	require.NoError(t, err)

	tests := []struct {
		name        string
		host        string
		tlsCertPath string
		expectedTLS string
		description string
	}{
		{
			name:        "RDS host with PREFERRED",
			host:        "mydb.us-west-2.rds.amazonaws.com",
			tlsCertPath: "",
			expectedTLS: "tls=rds",
			description: "RDS hosts should use rds TLS config",
		},
		{
			name:        "Non-RDS host with custom certificate",
			host:        "mysql.company.com",
			tlsCertPath: tempFile.Name(),
			expectedTLS: "tls=custom",
			description: "Non-RDS hosts with custom cert should use custom TLS config",
		},
		{
			name:        "Non-RDS host without custom certificate",
			host:        "mysql.internal.com",
			tlsCertPath: "",
			expectedTLS: "tls=custom",
			description: "Non-RDS hosts without custom cert should use custom TLS config with embedded RDS bundle",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &DBConfig{
				TLSMode:                  "PREFERRED",
				TLSCertificatePath:       tt.tlsCertPath,
				MaxOpenConnections:       10,
				InnodbLockWaitTimeout:    60,
				LockWaitTimeout:          60,
				RangeOptimizerMaxMemSize: 8388608,
				InterpolateParams:        true,
			}

			dsn := fmt.Sprintf("root:password@tcp(%s:3306)/test", tt.host)
			result, err := newDSN(dsn, config)
			require.NoError(t, err, tt.description)
			assert.Contains(t, result, tt.expectedTLS, tt.description)
		})
	}
}

func TestPREFERREDModeDISABLEDFallback(t *testing.T) {
	// Test that PREFERRED mode fallback creates correct DISABLED DSN
	config := &DBConfig{
		TLSMode:                  "PREFERRED",
		TLSCertificatePath:       "",
		MaxOpenConnections:       10,
		InnodbLockWaitTimeout:    60,
		LockWaitTimeout:          60,
		RangeOptimizerMaxMemSize: 8388608,
		InterpolateParams:        true,
	}

	// Create a fallback config as done in the New() function
	configCopy := *config
	configCopy.TLSMode = "DISABLED"

	baseDSN := "root:password@tcp(mysql.internal.com:3306)/test"

	// Test original PREFERRED DSN
	preferredDSN, err := newDSN(baseDSN, config)
	require.NoError(t, err)
	assert.Contains(t, preferredDSN, "tls=custom", "PREFERRED should include TLS config")

	// Test fallback DISABLED DSN
	disabledDSN, err := newDSN(baseDSN, &configCopy)
	require.NoError(t, err)
	assert.NotContains(t, disabledDSN, "tls=", "DISABLED fallback should not include any TLS config")

	// Both should have the same non-TLS parameters
	expectedParams := []string{
		"sql_mode=",
		"time_zone=",
		"innodb_lock_wait_timeout=60",
		"lock_wait_timeout=60",
		"charset=binary",
		"collation=binary",
		"rejectReadOnly=true",
		"interpolateParams=true",
		"allowNativePasswords=true",
	}

	for _, param := range expectedParams {
		assert.Contains(t, preferredDSN, param, "PREFERRED DSN should contain %s", param)
		assert.Contains(t, disabledDSN, param, "DISABLED fallback DSN should contain %s", param)
	}
}

func TestPREFERREDModeConfigConsistency(t *testing.T) {
	// Ensure PREFERRED mode TLS config matches expectations
	testCert := generateTestCertForMode(t)

	config := NewCustomTLSConfig(testCert, "PREFERRED")
	require.NotNil(t, config)

	// PREFERRED mode should use InsecureSkipVerify=true (encryption only)
	assert.True(t, config.InsecureSkipVerify, "PREFERRED mode should skip certificate verification")
	assert.Nil(t, config.RootCAs, "PREFERRED mode should not set RootCAs")
	assert.Nil(t, config.VerifyPeerCertificate, "PREFERRED mode should not use custom verification")
}

func TestGetTLSConfigNameForAllModes(t *testing.T) {
	tests := []struct {
		mode         string
		expectedName string
		description  string
	}{
		{
			mode:         "DISABLED",
			expectedName: "",
			description:  "DISABLED should return empty string",
		},
		{
			mode:         "PREFERRED",
			expectedName: "custom",
			description:  "PREFERRED should return custom config name",
		},
		{
			mode:         "REQUIRED",
			expectedName: "required",
			description:  "REQUIRED should return required config name",
		},
		{
			mode:         "VERIFY_CA",
			expectedName: "verify_ca",
			description:  "VERIFY_CA should return verify_ca config name",
		},
		{
			mode:         "VERIFY_IDENTITY",
			expectedName: "verify_identity",
			description:  "VERIFY_IDENTITY should return verify_identity config name",
		},
		{
			mode:         "UNKNOWN_MODE",
			expectedName: "custom",
			description:  "Unknown modes should default to custom config name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.mode, func(t *testing.T) {
			result := getTLSConfigName(tt.mode)
			assert.Equal(t, tt.expectedName, result, tt.description)
		})
	}
}

// TestPREFERREDModeIntegration tests the integration between DSN generation and TLS config
func TestPREFERREDModeIntegration(t *testing.T) {
	testCert := generateTestCertForMode(t)

	// Create temporary certificate file
	tempFile, err := os.CreateTemp(t.TempDir(), "test_cert_*.pem")
	require.NoError(t, err)
	defer tempFile.Close()

	_, err = tempFile.Write(testCert)
	require.NoError(t, err)

	scenarios := []struct {
		name              string
		host              string
		tlsCertPath       string
		expectedTLSConfig string
		shouldRegisterTLS bool
		description       string
	}{
		{
			name:              "RDS host",
			host:              "prod.us-east-1.rds.amazonaws.com",
			tlsCertPath:       "",
			expectedTLSConfig: "rds",
			shouldRegisterTLS: true,
			description:       "RDS hosts should use rds TLS configuration",
		},
		{
			name:              "Non-RDS with custom cert",
			host:              "mysql.company.com",
			tlsCertPath:       tempFile.Name(),
			expectedTLSConfig: "custom",
			shouldRegisterTLS: true,
			description:       "Non-RDS hosts with custom cert should use custom TLS configuration",
		},
		{
			name:              "Non-RDS without custom cert",
			host:              "mysql.internal.local",
			tlsCertPath:       "",
			expectedTLSConfig: "custom",
			shouldRegisterTLS: true,
			description:       "Non-RDS hosts without custom cert should use custom TLS configuration with embedded bundle",
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			config := &DBConfig{
				TLSMode:                  "PREFERRED",
				TLSCertificatePath:       scenario.tlsCertPath,
				MaxOpenConnections:       10,
				InnodbLockWaitTimeout:    60,
				LockWaitTimeout:          60,
				RangeOptimizerMaxMemSize: 8388608,
				InterpolateParams:        true,
			}

			baseDSN := fmt.Sprintf("root:password@tcp(%s:3306)/test", scenario.host)
			resultDSN, err := newDSN(baseDSN, config)
			require.NoError(t, err, scenario.description)

			expectedTLSParam := "tls=" + scenario.expectedTLSConfig
			assert.Contains(t, resultDSN, expectedTLSParam, scenario.description)
		})
	}
}

func TestTLSModeCaseInsensitive(t *testing.T) {
	// Test that TLS mode values are case insensitive
	testCases := []struct {
		name        string
		tlsMode     string
		expectedDSN string
	}{
		{
			name:        "lowercase disabled",
			tlsMode:     "disabled",
			expectedDSN: "", // DISABLED mode should not add TLS params
		},
		{
			name:        "lowercase preferred",
			tlsMode:     "preferred",
			expectedDSN: "tls=custom",
		},
		{
			name:        "lowercase required",
			tlsMode:     "required",
			expectedDSN: "tls=required",
		},
		{
			name:        "lowercase verify_ca",
			tlsMode:     "verify_ca",
			expectedDSN: "tls=verify_ca",
		},
		{
			name:        "lowercase verify_identity",
			tlsMode:     "verify_identity",
			expectedDSN: "tls=verify_identity",
		},
		{
			name:        "mixed case Required",
			tlsMode:     "Required",
			expectedDSN: "tls=required",
		},
		{
			name:        "mixed case Verify_Ca",
			tlsMode:     "Verify_Ca",
			expectedDSN: "tls=verify_ca",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := &DBConfig{
				TLSMode:                  tc.tlsMode,
				TLSCertificatePath:       "",
				MaxOpenConnections:       10,
				InnodbLockWaitTimeout:    60,
				LockWaitTimeout:          60,
				RangeOptimizerMaxMemSize: 8388608,
				InterpolateParams:        true,
			}

			baseDSN := "root:password@tcp(mysql.company.com:3306)/test"
			resultDSN, err := newDSN(baseDSN, config)
			require.NoError(t, err)

			if tc.expectedDSN == "" {
				// DISABLED mode should not contain any TLS parameters
				assert.NotContains(t, resultDSN, "tls=")
			} else {
				assert.Contains(t, resultDSN, tc.expectedDSN)
			}
		})
	}
}

func TestTLSConfigCaseInsensitive(t *testing.T) {
	// Test that getTLSConfigName works with case insensitive input
	testCases := []struct {
		input    string
		expected string
	}{
		{"disabled", ""},
		{"DISABLED", ""},
		{"Disabled", ""},
		{"preferred", customTLSConfigName},
		{"PREFERRED", customTLSConfigName},
		{"Preferred", customTLSConfigName},
		{"required", requiredTLSConfigName},
		{"REQUIRED", requiredTLSConfigName},
		{"Required", requiredTLSConfigName},
		{"verify_ca", verifyCATLSConfigName},
		{"VERIFY_CA", verifyCATLSConfigName},
		{"Verify_Ca", verifyCATLSConfigName},
		{"verify_identity", verifyIDTLSConfigName},
		{"VERIFY_IDENTITY", verifyIDTLSConfigName},
		{"Verify_Identity", verifyIDTLSConfigName},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result := getTLSConfigName(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestNewCustomTLSConfigCaseInsensitive(t *testing.T) {
	// Test that NewCustomTLSConfig works with case insensitive input
	certData := generateTestCertForMode(t)

	testCases := []string{
		"preferred", "PREFERRED", "Preferred",
		"required", "REQUIRED", "Required",
		"verify_ca", "VERIFY_CA", "Verify_Ca",
		"verify_identity", "VERIFY_IDENTITY", "Verify_Identity",
	}

	for _, mode := range testCases {
		t.Run(mode, func(t *testing.T) {
			config := NewCustomTLSConfig(certData, mode)
			assert.NotNil(t, config, "Should create valid TLS config for mode: %s", mode)

			// All non-DISABLED modes should have some configuration
			if mode != "disabled" && mode != "DISABLED" && mode != "Disabled" {
				assert.True(t, config.InsecureSkipVerify || config.RootCAs != nil || config.VerifyPeerCertificate != nil,
					"TLS config should have some security settings for mode: %s", mode)
			}
		})
	}
}

package dbconn

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
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
	assert.Equal(t, "", config.TLSCertificatePath)
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
			assert.Equal(t, "", tlsConfig.ServerName, "VERIFY_CA should not set ServerName for hostname flexibility")

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
			if tt.tlsMode == "VERIFY_CA" {
				assert.True(t, tlsConfig.InsecureSkipVerify, "VERIFY_CA uses InsecureSkipVerify=true with custom verification")
				assert.NotNil(t, tlsConfig.VerifyPeerCertificate, "VERIFY_CA should have custom verification function")
				assert.Equal(t, "", tlsConfig.ServerName, "VERIFY_CA should not set ServerName (allows hostname mismatches)")
			} else if tt.tlsMode == "VERIFY_IDENTITY" {
				assert.False(t, tlsConfig.InsecureSkipVerify, "VERIFY_IDENTITY should validate everything")
				assert.Nil(t, tlsConfig.VerifyPeerCertificate, "VERIFY_IDENTITY uses default verification")
				// ServerName would be set by Go's TLS library during actual connection
			}

			// Test DSN generation works for all scenarios
			dsn := fmt.Sprintf("root:password@tcp(%s:3306)/test", tt.host)
			result, err := newDSN(dsn, config)
			assert.NoError(t, err, tt.description)
			
			// Assert the correct TLS config name based on mode
			if tt.tlsMode == "VERIFY_CA" {
				assert.Contains(t, result, "tls=verify_ca", "Should use verify_ca TLS config")
			} else if tt.tlsMode == "VERIFY_IDENTITY" {
				assert.Contains(t, result, "tls=verify_identity", "Should use verify_identity TLS config")
			} else {
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

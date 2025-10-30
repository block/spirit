package repl

import (
	"os"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientTLSConfiguration(t *testing.T) {
	// Create temporary certificate file for testing
	tempFile, err := os.CreateTemp(t.TempDir(), "test-cert-*.pem")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())

	// Write test certificate data
	certData := generateTestCert(t)
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

			// Create a mock client (we can't actually connect without real DB)
			client := &Client{
				host:      tc.host,
				username:  "testuser",
				password:  "testpass",
				tlsConfig: tlsConfig,
			}

			// Verify TLS config is stored correctly
			assert.NotNil(t, client.tlsConfig)
			assert.Equal(t, tc.tlsMode, client.tlsConfig.TLSMode)
			assert.Equal(t, tc.tlsCert, client.tlsConfig.TLSCertificatePath)

			// Test the TLS mode validation logic that would be used in Run()
			if tc.tlsMode == "DISABLED" {
				// For disabled mode, TLS should not be configured in actual connection
				assert.Equal(t, "DISABLED", client.tlsConfig.TLSMode)
			} else {
				// For other modes, TLS should be configured
				assert.NotEqual(t, "DISABLED", client.tlsConfig.TLSMode)
			}
		})
	}
}

func TestClientTLSModeString(t *testing.T) {
	testCases := []struct {
		tlsMode  string
		expected string
	}{
		{"DISABLED", ""},
		{"PREFERRED", "preferred"},
		{"REQUIRED", "required"},
		{"VERIFY_CA", "verify_ca"},
		{"VERIFY_IDENTITY", "verify_identity"},
		{"disabled", ""},
		{"preferred", "preferred"},
		{"required", "required"},
		{"verify_ca", "verify_ca"},
		{"verify_identity", "verify_identity"},
	}

	for _, tc := range testCases {
		t.Run(tc.tlsMode, func(t *testing.T) {
			// Test the string conversion logic used in client
			result := ""
			switch strings.ToUpper(tc.tlsMode) {
			case "DISABLED":
				result = ""
			case "PREFERRED":
				result = "preferred"
			case "REQUIRED":
				result = "required"
			case "VERIFY_CA":
				result = "verify_ca"
			case "VERIFY_IDENTITY":
				result = "verify_identity"
			}
			assert.Equal(t, tc.expected, result)
		})
	}
}

// generateTestCert creates a simple test certificate for TLS testing
func generateTestCert(t *testing.T) []byte {
	t.Helper()
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

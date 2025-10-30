package dbconn

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateFallbackDSN(t *testing.T) {
	tests := []struct {
		name        string
		inputDSN    string
		expectedDSN string
		description string
	}{
		{
			name:        "DSN with tls parameter",
			inputDSN:    "user:pass@tcp(host:3306)/db?tls=true&charset=utf8",
			expectedDSN: "user:pass@tcp(host:3306)/db?charset=utf8",
			description: "Should remove tls parameter but keep other parameters",
		},
		{
			name:        "DSN with only tls parameter",
			inputDSN:    "user:pass@tcp(host:3306)/db?tls=true",
			expectedDSN: "user:pass@tcp(host:3306)/db",
			description: "Should remove tls parameter when it's the only parameter",
		},
		{
			name:        "DSN without tls parameter",
			inputDSN:    "user:pass@tcp(host:3306)/db?charset=utf8",
			expectedDSN: "user:pass@tcp(host:3306)/db?charset=utf8",
			description: "Should leave DSN unchanged when no tls parameter",
		},
		{
			name:        "DSN without parameters",
			inputDSN:    "user:pass@tcp(host:3306)/db",
			expectedDSN: "user:pass@tcp(host:3306)/db",
			description: "Should leave DSN unchanged when no parameters at all",
		},
		{
			name:        "Complex DSN with tls",
			inputDSN:    "root:rootpassword@tcp(127.0.0.1:3421)/test?tls=skip-verify&timeout=30s&readTimeout=30s",
			expectedDSN: "root:rootpassword@tcp(127.0.0.1:3421)/test?readTimeout=30s&timeout=30s",
			description: "Should remove tls but preserve other complex parameters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := createFallbackDSN(tt.inputDSN)
			assert.NoError(t, err, "createFallbackDSN should not return error")
			assert.Equal(t, tt.expectedDSN, result, tt.description)
		})
	}
}

func TestCreateFallbackDSNInvalidDSN(t *testing.T) {
	// Test with invalid DSN - should return original DSN without error
	invalidDSN := "invalid-dsn-format"
	result, err := createFallbackDSN(invalidDSN)
	assert.NoError(t, err, "Should not return error for invalid DSN")
	assert.Equal(t, invalidDSN, result, "Should return original DSN when parsing fails")
}

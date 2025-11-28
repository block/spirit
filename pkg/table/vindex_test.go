package table

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVindexProvider is a simple test implementation of VindexMetadataProvider
type TestVindexProvider struct {
	vindexColumn string
	vindexFunc   VindexFunc
	shouldError  bool
}

func NewTestVindexProvider(column string, hashFunc VindexFunc) *TestVindexProvider {
	return &TestVindexProvider{
		vindexColumn: column,
		vindexFunc:   hashFunc,
		shouldError:  false,
	}
}

func (t *TestVindexProvider) GetVindexMetadata(schemaName, tableName string) (string, VindexFunc, error) {
	if t.shouldError {
		return "", nil, fmt.Errorf("test error for table %s.%s", schemaName, tableName)
	}
	return t.vindexColumn, t.vindexFunc, nil
}

// TestVindexProviderInterface verifies the interface contract
func TestVindexProviderInterface(t *testing.T) {
	// Simple hash function for testing
	simpleHash := func(value any) (uint64, error) {
		switch v := value.(type) {
		case int:
			return uint64(v), nil
		case int64:
			return uint64(v), nil
		case string:
			// Simple string hash
			hash := uint64(0)
			for _, c := range v {
				hash = hash*31 + uint64(c)
			}
			return hash, nil
		default:
			return 0, fmt.Errorf("unsupported type: %T", value)
		}
	}

	t.Run("returns vindex metadata", func(t *testing.T) {
		provider := NewTestVindexProvider("user_id", simpleHash)

		column, hashFunc, err := provider.GetVindexMetadata("testdb", "users")
		require.NoError(t, err)
		assert.Equal(t, "user_id", column)
		assert.NotNil(t, hashFunc)

		// Test the hash function
		hash, err := hashFunc(123)
		require.NoError(t, err)
		assert.Equal(t, uint64(123), hash)
	})

	t.Run("returns empty for no vindex", func(t *testing.T) {
		provider := NewTestVindexProvider("", nil)

		column, hashFunc, err := provider.GetVindexMetadata("testdb", "config")
		require.NoError(t, err)
		assert.Empty(t, column)
		assert.Nil(t, hashFunc)
	})

	t.Run("returns error", func(t *testing.T) {
		provider := &TestVindexProvider{
			shouldError: true,
		}

		_, _, err := provider.GetVindexMetadata("testdb", "users")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "test error")
	})
}

// TestVindexFunc verifies VindexFunc behavior
func TestVindexFunc(t *testing.T) {
	t.Run("handles different types", func(t *testing.T) {
		hashFunc := func(value any) (uint64, error) {
			switch v := value.(type) {
			case int:
				return uint64(v * 2), nil
			case string:
				return uint64(len(v)), nil
			default:
				return 0, fmt.Errorf("unsupported type: %T", value)
			}
		}

		// Test with int
		hash, err := hashFunc(42)
		require.NoError(t, err)
		assert.Equal(t, uint64(84), hash)

		// Test with string
		hash, err = hashFunc("hello")
		require.NoError(t, err)
		assert.Equal(t, uint64(5), hash)

		// Test with unsupported type
		_, err = hashFunc(3.14)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported type")
	})
}

// MultiTableVindexProvider is a test provider that can return different
// vindex configurations for different tables
type MultiTableVindexProvider struct {
	tableConfigs map[string]struct {
		column   string
		hashFunc VindexFunc
	}
}

func NewMultiTableVindexProvider() *MultiTableVindexProvider {
	return &MultiTableVindexProvider{
		tableConfigs: make(map[string]struct {
			column   string
			hashFunc VindexFunc
		}),
	}
}

func (m *MultiTableVindexProvider) AddTable(tableName, column string, hashFunc VindexFunc) {
	m.tableConfigs[tableName] = struct {
		column   string
		hashFunc VindexFunc
	}{
		column:   column,
		hashFunc: hashFunc,
	}
}

func (m *MultiTableVindexProvider) GetVindexMetadata(schemaName, tableName string) (string, VindexFunc, error) {
	config, exists := m.tableConfigs[tableName]
	if !exists {
		return "", nil, nil // No vindex for this table
	}
	return config.column, config.hashFunc, nil
}

func TestMultiTableVindexProvider(t *testing.T) {
	userHash := func(value any) (uint64, error) {
		return uint64(1), nil
	}
	orderHash := func(value any) (uint64, error) {
		return uint64(2), nil
	}

	provider := NewMultiTableVindexProvider()
	provider.AddTable("users", "user_id", userHash)
	provider.AddTable("orders", "order_id", orderHash)

	t.Run("returns correct config for users table", func(t *testing.T) {
		column, hashFunc, err := provider.GetVindexMetadata("testdb", "users")
		require.NoError(t, err)
		assert.Equal(t, "user_id", column)
		assert.NotNil(t, hashFunc)

		hash, err := hashFunc(123)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), hash)
	})

	t.Run("returns correct config for orders table", func(t *testing.T) {
		column, hashFunc, err := provider.GetVindexMetadata("testdb", "orders")
		require.NoError(t, err)
		assert.Equal(t, "order_id", column)
		assert.NotNil(t, hashFunc)

		hash, err := hashFunc(456)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), hash)
	})

	t.Run("returns empty for unconfigured table", func(t *testing.T) {
		column, hashFunc, err := provider.GetVindexMetadata("testdb", "config")
		require.NoError(t, err)
		assert.Empty(t, column)
		assert.Nil(t, hashFunc)
	})
}

package dbconn

import (
	"context"
	"testing"
	"time"

	"github.com/cashapp/spirit/pkg/table"
	"github.com/siddontang/loggers"

	"github.com/cashapp/spirit/pkg/testutils"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestMetadataLock(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "test"}
	logger := logrus.New()
	mdl, err := NewMetadataLock(t.Context(), testutils.DSN(), &lockTableInfo, logger)
	assert.NoError(t, err)
	assert.NotNil(t, mdl)

	// Confirm a second lock cannot be acquired
	_, err = NewMetadataLock(t.Context(), testutils.DSN(), &lockTableInfo, logger)
	assert.ErrorContains(t, err, "lock is held by another connection")

	// Close the original mdl
	assert.NoError(t, mdl.Close())

	// Confirm a new lock can be acquired
	mdl3, err := NewMetadataLock(t.Context(), testutils.DSN(), &lockTableInfo, logger)
	assert.NoError(t, err)
	assert.NoError(t, mdl3.Close())
}

func TestMetadataLockContextCancel(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "test-cancel"}

	logger := logrus.New()
	ctx, cancel := context.WithCancel(t.Context())
	mdl, err := NewMetadataLock(ctx, testutils.DSN(), &lockTableInfo, logger)
	assert.NoError(t, err)
	assert.NotNil(t, mdl)

	// Cancel the context
	cancel()

	// Wait for the lock to be released
	<-mdl.closeCh

	// Confirm the lock is released by acquiring a new one
	mdl2, err := NewMetadataLock(t.Context(), testutils.DSN(), &lockTableInfo, logger)
	assert.NoError(t, err)
	assert.NotNil(t, mdl2)
	assert.NoError(t, mdl2.Close())
}

func TestMetadataLockRefresh(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "test-refresh"}
	logger := logrus.New()

	mdl, err := NewMetadataLock(t.Context(), testutils.DSN(), &lockTableInfo, logger, func(mdl *MetadataLock) {
		// override the refresh interval for faster testing
		mdl.refreshInterval = 2 * time.Second
	})
	assert.NoError(t, err)
	assert.NotNil(t, mdl)

	// wait for the refresh to happen
	time.Sleep(5 * time.Second)

	// Confirm the lock is still held
	_, err = NewMetadataLock(t.Context(), testutils.DSN(), &lockTableInfo, logger)
	assert.ErrorContains(t, err, "lock is held by another connection")

	// Close the lock
	assert.NoError(t, mdl.Close())
}

func TestComputeLockName(t *testing.T) {
	tests := []struct {
		table    *table.TableInfo
		expected string
	}{
		{
			table:    &table.TableInfo{SchemaName: "shortschema", TableName: "shorttable"},
			expected: "shortschema.shorttable-",
		},
		{
			table:    &table.TableInfo{SchemaName: "averylongschemanamethatexceeds20chars", TableName: "averylongtablenamewhichexceeds32characters"},
			expected: "averylongschemanamet.averylongtablenamewhichexceeds32-",
		},
	}

	for _, test := range tests {
		lockName := computeLockName(test.table)
		assert.Contains(t, lockName, test.expected, "Lock name should contain the expected prefix")
		assert.Len(t, lockName, len(test.expected)+8, "Lock name should have the correct length")
	}
}

func TestMetadataLockLength(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "thisisareallylongtablenamethisisareallylongtablenamethisisareallylongtablename"}
	var empty *table.TableInfo

	logger := logrus.New()

	_, err := NewMetadataLock(t.Context(), testutils.DSN(), &lockTableInfo, logger)
	// No error anymore after using a hash of the table name
	assert.NoError(t, err)

	_, err = NewMetadataLock(t.Context(), testutils.DSN(), empty, logger)
	assert.ErrorContains(t, err, "metadata lock table info is nil")
}

// simulateConnectionClose simulates a temporary network issue by closing the connection
func simulateConnectionClose(t *testing.T, mdl *MetadataLock, logger loggers.Advanced) {
	// close the existing connection to simulate a network issue
	err := mdl.CloseDBConnection(logger)
	assert.NoError(t, err)

	// wait a bit to ensure the connection is closed
	time.Sleep(1 * time.Second)
}

func TestMetadataLockRefreshWithConnIssueSimulation(t *testing.T) {
	lockTableInfo := table.TableInfo{SchemaName: "test", TableName: "test-refresh"}
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	// create a new MetadataLock with a short refresh interval for testing
	mdl, err := NewMetadataLock(t.Context(), testutils.DSN(), &lockTableInfo, logger, func(mdl *MetadataLock) {
		mdl.refreshInterval = 2 * time.Second
	})
	assert.NoError(t, err)
	assert.NotNil(t, mdl)

	time.Sleep(4 * time.Second)

	// simulate a temporary network issue by closing the connection
	simulateConnectionClose(t, mdl, logger)

	// wait for the refresh interval to trigger the connection failure and recovery
	time.Sleep(4 * time.Second)

	// confirm the lock is still held by attempting to acquire it with a new connection
	_, err = NewMetadataLock(t.Context(), testutils.DSN(), &lockTableInfo, logger)
	assert.ErrorContains(t, err, "lock is held by another connection")

	// close the lock
	assert.NoError(t, mdl.Close())
}

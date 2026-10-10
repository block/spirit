package testutils

import (
	"database/sql"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The version skips run a test only on the side of the version they name, so
// a gate that points the wrong way or is off by one at the boundary fails here
// instead of passing as a quiet skip.
func TestVersionSkipsPointTheRightWay(t *testing.T) {
	db, err := sql.Open(driverName, DSN())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	var version string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT version()").Scan(&version))
	for _, tc := range []struct {
		name    string
		skip    func(t *testing.T, version, reason string)
		version string
		wantRun bool
	}{
		{"before newer", SkipBeforeMySQLVersion, "999.0.0", false},
		{"before own", SkipBeforeMySQLVersion, version, true},
		{"before older", SkipBeforeMySQLVersion, "0.0.1", true},
		{"from newer", SkipFromMySQLVersion, "999.0.0", true},
		{"from own", SkipFromMySQLVersion, version, false},
		{"from older", SkipFromMySQLVersion, "0.0.1", false},
	} {
		ran := false
		t.Run(tc.name, func(t *testing.T) {
			tc.skip(t, tc.version, "probe")
			ran = true
		})
		require.Equal(t, tc.wantRun, ran, "%s: server %s, version %s", tc.name, version, tc.version)
	}
}

func TestUniqueDatabaseName(t *testing.T) {
	// The same long test name in two packages (two processes) must not
	// collide: the pid has to survive truncation.
	long := "TestCutOverChecksUnderLockRetryPolicy/transient_error_is_retried"
	a := uniqueDatabaseName(long, 12345, 1)
	b := uniqueDatabaseName(long, 67890, 1)
	assert.NotEqual(t, a, b)
	assert.LessOrEqual(t, len(a), 64)
	assert.True(t, strings.HasSuffix(a, "_12345_1"), a)
	assert.True(t, strings.HasSuffix(b, "_67890_1"), b)

	// Within one process, the counter has to survive truncation.
	assert.NotEqual(t, uniqueDatabaseName(long, 12345, 1), uniqueDatabaseName(long, 12345, 2))

	// Two long names that differ only past the cut differ in the hash.
	prefix := "Test" + strings.Repeat("x", 80)
	c := uniqueDatabaseName(prefix+"/one", 12345, 1)
	d := uniqueDatabaseName(prefix+"/two", 12345, 1)
	assert.NotEqual(t, c, d)
	assert.Len(t, c, 64)
	assert.Len(t, d, 64)

	// The largest pid and counter still fit.
	assert.LessOrEqual(t, len(uniqueDatabaseName(long, math.MaxInt32, math.MaxUint64)), 64)

	// Short names are kept whole; characters that would need quoting are
	// replaced.
	e := uniqueDatabaseName("TestFoo/sub-test#01", 12345, 7)
	assert.Regexp(t, `^t_testfoo_sub_test_01_[0-9a-f]{8}_12345_7$`, e)
}

// TestCreateUniqueTestDatabaseRefusesExistingName: if the generated name
// already exists (a live database from another process, or one left behind
// by a killed run whose pid has been reused), the helper must not hand that
// database to this test.
func TestCreateUniqueTestDatabaseRefusesExistingName(t *testing.T) {
	next := uniqueDatabaseName(t.Name(), os.Getpid(), dbCounter.Load()+1)
	RunSQL(t, "CREATE DATABASE "+next)
	t.Cleanup(func() { RunSQL(t, "DROP DATABASE IF EXISTS "+next) })
	RunSQL(t, "CREATE TABLE "+next+".left_behind (id INT PRIMARY KEY)")

	name, db := CreateUniqueTestDatabase(t)
	assert.NotEqual(t, next, name)
	var n int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?", name).Scan(&n))
	require.Zero(t, n, "CreateUniqueTestDatabase returned %s, which already held another run's tables", name)
}

func TestSanitizeIdentifier(t *testing.T) {
	assert.Equal(t, "testfoo_sub_test_01", SanitizeIdentifier("TestFoo/sub-test#01"))
	assert.Equal(t, "a_b_c", SanitizeIdentifier("a b.c"))
	assert.Equal(t, "_", SanitizeIdentifier("é"))
}

package repl

import (
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/applier"
	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/block/spirit/pkg/utils"
	mysql2 "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// uniqueTableNames derives a (src, dst) pair of unique MySQL table names
// from t.Name(). Each test in the package gets its own pair so that no
// previous test's schema can bleed into the next via the shared
// "subscription_test" / "_subscription_test_new" names.
//
// MySQL identifier limit is 64 chars. The destination is "_<src>_new",
// so the src base must be <= 59 chars to leave room. We sanitize, lowercase,
// and add a short hash suffix to disambiguate subtests / collisions.
func uniqueTableNames(t *testing.T) (srcName, dstName string) {
	t.Helper()
	raw := strings.ToLower(t.Name())
	var b strings.Builder
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	sanitized := b.String()

	// Short stable hash suffix from the original name, to keep names unique
	// even after truncation.
	sum := sha1.Sum([]byte(t.Name()))
	suffix := hex.EncodeToString(sum[:])[:6]

	// Cap base at 50 chars so "_<base>_<suffix>_new" stays <= 64.
	const maxBase = 50
	if len(sanitized) > maxBase {
		sanitized = sanitized[:maxBase]
	}
	srcName = fmt.Sprintf("%s_%s", sanitized, suffix)
	dstName = fmt.Sprintf("_%s_new", srcName)
	return srcName, dstName
}

// setupTestTables creates a unique pair of source/destination tables for
// the given test by substituting the literal placeholder names
// "subscription_test" and "_subscription_test_new" in the provided
// CREATE TABLE strings with names derived from t.Name(). This prevents
// schema bleed-through between tests in the same package.
//
// Callers must reference the returned TableInfo (e.g. via QuotedTableName /
// TableName) for any subsequent INSERT/SELECT/etc. statements; the original
// literal names will not exist in the database.
func setupTestTables(t *testing.T, t1, t2 string) (*table.TableInfo, *table.TableInfo) {
	t.Helper()
	srcName, dstName := uniqueTableNames(t)

	// Substitute placeholder names in the schema SQL. Order matters: the
	// destination ("_subscription_test_new") contains the source name as a
	// substring, so replace the longer one first.
	t1 = strings.ReplaceAll(t1, "_subscription_test_new", dstName)
	t1 = strings.ReplaceAll(t1, "subscription_test", srcName)
	t2 = strings.ReplaceAll(t2, "_subscription_test_new", dstName)
	t2 = strings.ReplaceAll(t2, "subscription_test", srcName)

	testutils.RunSQL(t, fmt.Sprintf("DROP TABLE IF EXISTS `%s`, `%s`", srcName, dstName))
	testutils.RunSQL(t, t1)
	testutils.RunSQL(t, t2)

	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	defer utils.CloseAndLog(db)

	srcTable := table.NewTableInfo(db, "test", srcName)
	dstTable := table.NewTableInfo(db, "test", dstName)

	require.NoError(t, srcTable.SetInfo(t.Context()))
	require.NoError(t, dstTable.SetInfo(t.Context()))

	return srcTable, dstTable
}

func setupBufferedTest(t *testing.T) (*sql.DB, *Client, *table.TableInfo, *table.TableInfo) {
	t.Helper()
	t1 := `CREATE TABLE subscription_test (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	t2 := `CREATE TABLE _subscription_test_new (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	)`
	srcTable, dstTable := setupTestTables(t, t1, t2)
	db, err := dbconn.New(testutils.DSN(), dbconn.NewDBConfig())
	require.NoError(t, err)
	logger := slog.Default()
	cfg, err := mysql2.ParseDSN(testutils.DSN())
	require.NoError(t, err)
	target := applier.Target{
		DB:       db,
		KeyRange: "0",
		Config:   cfg,
	}
	applier, err := applier.NewSingleTargetApplier(target, applier.NewApplierDefaultConfig())
	require.NoError(t, err)
	client := NewClient(db, cfg.Addr, cfg.User, cfg.Passwd, applier, &ClientConfig{
		Logger:          logger,
		Concurrency:     4,
		TargetBatchTime: time.Second,
		ServerID:        NewServerID(),
	})
	chunker, err := table.NewChunker(srcTable, table.ChunkerConfig{NewTable: dstTable})
	require.NoError(t, err)
	require.NoError(t, client.AddSubscription(srcTable, dstTable, chunker))
	require.NoError(t, client.Run(t.Context()))
	return db, client, srcTable, dstTable
}

package migration

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/block/spirit/pkg/status"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// TestAmbiguousDDLError draws the line that decides whether spirit may fall
// through to the copy algorithm after a direct DDL attempt fails. A server
// that positively rejected the DDL leaves the table untouched (safe to copy);
// a connection that died leaves it unknown (must not copy).
func TestAmbiguousDDLError(t *testing.T) {
	t.Parallel()

	// Deterministic server errors: the ALTER definitely did not apply, so the
	// caller keeps its existing "ignore and fall through to copy" behavior.
	require.NoError(t, ambiguousDDLError(nil))
	require.NoError(t, ambiguousDDLError(errors.New("not a mysql error")))
	require.NoError(t, ambiguousDDLError(&mysql.MySQLError{Number: 1845})) // ALGORITHM=INSTANT not supported
	require.NoError(t, ambiguousDDLError(&mysql.MySQLError{Number: 1064})) // syntax error

	// Connection loss: the server may have applied the ALTER and the client
	// never saw the OK packet.
	for _, err := range []error{
		driver.ErrBadConn,
		mysql.ErrInvalidConn,
		io.EOF,
		&mysql.MySQLError{Number: 2013}, // CR_SERVER_LOST
		fmt.Errorf("ALTER TABLE t1: %w", driver.ErrBadConn),
	} {
		ambiguous := ambiguousDDLError(err)
		require.Error(t, ambiguous, "%v must be ownership-ambiguous", err)
		require.ErrorIs(t, ambiguous, status.ErrOwnershipAmbiguous)
		require.ErrorIs(t, ambiguous, err, "the underlying cause must stay inspectable")
	}
}

package migration

import (
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPrimaryKeyCollationChangeRefused runs a schema change that re-collates a
// composite character primary key, on a table holding keys whose order differs
// between the two collations. The checksum could never pass on such a table
// once it spans more than one chunk, so the runner refuses the statement at
// preflight, before any copy starts, and the table is left as it was.
func TestPrimaryKeyCollationChangeRefused(t *testing.T) {
	t.Parallel()

	tt := testutils.NewTestTable(t, "pkcollation", `CREATE TABLE pkcollation (
		owner_token varchar(64) NOT NULL,
		currency char(3) NOT NULL,
		amount bigint NOT NULL,
		PRIMARY KEY (owner_token, currency)
	) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`)
	_, err := tt.DB.ExecContext(t.Context(),
		`INSERT INTO pkcollation VALUES ('apple', 'USD', 1), ('Banana', 'USD', 2), ('cherry', 'CAD', 3), ('Date', 'EUR', 4)`)
	require.NoError(t, err)
	before := showCreateTable(t, tt.DB, "pkcollation")

	for _, tc := range []struct {
		alter      string
		wantReason string
	}{
		{
			alter:      "MODIFY COLUMN owner_token varchar(64) COLLATE utf8mb4_bin NOT NULL",
			wantReason: `changing the collation of primary key column "owner_token" is not supported`,
		},
		{
			alter:      "CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
			wantReason: "converting the table's character set changes the collation of its primary key",
		},
	} {
		runner := NewTestRunner(t, "pkcollation", tc.alter)
		err := runner.Run(t.Context())
		require.ErrorContains(t, err, tc.wantReason)
		require.NoError(t, runner.Close())
	}

	assert.Equal(t, before, showCreateTable(t, tt.DB, "pkcollation"))
	var rows int
	require.NoError(t, tt.DB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM pkcollation").Scan(&rows))
	assert.Equal(t, 4, rows)
}

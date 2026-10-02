package statement

import (
	"fmt"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/require"
)

func TestDiffIntegrationIndexSwapWithImplicitForeignKeyIndex(t *testing.T) {
	// Replacement names truncate long identifiers. These two distinct names
	// therefore request the same temporary name when planned independently.
	prefix := strings.Repeat("n", 59)
	for _, tc := range []struct {
		name, indexName, sourceBody, targetBody string
	}{
		{
			name: "named foreign key with case-insensitive collision", indexName: "k",
			sourceBody: `(id INT PRIMARY KEY, a INT, b INT, KEY k(a))`,
			targetBody: `(id INT PRIMARY KEY, a INT, b INT, KEY k(a) SECONDARY_ENGINE_ATTRIBUTE='{"x":1}', CONSTRAINT _K_NEW FOREIGN KEY(b) REFERENCES p(id))`,
		},
		{
			name: "unnamed foreign key names its index after the column", indexName: "k",
			sourceBody: `(id INT PRIMARY KEY, a INT, _k_new INT, KEY k(a))`,
			targetBody: `(id INT PRIMARY KEY, a INT, _k_new INT, KEY k(a) SECONDARY_ENGINE_ATTRIBUTE='{"x":1}', FOREIGN KEY(_k_new) REFERENCES p(id))`,
		},
		{
			name: "replacement foreign key shares the name allocator", indexName: prefix + "a",
			sourceBody: fmt.Sprintf(`(id INT PRIMARY KEY, a INT, b INT, c INT, KEY %sa(a), KEY u(b), CONSTRAINT %sb FOREIGN KEY(b) REFERENCES p(id))`, prefix, prefix),
			targetBody: fmt.Sprintf(`(id INT PRIMARY KEY, a INT, b INT, c INT, KEY %sa(a) SECONDARY_ENGINE_ATTRIBUTE='{"x":1}', KEY u(b), CONSTRAINT %sb FOREIGN KEY(c) REFERENCES p(id))`, prefix, prefix),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Isolate the schema-scoped FK names from other tests.
			_, db := testutils.CreateUniqueTestDatabase(t)
			exec := func(query string) {
				t.Helper()
				_, err := db.ExecContext(t.Context(), query)
				require.NoError(t, err, query)
			}
			exec("CREATE TABLE p(id INT PRIMARY KEY)")
			// Check the target is valid and capture MySQL's canonical form.
			exec("CREATE TABLE t " + tc.targetBody)
			want, err := ParseCreateTable(showCreateTable(t, db, "t"))
			require.NoError(t, err)
			exec("DROP TABLE t")
			exec("CREATE TABLE t " + tc.sourceBody)

			stmts := diffLiveTable(t, db, "t", "CREATE TABLE t "+tc.targetBody)
			execStatements(t, db, stmts)
			got, err := ParseCreateTable(showCreateTable(t, db, "t"))
			require.NoError(t, err)
			require.Len(t, got.Constraints, 1)
			require.True(t, constraintsEqualIgnoreName(&want.Constraints[0], &got.Constraints[0]))
			var expectedIndex, actualIndex *Index
			for i := range want.Indexes {
				if want.Indexes[i].Name == tc.indexName {
					expectedIndex = &want.Indexes[i]
				}
			}
			for i := range got.Indexes {
				if got.Indexes[i].Name == tc.indexName {
					actualIndex = &got.Indexes[i]
				}
			}
			require.NotNil(t, expectedIndex)
			require.NotNil(t, actualIndex)
			require.True(t, indexesEqual(expectedIndex, actualIndex), "the swap must apply the requested index options")
		})
	}
}

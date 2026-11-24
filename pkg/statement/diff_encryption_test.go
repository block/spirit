package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDiffEncryption covers the table ENCRYPTION option through
// DiffCreateTables: want is the declared schema, got is the live one, and the
// result is the ALTER that takes got to want. SHOW CREATE TABLE omits
// ENCRYPTION='N' on a server whose default_table_encryption is OFF, so an
// absent option and 'N' are the same table.
func TestDiffEncryption(t *testing.T) {
	const base = "CREATE TABLE t (id INT PRIMARY KEY)"
	tests := []struct {
		name string
		want string // declared
		got  string // live
		diff string
	}{
		{"YvsN", base + " ENCRYPTION='Y'", base + " ENCRYPTION='N'", "ALTER TABLE `t` ENCRYPTION='Y'"},
		{"NvsY", base + " ENCRYPTION='N'", base + " ENCRYPTION='Y'", "ALTER TABLE `t` ENCRYPTION='N'"},
		{"NvsN", base + " ENCRYPTION='N'", base + " ENCRYPTION='N'", ""},
		{"YvsY", base + " ENCRYPTION='Y'", base + " ENCRYPTION='Y'", ""},
		{"LowerYvsUpperY", base + " ENCRYPTION='y'", base + " ENCRYPTION='Y'", ""},
		{"UpperYvsLowerY", base + " ENCRYPTION='Y'", base + " ENCRYPTION='y'", ""},
		{"LowerNvsUpperN", base + " ENCRYPTION='n'", base + " ENCRYPTION='N'", ""},
		{"LowerNvsUpperY", base + " ENCRYPTION='n'", base + " ENCRYPTION='Y'", "ALTER TABLE `t` ENCRYPTION='N'"},
		{"LowerYvsUpperN", base + " ENCRYPTION='y'", base + " ENCRYPTION='N'", "ALTER TABLE `t` ENCRYPTION='Y'"},
		{"DeclaredNvsLiveAbsent", base + " ENCRYPTION='N'", base, ""},
		{"DeclaredLowerNvsLiveAbsent", base + " ENCRYPTION='n'", base, ""},
		{"DeclaredAbsentvsLiveN", base, base + " ENCRYPTION='N'", ""},
		{"AbsentvsAbsent", base, base, ""},
		{"DeclaredYvsLiveAbsent", base + " ENCRYPTION='Y'", base, "ALTER TABLE `t` ENCRYPTION='Y'"},
		{"DeclaredLowerYvsLiveAbsent", base + " ENCRYPTION='y'", base, "ALTER TABLE `t` ENCRYPTION='Y'"},
		{"DeclaredAbsentvsLiveY", base, base + " ENCRYPTION='Y'", "ALTER TABLE `t` ENCRYPTION='N'"},
		{
			// The shape of a live table: SHOW CREATE TABLE prints the table
			// options in its own order and with its own spelling.
			"LiveShowCreateTable",
			base + " ENGINE=InnoDB ENCRYPTION='N'",
			"CREATE TABLE `t` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			"",
		},
		{
			"LiveEncryptedShowCreateTable",
			base + " ENGINE=InnoDB ENCRYPTION='y'",
			"CREATE TABLE `t` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ENCRYPTION='Y'",
			"",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := NewDiffOptions()
			opts.IgnoreEngine = false
			diff, err := DiffCreateTables("t", tc.want, tc.got, opts)
			require.NoError(t, err)
			assert.Equal(t, tc.diff, diff)
		})
	}
}

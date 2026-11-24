package statement

import (
	"testing"

	"github.com/block/spirit/pkg/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDiffEncryption covers the table ENCRYPTION option through
// DiffCreateTables: want is the declared schema, got is the live one, and the
// result is the ALTER that takes got to want. SHOW CREATE TABLE omits
// ENCRYPTION='N' on a server whose default_table_encryption is OFF, so a live
// table with an absent option compares equal to a declared 'N'. A declared
// schema that omits ENCRYPTION never changes it: only an explicit 'N' decrypts.
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
		{"DeclaredAbsentvsLiveY", base, base + " ENCRYPTION='Y'", ""},
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

// liveEncryptionTable is a live table as SHOW CREATE TABLE prints it;
// encryption is the option text to append ("" for none).
func liveEncryptionTable(encryption string) string {
	return "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci" + encryption
}

// TestDeclarativeEncryption runs ENCRYPTION through DeclarativeToImperative,
// the entry point spirit diff uses. An omitted ENCRYPTION in the desired schema
// is never changed, so a schema written without the option does not decrypt a
// table on a server where encryption is the default.
func TestDeclarativeEncryption(t *testing.T) {
	const bare = "CREATE TABLE t (id INT PRIMARY KEY)"
	tests := []struct {
		name    string
		live    string
		desired string
		want    []string
	}{
		{"DesiredYvsLiveAbsentEncrypts", liveEncryptionTable(""), bare + " ENCRYPTION='Y'", []string{"ALTER TABLE `t` ENCRYPTION='Y'"}},
		{"DesiredNvsLiveYDecrypts", liveEncryptionTable(" ENCRYPTION='Y'"), bare + " ENCRYPTION='N'", []string{"ALTER TABLE `t` ENCRYPTION='N'"}},
		{"DesiredNvsLiveAbsentNothing", liveEncryptionTable(""), bare + " ENCRYPTION='N'", []string{}},
		{"DesiredAbsentvsLiveNNothing", liveEncryptionTable(" ENCRYPTION='N'"), bare, []string{}},
		{"DesiredAbsentvsLiveYNothing", liveEncryptionTable(" ENCRYPTION='Y'"), bare, []string{}},
		{"DesiredYvsLiveYNothing", liveEncryptionTable(" ENCRYPTION='Y'"), bare + " ENCRYPTION='y'", []string{}},
		{"DesiredYvsLiveNEncrypts", liveEncryptionTable(" ENCRYPTION='N'"), bare + " ENCRYPTION='Y'", []string{"ALTER TABLE `t` ENCRYPTION='Y'"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			changes, err := DeclarativeToImperative(
				[]table.TableSchema{{Name: "t", Schema: tc.live}},
				[]table.TableSchema{{Name: "t", Schema: tc.desired}}, NewDiffOptions())
			require.NoError(t, err)
			assert.Equal(t, tc.want, statementsToStrings(changes))
		})
	}
}

// TestAbsentDeclaredDoesNotDecryptLiveEncrypted: a schema that omits ENCRYPTION
// must not plan ENCRYPTION='N' for a live table that is encrypted.
func TestAbsentDeclaredDoesNotDecryptLiveEncrypted(t *testing.T) {
	current := []table.TableSchema{{Name: "t", Schema: liveEncryptionTable(" ENCRYPTION='Y'")}}
	desired := []table.TableSchema{{Name: "t", Schema: "CREATE TABLE t (id INT PRIMARY KEY)"}}
	changes, err := DeclarativeToImperative(current, desired, NewDiffOptions())
	require.NoError(t, err)
	require.Empty(t, statementsToStrings(changes))
}

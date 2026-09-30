package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestDBLevelGrantCoversSchema exercises the wildcard-aware grant matching
// without needing a live MySQL connection.
func TestDBLevelGrantCoversSchema(t *testing.T) {
	const schema = "strata_boardgames_sharded_n80"
	allPrivs := "ALTER,CREATE,DELETE,DROP,INDEX,INSERT,LOCK TABLES,SELECT,TRIGGER,UPDATE"

	tests := []struct {
		name   string
		grant  string
		schema string
		want   bool
	}{
		{
			name:   "wildcard with full privilege set",
			grant:  "GRANT " + allPrivs + " ON `strata_%`.* TO `cdb-test_ddl`@`%`",
			schema: schema,
			want:   true,
		},
		{
			name:   "wildcard with ALL PRIVILEGES",
			grant:  "GRANT ALL PRIVILEGES ON `strata_%`.* TO `cdb-test_ddl`@`%`",
			schema: schema,
			want:   true,
		},
		{
			name:   "wildcard missing TRIGGER is not enough",
			grant:  "GRANT ALTER,CREATE,DELETE,DROP,INDEX,INSERT,LOCK TABLES,SELECT,UPDATE ON `strata_%`.* TO `cdb-test_ddl`@`%`",
			schema: schema,
			want:   false,
		},
		{
			// CREATE VIEW / ALTER ROUTINE must not satisfy the CREATE / ALTER
			// requirements via substring matching: base CREATE and ALTER are absent.
			name:   "substring privilege names are not false positives",
			grant:  "GRANT CREATE VIEW,ALTER ROUTINE,DELETE,DROP,INDEX,INSERT,LOCK TABLES,SELECT,TRIGGER,UPDATE ON `strata_%`.* TO `cdb-test_ddl`@`%`",
			schema: schema,
			want:   false,
		},
		{
			name:   "privilege list with spaces after commas",
			grant:  "GRANT ALTER, CREATE, DELETE, DROP, INDEX, INSERT, LOCK TABLES, SELECT, TRIGGER, UPDATE ON `strata_%`.* TO `cdb-test_ddl`@`%`",
			schema: schema,
			want:   true,
		},
		{
			name:   "exact schema name",
			grant:  "GRANT " + allPrivs + " ON `" + schema + "`.* TO `cdb-test_ddl`@`%`",
			schema: schema,
			want:   true,
		},
		{
			name:   "escaped-underscore literal schema name",
			grant:  "GRANT " + allPrivs + " ON `strata\\_boardgames\\_sharded\\_n80`.* TO `cdb-test_ddl`@`%`",
			schema: schema,
			want:   true,
		},
		{
			name:   "non-matching wildcard",
			grant:  "GRANT " + allPrivs + " ON `polt_%`.* TO `cdb-test_ddl`@`%`",
			schema: schema,
			want:   false,
		},
		{
			name:   "global grant is not a database-level grant",
			grant:  "GRANT " + allPrivs + " ON *.* TO `cdb-test_ddl`@`%`",
			schema: schema,
			want:   false,
		},
		{
			name:   "table-level grant does not match",
			grant:  "GRANT SELECT ON `strata_%`.`some_table` TO `cdb-test_ddl`@`%`",
			schema: schema,
			want:   false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, DBLevelGrantCoversSchema(tc.grant, tc.schema))
		})
	}
}

// TestGlobalGrantConfersAny matches privilege names exactly, on global grants
// only.
func TestGlobalGrantConfersAny(t *testing.T) {
	tests := []struct {
		name  string
		grant string
		want  bool
	}{
		{"dynamic privilege", "GRANT CONNECTION_ADMIN,SYSTEM_VARIABLES_ADMIN ON *.* TO `u`@`%`", true},
		{"static privilege", "GRANT SELECT, PROCESS, SUPER ON *.* TO `u`@`%`", true},
		{"all privileges", "GRANT ALL PRIVILEGES ON *.* TO `u`@`%` WITH GRANT OPTION", true},
		{"missing", "GRANT SELECT, PROCESS ON *.* TO `u`@`%`", false},
		{"database-level grant", "GRANT ALL PRIVILEGES ON `test`.* TO `u`@`%`", false},
		{"name inside a role", "GRANT `rds_SUPERuser_role`@`%` TO `u`@`%`", false},
		{"name inside a user", "GRANT SELECT ON *.* TO `SUPER`@`%`", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, GlobalGrantConfersAny(tt.grant, "CONNECTION_ADMIN", "SUPER"))
		})
	}
}

func TestMySQLLikeMatch(t *testing.T) {
	tests := []struct {
		pattern string
		name    string
		want    bool
	}{
		// Exact, literal names (no wildcards).
		{"test", "test", true},
		{"test", "test2", false},
		{"test", "tes", false},
		{"", "", true},
		{"", "x", false},

		// '%' matches any sequence, including empty.
		{"%", "anything", true},
		{"%", "", true},
		{"strata_%", "strata_boardgames_sharded_n80", true},
		{"strata_%", "strata_x", true},
		{"strata%", "strata", true},
		{"strata_%", "stratax", true},   // 'strata' + '_'->'x' + '%'->""
		{"strata_%", "strata", false},   // '_' has no character to match
		{"strata_%", "strataXyz", true}, // 'strata' + '_'->'X' + '%'->"yz"
		{"%boardgames%", "strata_boardgames_n1", true},
		{"prod_%", "staging_db", false},

		// '_' matches exactly one character.
		{"strata_db", "strata_db", true}, // '_' also matches the literal underscore
		{"strata_db", "strataXdb", true}, // unescaped '_' is a wildcard
		{"a_c", "abc", true},
		{"a_c", "ac", false},
		{"a_c", "abbc", false},

		// Backslash escapes the wildcard so it is literal.
		{`strata\_%`, "strata_boardgames", true},
		{`strata\_%`, "strataXboardgames", false}, // escaped '_' must be a literal underscore
		{`a\%b`, "a%b", true},
		{`a\%b`, "axb", false},
		{`100\%`, "100%", true},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%s~%s", tc.pattern, tc.name), func(t *testing.T) {
			assert.Equal(t, tc.want, MySQLLikeMatch(tc.pattern, tc.name))
		})
	}
}

func TestParseRoleNames(t *testing.T) {
	assert.Equal(t, []string{"rds_superuser_role"}, ParseRoleNames("GRANT `rds_superuser_role`@`%` TO `user`@`%`"))
	assert.Equal(t, []string{"role_a", "role_b"}, ParseRoleNames("GRANT `role_a`@`%`,`role_b`@`localhost` TO `user`@`%`"))
	// The target user after TO is not a granted role.
	assert.Nil(t, ParseRoleNames("GRANT `role_a`@`%`"))
	assert.Nil(t, ParseRoleNames("GRANT SELECT ON *.* TO `user`@`%`"))
}

func TestStringContainsAll(t *testing.T) {
	assert.True(t, StringContainsAll("GRANT SELECT, INSERT ON *.*", "SELECT", "INSERT", " ON *.*"))
	assert.False(t, StringContainsAll("GRANT SELECT ON *.*", "SELECT", "INSERT"))
	// Empty substrings are ignored; with nothing left to find, the answer is false.
	assert.True(t, StringContainsAll("GRANT SELECT", "", "SELECT"))
	assert.False(t, StringContainsAll("GRANT SELECT", ""))
	assert.False(t, StringContainsAll("GRANT SELECT"))
}

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
			assert.Equal(t, tc.want, DBLevelGrantCoversSchema(tc.grant, tc.schema, false))
		})
	}
}

// TestDBLevelGrantCoversSchemaPartialRevokes checks that with
// partial_revokes=ON the granted database name is taken literally: '%' and
// '_' are not wildcards and a backslash is part of the name, as MySQL 8.0.45
// does.
func TestDBLevelGrantCoversSchemaPartialRevokes(t *testing.T) {
	allPrivs := "ALTER,CREATE,DELETE,DROP,INDEX,INSERT,LOCK TABLES,SELECT,TRIGGER,UPDATE"
	grant := func(db string) string { return "GRANT " + allPrivs + " ON `" + db + "`.* TO `u`@`%`" }
	tests := []struct {
		name, grant, schema string
		want                bool
	}{
		{"literal name covers itself", grant("app_one"), "app_one", true},
		{"percent is not a wildcard", grant("a%"), "app", false},
		{"underscore is not a wildcard", grant("app_one"), "appxone", false},
		{"percent covers a schema with that literal name", grant("a%"), "a%", true},
		{"escaped underscore keeps its backslash", grant(`app\_one`), "app_one", false},
		{"doubled backquote is one backquote", grant("app``one"), "app`one", true},
		{"ALL PRIVILEGES on a literal name", "GRANT ALL PRIVILEGES ON `app`.* TO `u`@`%`", "app", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, DBLevelGrantCoversSchema(tc.grant, tc.schema, true))
		})
	}
	// The same grants with partial_revokes=OFF are patterns.
	assert.True(t, DBLevelGrantCoversSchema(grant("a%"), "app", false))
	assert.True(t, DBLevelGrantCoversSchema(grant("app_one"), "appxone", false))
	assert.True(t, DBLevelGrantCoversSchema(grant(`app\_one`), "app_one", false))
	assert.True(t, DBLevelGrantCoversSchema(grant("app``one"), "app`one", false))
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

func TestGlobalGrantHasAny(t *testing.T) {
	tests := []struct {
		grant string
		privs []string
		want  bool
	}{
		{"GRANT SELECT, EVENT ON *.* TO `u`@`%`", []string{"EVENT"}, true},
		{"GRANT CONNECTION_ADMIN,SHOW_ROUTINE ON *.* TO `u`@`%`", []string{"SHOW_ROUTINE"}, true},
		{"GRANT ALL PRIVILEGES ON *.* TO `u`@`%` WITH GRANT OPTION", []string{"EVENT"}, true},
		{"GRANT SELECT ON *.* TO `u`@`%`", []string{"SHOW_ROUTINE", "SELECT"}, true},
		{"GRANT REPLICATION CLIENT ON *.* TO `u`@`%`", []string{"SELECT"}, false},
		{"GRANT CREATE VIEW ON *.* TO `u`@`%`", []string{"CREATE"}, false},
		{"GRANT EVENT ON `app`.* TO `u`@`%`", []string{"EVENT"}, false},
		{"GRANT SELECT ON `performance_schema`.* TO `u`@`%`", []string{"SELECT"}, false},
		{"GRANT `role1`@`%` TO `u`@`%`", []string{"SELECT"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.grant, func(t *testing.T) {
			assert.Equal(t, tc.want, GlobalGrantHasAny(tc.grant, tc.privs...))
		})
	}
}

func TestDBLevelGrantHasAny(t *testing.T) {
	tests := []struct {
		grant, schema  string
		partialRevokes bool
		privs          []string
		want           bool
	}{
		{"GRANT EVENT ON `app`.* TO `u`@`%`", "app", false, []string{"EVENT"}, true},
		{"GRANT SELECT, EXECUTE ON `app\\_%`.* TO `u`@`%`", "app_one", false, []string{"EXECUTE"}, true},
		{"GRANT ALL PRIVILEGES ON `app`.* TO `u`@`%`", "app", false, []string{"EVENT"}, true},
		{"GRANT ALTER ROUTINE ON `app`.* TO `u`@`%`", "app", false, []string{"EXECUTE", "ALTER ROUTINE"}, true},
		{"GRANT ALTER ON `app`.* TO `u`@`%`", "app", false, []string{"ALTER ROUTINE"}, false},
		{"GRANT EVENT ON `other`.* TO `u`@`%`", "app", false, []string{"EVENT"}, false},
		{"GRANT EVENT ON *.* TO `u`@`%`", "app", false, []string{"EVENT"}, false},
		{"GRANT SELECT ON `app`.`t1` TO `u`@`%`", "app", false, []string{"SELECT"}, false},
		// partial_revokes=ON: the name is literal.
		{"GRANT EVENT ON `app`.* TO `u`@`%`", "app", true, []string{"EVENT"}, true},
		{"GRANT EVENT, EXECUTE ON `a%`.* TO `u`@`%`", "app", true, []string{"EVENT", "EXECUTE"}, false},
		{"GRANT EVENT, EXECUTE ON `a%`.* TO `u`@`%`", "app", false, []string{"EVENT", "EXECUTE"}, true},
		{"GRANT SELECT, EXECUTE ON `app\\_%`.* TO `u`@`%`", "app_one", true, []string{"EXECUTE"}, false},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%s partial_revokes=%t", tc.grant, tc.partialRevokes), func(t *testing.T) {
			assert.Equal(t, tc.want, DBLevelGrantHasAny(tc.grant, tc.schema, tc.partialRevokes, tc.privs...))
		})
	}
}

func TestDBLevelRevokeHasAny(t *testing.T) {
	tests := []struct {
		grant, schema string
		privs         []string
		want          bool
	}{
		{"REVOKE SELECT, EVENT, TRIGGER ON `app`.* FROM `u`@`%`", "app", []string{"EVENT"}, true},
		{"REVOKE SELECT ON `app_one`.* FROM `u`@`%`", "app_one", []string{"SELECT"}, true},
		{"REVOKE ALL PRIVILEGES ON `app`.* FROM `u`@`%`", "app", []string{"TRIGGER"}, true},
		{"REVOKE SELECT ON `app`.* FROM `u`@`%`", "app", []string{"EVENT"}, false},
		{"REVOKE EVENT ON `other`.* FROM `u`@`%`", "app", []string{"EVENT"}, false},
		// Taken literally, not as a pattern.
		{"REVOKE EVENT ON `app_%`.* FROM `u`@`%`", "app_one", []string{"EVENT"}, false},
		{"GRANT EVENT ON `app`.* TO `u`@`%`", "app", []string{"EVENT"}, false},
		{"REVOKE EVENT ON `app``one`.* FROM `u`@`%`", "app`one", []string{"EVENT"}, true},
	}
	for _, tc := range tests {
		t.Run(tc.grant, func(t *testing.T) {
			assert.Equal(t, tc.want, DBLevelRevokeHasAny(tc.grant, tc.schema, tc.privs...))
		})
	}
}

func TestGlobalGrantNamesAny(t *testing.T) {
	assert.True(t, GlobalGrantNamesAny("GRANT SELECT, SHOW_ROUTINE ON *.* TO `u`@`%`", "SHOW_ROUTINE"))
	assert.True(t, GlobalGrantNamesAny("GRANT CONNECTION_ADMIN,SHOW_ROUTINE ON *.* TO `u`@`%`", "SHOW_ROUTINE"))
	// ALL PRIVILEGES does not name a dynamic privilege.
	assert.False(t, GlobalGrantNamesAny("GRANT ALL PRIVILEGES ON *.* TO `u`@`%`", "SHOW_ROUTINE"))
	assert.False(t, GlobalGrantNamesAny("GRANT SELECT ON *.* TO `u`@`%`", "SHOW_ROUTINE"))
	assert.False(t, GlobalGrantNamesAny("GRANT SHOW_ROUTINE ON `app`.* TO `u`@`%`", "SHOW_ROUTINE"))
}

func TestDBLevelGrantName(t *testing.T) {
	tests := []struct {
		grant, schema  string
		partialRevokes bool
		want           string
		ok             bool
	}{
		{"GRANT EVENT ON `app`.* TO `u`@`%`", "app", false, "app", true},
		// A pattern is returned as granted, escapes included.
		{"GRANT EVENT ON `app\\_%`.* TO `u`@`%`", "app_1", false, "app\\_%", true},
		{"GRANT EVENT ON `app%`.* TO `u`@`%`", "app_1", false, "app%", true},
		{"GRANT EVENT ON `app\\_%`.* TO `u`@`%`", "appx1", false, "", false},
		// SHOW GRANTS doubles a backquote in the name.
		{"GRANT EVENT ON `a``b`.* TO `u`@`%`", "a`b", false, "a`b", true},
		{"GRANT EVENT ON `a``b`.* TO `u`@`%`", "a``b", false, "", false},
		// partial_revokes=ON: the name is literal, including a backslash.
		{"GRANT EVENT ON `app`.* TO `u`@`%`", "app", true, "app", true},
		{"GRANT EVENT ON `app%`.* TO `u`@`%`", "app_1", true, "", false},
		{"GRANT EVENT ON `app\\_1`.* TO `u`@`%`", "app_1", true, "", false},
		{"GRANT EVENT ON `app\\_1`.* TO `u`@`%`", "app\\_1", true, "app\\_1", true},
		// Not database-level grants.
		{"GRANT EVENT ON *.* TO `u`@`%`", "app", false, "", false},
		{"GRANT SELECT ON `app`.`t1` TO `u`@`%`", "app", false, "", false},
		{"REVOKE EVENT ON `app`.* FROM `u`@`%`", "app", true, "", false},
		{"GRANT `r`@`%` TO `u`@`%`", "app", false, "", false},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%s %s partial_revokes=%t", tc.grant, tc.schema, tc.partialRevokes), func(t *testing.T) {
			name, ok := DBLevelGrantName(tc.grant, tc.schema, tc.partialRevokes)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.want, name)
		})
	}
}

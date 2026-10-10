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
	// SHOW GRANTS doubles a backquote in the name.
	assert.True(t, DBLevelGrantCoversSchema("GRANT ALL PRIVILEGES ON `app``one`.* TO `u`@`%`", "app`one"))
}

// TestDBLevelGrantsCoverSchema checks that the database-level grants are
// evaluated together: MySQL applies one mysql.db row to a schema, so an
// exact-name row with a smaller set can shadow a pattern row with the full set.
func TestDBLevelGrantsCoverSchema(t *testing.T) {
	const full = "ALTER, CREATE, DELETE, DROP, INDEX, INSERT, LOCK TABLES, SELECT, TRIGGER, UPDATE"
	const partial = "SELECT, INSERT, UPDATE, DELETE"
	grant := func(privs, db string) string {
		return "GRANT " + privs + " ON `" + db + "`.* TO `u`@`%`"
	}
	tests := []struct {
		name   string
		grants []string
		want   bool
	}{
		{"pattern only, full set", []string{grant(full, `app\_%`)}, true},
		{"pattern only, ALL PRIVILEGES", []string{grant("ALL PRIVILEGES", `app\_%`)}, true},
		{"exact partial shadows pattern full", []string{grant(full, `app\_%`), grant(partial, "app_1")}, false},
		{"exact partial listed first", []string{grant(partial, "app_1"), grant(full, `app\_%`)}, false},
		{"exact full and pattern full", []string{grant(full, `app\_%`), grant(full, "app_1")}, true},
		{"exact full, pattern partial", []string{grant(partial, `app\_%`), grant(full, "app_1")}, false},
		{"pattern partial only", []string{grant(partial, `app\_%`)}, false},
		{"non-matching grants ignored", []string{grant(full, "app_1"), grant(partial, "other")}, true},
		{"no grants", nil, false},
		{"global grant is not database-level", []string{"GRANT " + full + " ON *.* TO `u`@`%`"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, DBLevelGrantsCoverSchema(tc.grants, "app_1"))
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
		{"GRANT CONNECTION_ADMIN,SYSTEM_VARIABLES_ADMIN ON *.* TO `u`@`%`", []string{"CONNECTION_ADMIN", "SUPER"}, true},
		{"GRANT SELECT, PROCESS ON *.* TO `u`@`%`", []string{"CONNECTION_ADMIN", "SUPER"}, false},
		{"GRANT `rds_SUPERuser_role`@`%` TO `u`@`%`", []string{"SUPER"}, false},
		{"GRANT SELECT ON *.* TO `SUPER`@`%`", []string{"SUPER"}, false},
	}
	for _, tc := range tests {
		t.Run(tc.grant, func(t *testing.T) {
			assert.Equal(t, tc.want, GlobalGrantHasAny(tc.grant, tc.privs...))
		})
	}
}

func TestDBLevelGrantHasAny(t *testing.T) {
	tests := []struct {
		grant, schema string
		privs         []string
		want          bool
	}{
		{"GRANT EVENT ON `app`.* TO `u`@`%`", "app", []string{"EVENT"}, true},
		{"GRANT SELECT, EXECUTE ON `app\\_%`.* TO `u`@`%`", "app_one", []string{"EXECUTE"}, true},
		{"GRANT SELECT, EXECUTE ON `app\\_%`.* TO `u`@`%`", "appxone", []string{"EXECUTE"}, false},
		{"GRANT EVENT, EXECUTE ON `a%`.* TO `u`@`%`", "app", []string{"EVENT", "EXECUTE"}, true},
		{"GRANT ALL PRIVILEGES ON `app`.* TO `u`@`%`", "app", []string{"EVENT"}, true},
		{"GRANT ALTER ROUTINE ON `app`.* TO `u`@`%`", "app", []string{"EXECUTE", "ALTER ROUTINE"}, true},
		{"GRANT ALTER ON `app`.* TO `u`@`%`", "app", []string{"ALTER ROUTINE"}, false},
		{"GRANT EVENT ON `other`.* TO `u`@`%`", "app", []string{"EVENT"}, false},
		{"GRANT EVENT ON *.* TO `u`@`%`", "app", []string{"EVENT"}, false},
		{"GRANT SELECT ON `app`.`t1` TO `u`@`%`", "app", []string{"SELECT"}, false},
		{"GRANT EVENT ON `app``one`.* TO `u`@`%`", "app`one", []string{"EVENT"}, true},
	}
	for _, tc := range tests {
		t.Run(tc.grant+" "+tc.schema, func(t *testing.T) {
			assert.Equal(t, tc.want, DBLevelGrantHasAny(tc.grant, tc.schema, tc.privs...))
		})
	}
}

func TestProcedureGrantHasAny(t *testing.T) {
	tests := []struct {
		grant, schema, proc string
		want                bool
	}{
		{"GRANT EXECUTE ON PROCEDURE `mysql`.`rds_kill` TO `u`@`%`", "mysql", "rds_kill", true},
		{"GRANT EXECUTE, ALTER ROUTINE ON PROCEDURE `mysql`.`rds_kill` TO `u`@`%`", "mysql", "rds_kill", true},
		// Routine names are not case sensitive.
		{"GRANT EXECUTE ON PROCEDURE `mysql`.`RDS_KILL` TO `u`@`%`", "mysql", "rds_kill", true},
		{"GRANT ALTER ROUTINE ON PROCEDURE `mysql`.`rds_kill` TO `u`@`%`", "mysql", "rds_kill", false},
		{"GRANT EXECUTE ON PROCEDURE `mysql`.`rds_kill_query` TO `u`@`%`", "mysql", "rds_kill", false},
		{"GRANT EXECUTE ON PROCEDURE `other`.`rds_kill` TO `u`@`%`", "mysql", "rds_kill", false},
		{"GRANT EXECUTE ON FUNCTION `mysql`.`rds_kill` TO `u`@`%`", "mysql", "rds_kill", false},
		// Not procedure-level grants.
		{"GRANT EXECUTE ON *.* TO `u`@`%`", "mysql", "rds_kill", false},
		{"GRANT EXECUTE ON `mysql`.* TO `u`@`%`", "mysql", "rds_kill", false},
		{"GRANT EXECUTE ON `mysql`.`rds_kill` TO `u`@`%`", "mysql", "rds_kill", false},
		// SHOW GRANTS doubles a backquote in a name.
		{"GRANT EXECUTE ON PROCEDURE `a``b`.`p``q` TO `u`@`%`", "a`b", "p`q", true},
	}
	for _, tc := range tests {
		t.Run(tc.grant, func(t *testing.T) {
			assert.Equal(t, tc.want, ProcedureGrantHasAny(tc.grant, tc.schema, tc.proc, "EXECUTE"))
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
		grant, schema string
		want          string
		ok            bool
	}{
		{"GRANT EVENT ON `app`.* TO `u`@`%`", "app", "app", true},
		// A pattern is returned as granted, escapes included.
		{"GRANT EVENT ON `app\\_%`.* TO `u`@`%`", "app_1", "app\\_%", true},
		{"GRANT EVENT ON `app%`.* TO `u`@`%`", "app_1", "app%", true},
		{"GRANT EVENT ON `app\\_%`.* TO `u`@`%`", "appx1", "", false},
		{"GRANT EVENT ON `app\\_1`.* TO `u`@`%`", "app_1", "app\\_1", true},
		{"GRANT EVENT ON `app\\_1`.* TO `u`@`%`", "appx1", "", false},
		// SHOW GRANTS doubles a backquote in the name.
		{"GRANT EVENT ON `a``b`.* TO `u`@`%`", "a`b", "a`b", true},
		{"GRANT EVENT ON `a``b`.* TO `u`@`%`", "a``b", "", false},
		// Not database-level grants.
		{"GRANT EVENT ON *.* TO `u`@`%`", "app", "", false},
		{"GRANT SELECT ON `app`.`t1` TO `u`@`%`", "app", "", false},
		{"REVOKE EVENT ON `app`.* FROM `u`@`%`", "app", "", false},
		{"GRANT `r`@`%` TO `u`@`%`", "app", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.grant+" "+tc.schema, func(t *testing.T) {
			name, ok := DBLevelGrantName(tc.grant, tc.schema)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.want, name)
		})
	}
}

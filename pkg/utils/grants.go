package utils

import (
	"regexp"
	"strings"
)

// dbGrantRegexp captures the privilege list and database-name pattern from a
// database-level grant line, e.g.
//
//	GRANT ALL PRIVILEGES ON `strata_%`.* TO `user`@`%`
//
// capturing "ALL PRIVILEGES" and "strata_%". It only matches database-level
// grants (`db`.*); global (*.*), table-level, and routine grants do not match.
// SHOW GRANTS doubles a backquote inside the name; see unquoteDBName.
var dbGrantRegexp = regexp.MustCompile("^GRANT (.+) ON `((?:[^`]|``)+)`\\.\\* TO ")

// globalGrantRegexp captures the privilege list from a global grant line, e.g.
//
//	GRANT SELECT, EVENT ON *.* TO `user`@`%`
//	GRANT CONNECTION_ADMIN,SHOW_ROUTINE ON *.* TO `user`@`%`
//
// capturing "SELECT, EVENT" or "CONNECTION_ADMIN,SHOW_ROUTINE".
var globalGrantRegexp = regexp.MustCompile(`^GRANT (.+) ON \*\.\* TO `)

// dbRevokeRegexp captures the privilege list and database name from a partial
// revoke line, which SHOW GRANTS prints when partial_revokes=ON and a global
// privilege has been revoked for one database, e.g.
//
//	REVOKE SELECT, EVENT ON `app`.* FROM `user`@`%`
//
// capturing "SELECT, EVENT" and "app".
var dbRevokeRegexp = regexp.MustCompile("^REVOKE (.+) ON `((?:[^`]|``)+)`\\.\\* FROM ")

// migrationDBPrivileges is the database-level privilege set spirit requires to
// run a migration or move (mirroring gh-ost's historical requirement). A grant
// of ALL PRIVILEGES, or of every privilege in this set, satisfies the check.
var migrationDBPrivileges = []string{
	"ALTER", "CREATE", "DELETE", "DROP", "INDEX", "INSERT",
	"LOCK TABLES", "SELECT", "TRIGGER", "UPDATE",
}

// DBLevelGrantCoversSchema reports whether a single SHOW GRANTS line is a
// database-level grant that confers the privileges spirit needs on schemaName.
// The granted database name is matched as described in DBNameMatches, so with
// partialRevokes=false a grant on `strata_%`.* covers
// strata_boardgames_sharded_n80, and with partialRevokes=true it does not.
// partialRevokes is the server's partial_revokes setting
// (dbconn.PartialRevokesEnabled).
func DBLevelGrantCoversSchema(grant, schemaName string, partialRevokes bool) bool {
	m := dbGrantRegexp.FindStringSubmatch(grant)
	if m == nil || !DBNameMatches(unquoteDBName(m[2]), schemaName, partialRevokes) {
		return false
	}
	granted := splitPrivileges(m[1])
	if granted["ALL PRIVILEGES"] {
		return true
	}
	for _, p := range migrationDBPrivileges {
		if !granted[p] {
			return false
		}
	}
	return true
}

// GlobalGrantHasAny reports whether a single SHOW GRANTS line is a global
// (*.*) grant of ALL PRIVILEGES or of any privilege in privs. ALL PRIVILEGES
// counts because it includes every static privilege, so privs should contain
// at least one static privilege for it to be meaningful.
func GlobalGrantHasAny(grant string, privs ...string) bool {
	m := globalGrantRegexp.FindStringSubmatch(grant)
	if m == nil {
		return false
	}
	return hasAnyPrivilege(splitPrivileges(m[1]), privs)
}

// GlobalGrantNamesAny reports whether a single SHOW GRANTS line is a global
// (*.*) grant that names any privilege in privs explicitly. Unlike
// GlobalGrantHasAny, ALL PRIVILEGES does not count. Use it for dynamic
// privileges such as SHOW_ROUTINE: GRANT ALL includes a dynamic privilege only
// if it was registered when the grant was issued, so a global ALL grant made
// before an upgrade can lack it, and SHOW GRANTS still prints ALL PRIVILEGES.
func GlobalGrantNamesAny(grant string, privs ...string) bool {
	m := globalGrantRegexp.FindStringSubmatch(grant)
	if m == nil {
		return false
	}
	granted := splitPrivileges(m[1])
	for _, p := range privs {
		if granted[p] {
			return true
		}
	}
	return false
}

// DBLevelGrantName returns the database name of a single SHOW GRANTS line if
// it is a database-level grant whose name matches schemaName (see
// DBNameMatches), with the doubled backquotes SHOW GRANTS writes undone. The
// name is returned as granted, so a pattern keeps its wildcards and escapes.
//
// SHOW GRANTS prints one line per mysql.db row, and MySQL applies only one
// row to a schema, not the union of every row whose name matches it: an
// exact-name row can shadow a pattern row, depending on the order the grants
// were created. Callers that need a privilege on the schema can group the
// matching lines by this name and require the privilege on every name.
func DBLevelGrantName(grant, schemaName string, partialRevokes bool) (string, bool) {
	m := dbGrantRegexp.FindStringSubmatch(grant)
	if m == nil {
		return "", false
	}
	name := unquoteDBName(m[2])
	if !DBNameMatches(name, schemaName, partialRevokes) {
		return "", false
	}
	return name, true
}

// DBLevelGrantHasAny reports whether a single SHOW GRANTS line is a
// database-level grant whose database name matches schemaName (see
// DBNameMatches) and that confers ALL PRIVILEGES or any privilege in privs.
func DBLevelGrantHasAny(grant, schemaName string, partialRevokes bool, privs ...string) bool {
	m := dbGrantRegexp.FindStringSubmatch(grant)
	if m == nil || !DBNameMatches(unquoteDBName(m[2]), schemaName, partialRevokes) {
		return false
	}
	return hasAnyPrivilege(splitPrivileges(m[1]), privs)
}

// DBLevelRevokeHasAny reports whether a single SHOW GRANTS line is a partial
// revoke (partial_revokes=ON) on schemaName of ALL PRIVILEGES or any privilege
// in privs. Such a line cancels a global grant of that privilege for that one
// schema. With partial_revokes=ON, MySQL takes the database name in a revoke
// literally, so it is compared exactly rather than as a pattern.
func DBLevelRevokeHasAny(grant, schemaName string, privs ...string) bool {
	m := dbRevokeRegexp.FindStringSubmatch(grant)
	if m == nil || unquoteDBName(m[2]) != schemaName {
		return false
	}
	return hasAnyPrivilege(splitPrivileges(m[1]), privs)
}

// DBNameMatches reports whether the database name in a database-level grant
// applies to schemaName. With partialRevokes=false (the MySQL default), the
// name is a LIKE pattern (see MySQLLikeMatch). With partialRevokes=true, MySQL
// takes the name literally: '%' and '_' are ordinary characters, and so is a
// backslash, so a grant on `app\_db`.* applies only to a schema whose name
// contains the backslash, not to app_db (verified on MySQL 8.0.45).
func DBNameMatches(grantedName, schemaName string, partialRevokes bool) bool {
	if partialRevokes {
		return grantedName == schemaName
	}
	return MySQLLikeMatch(grantedName, schemaName)
}

// unquoteDBName undoes the only escaping SHOW GRANTS applies inside a
// backquoted database name: a doubled backquote.
func unquoteDBName(name string) string {
	return strings.ReplaceAll(name, "``", "`")
}

func hasAnyPrivilege(granted map[string]bool, privs []string) bool {
	if granted["ALL PRIVILEGES"] {
		return true
	}
	for _, p := range privs {
		if granted[p] {
			return true
		}
	}
	return false
}

// splitPrivileges parses the privilege list from a GRANT statement into a set
// of individual privilege names. Database-level grants never carry column-level
// privilege lists, so a plain comma split is sufficient. Matching exact names
// (rather than substrings) avoids false positives where one privilege name
// contains another, e.g. CREATE inside CREATE VIEW or ALTER inside ALTER ROUTINE.
func splitPrivileges(privs string) map[string]bool {
	set := make(map[string]bool)
	for p := range strings.SplitSeq(privs, ",") {
		set[strings.TrimSpace(p)] = true
	}
	return set
}

// MySQLLikeMatch reports whether name matches the given MySQL LIKE-style
// pattern. As in MySQL pattern matching, '%' matches any sequence of
// characters (including the empty string), '_' matches any single character,
// and a backslash escapes the following character so that it is treated as a
// literal (so `\%` and `\_` match a literal '%' and '_').
//
// This mirrors how MySQL evaluates the database-name portion of a
// database-level GRANT. A grant on `strata_%`.* applies to a database named
// strata_boardgames, even though SHOW GRANTS reports the pattern verbatim.
// Privilege checks therefore cannot compare the granted database name to the
// target schema with a plain string match; they must expand the pattern.
func MySQLLikeMatch(pattern, name string) bool {
	p, s := []byte(pattern), []byte(name)
	var pi, si int
	// Position of the most recent '%' in the pattern and the input position
	// where it began matching, so we can backtrack and let it consume one
	// more character when a later literal fails.
	starPi, starSi := -1, 0
	for si < len(s) {
		if pi < len(p) {
			switch c := p[pi]; {
			case c == '\\' && pi+1 < len(p):
				if p[pi+1] == s[si] {
					pi += 2
					si++
					continue
				}
			case c == '%':
				starPi, starSi = pi, si
				pi++
				continue
			case c == '_' || c == s[si]:
				pi++
				si++
				continue
			}
		}
		if starPi != -1 {
			// Backtrack: let the '%' absorb one more character.
			pi = starPi + 1
			starSi++
			si = starSi
			continue
		}
		return false
	}
	// Any pattern remaining must be all '%' to match the empty suffix.
	for pi < len(p) && p[pi] == '%' {
		pi++
	}
	return pi == len(p)
}

// grantedRolesRegexp matches role grants in SHOW GRANTS output.
// MySQL outputs role grants as: GRANT `role_name`@`%` TO `user`@`%`
// There may be multiple roles in a single line, comma-separated.
var grantedRolesRegexp = regexp.MustCompile("`([^`]+)`@`[^`]+`")

// ParseRoleNames extracts role names from a SHOW GRANTS line that grants roles.
// e.g. "GRANT `rds_superuser_role`@`%`,`other_role`@`%` TO `user`@`%`"
// returns ["rds_superuser_role", "other_role"]
func ParseRoleNames(grant string) []string {
	// Split on " TO " to get only the roles part (before the target user)
	parts := strings.SplitN(grant, " TO ", 2)
	if len(parts) < 2 {
		return nil
	}
	rolesPart := parts[0] // "GRANT `role1`@`%`,`role2`@`%`"
	matches := grantedRolesRegexp.FindAllStringSubmatch(rolesPart, -1)
	var roles []string
	for _, match := range matches {
		if len(match) >= 2 {
			roles = append(roles, match[1])
		}
	}
	return roles
}

// StringContainsAll returns true if `s` contains all non empty given `substrings`
// The function returns `false` if no non-empty arguments are given.
func StringContainsAll(s string, substrings ...string) bool {
	nonEmptyStringsFound := false
	for _, substring := range substrings {
		if substring == "" {
			continue
		}
		if strings.Contains(s, substring) {
			nonEmptyStringsFound = true
		} else {
			// Immediate failure
			return false
		}
	}
	return nonEmptyStringsFound
}

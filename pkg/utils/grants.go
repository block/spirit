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
var dbGrantRegexp = regexp.MustCompile("^GRANT (.+) ON `([^`]+)`\\.\\* TO ")

// globalGrantRegexp captures the privilege list from a global grant line,
// e.g. GRANT PROCESS, RELOAD ON *.* TO `user`@`%` captures "PROCESS, RELOAD".
var globalGrantRegexp = regexp.MustCompile(`^GRANT (.+) ON \*\.\* TO `)

// GlobalGrantConfersAny reports whether a single SHOW GRANTS line is a global
// (*.*) grant of ALL PRIVILEGES or of any of privs. It matches privilege names
// exactly, so SUPER does not match a role or user whose name contains it.
func GlobalGrantConfersAny(grant string, privs ...string) bool {
	m := globalGrantRegexp.FindStringSubmatch(grant)
	if m == nil {
		return false
	}
	granted := splitPrivileges(m[1])
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

// migrationDBPrivileges is the database-level privilege set spirit requires to
// run a migration or move (mirroring gh-ost's historical requirement). A grant
// of ALL PRIVILEGES, or of every privilege in this set, satisfies the check.
var migrationDBPrivileges = []string{
	"ALTER", "CREATE", "DELETE", "DROP", "INDEX", "INSERT",
	"LOCK TABLES", "SELECT", "TRIGGER", "UPDATE",
}

// DBLevelGrantCoversSchema reports whether a single SHOW GRANTS line is a
// database-level grant that confers the privileges spirit needs on schemaName.
// Unlike a literal substring match, it expands MySQL wildcard patterns in the
// granted database name (see MySQLLikeMatch), so a grant on `strata_%`.* is
// recognized as covering strata_boardgames_sharded_n80. The previous literal
// match handled only exact and escaped-underscore database names.
func DBLevelGrantCoversSchema(grant, schemaName string) bool {
	m := dbGrantRegexp.FindStringSubmatch(grant)
	if m == nil {
		return false
	}
	privs, dbPattern := m[1], m[2]
	if !MySQLLikeMatch(dbPattern, schemaName) {
		return false
	}
	granted := splitPrivileges(privs)
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

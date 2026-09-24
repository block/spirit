package lint

import (
	"fmt"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/require"
)

// parseTables is a helper that parses one or more CREATE TABLE statements.
func parseTables(t *testing.T, sqls ...string) []*statement.CreateTable {
	t.Helper()
	out := make([]*statement.CreateTable, 0, len(sqls))
	for _, sql := range sqls {
		ct, err := statement.ParseCreateTable(sql)
		require.NoError(t, err)
		out = append(out, ct)
	}
	return out
}

func newTypePedantic(t *testing.T) *TypePedanticLinter {
	t.Helper()
	l := &TypePedanticLinter{}
	require.NoError(t, l.Configure(l.DefaultConfig()))
	return l
}

func TestTypePedantic_NoViolations_ConsistentTypes(t *testing.T) {
	tables := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255))`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id BIGINT UNSIGNED NOT NULL)`,
	)
	violations := newTypePedantic(t).Lint(tables, nil)
	require.Empty(t, violations)
}

func TestTypePedantic_SameName_TypeMismatch(t *testing.T) {
	tables := parseTables(t,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id BIGINT UNSIGNED NOT NULL, INDEX idx_customer (customer_id))`,
		`CREATE TABLE returns (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT NOT NULL, INDEX idx_customer (customer_id))`,
		`CREATE TABLE invoices (id BIGINT UNSIGNED PRIMARY KEY, customer_id BIGINT UNSIGNED NOT NULL, INDEX idx_customer (customer_id))`,
	)
	violations := newTypePedantic(t).Lint(tables, nil)

	sameName := filterRule(violations, "same_name")
	require.Len(t, sameName, 1, "only the minority customer_id should be flagged")
	v := sameName[0]
	require.Equal(t, "type_pedantic", v.Linter.Name())
	require.Equal(t, SeverityWarning, v.Severity)
	require.Equal(t, "returns", v.Location.Table)
	require.NotNil(t, v.Location.Column)
	require.Equal(t, "customer_id", *v.Location.Column)
	require.Contains(t, v.Message, "int(11)")
	require.Contains(t, v.Message, "bigint(20) unsigned")
	require.Equal(t, "int(11)", v.Context["current_type"])
	require.Equal(t, "bigint(20) unsigned", v.Context["expected_type"])
}

func TestTypePedantic_SameName_IgnoresIDByDefault(t *testing.T) {
	// Two tables with intentionally different `id` types — ignored by default.
	tables := parseTables(t,
		`CREATE TABLE users (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE sessions (id BINARY(16) PRIMARY KEY)`,
	)
	violations := newTypePedantic(t).Lint(tables, nil)
	require.Empty(t, filterRule(violations, "same_name"))
}

func TestTypePedantic_SameName_ConfigurableIgnore(t *testing.T) {
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, status VARCHAR(20))`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, status VARCHAR(50))`,
	)
	l := &TypePedanticLinter{}
	require.NoError(t, l.Configure(map[string]string{
		"checkSameName":    "true",
		"checkInferredFK":  "false",
		"ignoreColumns":    "id, status",
		"fkSeverity":       "error",
		"sameNameSeverity": "warning",
	}))
	require.Empty(t, l.Lint(tables, nil))
}

func TestTypePedantic_SameName_DisabledViaConfig(t *testing.T) {
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, val INT)`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, val BIGINT)`,
	)
	l := &TypePedanticLinter{}
	require.NoError(t, l.Configure(map[string]string{
		"checkSameName":    "false",
		"checkInferredFK":  "true",
		"ignoreColumns":    "id",
		"fkSeverity":       "error",
		"sameNameSeverity": "warning",
	}))
	require.Empty(t, l.Lint(tables, nil))
}

func TestTypePedantic_SameName_SignednessMismatch(t *testing.T) {
	// Two tables agree on INT UNSIGNED, one dissents with signed INT — clear majority.
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, qty INT UNSIGNED NOT NULL, INDEX idx_qty (qty))`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, qty INT UNSIGNED NOT NULL, INDEX idx_qty (qty))`,
		`CREATE TABLE c (id BIGINT UNSIGNED PRIMARY KEY, qty INT NOT NULL, INDEX idx_qty (qty))`,
	)
	violations := filterRule(newTypePedantic(t).Lint(tables, nil), "same_name")
	require.Len(t, violations, 1)
	require.Equal(t, "c", violations[0].Location.Table)
	require.Contains(t, violations[0].Message, "int(11)")
	require.Contains(t, violations[0].Message, "unsigned")
}

func TestTypePedantic_InferredFK_Mismatch(t *testing.T) {
	tables := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY, name VARCHAR(100))`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT UNSIGNED NOT NULL)`,
	)
	violations := filterRule(newTypePedantic(t).Lint(tables, nil), "inferred_fk")
	require.Len(t, violations, 1)
	v := violations[0]
	require.Equal(t, SeverityWarning, v.Severity)
	require.Equal(t, "orders", v.Location.Table)
	require.Equal(t, "customer_id", *v.Location.Column)
	require.Contains(t, v.Message, "customers")
	require.Equal(t, "customers", v.Context["referenced_table"])
}

func TestTypePedantic_InferredFK_Match(t *testing.T) {
	tables := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id BIGINT UNSIGNED NOT NULL)`,
	)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(tables, nil), "inferred_fk"))
}

func TestTypePedantic_InferredFK_PluralizedTable(t *testing.T) {
	// orders.user_id should resolve to users.id
	tables := parseTables(t,
		`CREATE TABLE users (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, user_id INT NOT NULL)`,
	)
	violations := filterRule(newTypePedantic(t).Lint(tables, nil), "inferred_fk")
	require.Len(t, violations, 1)
	require.Equal(t, "users", violations[0].Context["referenced_table"])
}

func TestTypePedantic_InferredFK_IESPluralization(t *testing.T) {
	// activities.id referenced by activity_id
	tables := parseTables(t,
		`CREATE TABLE activities (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE logs (id BIGINT UNSIGNED PRIMARY KEY, activity_id INT NOT NULL)`,
	)
	violations := filterRule(newTypePedantic(t).Lint(tables, nil), "inferred_fk")
	require.Len(t, violations, 1)
	require.Equal(t, "activities", violations[0].Context["referenced_table"])
}

func TestTypePedantic_InferredFK_NoMatchingTable(t *testing.T) {
	// session_id but no `session` or `sessions` table - silently skip.
	tables := parseTables(t,
		`CREATE TABLE events (id BIGINT UNSIGNED PRIMARY KEY, session_id VARCHAR(64) NOT NULL)`,
	)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(tables, nil), "inferred_fk"))
}

func TestTypePedantic_InferredFK_TargetMissingIDColumn(t *testing.T) {
	// Target table has a composite PK with no `id` column - skip silently.
	tables := parseTables(t,
		`CREATE TABLE products (sku VARCHAR(20) NOT NULL, region VARCHAR(2) NOT NULL, PRIMARY KEY(sku, region))`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, product_id INT NOT NULL)`,
	)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(tables, nil), "inferred_fk"))
}

func TestTypePedantic_InferredFK_DisabledViaConfig(t *testing.T) {
	tables := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT NOT NULL)`,
	)
	l := &TypePedanticLinter{}
	require.NoError(t, l.Configure(map[string]string{
		"checkSameName":    "false",
		"checkInferredFK":  "false",
		"ignoreColumns":    "id",
		"fkSeverity":       "error",
		"sameNameSeverity": "warning",
	}))
	require.Empty(t, l.Lint(tables, nil))
}

func TestTypePedantic_InferredFK_ConfigurableSeverity(t *testing.T) {
	// Default is warning (heuristic can false-positive on generic names like
	// client_id); users who want strict gating can opt back into error.
	tables := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT NOT NULL)`,
	)
	l := &TypePedanticLinter{}
	require.NoError(t, l.Configure(map[string]string{
		"checkSameName":    "true",
		"checkInferredFK":  "true",
		"ignoreColumns":    "id",
		"fkSeverity":       "error",
		"sameNameSeverity": "warning",
	}))
	violations := filterRule(l.Lint(tables, nil), "inferred_fk")
	require.Len(t, violations, 1)
	require.Equal(t, SeverityError, violations[0].Severity)
}

func TestTypePedantic_InferredFK_FromChanges(t *testing.T) {
	// Existing customers table; the change is a new orders table.
	existing := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
	)
	stmts, err := statement.New(`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT NOT NULL)`)
	require.NoError(t, err)
	violations := filterRule(newTypePedantic(t).Lint(existing, stmts), "inferred_fk")
	require.Len(t, violations, 1)
	require.Equal(t, "orders", violations[0].Location.Table)
}

func TestTypePedantic_InferredFK_IgnoresEmptyBase(t *testing.T) {
	// Plain `_id` column would have base "" — should be skipped.
	tables := parseTables(t,
		"CREATE TABLE oddly_named (id BIGINT UNSIGNED PRIMARY KEY, `_id` VARCHAR(36) NOT NULL)",
	)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(tables, nil), "inferred_fk"))
}

func TestTypePedantic_InferredFK_DoesNotMatchSelf(t *testing.T) {
	// orders.order_id pointing to orders.id would be a self-reference and is skipped.
	tables := parseTables(t,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, order_id INT NOT NULL)`,
	)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(tables, nil), "inferred_fk"))
}

func TestTypePedantic_BothRulesEmitForSameColumn(t *testing.T) {
	// orders.customer_id is INT — flagged by both rules:
	//  - same_name: minority against two tables using BIGINT UNSIGNED
	//  - inferred_fk: doesn't match customers.id (BIGINT UNSIGNED)
	tables := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT NOT NULL, INDEX idx_customer (customer_id))`,
		`CREATE TABLE invoices (id BIGINT UNSIGNED PRIMARY KEY, customer_id BIGINT UNSIGNED NOT NULL, INDEX idx_customer (customer_id))`,
		`CREATE TABLE receipts (id BIGINT UNSIGNED PRIMARY KEY, customer_id BIGINT UNSIGNED NOT NULL, INDEX idx_customer (customer_id))`,
	)
	violations := newTypePedantic(t).Lint(tables, nil)
	require.Len(t, filterRule(violations, "same_name"), 1)
	require.Len(t, filterRule(violations, "inferred_fk"), 1)
}

func TestTypePedantic_Configure_RejectsUnknownKey(t *testing.T) {
	l := &TypePedanticLinter{}
	err := l.Configure(map[string]string{"bogus": "value"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown config key")
}

func TestTypePedantic_Configure_RejectsBadSeverity(t *testing.T) {
	l := &TypePedanticLinter{}
	err := l.Configure(map[string]string{"fkSeverity": "panic"})
	require.Error(t, err)
}

func TestTypePedantic_Configure_RejectsBadBool(t *testing.T) {
	l := &TypePedanticLinter{}
	err := l.Configure(map[string]string{"checkSameName": "maybe"})
	require.Error(t, err)
}

func TestTypePedantic_SameName_RequireIndexed_SkipsUnindexed(t *testing.T) {
	// Neither table indexes `note` — by default this is treated as noise and skipped.
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, note VARCHAR(50))`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, note VARCHAR(255))`,
	)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(tables, nil), "same_name"))
}

func TestTypePedantic_SameName_RequireIndexed_FlagsWhenAnyIndexed(t *testing.T) {
	// `customer_id` is indexed in `orders` only — the unindexed `returns` copy is
	// still flagged because the join surface against the indexed copy is real.
	// Three tables so the minority is unambiguous.
	tables := parseTables(t,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id BIGINT UNSIGNED NOT NULL, INDEX idx_customer (customer_id))`,
		`CREATE TABLE invoices (id BIGINT UNSIGNED PRIMARY KEY, customer_id BIGINT UNSIGNED NOT NULL)`,
		`CREATE TABLE returns (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT NOT NULL)`,
	)
	violations := filterRule(newTypePedantic(t).Lint(tables, nil), "same_name")
	require.Len(t, violations, 1)
	require.Equal(t, "returns", violations[0].Location.Table)
}

func TestTypePedantic_SameName_RequireIndexed_DisabledFlagsAll(t *testing.T) {
	// With requireIndexed=false the noisy non-indexed mismatch fires.
	// Three tables so we get a clear majority rather than a tie.
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, note VARCHAR(50))`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, note VARCHAR(255))`,
		`CREATE TABLE c (id BIGINT UNSIGNED PRIMARY KEY, note VARCHAR(255))`,
	)
	l := &TypePedanticLinter{}
	require.NoError(t, l.Configure(map[string]string{
		"checkSameName":    "true",
		"checkInferredFK":  "false",
		"requireIndexed":   "false",
		"ignoreColumns":    "id",
		"fkSeverity":       "error",
		"sameNameSeverity": "warning",
	}))
	violations := filterRule(l.Lint(tables, nil), "same_name")
	require.Len(t, violations, 1)
	require.Equal(t, "a", violations[0].Location.Table)
}

func TestTypePedantic_SameName_RequireIndexed_CompositeIndexAnyPosition(t *testing.T) {
	// Trailing position in a composite index still counts as indexed.
	// Three tables for a clear majority.
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, tenant_id BIGINT UNSIGNED, customer_id BIGINT UNSIGNED, INDEX idx_t_c (tenant_id, customer_id))`,
		`CREATE TABLE c (id BIGINT UNSIGNED PRIMARY KEY, tenant_id BIGINT UNSIGNED, customer_id BIGINT UNSIGNED, INDEX idx_t_c (tenant_id, customer_id))`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, tenant_id BIGINT UNSIGNED, customer_id INT, INDEX idx_t_c (tenant_id, customer_id))`,
	)
	violations := filterRule(newTypePedantic(t).Lint(tables, nil), "same_name")
	require.Len(t, violations, 1)
	require.Equal(t, "b", violations[0].Location.Table)
	require.Equal(t, "customer_id", *violations[0].Location.Column)
}

func TestTypePedantic_InferredFK_UnaffectedByRequireIndexed(t *testing.T) {
	// The FK rule fires regardless of whether the FK column itself is indexed —
	// joins are the problem and the target's id column is always indexed.
	tables := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT NOT NULL)`,
	)
	violations := filterRule(newTypePedantic(t).Lint(tables, nil), "inferred_fk")
	require.Len(t, violations, 1)
}

func TestTypePedantic_SameName_TieEmitsInconsistentForAll(t *testing.T) {
	// One column of each type — no majority. New behavior: every occurrence is
	// reported as inconsistent so neither side is silently treated as canonical.
	tables := parseTables(t,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, qty INT UNSIGNED NOT NULL, INDEX idx_qty (qty))`,
		`CREATE TABLE returns (id BIGINT UNSIGNED PRIMARY KEY, qty INT NOT NULL, INDEX idx_qty (qty))`,
	)
	violations := filterRule(newTypePedantic(t).Lint(tables, nil), "same_name")
	require.Len(t, violations, 2)
	tables_seen := map[string]bool{}
	for _, v := range violations {
		require.Contains(t, v.Message, "inconsistent across schema")
		require.Contains(t, v.Message, `types in use: "int(11)", "int(11) unsigned"`)
		require.NotNil(t, v.Context["conflicting_types"])
		tables_seen[v.Location.Table] = true
	}
	require.True(t, tables_seen["orders"] && tables_seen["returns"])
}

func TestTypePedantic_SameName_InlinePrimaryKeyCountsAsIndexed(t *testing.T) {
	// Inline `PRIMARY KEY` doesn't appear in t.Indexes but DOES via GetIndexes().
	// Bug fix: type mismatches against inline-PK columns must still fire.
	tables := parseTables(t,
		`CREATE TABLE customers (customer_key BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (customer_key BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE returns (customer_key INT PRIMARY KEY)`,
	)
	violations := filterRule(newTypePedantic(t).Lint(tables, nil), "same_name")
	require.Len(t, violations, 1, "inline PRIMARY KEY should register as indexed")
	require.Equal(t, "returns", violations[0].Location.Table)
}

func TestTypePedantic_SameName_InlineUniqueCountsAsIndexed(t *testing.T) {
	// Inline `UNIQUE` should also register as indexed.
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255) UNIQUE)`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255) UNIQUE)`,
		`CREATE TABLE c (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(500) UNIQUE)`,
	)
	violations := filterRule(newTypePedantic(t).Lint(tables, nil), "same_name")
	require.Len(t, violations, 1)
	require.Equal(t, "c", violations[0].Location.Table)
}

func TestTypePedantic_InferredFK_PluralizationESStems(t *testing.T) {
	// address_id → addresses, process_id → processes, bus_id → buses.
	// The old code missed all of these because base ends in 's'.
	cases := []struct {
		name   string
		target string
	}{
		{"address", "addresses"},
		{"process", "processes"},
		{"bus", "buses"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tables := parseTables(t,
				fmt.Sprintf(`CREATE TABLE %s (id BIGINT UNSIGNED PRIMARY KEY)`, tc.target),
				fmt.Sprintf(`CREATE TABLE owner (id BIGINT UNSIGNED PRIMARY KEY, %s_id INT NOT NULL)`, tc.name),
			)
			violations := filterRule(newTypePedantic(t).Lint(tables, nil), "inferred_fk")
			require.Len(t, violations, 1)
			require.Equal(t, tc.target, violations[0].Context["referenced_table"])
		})
	}
}

func TestTypePedantic_InferredFK_IgnoreColumnsAppliesToFKRule(t *testing.T) {
	// ignoreColumns scopes to both rules now.
	tables := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT NOT NULL)`,
	)
	l := &TypePedanticLinter{}
	require.NoError(t, l.Configure(map[string]string{
		"ignoreColumns": "id,customer_id",
	}))
	require.Empty(t, l.Lint(tables, nil))
}

func TestTypePedantic_PostState_AlterAddColumnMismatch(t *testing.T) {
	// Existing: customers (id BIGINT UNSIGNED PK)
	// Change:   ALTER TABLE orders ADD COLUMN customer_id INT
	// Expect:   inferred_fk violation against customers.id
	existing := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY)`,
	)
	stmts, err := statement.New(`ALTER TABLE orders ADD COLUMN customer_id INT NOT NULL`)
	require.NoError(t, err)
	violations := filterRule(newTypePedantic(t).Lint(existing, stmts), "inferred_fk")
	require.Len(t, violations, 1)
	require.Equal(t, "orders", violations[0].Location.Table)
	require.Equal(t, "customer_id", *violations[0].Location.Column)
	require.Equal(t, "customers", violations[0].Context["referenced_table"])
}

func TestTypePedantic_PostState_AlterModifyColumnFixesViolation(t *testing.T) {
	// Existing schema has a violation; the proposed ALTER fixes it.
	existing := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT NOT NULL)`,
	)
	stmts, err := statement.New(`ALTER TABLE orders MODIFY COLUMN customer_id BIGINT UNSIGNED NOT NULL`)
	require.NoError(t, err)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(existing, stmts), "inferred_fk"))
}

func TestTypePedantic_PostState_AlterAddIndexEnablesSameNameCheck(t *testing.T) {
	// Without the index the same-name group is unindexed → silently skipped.
	// The ALTER adds an index → group becomes indexed → flagged.
	existing := parseTables(t,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, ext_id BIGINT UNSIGNED NOT NULL)`,
		`CREATE TABLE invoices (id BIGINT UNSIGNED PRIMARY KEY, ext_id BIGINT UNSIGNED NOT NULL)`,
		`CREATE TABLE returns (id BIGINT UNSIGNED PRIMARY KEY, ext_id INT NOT NULL)`,
	)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(existing, nil), "same_name"),
		"baseline: unindexed group is silent")

	stmts, err := statement.New(`ALTER TABLE orders ADD INDEX idx_ext (ext_id)`)
	require.NoError(t, err)
	violations := filterRule(newTypePedantic(t).Lint(existing, stmts), "same_name")
	require.Len(t, violations, 1)
	require.Equal(t, "returns", violations[0].Location.Table)
}

func TestTypePedantic_PostState_AlterDropColumnRemovesViolation(t *testing.T) {
	existing := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id INT NOT NULL)`,
	)
	stmts, err := statement.New(`ALTER TABLE orders DROP COLUMN customer_id`)
	require.NoError(t, err)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(existing, stmts), "inferred_fk"))
}

func TestTypePedantic_PostState_AlterChangeColumnRenameAndRetype(t *testing.T) {
	// CHANGE COLUMN renames and retypes in one shot.
	existing := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, cust_id BIGINT UNSIGNED NOT NULL)`,
	)
	stmts, err := statement.New(`ALTER TABLE orders CHANGE COLUMN cust_id customer_id INT NOT NULL`)
	require.NoError(t, err)
	violations := filterRule(newTypePedantic(t).Lint(existing, stmts), "inferred_fk")
	require.Len(t, violations, 1)
	require.Equal(t, "customer_id", *violations[0].Location.Column)
}

func TestTypePedantic_PostState_DropPrimaryKeyClearsInlineFlag(t *testing.T) {
	// Bug from PR #836 review: DROP PRIMARY KEY was only scrubbing the
	// Indexes slice, but inline `id BIGINT PRIMARY KEY` produces no entry
	// there — GetIndexes() synthesizes it from col.PrimaryKey. Without
	// clearing the flag, the column kept counting as indexed in post-state.
	existing := parseTables(t,
		`CREATE TABLE customers (customer_key BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (customer_key BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE returns (customer_key INT PRIMARY KEY)`,
	)
	// Baseline: requireIndexed sees inline PKs → mismatch on returns is flagged.
	require.Len(t, filterRule(newTypePedantic(t).Lint(existing, nil), "same_name"), 1)

	// After dropping the PK on every table, none of the customer_key columns
	// are indexed anymore — the same-name group should be silently skipped.
	stmts, err := statement.New(`
		ALTER TABLE customers DROP PRIMARY KEY;
		ALTER TABLE orders DROP PRIMARY KEY;
		ALTER TABLE returns DROP PRIMARY KEY;
	`)
	require.NoError(t, err)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(existing, stmts), "same_name"),
		"DROP PRIMARY KEY should have cleared the inline PK flag")
}

func TestTypePedantic_PostState_DropIndexClearsInlineUnique(t *testing.T) {
	// Parallel to the DROP PRIMARY KEY fix: an inline `col TYPE UNIQUE`
	// produces an implicit index named after the column. DROP INDEX <col>
	// must clear the inline Unique flag too, otherwise GetIndexes() keeps
	// synthesizing the implicit index in post-state.
	existing := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255) UNIQUE)`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255) UNIQUE)`,
		`CREATE TABLE c (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(500) UNIQUE)`,
	)
	require.Len(t, filterRule(newTypePedantic(t).Lint(existing, nil), "same_name"), 1,
		"baseline: inline UNIQUE makes email indexed in all three tables")

	stmts, err := statement.New(`
		ALTER TABLE a DROP INDEX email;
		ALTER TABLE b DROP INDEX email;
		ALTER TABLE c DROP INDEX email;
	`)
	require.NoError(t, err)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(existing, stmts), "same_name"),
		"DROP INDEX <colname> should clear the inline UNIQUE flag")
}

func TestTypePedantic_PostState_ModifyColumnPreservesPrimaryKey(t *testing.T) {
	// MODIFY COLUMN retypes the column but doesn't drop the PRIMARY KEY.
	// In our model the inline PK lives on col.PrimaryKey; replacing the
	// column with a fresh parse from the spec would drop the flag unless
	// we explicitly preserve it.
	existing := parseTables(t,
		`CREATE TABLE customers (id BIGINT UNSIGNED PRIMARY KEY)`,
		`CREATE TABLE orders (id INT PRIMARY KEY)`,
	)
	// Baseline: orders.id (INT) mismatches customers.id (BIGINT UNSIGNED) on
	// the inferred-FK rule via order_id-style lookup wouldn't apply here, but
	// the same-name rule on `id` is silenced by default ignoreColumns. So we
	// look at the rule via a follow-up MODIFY: after retyping orders.id to
	// BIGINT UNSIGNED, it should still register as indexed (PK preserved).
	stmts, err := statement.New(`ALTER TABLE orders MODIFY COLUMN id BIGINT UNSIGNED NOT NULL`)
	require.NoError(t, err)

	// Configure ignoreColumns="" so `id` is not skipped, letting us observe
	// that the post-state PK is preserved.
	l := &TypePedanticLinter{}
	require.NoError(t, l.Configure(map[string]string{"ignoreColumns": ""}))
	violations := filterRule(l.Lint(existing, stmts), "same_name")
	require.Empty(t, violations, "post-MODIFY orders.id should match customers.id and still be indexed")
}

func TestTypePedantic_PostState_ChangeColumnPreservesUnique(t *testing.T) {
	// CHANGE COLUMN renames + retypes; inline UNIQUE on the old column
	// should carry over so the post-state still considers it indexed.
	existing := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255) UNIQUE)`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255) UNIQUE)`,
		`CREATE TABLE c (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255) UNIQUE)`,
	)
	// CHANGE COLUMN renames email → contact_email and bumps the length.
	stmts, err := statement.New(`ALTER TABLE c CHANGE COLUMN email contact_email VARCHAR(500) NOT NULL`)
	require.NoError(t, err)

	// `contact_email` only exists in c, so no same-name group there.
	// `email` exists in a and b only after the CHANGE — they still agree.
	require.Empty(t, filterRule(newTypePedantic(t).Lint(existing, stmts), "same_name"))

	// Now make c's renamed column mismatch a/b on a still-shared name:
	stmts, err = statement.New(`ALTER TABLE c MODIFY COLUMN email VARCHAR(1000) NOT NULL`)
	require.NoError(t, err)
	violations := filterRule(newTypePedantic(t).Lint(existing, stmts), "same_name")
	require.Len(t, violations, 1, "post-MODIFY c.email should still be indexed via preserved UNIQUE flag")
	require.Equal(t, "c", violations[0].Location.Table)
}

func TestTypePedantic_Configure_PartialDoesNotResetOtherFields(t *testing.T) {
	// Calling Configure twice with disjoint keys should compose: the second
	// call shouldn't leave state from a missing key undefined.
	l := &TypePedanticLinter{}
	require.NoError(t, l.Configure(map[string]string{"checkSameName": "false"}))
	require.False(t, l.checkSameName)
	require.True(t, l.checkInferredFK, "checkInferredFK should default to true")
	require.True(t, l.requireIndexed, "requireIndexed should default to true")

	// Subsequent partial call resets to defaults and applies — checkSameName
	// returns to true.
	require.NoError(t, l.Configure(map[string]string{"requireIndexed": "false"}))
	require.True(t, l.checkSameName)
	require.False(t, l.requireIndexed)
}

func TestTypePedantic_SameName_CollationMismatchAgainstImpliedDefault(t *testing.T) {
	// The motivating case: SHOW CREATE TABLE omits COLLATE when it is the
	// charset default, so `DEFAULT CHARSET=utf8mb4` (utf8mb4_0900_ai_ci) has to
	// compare unequal to an explicit utf8mb4_general_ci. Types match exactly,
	// so only the collation rule fires.
	tables := parseTables(t,
		`CREATE TABLE users (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255), KEY k (email)) DEFAULT CHARSET=utf8mb4`,
		`CREATE TABLE profiles (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255), KEY k (email)) DEFAULT CHARSET=utf8mb4`,
		`CREATE TABLE legacy (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255), KEY k (email)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci`,
	)
	violations := newTypePedantic(t).Lint(tables, nil)
	require.Empty(t, filterRule(violations, "same_name"), "types are identical")

	flagged := filterRule(violations, "same_name_collation")
	require.Len(t, flagged, 1)
	v := flagged[0]
	require.Equal(t, SeverityWarning, v.Severity)
	require.Equal(t, "legacy", v.Location.Table)
	require.Equal(t, "email", *v.Location.Column)
	require.Equal(t, "utf8mb4_general_ci", v.Context["current_collation"])
	require.Equal(t, "utf8mb4_0900_ai_ci", v.Context["expected_collation"])
	require.Equal(t, false, v.Context["charset_differs"])
	require.Contains(t, v.Message, "ERROR 1267", "same charset, different collation is a hard error, not just slow")
	require.NotContains(t, v.Message, "different charsets", "the charsets match here")
}

func TestTypePedantic_SameName_CharsetMismatchWording(t *testing.T) {
	// Different charsets: the consequence is an implicit conversion that
	// prevents index use, not ERROR 1267.
	tables := parseTables(t,
		`CREATE TABLE users (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255), KEY k (email)) DEFAULT CHARSET=utf8mb4`,
		`CREATE TABLE profiles (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255), KEY k (email)) DEFAULT CHARSET=utf8mb4`,
		`CREATE TABLE legacy (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255), KEY k (email)) DEFAULT CHARSET=latin1`,
	)
	flagged := filterRule(newTypePedantic(t).Lint(tables, nil), "same_name_collation")
	require.Len(t, flagged, 1)
	v := flagged[0]
	require.Equal(t, "legacy", v.Location.Table)
	require.Equal(t, "latin1", v.Context["current_charset"])
	require.Equal(t, "utf8mb4", v.Context["expected_charset"])
	require.Equal(t, true, v.Context["charset_differs"])
	require.Contains(t, v.Message, "prevents index use")
	require.NotNil(t, v.Suggestion)
	require.Contains(t, *v.Suggestion, "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
}

func TestTypePedantic_SameName_CollationUndeclaredTablesAgree(t *testing.T) {
	// No DEFAULT CHARSET anywhere: both tables take the assumed default, so
	// they agree and nothing is reported.
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255), KEY k (email))`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255), KEY k (email))`,
	)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(tables, nil), "same_name_collation"))
}

func TestTypePedantic_SameName_CollationUndeclaredTableUsesAssumedCharset(t *testing.T) {
	// Regression: hand-written DDL that omits DEFAULT CHARSET used to drop out
	// of the vote, which made *deleting* a charset line silence the warning
	// the schema had before — the group was left with a single determined
	// column and nothing to disagree about.
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255), KEY k (email)) DEFAULT CHARSET=latin1`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255), KEY k (email))`,
	)
	flagged := filterRule(newTypePedantic(t).Lint(tables, nil), "same_name_collation")
	require.Len(t, flagged, 2, "1-vs-1 is a tie, so both sides are reported")
	for _, v := range flagged {
		require.Equal(t,
			[]string{"latin1_swedish_ci", "utf8mb4_0900_ai_ci"},
			v.Context["conflicting_collations"],
			"the undeclared table should compare as utf8mb4",
		)
	}
}

func TestTypePedantic_SameName_CollationAssumeCharsetConfigurable(t *testing.T) {
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255), KEY k (email)) DEFAULT CHARSET=latin1`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255), KEY k (email))`,
	)

	// A schema whose real default is latin1: the undeclared table now agrees
	// with the declared one.
	l := &TypePedanticLinter{}
	require.NoError(t, l.Configure(map[string]string{"assumeCharset": "latin1"}))
	require.Empty(t, filterRule(l.Lint(tables, nil), "same_name_collation"))

	// Empty restores the strict mode: compare only what the DDL states.
	strict := &TypePedanticLinter{}
	require.NoError(t, strict.Configure(map[string]string{"assumeCharset": ""}))
	require.Empty(t, filterRule(strict.Lint(tables, nil), "same_name_collation"))

	// ...and in strict mode two undeclared tables are skipped rather than
	// being assumed to match.
	undeclared := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255) COLLATE utf8mb4_bin, KEY k (email))`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255), KEY k (email))`,
	)
	require.Empty(t, filterRule(strict.Lint(undeclared, nil), "same_name_collation"))
	require.Len(t, filterRule(newTypePedantic(t).Lint(undeclared, nil), "same_name_collation"), 2,
		"with the default assumption the explicit utf8mb4_bin column disagrees")
}

func TestTypePedantic_Configure_RejectsUnknownAssumeCharset(t *testing.T) {
	l := &TypePedanticLinter{}
	err := l.Configure(map[string]string{"assumeCharset": "utf8mb5"})
	require.ErrorContains(t, err, "assumeCharset")
	require.ErrorContains(t, err, "not a known character set")
}

func TestTypePedantic_InferredFK_CollationUndeclaredTargetUsesAssumedCharset(t *testing.T) {
	// Same regression on the FK rule: the target table declares nothing, the
	// referencing table declares a non-default collation.
	tables := parseTables(t,
		`CREATE TABLE customers (id VARCHAR(64) NOT NULL PRIMARY KEY)`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id VARCHAR(64) NOT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci`,
	)
	flagged := filterRule(newTypePedantic(t).Lint(tables, nil), "inferred_fk_collation")
	require.Len(t, flagged, 1)
	require.Equal(t, "utf8mb4_0900_ai_ci", flagged[0].Context["expected_collation"])
}

func TestTypePedantic_SameName_CollationIgnoresNonTextColumns(t *testing.T) {
	// A table charset says nothing about how integers compare.
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, qty INT, KEY k (qty)) DEFAULT CHARSET=utf8mb4`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, qty INT, KEY k (qty)) DEFAULT CHARSET=latin1`,
	)
	require.Empty(t, newTypePedantic(t).Lint(tables, nil))
}

func TestTypePedantic_SameName_CollationColumnLevelOverrideConverges(t *testing.T) {
	// Column-level COLLATE wins over the table default on both sides, and both
	// resolve to the same collation — nothing to report even though the table
	// options differ.
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255) COLLATE utf8mb4_0900_ai_ci, KEY k (email)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255), KEY k (email)) DEFAULT CHARSET=utf8mb4`,
	)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(tables, nil), "same_name_collation"))
}

func TestTypePedantic_SameName_CollationUTF8MB3Alias(t *testing.T) {
	// utf8 and utf8mb3 are the same charset spelled two ways, and utf8_bin is
	// the same collation as utf8mb3_bin.
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255), KEY k (email)) DEFAULT CHARSET=utf8`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255), KEY k (email)) DEFAULT CHARSET=utf8mb3`,
		`CREATE TABLE c (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255) COLLATE utf8_general_ci, KEY k (email)) DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_bin`,
	)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(tables, nil), "same_name_collation"))
}

func TestTypePedantic_SameName_CollationRespectsRequireIndexed(t *testing.T) {
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, note VARCHAR(255)) DEFAULT CHARSET=utf8mb4`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, note VARCHAR(255)) DEFAULT CHARSET=latin1`,
	)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(tables, nil), "same_name_collation"))

	l := &TypePedanticLinter{}
	require.NoError(t, l.Configure(map[string]string{"requireIndexed": "false"}))
	// 1-vs-1 is a tie, so both occurrences are reported.
	require.Len(t, filterRule(l.Lint(tables, nil), "same_name_collation"), 2)
}

func TestTypePedantic_SameName_CollationTieEmitsForAll(t *testing.T) {
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255), KEY k (email)) DEFAULT CHARSET=utf8mb4`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255), KEY k (email)) DEFAULT CHARSET=latin1`,
	)
	flagged := filterRule(newTypePedantic(t).Lint(tables, nil), "same_name_collation")
	require.Len(t, flagged, 2)
	for _, v := range flagged {
		require.Contains(t, v.Message, "inconsistent across schema")
		require.Contains(t, v.Message, "different charsets",
			"a tie is the most common mixed-charset shape; it needs the consequence too")
		require.Contains(t, v.Message, "ERROR 1267")
		require.Equal(t,
			[]string{"latin1_swedish_ci", "utf8mb4_0900_ai_ci"},
			v.Context["conflicting_collations"],
		)
	}
}

func TestTypePedantic_SameName_CollationTieSameCharsetConsequence(t *testing.T) {
	// Tied, but both sides are utf8mb4: only the ERROR 1267 outcome applies.
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255), KEY k (email)) DEFAULT CHARSET=utf8mb4`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, email VARCHAR(255), KEY k (email)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`,
	)
	flagged := filterRule(newTypePedantic(t).Lint(tables, nil), "same_name_collation")
	require.Len(t, flagged, 2)
	for _, v := range flagged {
		require.Contains(t, v.Message, "ERROR 1267")
		require.NotContains(t, v.Message, "different charsets")
	}
}

func TestTypePedantic_SameName_NonConvertibleCharsetsAreNotUnderstated(t *testing.T) {
	// latin1 and latin2: neither is a superset of the other, so MySQL has
	// nothing to convert to and the join fails with ERROR 1267 rather than
	// merely losing an index. Verified on 8.0.46. The wording has to name
	// that outcome, not just the index-degradation one.
	tables := parseTables(t,
		`CREATE TABLE a (id BIGINT UNSIGNED PRIMARY KEY, code VARCHAR(64), KEY k (code)) DEFAULT CHARSET=latin1`,
		`CREATE TABLE b (id BIGINT UNSIGNED PRIMARY KEY, code VARCHAR(64), KEY k (code)) DEFAULT CHARSET=latin2`,
		`CREATE TABLE c (id BIGINT UNSIGNED PRIMARY KEY, code VARCHAR(64), KEY k (code)) DEFAULT CHARSET=latin2`,
	)
	flagged := filterRule(newTypePedantic(t).Lint(tables, nil), "same_name_collation")
	require.Len(t, flagged, 1, "latin1 is the minority")
	v := flagged[0]
	require.Equal(t, "a", v.Location.Table)
	require.Equal(t, true, v.Context["charset_differs"])
	require.Contains(t, v.Message, "ERROR 1267",
		"latin1/latin2 do not convert, so this is a hard failure — not a performance nit")
	require.Contains(t, v.Message, "prevents index use on the narrower side")
}

func TestTypePedantic_InferredFK_CollationMismatch(t *testing.T) {
	// Identical types, so only the collation rule fires — the case the type
	// comparison alone cannot see.
	tables := parseTables(t,
		`CREATE TABLE customers (id VARCHAR(64) NOT NULL PRIMARY KEY) DEFAULT CHARSET=utf8mb4`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id VARCHAR(64) NOT NULL, KEY k (customer_id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci`,
	)
	violations := newTypePedantic(t).Lint(tables, nil)
	require.Empty(t, filterRule(violations, "inferred_fk"), "types are identical")

	flagged := filterRule(violations, "inferred_fk_collation")
	require.Len(t, flagged, 1)
	v := flagged[0]
	require.Equal(t, SeverityWarning, v.Severity)
	require.Equal(t, "orders", v.Location.Table)
	require.Equal(t, "customer_id", *v.Location.Column)
	require.Equal(t, "customers", v.Context["referenced_table"])
	require.Equal(t, "utf8mb4_general_ci", v.Context["current_collation"])
	require.Equal(t, "utf8mb4_0900_ai_ci", v.Context["expected_collation"])
	require.Contains(t, v.Message, "ERROR 1267")
}

func TestTypePedantic_InferredFK_CollationAndTypeBothFlagged(t *testing.T) {
	// A column can be wrong on both axes. Each violation stays precise about
	// its own axis rather than being folded into one message.
	tables := parseTables(t,
		`CREATE TABLE customers (id VARCHAR(64) NOT NULL PRIMARY KEY) DEFAULT CHARSET=utf8mb4`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id VARCHAR(32) NOT NULL) DEFAULT CHARSET=latin1`,
	)
	violations := newTypePedantic(t).Lint(tables, nil)
	require.Len(t, filterRule(violations, "inferred_fk"), 1)
	collation := filterRule(violations, "inferred_fk_collation")
	require.Len(t, collation, 1)
	require.Contains(t, collation[0].Message, "prevents index use")
}

func TestTypePedantic_InferredFK_CollationMatch(t *testing.T) {
	tables := parseTables(t,
		`CREATE TABLE customers (id VARCHAR(64) NOT NULL PRIMARY KEY) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id VARCHAR(64) NOT NULL COLLATE utf8mb4_general_ci) DEFAULT CHARSET=utf8mb4`,
	)
	require.Empty(t, newTypePedantic(t).Lint(tables, nil))
}

func TestTypePedantic_Collation_DisabledViaConfig(t *testing.T) {
	tables := parseTables(t,
		`CREATE TABLE customers (id VARCHAR(64) NOT NULL PRIMARY KEY) DEFAULT CHARSET=utf8mb4`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id VARCHAR(64) NOT NULL, KEY k (customer_id)) DEFAULT CHARSET=latin1`,
	)
	l := &TypePedanticLinter{}
	require.NoError(t, l.Configure(map[string]string{"checkCollation": "false"}))
	require.Empty(t, l.Lint(tables, nil))
}

func TestTypePedantic_Collation_ConfigurableSeverity(t *testing.T) {
	tables := parseTables(t,
		`CREATE TABLE customers (id VARCHAR(64) NOT NULL PRIMARY KEY) DEFAULT CHARSET=utf8mb4`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id VARCHAR(64) NOT NULL, KEY k (customer_id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci`,
	)
	l := &TypePedanticLinter{}
	require.NoError(t, l.Configure(map[string]string{"collationSeverity": "error"}))
	flagged := filterRule(l.Lint(tables, nil), "inferred_fk_collation")
	require.Len(t, flagged, 1)
	require.Equal(t, SeverityError, flagged[0].Severity)
}

func TestTypePedantic_Collation_PostStateAlterConvergesCollation(t *testing.T) {
	existing := parseTables(t,
		`CREATE TABLE customers (id VARCHAR(64) NOT NULL PRIMARY KEY) DEFAULT CHARSET=utf8mb4`,
		`CREATE TABLE orders (id BIGINT UNSIGNED PRIMARY KEY, customer_id VARCHAR(64) NOT NULL, KEY k (customer_id)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci`,
	)
	require.Len(t, filterRule(newTypePedantic(t).Lint(existing, nil), "inferred_fk_collation"), 1)

	changes, err := statement.New(`ALTER TABLE orders MODIFY COLUMN customer_id VARCHAR(64) NOT NULL COLLATE utf8mb4_0900_ai_ci`)
	require.NoError(t, err)
	require.Empty(t, filterRule(newTypePedantic(t).Lint(existing, changes), "inferred_fk_collation"))
}

func TestTypePedantic_RegisteredAndDescribed(t *testing.T) {
	l, err := Get("type_pedantic")
	require.NoError(t, err)
	require.NotEmpty(t, l.Description())
	require.Contains(t, l.String(), "type_pedantic")
}

// filterRule returns violations whose Context["rule"] equals the given rule name.
func filterRule(vs []Violation, rule string) []Violation {
	var out []Violation
	for _, v := range vs {
		if v.Context != nil && v.Context["rule"] == rule {
			out = append(out, v)
		}
	}
	return out
}

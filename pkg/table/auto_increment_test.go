package table

import (
	"slices"
	"strings"
	"testing"

	"github.com/block/spirit/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStripAutoIncrement(t *testing.T) {
	tests := []struct {
		name string
		stmt string
		want string
	}{
		{
			name: "counter between other table options",
			stmt: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT=500 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			want: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		},
		{
			name: "counter as the last table option",
			stmt: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci AUTO_INCREMENT=42",
			want: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		},
		{
			name: "no counter is returned byte for byte",
			stmt: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			want: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		},
		{
			name: "keyword inside a column default, comment and identifier survives the counter removal",
			stmt: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  `note` varchar(64) DEFAULT 'AUTO_INCREMENT=123',\n" +
				"  `x` int DEFAULT NULL COMMENT 'reset AUTO_INCREMENT 1000 on rollover',\n" +
				"  `auto_increment_2024` int DEFAULT NULL,\n" +
				"  `AUTO_INCREMENT=789` int DEFAULT NULL,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT=500 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='keep auto_increment=321'",
			want: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  `note` varchar(64) DEFAULT 'AUTO_INCREMENT=123',\n" +
				"  `x` int DEFAULT NULL COMMENT 'reset AUTO_INCREMENT 1000 on rollover',\n" +
				"  `auto_increment_2024` int DEFAULT NULL,\n" +
				"  `AUTO_INCREMENT=789` int DEFAULT NULL,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='keep auto_increment=321'",
		},
		{
			name: "single-line statement whose column default and table comment both spell the counter",
			stmt: "CREATE TABLE `counter` (`id` bigint AUTO_INCREMENT PRIMARY KEY, `value` varchar(255) DEFAULT 'AUTO_INCREMENT=42') ENGINE=InnoDB AUTO_INCREMENT=123 COMMENT='keep AUTO_INCREMENT=999'",
			want: "CREATE TABLE `counter` (`id` bigint AUTO_INCREMENT PRIMARY KEY, `value` varchar(255) DEFAULT 'AUTO_INCREMENT=42') ENGINE=InnoDB COMMENT='keep AUTO_INCREMENT=999'",
		},
		{
			name: "keyword inside literals with no counter present is untouched",
			stmt: "CREATE TABLE `orders` (\n" +
				"  `note` varchar(64) DEFAULT 'AUTO_INCREMENT=123',\n" +
				"  `x` int DEFAULT NULL COMMENT 'reset AUTO_INCREMENT 1000 on rollover'\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			want: "CREATE TABLE `orders` (\n" +
				"  `note` varchar(64) DEFAULT 'AUTO_INCREMENT=123',\n" +
				"  `x` int DEFAULT NULL COMMENT 'reset AUTO_INCREMENT 1000 on rollover'\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		},
		{
			name: "lowercase counter written without an equals sign",
			stmt: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB auto_increment 77 DEFAULT CHARSET=utf8mb4",
			want: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		},
		{
			name: "spacing around the equals sign goes with the counter",
			stmt: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT   =   018 DEFAULT CHARSET=utf8mb4",
			want: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		},
		{
			name: "a FORCE prefix is part of the counter and goes with it",
			stmt: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB FORCE AUTO_INCREMENT = 100 DEFAULT CHARSET=utf8mb4",
			want: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		},
		{
			name: "a comment between the equals sign and the value goes with the counter",
			stmt: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT=/* reset me */ 9 DEFAULT CHARSET=utf8mb4",
			want: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		},
		{
			name: "table comment spelling the counter is skipped to reach the real one",
			stmt: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB COMMENT='rolls over at auto_increment=999' AUTO_INCREMENT=500 DEFAULT CHARSET=utf8mb4",
			want: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB COMMENT='rolls over at auto_increment=999' DEFAULT CHARSET=utf8mb4",
		},
		{
			name: "table name spelling the counter is skipped to reach the real one",
			stmt: "CREATE TABLE `AUTO_INCREMENT=1` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT=500 DEFAULT CHARSET=utf8mb4",
			want: "CREATE TABLE `AUTO_INCREMENT=1` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		},
		{
			name: "schema-file form with a trailing semicolon and newline",
			stmt: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT=500 DEFAULT CHARSET=utf8mb4;\n",
			want: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n",
		},
		{
			name: "two dashes in an expression are operators, not a comment",
			stmt: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  `a` int NOT NULL,\n" +
				"  `b` int NOT NULL,\n" +
				"  `net` int GENERATED ALWAYS AS ((`a`--`b`)) VIRTUAL,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT=500 DEFAULT CHARSET=utf8mb4",
			want: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  `a` int NOT NULL,\n" +
				"  `b` int NOT NULL,\n" +
				"  `net` int GENERATED ALWAYS AS ((`a`--`b`)) VIRTUAL,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		},
		{
			name: "a real line comment before the counter is still skipped",
			stmt: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB -- reset AUTO_INCREMENT=9 before reload\n" +
				"DEFAULT CHARSET=utf8mb4 AUTO_INCREMENT=500 COLLATE=utf8mb4_0900_ai_ci",
			want: "CREATE TABLE `orders` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB -- reset AUTO_INCREMENT=9 before reload\n" +
				"DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		},
		{
			name: "counter on a partitioned table",
			stmt: "CREATE TABLE `events` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB AUTO_INCREMENT=900 DEFAULT CHARSET=utf8mb4\n" +
				"/*!50100 PARTITION BY RANGE (`id`)\n" +
				"(PARTITION p0 VALUES LESS THAN (100) ENGINE = InnoDB,\n" +
				" PARTITION p1 VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */",
			want: "CREATE TABLE `events` (\n" +
				"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
				"/*!50100 PARTITION BY RANGE (`id`)\n" +
				"(PARTITION p0 VALUES LESS THAN (100) ENGINE = InnoDB,\n" +
				" PARTITION p1 VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := StripAutoIncrement(tt.stmt)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)

			// Stripping an already-stripped statement is a no-op, so a schema
			// read twice produces the same DDL both times.
			again, err := StripAutoIncrement(got)
			require.NoError(t, err)
			assert.Equal(t, tt.want, again)
		})
	}
}

// The column-level attribute is what makes ids generate, so it must survive
// even when the table-level counter next to it is removed.
func TestStripAutoIncrementKeepsColumnAttribute(t *testing.T) {
	stmt := "CREATE TABLE `orders` (\n" +
		"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB AUTO_INCREMENT=500 DEFAULT CHARSET=utf8mb4"

	got, err := StripAutoIncrement(stmt)
	require.NoError(t, err)
	assert.Contains(t, got, "`id` bigint unsigned NOT NULL AUTO_INCREMENT,")
	assert.NotContains(t, got, "AUTO_INCREMENT=500")
}

// The corpus case with a decoy counter inside a table COMMENT only proves
// something if the decoy is genuinely in the way. Pin that: the decoy appears
// earlier in the text than the real counter, and the parser's span still starts
// past it, so the right answer comes from the span rather than from a search
// that happened to start in the right place.
func TestStripAutoIncrementUsesTheParserSpanPastADecoy(t *testing.T) {
	stmt := "CREATE TABLE `orders` (\n" +
		"  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB COMMENT='rolls over at auto_increment=999' AUTO_INCREMENT=500 DEFAULT CHARSET=utf8mb4"

	create, err := parseCreateTable(stmt)
	require.NoError(t, err)
	i := slices.IndexFunc(create.Options, isAutoIncrementOption)
	require.GreaterOrEqual(t, i, 0)
	start, end, ok := counterSpan(stmt, create.Options[i])
	require.True(t, ok)

	decoy := strings.Index(strings.ToLower(stmt), "auto_increment=999")
	require.Positive(t, decoy, "the decoy must be present")
	require.Greater(t, start, decoy, "the parser must point past the decoy, not at the first match")
	require.Equal(t, "AUTO_INCREMENT=500", stmt[start:end], "the span must cover the counter and stop there")

	got, err := StripAutoIncrement(stmt)
	require.NoError(t, err)
	assert.Contains(t, got, "COMMENT='rolls over at auto_increment=999'")
	assert.NotContains(t, got, "AUTO_INCREMENT=500")
}

func TestStripAutoIncrementRejectsUnparseableStatements(t *testing.T) {
	_, err := StripAutoIncrement("CREATE TABLE `orders` (")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse CREATE TABLE to strip its auto-increment counter")
}

func TestStripAutoIncrementRejectsNonCreateTableStatements(t *testing.T) {
	_, err := StripAutoIncrement("ALTER TABLE `orders` AUTO_INCREMENT=500")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a CREATE TABLE")
}

// counterSpan says so rather than slicing out of range when the span and the
// text disagree. Nothing in this package can make them disagree, so the case is
// built by hand: an option parsed from one statement, measured against another.
func TestCounterSpanRejectsAnOptionFromAnotherStatement(t *testing.T) {
	create, err := parseCreateTable("CREATE TABLE `orders` (`id` bigint) ENGINE=InnoDB AUTO_INCREMENT=500")
	require.NoError(t, err)
	i := slices.IndexFunc(create.Options, isAutoIncrementOption)
	require.GreaterOrEqual(t, i, 0)

	for _, other := range []string{
		"",
		"CREATE TABLE `orders` (`id` bigint)",
		"CREATE TABLE `orders` (`id` bigint) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
	} {
		_, _, ok := counterSpan(other, create.Options[i])
		assert.False(t, ok, "span from another statement must not be trusted against %q", other)
	}
}

func TestNextAutoIncrementFromCreateTable(t *testing.T) {
	tests := []struct {
		name      string
		stmt      string
		next      uint64
		hasColumn bool
	}{
		{
			name:      "counter present",
			stmt:      "CREATE TABLE `t` (`id` int NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB AUTO_INCREMENT=42 DEFAULT CHARSET=utf8mb4",
			next:      42,
			hasColumn: true,
		},
		{
			// MySQL omits the option until the first id has been generated.
			name:      "auto_increment column without counter",
			stmt:      "CREATE TABLE `t` (`id` int NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB",
			next:      1,
			hasColumn: true,
		},
		{
			name:      "auto_increment column that is not the primary key",
			stmt:      "CREATE TABLE `t` (`pk` varchar(10) NOT NULL, `seq` bigint unsigned NOT NULL AUTO_INCREMENT, PRIMARY KEY (`pk`), KEY `seq` (`seq`)) ENGINE=InnoDB AUTO_INCREMENT=7",
			next:      7,
			hasColumn: true,
		},
		{
			name: "no auto_increment column",
			stmt: "CREATE TABLE `t` (`id` int NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB",
		},
		{
			name: "keyword only in a comment",
			stmt: "CREATE TABLE `t` (`id` int NOT NULL COMMENT 'AUTO_INCREMENT', PRIMARY KEY (`id`)) ENGINE=InnoDB COMMENT='AUTO_INCREMENT=9'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			next, hasColumn, err := nextAutoIncrementFromCreateTable(tt.stmt)
			require.NoError(t, err)
			assert.Equal(t, tt.hasColumn, hasColumn)
			assert.Equal(t, tt.next, next)
		})
	}
	_, _, err := nextAutoIncrementFromCreateTable("not sql")
	require.Error(t, err)
}

// TestNextAutoIncrementIsLive shows why NextAutoIncrement reads SHOW CREATE
// TABLE: once anything has read the table's statistics,
// information_schema.TABLES.AUTO_INCREMENT keeps returning that value for
// information_schema_stats_expiry seconds, however far the counter moves.
func TestNextAutoIncrementIsLive(t *testing.T) {
	dbName, db := testutils.CreateUniqueTestDatabase(t)
	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE autoinc_live (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, v INT)`)
	testutils.RunSQLInDatabase(t, dbName, `INSERT INTO autoinc_live (v) VALUES (1), (2), (3)`)

	cached := func() uint64 {
		var n uint64
		require.NoError(t, db.QueryRowContext(t.Context(),
			"SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'autoinc_live'",
			dbName).Scan(&n))
		return n
	}
	require.Equal(t, uint64(4), cached()) // populates the statistics cache

	testutils.RunSQLInDatabase(t, dbName, `INSERT INTO autoinc_live (v) VALUES (4), (5), (6), (7)`)
	testutils.RunSQLInDatabase(t, dbName, `DELETE FROM autoinc_live WHERE id > 3`)

	var expiry int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT @@information_schema_stats_expiry").Scan(&expiry))
	if expiry > 0 {
		require.Equal(t, uint64(4), cached(), "information_schema is expected to serve the cached counter")
	}

	next, hasColumn, err := NextAutoIncrement(t.Context(), db, dbName, "autoinc_live")
	require.NoError(t, err)
	require.True(t, hasColumn)
	require.Equal(t, uint64(8), next)

	testutils.RunSQLInDatabase(t, dbName, `CREATE TABLE autoinc_none (id INT NOT NULL PRIMARY KEY)`)
	next, hasColumn, err = NextAutoIncrement(t.Context(), db, dbName, "autoinc_none")
	require.NoError(t, err)
	require.False(t, hasColumn)
	require.Zero(t, next)

	_, _, err = NextAutoIncrement(t.Context(), db, dbName, "autoinc_missing")
	require.Error(t, err)
}

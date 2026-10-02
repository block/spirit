package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestColumnReferenceCase: a column reference in a stored expression is
// respelled to the declared case of the column, in every expression MySQL
// stores one in, and a name that is not a column is left as written.
func TestColumnReferenceCase(t *testing.T) {
	ct, err := ParseCreateTable(`CREATE TABLE t (
		id INT NOT NULL,
		Col INT,
		g INT AS (COL + 1) STORED,
		h INT AS (col * 2) CHECK (H > 0),
		s VARCHAR(10) AS (concat(cOl, 'x')),
		KEY k ((CoL + 1), id),
		KEY k2 ((lower(S))),
		CONSTRAINT chk CHECK (COL > 0 AND Id > 0),
		PRIMARY KEY (id, Col)
	) PARTITION BY RANGE (COL) SUBPARTITION BY HASH (ID) SUBPARTITIONS 2 (PARTITION p0 VALUES LESS THAN (10), PARTITION p1 VALUES LESS THAN MAXVALUE)`)
	require.NoError(t, err)

	assert.Equal(t, "`Col`+1", *ct.Columns.ByName("g").GeneratedExpr)
	assert.Equal(t, "`Col`*2", *ct.Columns.ByName("h").GeneratedExpr)
	assert.Equal(t, "CONCAT(`Col`, 'x')", *ct.Columns.ByName("s").GeneratedExpr)
	assert.Equal(t, "`Col`+1", *ct.Indexes.ByName("k").ColumnList[0].Expression)
	assert.Equal(t, "id", ct.Indexes.ByName("k").ColumnList[1].Name)
	assert.Equal(t, "LOWER(`s`)", *ct.Indexes.ByName("k2").ColumnList[0].Expression)
	assert.Equal(t, "`Col`", *ct.Partition.Expression)
	assert.Equal(t, "`id`", *ct.Partition.SubPartition.Expression)

	checks := make(map[string]string)
	for _, c := range ct.Constraints {
		if c.Type == "CHECK" {
			checks[c.Name] = *c.Definition
		}
	}
	assert.Equal(t, "CHECK (`Col`>0 AND `id`>0)", checks["chk"])
	// The column-level CHECK is hoisted into a table-level constraint and
	// respelled too, whichever rule runs first.
	assert.Equal(t, "CHECK (`h`>0)", checks["t_chk_1"])
}

// TestColumnReferenceCaseLeavesUnknownNames: a reference that names no
// declared column is left as written.
func TestColumnReferenceCaseLeavesUnknownNames(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE t (id INT PRIMARY KEY, c INT, g INT AS (Missing + 1), CONSTRAINT chk CHECK (c > OTHER))")
	require.NoError(t, err)
	assert.Equal(t, "`Missing`+1", *ct.Columns.ByName("g").GeneratedExpr)
	assert.Equal(t, "CHECK (`c`>`OTHER`)", *ct.Constraints[0].Definition)
}

// TestColumnReferenceCaseConverges: the authored spelling and MySQL's reported
// one (as read back from SHOW CREATE TABLE on 8.0.43) are the same definition.
func TestColumnReferenceCaseConverges(t *testing.T) {
	tests := []struct {
		name     string
		authored string
		live     string
	}{
		{
			name:     "generated column",
			authored: "CREATE TABLE t (id int NOT NULL PRIMARY KEY, c int, g int AS (C + 1) STORED)",
			live:     "CREATE TABLE `t` (`id` int NOT NULL, `c` int DEFAULT NULL, `g` int GENERATED ALWAYS AS ((`c` + 1)) STORED, PRIMARY KEY (`id`))",
		},
		{
			name:     "functional index",
			authored: "CREATE TABLE t (id int NOT NULL PRIMARY KEY, c int, KEY k ((C + 1)))",
			live:     "CREATE TABLE `t` (`id` int NOT NULL, `c` int DEFAULT NULL, PRIMARY KEY (`id`), KEY `k` (((`c` + 1))))",
		},
		{
			name:     "check constraint",
			authored: "CREATE TABLE t (id int NOT NULL PRIMARY KEY, c int, CONSTRAINT t_chk_1 CHECK (C > 0))",
			live:     "CREATE TABLE `t` (`id` int NOT NULL, `c` int DEFAULT NULL, PRIMARY KEY (`id`), CONSTRAINT `t_chk_1` CHECK ((`c` > 0)))",
		},
		{
			name:     "partition expression",
			authored: "CREATE TABLE t (id int NOT NULL PRIMARY KEY) PARTITION BY HASH (ID) PARTITIONS 2",
			live:     "CREATE TABLE `t` (`id` int NOT NULL, PRIMARY KEY (`id`)) PARTITION BY HASH (`id`) PARTITIONS 2",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			authored, err := ParseCreateTable(tc.authored)
			require.NoError(t, err)
			live, err := ParseCreateTable(tc.live)
			require.NoError(t, err)
			stmts, err := live.Diff(authored, nil)
			require.NoError(t, err)
			assert.Empty(t, stmts, "live -> authored")
			stmts, err = authored.Diff(live, nil)
			require.NoError(t, err)
			assert.Empty(t, stmts, "authored -> live")
		})
	}
}

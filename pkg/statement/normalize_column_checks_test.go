package statement

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A column can carry several CHECKs, each with its own name and enforcement;
// every one is hoisted, in declaration order, and MySQL's numbering of the
// unnamed ones skips the named ones.
func TestColumnChecksHoistEveryCheck(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE t (id INT PRIMARY KEY, " +
		"c INT CONSTRAINT ck_c CHECK (c > 0) NOT ENFORCED CHECK (c < 10), d INT CHECK (d > 0))")
	require.NoError(t, err)
	for _, col := range ct.Columns {
		require.Empty(t, col.Checks, "column %s still carries its CHECKs", col.Name)
	}
	type want struct {
		name, definition string
		notEnforced      bool
	}
	wants := []want{
		{"ck_c", "CHECK (`c`>0) NOT ENFORCED", true},
		{"t_chk_1", "CHECK (`c`<10)", false},
		{"t_chk_2", "CHECK (`d`>0)", false},
	}
	require.Len(t, ct.Constraints, len(wants))
	for i, w := range wants {
		c := ct.Constraints[i]
		require.Equal(t, "CHECK", c.Type)
		require.Equal(t, w.name, c.Name)
		require.Equal(t, w.notEnforced, c.NotEnforced)
		require.NotNil(t, c.Definition)
		require.Equal(t, w.definition, *c.Definition)
	}
}

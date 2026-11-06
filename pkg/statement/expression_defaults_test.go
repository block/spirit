package statement

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplitAddColumnExpressionDefaults(t *testing.T) {
	for _, tc := range []struct{ name, alter, add, defaults string }{
		{"uuid", "ADD COLUMN uuid CHAR(36) NOT NULL DEFAULT (UUID()) AFTER id", "ADD COLUMN `uuid` CHAR(36) NOT NULL AFTER `id`", "ALTER COLUMN `uuid` SET DEFAULT (UUID())"},
		{"multiple", "ADD COLUMN (u CHAR(36) DEFAULT (UUID()), b BINARY(16) DEFAULT (UUID_TO_BIN(UUID())), n INT DEFAULT 7)", "ADD COLUMN (`u` CHAR(36), `b` BINARY(16), `n` INT DEFAULT 7)", "ALTER COLUMN `u` SET DEFAULT (UUID()), ALTER COLUMN `b` SET DEFAULT (UUID_TO_BIN(UUID()))"},
		{"quoted", "ADD COLUMN `u``id` CHAR(36) NOT NULL DEFAULT (UUID()) COMMENT '100% ready' FIRST", "ADD COLUMN `u``id` CHAR(36) NOT NULL COMMENT '100% ready' FIRST", "ALTER COLUMN `u``id` SET DEFAULT (UUID())"},
		{"literal", "ADD COLUMN n INT DEFAULT 42", "", ""},
		{"timestamp", "ADD COLUMN ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "", ""},
		{"mixed", "ADD COLUMN u CHAR(36) DEFAULT (UUID()), DROP COLUMN old", "", ""},
		{"index", "ADD COLUMN u CHAR(36) DEFAULT (UUID()), ADD UNIQUE INDEX uidx(u)", "", ""},
		{"column reference", "ADD COLUMN u VARCHAR(80) DEFAULT (CONCAT(UUID(), id))", "", ""},
		{"dependent default", "ADD COLUMN u CHAR(36) DEFAULT (UUID()), ADD COLUMN v CHAR(36) DEFAULT (u)", "", ""},
		{"generated", "ADD COLUMN u CHAR(36) DEFAULT (UUID()), ADD COLUMN n INT AS (id+1)", "", ""},
		{"check", "ADD COLUMN u CHAR(36) DEFAULT (UUID()) CHECK (LENGTH(u) > 0)", "", ""},
		{"duplicate default", "ADD COLUMN u CHAR(36) DEFAULT (UUID()) DEFAULT 'x'", "", ""},
		{"multiple statements", "ADD COLUMN u CHAR(36) DEFAULT (UUID()); ALTER TABLE t ADD COLUMN n INT", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			add, defaults, err := SplitAddColumnExpressionDefaults(tc.alter)
			require.NoError(t, err)
			require.Equal(t, tc.add, add)
			require.Equal(t, tc.defaults, defaults)
		})
	}
	_, _, err := SplitAddColumnExpressionDefaults("not valid sql")
	require.Error(t, err)
}

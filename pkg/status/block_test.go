package status

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBlockRendering(t *testing.T) {
	b := NewBlock("migration status: state=%s total-time=%s", "copyRows", "2m6s")
	b.Row("copier", "%6.2f%%  %d/%d", 30.84, 5048712, 16370180)
	b.Row("ckpt", "20s ago  binlog.000123:41909012")

	// Labels are padded to a common width, so every row's fields start in the
	// same column.
	require.Equal(t, strings.Join([]string{
		"migration status: state=copyRows total-time=2m6s",
		"  copier  30.84%  5048712/16370180",
		"  ckpt   20s ago  binlog.000123:41909012",
	}, "\n"), b.String())
}

func TestBlockDropsEmptyRows(t *testing.T) {
	// A helper that reports nothing (a nil applier, a feed that publishes no
	// stats) must not leave a dangling label behind, and neither must the
	// trailing separator of the row it would have filled.
	b := NewBlock("sync status: state=%s", "copyRows")
	b.Row("applier", "")
	b.Row("binlog", "deltas=%d  %s", 0, "")
	require.Equal(t, "sync status: state=copyRows\n  binlog deltas=0", b.String())
}

func TestBlockHeaderOnly(t *testing.T) {
	require.Equal(t, "sync status: state=initial", NewBlock("sync status: state=%s", "initial").String())
}

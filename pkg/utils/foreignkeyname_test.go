package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRenamedForeignKeyName(t *testing.T) {
	assert.Equal(t, "kid_ibfk_7", RenamedForeignKeyName("child_ibfk_7", "child", "kid"))
	assert.Equal(t, "kid_ibfk_x", RenamedForeignKeyName("child_ibfk_x", "child", "kid"))
	// The match is case-sensitive.
	assert.Equal(t, "CHILD_IBFK_8", RenamedForeignKeyName("CHILD_IBFK_8", "child", "kid"))
	assert.Equal(t, "fk_parent", RenamedForeignKeyName("fk_parent", "child", "kid"))
	// The table's name has to be followed by _ibfk_, not merely start it.
	assert.Equal(t, "children_ibfk_1", RenamedForeignKeyName("children_ibfk_1", "child", "kid"))
}

func TestNewForeignKeyName(t *testing.T) {
	assert.Equal(t, "_child_new_ibfk_1", NewForeignKeyName("child", "child_ibfk_1"))
	assert.Equal(t, "_fk_parent_new", NewForeignKeyName("child", "fk_parent"))
	assert.Equal(t, "_CHILD_IBFK_8_new", NewForeignKeyName("child", "CHILD_IBFK_8"))
	// The cutover's RENAME TABLE takes a generated name back to the original.
	assert.Equal(t, "child_ibfk_1", RenamedForeignKeyName(NewForeignKeyName("child", "child_ibfk_1"), NewTableName("child"), "child"))
}

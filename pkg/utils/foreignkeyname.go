package utils

import "strings"

// generatedForeignKeyInfix is what MySQL puts between a table's name and a
// number to generate the name of a foreign key declared without one.
const generatedForeignKeyInfix = "_ibfk_"

// RenamedForeignKeyName returns the name MySQL gives a foreign key of table
// from when RENAME TABLE moves the table to to.
//
// MySQL renames a foreign key whose name starts with the table's name followed
// by _ibfk_ along with its table, so a generated name keeps following the
// table it belongs to. The match is case-sensitive and the rest of the name can
// be anything: verified against MySQL 9.7, a table child renamed to kid takes
// child_ibfk_7 and child_ibfk_x to kid_ibfk_7 and kid_ibfk_x, and leaves
// CHILD_IBFK_8 alone. Every other name is kept.
func RenamedForeignKeyName(name, from, to string) string {
	if suffix, ok := strings.CutPrefix(name, from+generatedForeignKeyInfix); ok {
		return to + generatedForeignKeyInfix + suffix
	}
	return name
}

// NewForeignKeyName returns the name a migration gives the copy of foreign key
// name, of table tableName, on the table's _new table.
//
// A foreign key name is unique per schema, not per table, so the copy cannot
// have the name of the foreign key it copies while the original table exists.
// A name MySQL renames along with its table (see RenamedForeignKeyName) is
// given the _new table's prefix instead, which the cutover's RENAME TABLE turns
// back into the original name. Any other name becomes _<name>_new, which the
// cutover renames back under its table lock, once it has dropped the foreign
// keys of the old table.
func NewForeignKeyName(tableName, name string) string {
	if renamed := RenamedForeignKeyName(name, tableName, NewTableName(tableName)); renamed != name {
		return renamed
	}
	return "_" + name + suffixNew
}

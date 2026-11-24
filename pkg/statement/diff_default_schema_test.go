package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDiffDefaultSchema covers how DiffOptions.DefaultSchema resolves an
// unqualified foreign key reference. SHOW CREATE TABLE leaves the schema off a
// reference to a table in the table's own schema and writes it for any other,
// and a parsed CREATE TABLE does not know its own schema.
func TestDiffDefaultSchema(t *testing.T) {
	fk := func(ref string) string {
		return "CREATE TABLE t (id INT PRIMARY KEY, p INT, CONSTRAINT fk_p FOREIGN KEY (p) REFERENCES " + ref + " (id))"
	}
	tests := []struct {
		name          string
		want          string // declared
		got           string // live
		defaultSchema string
		diff          string
	}{
		// DefaultSchema unset: an unqualified reference matches any schema, so
		// these two pairs are not told apart.
		{"UnsetQualifiedOtherVsUnqualifiedIgnored", fk("other_db.parent"), fk("parent"), "", ""},
		{"UnsetUnqualifiedVsQualifiedOtherIgnored", fk("parent"), fk("other_db.parent"), "", ""},
		// DefaultSchema set: an unqualified reference is the table's own schema.
		{"SetWantQualifiedOtherVsGotUnqualified", fk("other_db.parent"), fk("parent"), "mydb", "ALTER TABLE `t` DROP FOREIGN KEY `fk_p`, ADD CONSTRAINT `_fk_p_new` FOREIGN KEY (`p`) REFERENCES `other_db`.`parent` (`id`)"},
		{"SetWantUnqualifiedVsGotQualifiedOther", fk("parent"), fk("other_db.parent"), "mydb", "ALTER TABLE `t` DROP FOREIGN KEY `fk_p`, ADD CONSTRAINT `_fk_p_new` FOREIGN KEY (`p`) REFERENCES `parent` (`id`)"},
		{"SetWantQualifiedOwnVsGotUnqualified", fk("mydb.parent"), fk("parent"), "mydb", ""},
		{"SetWantUnqualifiedVsGotQualifiedOwn", fk("parent"), fk("mydb.parent"), "mydb", ""},
		// Both qualified or both unqualified: DefaultSchema changes nothing
		// unless it is what separates them.
		{"SetBothQualifiedSame", fk("other_db.parent"), fk("other_db.parent"), "mydb", ""},
		{"SetBothQualifiedDifferent", fk("other_db.parent"), fk("third_db.parent"), "mydb", "ALTER TABLE `t` DROP FOREIGN KEY `fk_p`, ADD CONSTRAINT `_fk_p_new` FOREIGN KEY (`p`) REFERENCES `other_db`.`parent` (`id`)"},
		{"SetBothQualifiedOwn", fk("mydb.parent"), fk("`mydb`.`parent`"), "mydb", ""},
		{"SetBothUnqualified", fk("parent"), fk("parent"), "mydb", ""},
		{"UnsetBothQualifiedSame", fk("other_db.parent"), fk("other_db.parent"), "", ""},
		{"UnsetBothQualifiedDifferent", fk("other_db.parent"), fk("third_db.parent"), "", "ALTER TABLE `t` DROP FOREIGN KEY `fk_p`, ADD CONSTRAINT `_fk_p_new` FOREIGN KEY (`p`) REFERENCES `other_db`.`parent` (`id`)"},
		{"UnsetBothUnqualified", fk("parent"), fk("parent"), "", ""},
		// A different referenced table still differs.
		{"SetDifferentTable", fk("other"), fk("parent"), "mydb", "ALTER TABLE `t` DROP FOREIGN KEY `fk_p`, ADD CONSTRAINT `_fk_p_new` FOREIGN KEY (`p`) REFERENCES `other` (`id`)"},
		// Schemas are compared exactly, as the referenced table names are.
		{"SetSchemaCaseDiffers", fk("MYDB.parent"), fk("parent"), "mydb", "ALTER TABLE `t` DROP FOREIGN KEY `fk_p`, ADD CONSTRAINT `_fk_p_new` FOREIGN KEY (`p`) REFERENCES `MYDB`.`parent` (`id`)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := NewDiffOptions()
			opts.DefaultSchema = tc.defaultSchema
			diff, err := DiffCreateTables("t", tc.want, tc.got, opts)
			require.NoError(t, err)
			assert.Equal(t, tc.diff, diff)
		})
	}
}

// An added foreign key is unaffected by DefaultSchema, and an unchanged one is
// not re-created when DefaultSchema is set.
func TestDiffDefaultSchemaAddAndDrop(t *testing.T) {
	opts := NewDiffOptions()
	opts.DefaultSchema = "mydb"
	const bare = "CREATE TABLE t (id INT PRIMARY KEY, p INT)"
	const withFK = "CREATE TABLE t (id INT PRIMARY KEY, p INT, CONSTRAINT fk_p FOREIGN KEY (p) REFERENCES parent (id))"

	diff, err := DiffCreateTables("t", withFK, bare, opts)
	require.NoError(t, err)
	assert.Equal(t, "ALTER TABLE `t` ADD CONSTRAINT `fk_p` FOREIGN KEY (`p`) REFERENCES `parent` (`id`)", diff)

	diff, err = DiffCreateTables("t", bare, withFK, opts)
	require.NoError(t, err)
	assert.Equal(t, "ALTER TABLE `t` DROP FOREIGN KEY `fk_p`", diff)
}

// A foreign key that is renamed pairs with its equivalent under a new name
// (constraintsEqualIgnoreName), so DefaultSchema has to apply there too.
func TestDiffDefaultSchemaRenamedForeignKey(t *testing.T) {
	opts := NewDiffOptions()
	opts.DefaultSchema = "mydb"
	want := "CREATE TABLE t (id INT PRIMARY KEY, p INT, CONSTRAINT fk_new FOREIGN KEY (p) REFERENCES mydb.parent (id))"
	got := "CREATE TABLE t (id INT PRIMARY KEY, p INT, CONSTRAINT fk_old FOREIGN KEY (p) REFERENCES parent (id))"
	diff, err := DiffCreateTables("t", want, got, opts)
	require.NoError(t, err)
	assert.Empty(t, diff)

	want = "CREATE TABLE t (id INT PRIMARY KEY, p INT, CONSTRAINT fk_new FOREIGN KEY (p) REFERENCES other_db.parent (id))"
	diff, err = DiffCreateTables("t", want, got, opts)
	require.NoError(t, err)
	assert.NotEmpty(t, diff)
}

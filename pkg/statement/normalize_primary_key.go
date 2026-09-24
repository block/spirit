package statement

func init() { registerNormalizer(primaryKeyNormalizer{}) }

// primaryKeyNormalizer materializes an inline column-level PRIMARY KEY
// (`id INT PRIMARY KEY`) into a table-level PRIMARY KEY index, the form MySQL
// reports in SHOW CREATE TABLE. After this runs the primary key is always a
// single entry in ct.Indexes (Type "PRIMARY KEY", empty name — the parser
// reports every PK, named or not, with an empty index name), so diff can treat
// the inline and table-level spellings identically without special-casing.
//
// Only the PrimaryKey flag is cleared; the column definition is otherwise
// untouched. Nullability is left to primaryKeyNotNullNormalizer, which marks
// primary key columns NOT NULL, whichever way the key was spelled, unless they
// explicitly declare NULL.
type primaryKeyNormalizer struct{}

func (primaryKeyNormalizer) Name() string { return "primary-key" }

func (primaryKeyNormalizer) Normalize(ct *CreateTable) *CreateTable {
	// A valid table has at most one PRIMARY KEY. If a table-level PK already
	// exists, an inline PrimaryKey flag would be invalid SQL; leave it alone
	// rather than synthesize a second PK index.
	if ct.getPrimaryKeyIndex() != nil {
		return ct
	}
	for i := range ct.Columns {
		col := &ct.Columns[i]
		if !col.PrimaryKey {
			continue
		}
		ct.Indexes = append(ct.Indexes, Index{
			Name:       "",
			Type:       "PRIMARY KEY",
			Columns:    []string{col.Name},
			ColumnList: []IndexColumn{{Name: col.Name}},
		})
		col.PrimaryKey = false
		break // only one column can carry an inline PRIMARY KEY
	}
	return ct
}

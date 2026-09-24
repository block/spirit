package statement

func init() { registerNormalizer(vectorDimensionNormalizer{}) }

// defaultVectorDimension is the number of entries MySQL gives a VECTOR column
// declared without a dimension. MySQL applies it server side and reports the
// column as vector(2048) in SHOW CREATE TABLE.
const defaultVectorDimension = 2048

// vectorDimensionNormalizer fills in the default dimension of a VECTOR column
// (MySQL 9.7+) that was declared without one. `VECTOR` and `VECTOR(2048)` are
// the same column, but MySQL always reports the explicit form, so a schema file
// written as `v VECTOR` would otherwise produce a spurious — and, since the
// generated statement is a no-op, non-converging — MODIFY COLUMN when diffed
// against the live table.
type vectorDimensionNormalizer struct{}

func (vectorDimensionNormalizer) Name() string { return "vector-dimension" }

func (vectorDimensionNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if c.Type != "vector" || c.Length != nil {
			continue
		}
		length := defaultVectorDimension
		c.Length = &length
	}
	return ct
}

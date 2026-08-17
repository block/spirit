package statement

func init() { registerNormalizer(charsetlessTypeNormalizer{}) }

// charsetlessTypes are the column types MySQL stores without any
// charset/collation, no matter what the author wrote. They are byte types
// with a synthetic "binary" charset internally, and SHOW CREATE TABLE reports
// them bare — `geometry`, `vector(3)`.
var charsetlessTypes = map[string]struct{}{
	"vector":             {},
	"geometry":           {},
	"point":              {},
	"linestring":         {},
	"polygon":            {},
	"multipoint":         {},
	"multilinestring":    {},
	"multipolygon":       {},
	"geometrycollection": {},
}

// charsetlessTypeNormalizer drops charset/collation from the types that cannot
// carry one. It closes two distinct routes by which one can arrive:
//
//   - The parser assigns these types a synthetic "binary" charset/collation of
//     its own, which is not valid SQL to emit — MySQL rejects
//     `vector(3) CHARACTER SET binary` outright (1064), and so does this
//     parser when it re-parses the generated ALTER.
//   - An author can write `v VECTOR(3) COLLATE binary` by hand. MySQL *accepts*
//     that and silently drops it, reporting the column back as plain
//     `vector(3)`. Without this rule the authored collation survives into the
//     diff, which emits `MODIFY COLUMN v vector(3) COLLATE binary` — the server
//     applies it, still reports `vector(3)`, and the next run emits the same
//     statement again. That is a non-converging diff that costs a full table
//     copy every time, the same failure mode [vectorDimensionNormalizer]
//     exists to prevent, reached through COLLATE instead of the dimension.
//
// Both routes affect the spatial types identically, so they are handled here
// together rather than one type family at a time.
type charsetlessTypeNormalizer struct{}

func (charsetlessTypeNormalizer) Name() string { return "charsetless-types" }

func (charsetlessTypeNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if _, ok := charsetlessTypes[c.Type]; !ok {
			continue
		}
		c.Charset = nil
		c.Collation = nil
	}
	return ct
}

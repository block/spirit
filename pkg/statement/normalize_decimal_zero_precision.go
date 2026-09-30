package statement

func init() { registerNormalizer(decimalZeroPrecisionNormalizer{}) }

// defaultDecimalPrecision is the precision MySQL gives a DECIMAL column declared
// without one, or with a precision of 0: DECIMAL, DECIMAL(0) and DECIMAL(0,0)
// are all stored and reported as decimal(10,0).
const defaultDecimalPrecision = 10

// decimalZeroPrecisionNormalizer rewrites a zero-precision DECIMAL to the
// decimal(10,0) MySQL stores for it. The parser already expands a bare DECIMAL
// to decimal(10,0), but keeps an explicit zero, so a schema file written as
// `d DECIMAL(0)` would otherwise produce a spurious — and, since MySQL rewrites
// the generated statement straight back to decimal(10,0), non-converging —
// MODIFY COLUMN when diffed against the live table.
//
// It sets both Length and Precision, because that is how decimal(10,0) parses.
type decimalZeroPrecisionNormalizer struct{}

func (decimalZeroPrecisionNormalizer) Name() string { return "decimal-zero-precision" }

func (decimalZeroPrecisionNormalizer) Normalize(ct *CreateTable) *CreateTable {
	for i := range ct.Columns {
		c := &ct.Columns[i]
		if c.Type != "decimal" || c.Precision != nil || c.Length == nil || *c.Length != 0 {
			continue
		}
		length, precision := defaultDecimalPrecision, defaultDecimalPrecision
		c.Length, c.Precision = &length, &precision
	}
	return ct
}

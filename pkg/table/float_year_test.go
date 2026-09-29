package table

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsFloatColumnType(t *testing.T) {
	for _, tp := range []string{"float", "FLOAT", "float unsigned", "float(7,4)", "float(7,4) unsigned zerofill", " float "} {
		assert.True(t, isFloatColumnType(tp), tp)
	}
	for _, tp := range []string{"double", "double unsigned", "decimal(10,2)", "floaty", "int", "varchar(10)", ""} {
		assert.False(t, isFloatColumnType(tp), tp)
	}
}

// TestYearDatumIsANumber pins YEAR as a bare-number literal. MySQL stores the
// string '0' in a YEAR column as 2000 but the number 0 as 0000, and the
// driver and the binlog both deliver YEAR 0000 as the integer 0.
func TestYearDatumIsANumber(t *testing.T) {
	for _, val := range []any{int64(0), "0", int64(2024), "2024"} {
		d, err := NewDatumFromValue(val, "year")
		require.NoError(t, err)
		assert.Equal(t, signedType, d.Tp, "%v", val)
	}
	d, err := NewDatumFromValue(int64(0), "year")
	require.NoError(t, err)
	assert.Equal(t, "0", d.String())
	d, err = NewDatumFromValue(int64(2024), "year")
	require.NoError(t, err)
	assert.Equal(t, "2024", d.String())
}

func TestSourceSelectListFloat(t *testing.T) {
	src, err := NewTableInfoFromMeta("test", "t1", []ColumnMeta{
		{Name: "id", MySQLType: "int"},
		{Name: "f", MySQLType: "float"},
		{Name: "f_to_double", MySQLType: "float"},
		{Name: "f_to_decimal", MySQLType: "float unsigned"},
		{Name: "f_to_char", MySQLType: "float"},
		{Name: "f_to_bit", MySQLType: "float"},
		{Name: "d", MySQLType: "double"},
	}, []string{"id"})
	require.NoError(t, err)
	dst, err := NewTableInfoFromMeta("test", "_t1_new", []ColumnMeta{
		{Name: "id", MySQLType: "bigint"},
		{Name: "f", MySQLType: "float"},
		{Name: "f_to_double", MySQLType: "double"},
		{Name: "f_to_decimal", MySQLType: "decimal(20,10)"},
		{Name: "f_to_char", MySQLType: "varchar(32)"},
		{Name: "f_to_bit", MySQLType: "bit(32)"},
		{Name: "d", MySQLType: "float"},
	}, []string{"id"})
	require.NoError(t, err)

	// A FLOAT read into a numeric target is widened to its exact DOUBLE,
	// since the text protocol rounds a FLOAT to 6 significant digits. A
	// string target reads MySQL's own text form, which is what ALTER TABLE
	// writes; a BIT target is left alone.
	want := "`id`, (`f` + 0E0), (`f_to_double` + 0E0), (`f_to_decimal` + 0E0), CAST(`f_to_char` AS char), `f_to_bit`, `d`"
	assert.Equal(t, want, NewColumnMapping(src, dst, nil).SourceSelectList())
	assert.Equal(t, want, SourceSelectList(src.NonGeneratedColumns, src, dst))

	// With a rename, the source column is read and the target column's type
	// decides the expression.
	dst2, err := NewTableInfoFromMeta("test", "_t1_new", []ColumnMeta{
		{Name: "id", MySQLType: "int"},
		{Name: "g", MySQLType: "float"},
	}, []string{"id"})
	require.NoError(t, err)
	src2, err := NewTableInfoFromMeta("test", "t1", []ColumnMeta{
		{Name: "id", MySQLType: "int"},
		{Name: "f", MySQLType: "float"},
	}, []string{"id"})
	require.NoError(t, err)
	assert.Equal(t, "`id`, (`f` + 0E0)", NewColumnMapping(src2, dst2, map[string]string{"f": "g"}).SourceSelectList())
}

func TestDecodeBinlogRowWidensFloat(t *testing.T) {
	ti, err := NewTableInfoFromMeta("test", "t1", []ColumnMeta{
		{Name: "id", MySQLType: "int"},
		{Name: "f", MySQLType: "float"},
		{Name: "d", MySQLType: "double"},
	}, []string{"id"})
	require.NoError(t, err)
	require.True(t, ti.NeedsBinlogRowDecoding())

	row := []any{int32(1), float32(math.MaxFloat32), float64(0.1)}
	require.NoError(t, ti.DecodeBinlogRow(row))
	// The float64 holds the FLOAT's exact value, so its literal is exact too.
	// Compared by bits: the conversion must be exact.
	require.IsType(t, float64(0), row[1])
	assert.Equal(t, math.Float64bits(float64(float32(math.MaxFloat32))), math.Float64bits(row[1].(float64)))
	assert.Equal(t, math.Float64bits(0.1), math.Float64bits(row[2].(float64)))

	row = []any{int32(2), nil, nil}
	require.NoError(t, ti.DecodeBinlogRow(row))
	assert.Nil(t, row[1])

	// A table without FLOAT (or other decoded) columns is not decoded.
	plain, err := NewTableInfoFromMeta("test", "t2", []ColumnMeta{
		{Name: "id", MySQLType: "int"},
		{Name: "d", MySQLType: "double"},
	}, []string{"id"})
	require.NoError(t, err)
	assert.False(t, plain.NeedsBinlogRowDecoding())
}

func TestFloatPrimaryKeyError(t *testing.T) {
	for _, tc := range []struct {
		name    string
		columns []ColumnMeta
		key     []string
		wantErr bool
	}{
		{"float pk", []ColumnMeta{{Name: "f", MySQLType: "float"}}, []string{"f"}, true},
		{"float unsigned in composite pk", []ColumnMeta{{Name: "id", MySQLType: "int"}, {Name: "f", MySQLType: "float(7,4) unsigned"}}, []string{"id", "f"}, true},
		{"double pk", []ColumnMeta{{Name: "d", MySQLType: "double"}}, []string{"d"}, false},
		{"float non-key column", []ColumnMeta{{Name: "id", MySQLType: "int"}, {Name: "f", MySQLType: "float"}}, []string{"id"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ti, err := NewTableInfoFromMeta("test", "t1", tc.columns, tc.key)
			require.NoError(t, err)
			err = ti.FloatPrimaryKeyError()
			if tc.wantErr {
				require.ErrorContains(t, err, "is a FLOAT, which is not supported")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncryptionNormalizer(t *testing.T) {
	tests := []struct {
		options string
		want    *string // nil = no ENCRYPTION recorded
	}{
		{"", nil},
		{" ENCRYPTION='N'", new("N")},
		{" ENCRYPTION='n'", new("N")},
		{" ENCRYPTION = 'N'", new("N")},
		{" ENCRYPTION='Y'", new("Y")},
		{" ENCRYPTION='y'", new("Y")},
		{" encryption 'y'", new("Y")},
		{" ENCRYPTION='foo'", new("foo")}, // MySQL rejects it; left as written
	}
	for _, tc := range tests {
		t.Run(tc.options, func(t *testing.T) {
			ct, err := ParseCreateTable("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)" + tc.options)
			require.NoError(t, err)
			if tc.want == nil {
				if ct.TableOptions != nil {
					assert.Nil(t, ct.TableOptions.Encryption)
				}
				return
			}
			require.NotNil(t, ct.TableOptions)
			assert.Equal(t, tc.want, ct.TableOptions.Encryption)
		})
	}
}

// An explicit ENCRYPTION='N' is kept distinct from an absent option, and it
// does not disturb the other table options.
func TestEncryptionNormalizerKeepsOtherOptions(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE t (id INT NOT NULL PRIMARY KEY) ENGINE=InnoDB ENCRYPTION='n'")
	require.NoError(t, err)
	require.NotNil(t, ct.TableOptions)
	assert.Equal(t, new("N"), ct.TableOptions.Encryption)
	assert.Equal(t, "InnoDB", *ct.TableOptions.Engine)

	ct, err = ParseCreateTable("CREATE TABLE t (id INT NOT NULL PRIMARY KEY) ENCRYPTION='N'")
	require.NoError(t, err)
	require.NotNil(t, ct.TableOptions)
	assert.Equal(t, new("N"), ct.TableOptions.Encryption)

	ct, err = ParseCreateTable("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)")
	require.NoError(t, err)
	assert.Nil(t, ct.TableOptions)
}

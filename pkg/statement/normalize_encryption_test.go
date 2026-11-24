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
		{" ENCRYPTION='N'", nil},
		{" ENCRYPTION='n'", nil},
		{" ENCRYPTION = 'N'", nil},
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

// ENCRYPTION='N' alone leaves no table options, so a table that declares it
// parses to the same structure as one that does not.
func TestEncryptionNormalizerKeepsOtherOptions(t *testing.T) {
	ct, err := ParseCreateTable("CREATE TABLE t (id INT NOT NULL PRIMARY KEY) ENGINE=InnoDB ENCRYPTION='N'")
	require.NoError(t, err)
	require.NotNil(t, ct.TableOptions)
	assert.Nil(t, ct.TableOptions.Encryption)
	assert.Equal(t, "InnoDB", *ct.TableOptions.Engine)

	ct, err = ParseCreateTable("CREATE TABLE t (id INT NOT NULL PRIMARY KEY) ENCRYPTION='N'")
	require.NoError(t, err)
	assert.Nil(t, ct.TableOptions)
}

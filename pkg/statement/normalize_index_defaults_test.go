package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexDefaultsNormalizer(t *testing.T) {
	t.Run("drops VISIBLE, USING HASH and an uncompressed KEY_BLOCK_SIZE on InnoDB", func(t *testing.T) {
		ct, err := ParseCreateTable("CREATE TABLE t (id INT, c INT, PRIMARY KEY (id) VISIBLE, " +
			"KEY kv (c) VISIBLE, KEY ki (c) INVISIBLE, KEY kh (c) USING HASH, KEY kb (c) USING BTREE, KEY kk (c) KEY_BLOCK_SIZE=8)")
		require.NoError(t, err)
		for i := range ct.Indexes {
			if ct.Indexes[i].Type == "PRIMARY KEY" {
				assert.Nil(t, ct.Indexes[i].Invisible)
			}
		}
		assert.Nil(t, ct.Indexes.ByName("kv").Invisible)
		require.NotNil(t, ct.Indexes.ByName("ki").Invisible)
		assert.True(t, *ct.Indexes.ByName("ki").Invisible)
		assert.Nil(t, ct.Indexes.ByName("kh").Using)
		require.NotNil(t, ct.Indexes.ByName("kb").Using)
		assert.Equal(t, "BTREE", *ct.Indexes.ByName("kb").Using)
		assert.Nil(t, ct.Indexes.ByName("kk").KeyBlockSize)
	})
	t.Run("keeps KEY_BLOCK_SIZE on a compressed table", func(t *testing.T) {
		for _, options := range []string{"ROW_FORMAT=COMPRESSED", "KEY_BLOCK_SIZE=4", "ENGINE=InnoDB ROW_FORMAT=compressed"} {
			ct, err := ParseCreateTable("CREATE TABLE t (id INT PRIMARY KEY, c INT, KEY kk (c) KEY_BLOCK_SIZE=8) " + options)
			require.NoError(t, err, options)
			require.NotNil(t, ct.Indexes.ByName("kk").KeyBlockSize, options)
			assert.Equal(t, uint64(8), *ct.Indexes.ByName("kk").KeyBlockSize, options)
		}
	})
	t.Run("another engine keeps its options", func(t *testing.T) {
		ct, err := ParseCreateTable("CREATE TABLE t (id INT PRIMARY KEY, c INT, KEY kh (c) USING HASH, KEY kk (c) KEY_BLOCK_SIZE=8 VISIBLE) ENGINE=MEMORY")
		require.NoError(t, err)
		require.NotNil(t, ct.Indexes.ByName("kh").Using)
		assert.Equal(t, "HASH", *ct.Indexes.ByName("kh").Using)
		require.NotNil(t, ct.Indexes.ByName("kk").KeyBlockSize)
		assert.Nil(t, ct.Indexes.ByName("kk").Invisible, "VISIBLE is the default on every engine")
	})
}

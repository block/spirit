package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngineNormalizer(t *testing.T) {
	tests := []struct {
		engine string
		want   *string // nil = no ENGINE option
	}{
		{"", nil},
		{" ENGINE=InnoDB", new("InnoDB")},
		{" ENGINE=innodb", new("InnoDB")},
		{" ENGINE=INNODB", new("InnoDB")},
		{" ENGINE=innobase", new("InnoDB")},
		{" ENGINE=myisam", new("MyISAM")},
		{" ENGINE=memory", new("MEMORY")},
		{" ENGINE=heap", new("MEMORY")},
		{" ENGINE=merge", new("MRG_MyISAM")},
		{" ENGINE=MRG_MYISAM", new("MRG_MyISAM")},
		{" ENGINE=csv", new("CSV")},
		{" ENGINE=archive", new("ARCHIVE")},
		{" ENGINE=blackhole", new("BLACKHOLE")},
		{" ENGINE=SomeOtherEngine", new("SomeOtherEngine")}, // unknown engines keep their spelling
	}
	for _, tc := range tests {
		t.Run(tc.engine, func(t *testing.T) {
			ct, err := ParseCreateTable("CREATE TABLE t (id INT NOT NULL PRIMARY KEY)" + tc.engine)
			require.NoError(t, err)
			assert.Equal(t, tc.want, ct.TableOptions.getEngine())
		})
	}
}

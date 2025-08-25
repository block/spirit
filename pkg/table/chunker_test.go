package table

import (
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/testutils"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestCompositeChunker(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS composite`)
	table := `CREATE TABLE composite (
		id bigint NOT NULL AUTO_INCREMENT,
		age int(11) NOT NULL,
		PRIMARY KEY (id, age)
	)`
	testutils.RunSQL(t, table)

	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer db.Close()

	t1 := NewTableInfo(db, "test", "composite")
	assert.NoError(t, t1.SetInfo(t.Context()))

	chunker, err := NewChunker(t1, 0, logrus.New())
	assert.NoError(t, err)
	assert.IsType(t, &chunkerComposite{}, chunker)
}

func TestOptimisticChunker(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS optimistic`)
	table := `CREATE TABLE optimistic (
		id bigint NOT NULL AUTO_INCREMENT,
		PRIMARY KEY (id)
	)`
	testutils.RunSQL(t, table)

	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer db.Close()

	t1 := NewTableInfo(db, "test", "optimistic")
	assert.NoError(t, t1.SetInfo(t.Context()))

	chunker, err := NewChunker(t1, 0, logrus.New())
	assert.NoError(t, err)
	assert.IsType(t, &chunkerOptimistic{}, chunker)
}

func TestNewCompositeChunker(t *testing.T) {
	testutils.RunSQL(t, `DROP TABLE IF EXISTS composite`)
	table := `CREATE TABLE composite (
		id bigint NOT NULL AUTO_INCREMENT,
		age int(11) NOT NULL,
		PRIMARY KEY (id),
        KEY age_idx (age)
	)`
	testutils.RunSQL(t, table)

	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)
	defer db.Close()

	t1 := NewTableInfo(db, "test", "composite")
	assert.NoError(t, t1.SetInfo(t.Context()))

	chunker, err := NewCompositeChunker(t1, 0, logrus.New(), "age_idx", "age > 50")
	assert.NoError(t, err)
	assert.IsType(t, &chunkerComposite{}, chunker)
	assert.Equal(t, "age_idx", chunker.(*chunkerComposite).keyName)
	assert.Equal(t, "age > 50", chunker.(*chunkerComposite).where)
}

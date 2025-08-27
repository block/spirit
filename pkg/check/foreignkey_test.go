package check

import (
	"database/sql"
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/testutils"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestAddForeignKey(t *testing.T) {
	var err error
	r := Resources{
		Statement: statement.MustNew("ALTER TABLE t1 ADD FOREIGN KEY (customer_id) REFERENCES customers (id)"),
	}
	err = addForeignKeyCheck(t.Context(), r, logrus.New())
	assert.Error(t, err) // add foreign key
	assert.ErrorContains(t, err, "adding foreign key constraints is not supported")

	r.Statement = statement.MustNew("ALTER TABLE t1 DROP COLUMN foo")
	err = addForeignKeyCheck(t.Context(), r, logrus.New())
	assert.NoError(t, err) // regular DDL
}

func TestHasForeignKey(t *testing.T) {
	db, err := sql.Open("mysql", testutils.DSN())
	assert.NoError(t, err)

	_, err = db.Exec(`drop table if exists customers, customer_contacts`)
	assert.NoError(t, err)
	sql := `CREATE TABLE customers (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		PRIMARY KEY (id)
	);`
	_, err = db.Exec(sql)
	assert.NoError(t, err)
	sql = `CREATE TABLE customer_contacts (
		id INT NOT NULL,
		name VARCHAR(255) NOT NULL,
		customer_id INT NOT NULL,
		PRIMARY KEY (id),
		INDEX  (customer_id),  
		CONSTRAINT fk_customer FOREIGN KEY (customer_id)  
		REFERENCES customers(id)  
		ON DELETE CASCADE  
		ON UPDATE CASCADE  
	);`
	_, err = db.Exec(sql)
	assert.NoError(t, err)

	// Under this model, both customers and customer_contacts are said to have foreign keys.
	r := Resources{
		DB:        db,
		Table:     &table.TableInfo{SchemaName: "test", TableName: "customers"},
		Statement: statement.MustNew("ALTER TABLE customers ENGINE=innodb"),
	}
	err = hasForeignKeysCheck(t.Context(), r, logrus.New())
	assert.Error(t, err) // already has foreign keys.

	r.Table.TableName = "customer_contacts"
	r.Statement = statement.MustNew("ALTER TABLE customer_contacts ENGINE=innodb")
	err = hasForeignKeysCheck(t.Context(), r, logrus.New())
	assert.Error(t, err) // already has foreign keys.

	_, err = db.Exec(`drop table if exists customer_contacts`)
	assert.NoError(t, err)
	r.Table.TableName = "customers"
	r.Statement = statement.MustNew("ALTER TABLE customers ENGINE=innodb")
	err = hasForeignKeysCheck(t.Context(), r, logrus.New())
	assert.NoError(t, err) // no longer said to have foreign keys.
}

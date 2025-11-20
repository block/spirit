package check

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
)

func init() {
	registerCheck("replicahealth", replicaHealth, ScopePostSetup|ScopeCutover)
}

// replicaHealth checks SHOW REPLICA STATUS for Yes and Yes.
// It should be run at various stages of the migration if a replica is present.
func replicaHealth(ctx context.Context, r Resources, logger *slog.Logger) error {
	if r.Replica == nil {
		return nil // The user is not using the replica DSN feature.
	}
	rows, err := r.Replica.Query("SHOW REPLICA STATUS")
	if err != nil {
		return err
	}
	defer rows.Close()
	status, err := scanToMap(rows)
	if err != nil {
		return err
	}
	if status["Replica_IO_Running"].String != "Yes" || status["Replica_SQL_Running"].String != "Yes" {
		return errors.New("replica is not healthy")
	}
	return nil
}

func scanToMap(rows *sql.Rows) (map[string]sql.NullString, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		err = rows.Err()
		if err != nil {
			return nil, err
		} else {
			return nil, nil
		}
	}
	values := make([]any, len(columns))
	for index := range values {
		values[index] = new(sql.NullString)
	}
	err = rows.Scan(values...)
	if err != nil {
		return nil, err
	}
	result := make(map[string]sql.NullString)
	for index, columnName := range columns {
		result[columnName] = *values[index].(*sql.NullString)
	}
	return result, nil
}

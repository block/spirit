package dbconn

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// RequireDifferentDatabase refuses a copy whose target aliases its source.
// Connection strings are insufficient: different users or hostnames can reach
// the same database. Check the selected schemas and, when they overlap, the
// server UUIDs before any target writes or destructive recovery.
func RequireDifferentDatabase(ctx context.Context, source, target *sql.DB) error {
	return RequireDifferentDatabases(ctx, []*sql.DB{source}, []*sql.DB{target})
}

type databaseIdentity struct {
	db       *sql.DB
	role     string
	schema   string
	uuid     string
	uuidRead bool
}

// RequireDifferentDatabases refuses any source/target database alias. Each
// connection's identity is read at most once, which keeps sharded moves from
// issuing the same identity queries for every source/target pair.
func RequireDifferentDatabases(ctx context.Context, sources, targets []*sql.DB) error {
	sourceIdentities, err := databaseIdentities(ctx, sources, "source")
	if err != nil {
		return err
	}
	targetIdentities, err := databaseIdentities(ctx, targets, "target")
	if err != nil {
		return err
	}
	for si := range sourceIdentities {
		for ti := range targetIdentities {
			source := &sourceIdentities[si]
			target := &targetIdentities[ti]
			// Be conservative about case: servers may fold database names.
			if !strings.EqualFold(source.schema, target.schema) {
				continue
			}
			if err := source.readUUID(ctx); err != nil {
				return err
			}
			if err := target.readUUID(ctx); err != nil {
				return err
			}
			if strings.EqualFold(source.uuid, target.uuid) {
				return fmt.Errorf("%s and %s refer to the same database %q on server %s; refusing to modify the source", source.role, target.role, source.schema, source.uuid)
			}
		}
	}
	return nil
}

func databaseIdentities(ctx context.Context, dbs []*sql.DB, role string) ([]databaseIdentity, error) {
	identities := make([]databaseIdentity, len(dbs))
	for i, db := range dbs {
		name := role
		if len(dbs) > 1 {
			name = fmt.Sprintf("%s %d", role, i)
		}
		schema, err := selectedDatabase(ctx, db, name)
		if err != nil {
			return nil, err
		}
		identities[i] = databaseIdentity{db: db, role: name, schema: schema}
	}
	return identities, nil
}

func (identity *databaseIdentity) readUUID(ctx context.Context) error {
	if identity.uuidRead {
		return nil
	}
	if err := identity.db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&identity.uuid); err != nil {
		return fmt.Errorf("verify source and target databases are different: read %s server UUID: %w", identity.role, err)
	}
	if identity.uuid == "" {
		return fmt.Errorf("cannot verify source and target databases are different: empty %s server UUID", identity.role)
	}
	identity.uuidRead = true
	return nil
}

func selectedDatabase(ctx context.Context, db *sql.DB, role string) (string, error) {
	var schema sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&schema); err != nil {
		return "", fmt.Errorf("read %s database identity: %w", role, err)
	}
	if !schema.Valid || schema.String == "" {
		return "", fmt.Errorf("%s connection has no selected database; specify one in its DSN", role)
	}
	return schema.String, nil
}

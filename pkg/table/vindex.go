package table

// VindexMetadataProvider is an interface that provides vindex configuration
// for tables. This allows external systems (like Vitess) to provide sharding
// metadata without Spirit having direct dependencies on those systems.
//
// The provider is called during table discovery to optionally configure
// vindex information for each table. If a table doesn't require vindex
// configuration (e.g., in simple MoveTables operations), the provider
// can return empty values.
//
// Example implementation for Vitess:
//
//	type VitessVindexProvider struct {
//	    vschema *vschemapb.Keyspace
//	}
//
//	func (v *VitessVindexProvider) GetVindexMetadata(schemaName, tableName string) (string, VindexFunc, error) {
//	    tableVindex := v.vschema.Tables[tableName]
//	    if tableVindex == nil {
//	        return "", nil, nil // No vindex for this table
//	    }
//	    primaryVindex := tableVindex.ColumnVindexes[0]
//	    vindexColumn := primaryVindex.Column
//	    vindexFunc := func(value any) (uint64, error) {
//	        return vitessVindexHash(primaryVindex.Name, value)
//	    }
//	    return vindexColumn, vindexFunc, nil
//	}
type VindexMetadataProvider interface {
	// GetVindexMetadata returns the vindex column name and hash function for a table.
	// Returns empty string and nil function if the table doesn't have vindex configuration.
	//
	// Parameters:
	//   - schemaName: The database/schema name
	//   - tableName: The table name
	//
	// Returns:
	//   - vindexColumn: The column name to use for sharding (e.g., "user_id")
	//   - vindexFunc: The hash function to apply to the column value
	//   - error: Any error encountered while retrieving metadata
	GetVindexMetadata(schemaName, tableName string) (vindexColumn string, vindexFunc VindexFunc, err error)
}

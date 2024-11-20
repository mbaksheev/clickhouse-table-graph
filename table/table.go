// Package table provides a table info struct and interface which can be used to
// get information about tables and provide it to the graph builder.
package table

import "fmt"

// Key represents a table key.
type Key struct {
	Database string
	Name     string
}

// String returns a string representation of the table key in "database_name.table_name" format.
func (key Key) String() string {
	return fmt.Sprintf("%s.%s", key.Database, key.Name)
}

// Info represents information about a table.
// This should contain info provided by the Clickhouse system.tables table.
type Info struct {
	// Key is the table key
	Key
	// Engine is the table engine.
	Engine string
	// EngineFull is the full table engine.
	EngineFull string
	// CreateTableQuery is the query used to create the table.
	CreateTableQuery string
	// AsSelect is the query used to create the table as select.
	AsSelect string
	// DependenciesDatabase is the list of dependent databases.
	DependenciesDatabase []string
	// DependenciesTable is the list of dependent tables.
	DependenciesTable []string
}

// InfoProvider is an interface for providing information about tables.
type InfoProvider interface {
	TableInfos() ([]Info, error)
}

package graph

import (
	"github.com/mbaksheev/clickhouse-table-graph/table"
)

// Link represents a link between two tables.
type Link struct {
	// FromTableKey is the key of the table from which the link starts.
	FromTableKey table.Key
	// ToTableKey is the key of the table to which the link leads.
	ToTableKey table.Key
}

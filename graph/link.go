package graph

import (
	"github.com/mbaksheev/clickhouse-table-graph/table"
)

type Link struct {
	FromTableKey table.Key
	ToTableKey   table.Key
}

package graph

import (
	"github.com/mbaksheev/clickhouse-table-graph/table"
)

type Link struct {
	FromTable table.Info
	ToTable   table.Info
}

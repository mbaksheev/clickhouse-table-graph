package graph

import (
	"github.com/mbaksheev/clickhouse-table-graph/internal/deps"
	"github.com/mbaksheev/clickhouse-table-graph/table"
)

type graphNode struct {
	fromLinks []table.Key
	toLinks   []table.Key
}

func createGraphNode(tableInfo table.Info) graphNode {
	fromLinks := make([]table.Key, 0)
	toLinks := make([]table.Key, 0)

	switch tableInfo.Engine {
	case "Distributed":
		fromLinks = append(fromLinks, deps.FromDistributedEngine(tableInfo.EngineFull)...)
	case "MaterializedView":
		toLinks = append(toLinks, deps.FromCreateQuery(tableInfo.CreateTableQuery)...)
	default:
		toLinks = append(toLinks, deps.FromDependencies(tableInfo.DependenciesDatabase, tableInfo.DependenciesTable)...)
	}
	return graphNode{
		fromLinks: fromLinks,
		toLinks:   toLinks,
	}
}

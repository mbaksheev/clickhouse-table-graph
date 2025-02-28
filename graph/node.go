package graph

import (
	"github.com/mbaksheev/clickhouse-table-graph/internal/deps"
	"github.com/mbaksheev/clickhouse-table-graph/table"
)

// graphNode represents a node in the graph.
type graphNode struct {
	// fromLinks is a list of links from the node.
	fromLinks []table.Key
	// toLinks is a list of links to the node.
	toLinks []table.Key
}

// createGraphNode creates a graph node depending on the Engine or Dependencies information provided in the specified table.Info
func createGraphNode(tableInfo table.Info) graphNode {
	fromLinks := make([]table.Key, 0)
	toLinks := make([]table.Key, 0)

	switch tableInfo.Engine {
	case "Distributed":
		fromLinks = append(fromLinks, deps.FromDistributedEngine(tableInfo.EngineFull)...)
	case "MaterializedView":
		fromLinks = append(fromLinks, deps.JoinedTablesFromCreateQuery(tableInfo.CreateTableQuery)...)
		toLinks = append(toLinks, deps.FromCreateQuery(tableInfo.CreateTableQuery)...)
	default:
		toLinks = append(toLinks, deps.FromDependencies(tableInfo.DependenciesDatabase, tableInfo.DependenciesTable)...)
	}
	return graphNode{
		fromLinks: fromLinks,
		toLinks:   toLinks,
	}
}

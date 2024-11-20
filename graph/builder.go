// Package graph provides a way to build a graph of specified tables.
//
// The main entry point is the [LinksBuilder] interface, which is implemented by the [New] function.
// Once the builder is created, you can add tables to it using the [LinksBuilder.AddTable] method.
// After all tables are added, you can get the list of links for a specific table using the [LinksBuilder.TableLinks] method.
package graph

import (
	"github.com/mbaksheev/clickhouse-table-graph/table"
)

// Links represents a graph (all linked tables) for the specified table.
type Links struct {
	// InitialTable is the key of the table for which the graph was built.
	InitialTable table.Key
	// Links is a list of links between tables connected to the InitialTable.
	Links []Link
	// tables is a map of all tables added to the graph.
	tables map[table.Key]table.Info
}

// TableInfo returns the table information for the specified key.
func (links *Links) TableInfo(key table.Key) (table.Info, bool) {
	info, exists := links.tables[key]
	return info, exists
}

// LinksBuilder is an interface for building a graph of tables.
// Once the builder is created, you can add tables to it using the [LinksBuilder.AddTable] method.
// After all tables are added, you can get the list of links for a specific table using the [LinksBuilder.TableLinks] method.
type LinksBuilder interface {
	// AddTable adds the specified table to the graph builder.
	AddTable(table table.Info)
	// TableLinks returns the graph of tables as a list of all linked tables for the specified TableKey.
	TableLinks(TableKey table.Key) (*Links, error)
}

// New creates a new [LinksBuilder].
func New() LinksBuilder {
	return &builder{
		nodes:  make(map[table.Key]*graphNode),
		tables: make(map[table.Key]table.Info),
	}
}

type builder struct {
	nodes  map[table.Key]*graphNode
	tables map[table.Key]table.Info
}

type stackItem struct {
	tableKey   table.Key
	isToParent bool
}

// TableLinks returns the graph of tables as a list of all linked tables for the specified table key.
//
// The algorithm starts with the specified initialTableKey and finds all linked tables.
// The result is a list of links between tables connected to the initialTableKey.
func (b *builder) TableLinks(initialTableKey table.Key) (*Links, error) {
	// use depth-first search to find all links for the specified initialTableKey
	graphLinks := make([]Link, 0)
	visited := make(map[table.Key]bool)
	stack := []stackItem{{tableKey: initialTableKey, isToParent: false}}

	for len(stack) > 0 {
		currentStackItem := stack[len(stack)-1]
		currentKey := currentStackItem.tableKey
		stack = stack[:len(stack)-1]

		if visited[currentKey] {
			continue
		}

		visited[currentKey] = true

		node, exists := b.nodes[currentKey]
		if !exists {
			continue
		}

		for _, toLink := range node.toLinks {
			if !visited[toLink] && !currentStackItem.isToParent {
				graphLinks = append(graphLinks, Link{
					FromTableKey: currentKey,
					ToTableKey:   toLink,
				})
				stack = append(stack, stackItem{tableKey: toLink, isToParent: false})
			}

		}

		for _, link := range node.fromLinks {
			if !visited[link] {
				graphLinks = append(graphLinks, Link{
					FromTableKey: link,
					ToTableKey:   currentKey,
				})
				stack = append(stack, stackItem{tableKey: link, isToParent: true})
			}
		}
	}
	return &Links{
			InitialTable: initialTableKey,
			Links:        graphLinks,
			tables:       b.tables,
		},
		nil
}

// AddTable adds the specified table to the graph builder.
func (b *builder) AddTable(tableInfo table.Info) {
	b.tables[tableInfo.Key] = tableInfo
	newNode := createGraphNode(tableInfo)

	if node, exists := b.nodes[tableInfo.Key]; exists {
		node.fromLinks = append(node.fromLinks, newNode.fromLinks...)
		node.toLinks = append(node.toLinks, newNode.toLinks...)
	} else {
		b.nodes[tableInfo.Key] = &newNode
	}
	for _, fromLink := range newNode.fromLinks {
		if node, exists := b.nodes[fromLink]; exists {
			node.toLinks = append(node.toLinks, tableInfo.Key)
		} else {
			b.nodes[fromLink] = &graphNode{
				fromLinks: make([]table.Key, 0),
				toLinks:   []table.Key{tableInfo.Key},
			}
		}
	}
	for _, toLink := range newNode.toLinks {
		if node, exists := b.nodes[toLink]; exists {
			node.fromLinks = append(node.fromLinks, tableInfo.Key)
		} else {
			b.nodes[toLink] = &graphNode{
				fromLinks: []table.Key{tableInfo.Key},
				toLinks:   make([]table.Key, 0),
			}
		}
	}
}

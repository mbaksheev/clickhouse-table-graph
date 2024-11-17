package graph

import (
	"github.com/mbaksheev/clickhouse-table-graph/table"
	"testing"
)

func TestCreateGraphNode(t *testing.T) {
	tests := []struct {
		name           string
		inputTableInfo table.Info
		wantToLinks    []table.Key
		wantFromLinks  []table.Key
	}{
		{
			name: "Distributed engine",
			inputTableInfo: table.Info{
				Engine:               "Distributed",
				EngineFull:           "Distributed('cluster', 'db', 'table')",
				CreateTableQuery:     "",
				DependenciesDatabase: nil,
				DependenciesTable:    nil,
			},
			wantToLinks: []table.Key{},
			wantFromLinks: []table.Key{
				{Database: "db", Name: "table"},
			},
		},
		{
			name: "MaterializedView engine",
			inputTableInfo: table.Info{
				Engine:               "MaterializedView",
				EngineFull:           "",
				CreateTableQuery:     "CREATE MATERIALIZED VIEW view TO db.table AS SELECT * FROM source",
				DependenciesDatabase: nil,
				DependenciesTable:    nil,
			},
			wantToLinks: []table.Key{
				{Database: "db", Name: "table"},
			},
			wantFromLinks: []table.Key{},
		},
		{
			name: "Null engine",
			inputTableInfo: table.Info{
				Engine:               "Null",
				EngineFull:           "",
				CreateTableQuery:     "",
				DependenciesDatabase: []string{"db1", "db2"},
				DependenciesTable:    []string{"table1", "table2"},
			},
			wantToLinks: []table.Key{
				{Database: "db1", Name: "table1"},
				{Database: "db2", Name: "table2"},
			},
			wantFromLinks: []table.Key{},
		},
		{
			name: "Any engine",
			inputTableInfo: table.Info{
				Engine:               "Any engine",
				EngineFull:           "",
				CreateTableQuery:     "",
				DependenciesDatabase: []string{"db1"},
				DependenciesTable:    []string{"table1"},
			},
			wantToLinks: []table.Key{
				{Database: "db1", Name: "table1"},
			},
			wantFromLinks: []table.Key{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := createGraphNode(tt.inputTableInfo)

			if !equal(node.fromLinks, tt.wantFromLinks) {
				t.Errorf("createGraphNode() fromLinks = %v, want %v", node.fromLinks, tt.wantFromLinks)
			}
			if !equal(node.toLinks, tt.wantToLinks) {
				t.Errorf("createGraphNode() toLinks = %v, want %v", node.toLinks, tt.wantToLinks)
			}
		})
	}
}

func equal(a, b []table.Key) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

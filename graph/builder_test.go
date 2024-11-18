package graph

import (
	"github.com/mbaksheev/clickhouse-table-graph/table"
	"slices"
	"testing"
)

func TestBuilder(t *testing.T) {
	// input_null -> table_materialized_view_1 -> table_merge_tree_1 -> table_distributed_1
	// input_null -> table_materialized_view_2 -> table_merge_tree_2 -> table_distributed_2
	// input_null_3 -> table_materialized_view_3 -> table_merge_tree_2
	// input_merge_tree_4 -> table_distributed_4
	tables := []table.Info{
		{
			Key:                  table.Key{Database: "db", Name: "input_null"},
			Engine:               "Null",
			EngineFull:           "",
			CreateTableQuery:     "",
			DependenciesDatabase: []string{"db", "db"},
			DependenciesTable:    []string{"table_materialized_view_1", "table_materialized_view_2"},
		},
		{
			Key:                  table.Key{Database: "db", Name: "table_materialized_view_1"},
			Engine:               "MaterializedView",
			EngineFull:           "",
			CreateTableQuery:     "CREATE MATERIALIZED VIEW db.table_materialized_view_1 TO db.table_merge_tree_1 AS SELECT * FROM db.input_null",
			DependenciesDatabase: nil,
			DependenciesTable:    nil,
		},
		{
			Key:                  table.Key{Database: "db", Name: "table_materialized_view_2"},
			Engine:               "MaterializedView",
			EngineFull:           "",
			CreateTableQuery:     "CREATE MATERIALIZED VIEW db.table_materialized_view_2 TO db.table_merge_tree_2 AS SELECT * FROM db.input_null",
			DependenciesDatabase: nil,
			DependenciesTable:    nil,
		},
		{
			Key:                  table.Key{Database: "db", Name: "table_merge_tree_1"},
			Engine:               "MergeTree",
			EngineFull:           "",
			CreateTableQuery:     "",
			DependenciesDatabase: nil,
			DependenciesTable:    nil,
		},
		{
			Key:                  table.Key{Database: "db", Name: "table_merge_tree_2"},
			Engine:               "ReplacingMergeTree",
			EngineFull:           "",
			CreateTableQuery:     "",
			DependenciesDatabase: nil,
			DependenciesTable:    nil,
		},
		{
			Key:                  table.Key{Database: "db", Name: "table_distributed_1"},
			Engine:               "Distributed",
			EngineFull:           "Distributed('cluster', 'db', 'table_merge_tree_1')",
			CreateTableQuery:     "",
			DependenciesDatabase: nil,
			DependenciesTable:    nil,
		},
		{
			Key:                  table.Key{Database: "db", Name: "table_distributed_2"},
			Engine:               "Distributed",
			EngineFull:           "Distributed('cluster', 'db', 'table_merge_tree_2')",
			CreateTableQuery:     "",
			DependenciesDatabase: nil,
			DependenciesTable:    nil,
		},
		{
			Key:                  table.Key{Database: "db", Name: "input_null_3"},
			Engine:               "Null",
			EngineFull:           "",
			CreateTableQuery:     "",
			DependenciesDatabase: []string{"db"},
			DependenciesTable:    []string{"table_materialized_view_3"},
		},
		{
			Key:                  table.Key{Database: "db", Name: "table_materialized_view_3"},
			Engine:               "MaterializedView",
			EngineFull:           "",
			CreateTableQuery:     "CREATE MATERIALIZED VIEW db.table_materialized_view_3 TO db.table_merge_tree_2 AS SELECT * FROM db.input_null_3",
			DependenciesDatabase: nil,
			DependenciesTable:    nil,
		},
		{
			Key:                  table.Key{Database: "db", Name: "input_merge_tree_4"},
			Engine:               "MergeTree",
			EngineFull:           "",
			CreateTableQuery:     "",
			DependenciesDatabase: nil,
			DependenciesTable:    nil,
		},
		{
			Key:                  table.Key{Database: "db", Name: "table_distributed_4"},
			Engine:               "Distributed",
			EngineFull:           "Distributed('cluster', 'db', 'input_merge_tree_4')",
			CreateTableQuery:     "",
			DependenciesDatabase: nil,
			DependenciesTable:    nil,
		},
	}

	tests := []struct {
		name            string
		initialTableKey table.Key
		//inputTables  []table.Info
		wantLinks []Link
	}{
		{
			name:            "Links of input_null",
			initialTableKey: table.Key{Database: "db", Name: "input_null"},
			wantLinks: []Link{
				{
					FromTableKey: table.Key{Database: "db", Name: "input_null"},
					ToTableKey:   table.Key{Database: "db", Name: "table_materialized_view_1"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "input_null"},
					ToTableKey:   table.Key{Database: "db", Name: "table_materialized_view_2"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "table_materialized_view_2"},
					ToTableKey:   table.Key{Database: "db", Name: "table_merge_tree_2"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "table_merge_tree_2"},
					ToTableKey:   table.Key{Database: "db", Name: "table_distributed_2"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "table_materialized_view_3"},
					ToTableKey:   table.Key{Database: "db", Name: "table_merge_tree_2"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "input_null_3"},
					ToTableKey:   table.Key{Database: "db", Name: "table_materialized_view_3"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "table_materialized_view_1"},
					ToTableKey:   table.Key{Database: "db", Name: "table_merge_tree_1"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "table_merge_tree_1"},
					ToTableKey:   table.Key{Database: "db", Name: "table_distributed_1"},
				},
			},
		},
		{
			name:            "Links of input_null_3",
			initialTableKey: table.Key{Database: "db", Name: "input_null_3"},
			wantLinks: []Link{
				{
					FromTableKey: table.Key{Database: "db", Name: "input_null_3"},
					ToTableKey:   table.Key{Database: "db", Name: "table_materialized_view_3"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "table_materialized_view_3"},
					ToTableKey:   table.Key{Database: "db", Name: "table_merge_tree_2"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "table_merge_tree_2"},
					ToTableKey:   table.Key{Database: "db", Name: "table_distributed_2"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "table_materialized_view_2"},
					ToTableKey:   table.Key{Database: "db", Name: "table_merge_tree_2"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "input_null"},
					ToTableKey:   table.Key{Database: "db", Name: "table_materialized_view_2"},
				},
			},
		},
		{
			name:            "Links of table_materialized_view_2",
			initialTableKey: table.Key{Database: "db", Name: "table_materialized_view_2"},
			wantLinks: []Link{
				{
					FromTableKey: table.Key{Database: "db", Name: "table_materialized_view_2"},
					ToTableKey:   table.Key{Database: "db", Name: "table_merge_tree_2"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "input_null"},
					ToTableKey:   table.Key{Database: "db", Name: "table_materialized_view_2"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "table_merge_tree_2"},
					ToTableKey:   table.Key{Database: "db", Name: "table_distributed_2"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "table_materialized_view_3"},
					ToTableKey:   table.Key{Database: "db", Name: "table_merge_tree_2"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "input_null_3"},
					ToTableKey:   table.Key{Database: "db", Name: "table_materialized_view_3"},
				},
			},
		},
		{
			name:            "Links of table_merge_tree_2",
			initialTableKey: table.Key{Database: "db", Name: "table_materialized_view_2"},
			wantLinks: []Link{
				{
					FromTableKey: table.Key{Database: "db", Name: "table_materialized_view_2"},
					ToTableKey:   table.Key{Database: "db", Name: "table_merge_tree_2"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "input_null"},
					ToTableKey:   table.Key{Database: "db", Name: "table_materialized_view_2"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "table_merge_tree_2"},
					ToTableKey:   table.Key{Database: "db", Name: "table_distributed_2"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "table_materialized_view_3"},
					ToTableKey:   table.Key{Database: "db", Name: "table_merge_tree_2"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "input_null_3"},
					ToTableKey:   table.Key{Database: "db", Name: "table_materialized_view_3"},
				},
			},
		},
		{
			name:            "Links of table_distributed_2",
			initialTableKey: table.Key{Database: "db", Name: "table_distributed_2"},
			wantLinks: []Link{
				{
					FromTableKey: table.Key{Database: "db", Name: "table_merge_tree_2"},
					ToTableKey:   table.Key{Database: "db", Name: "table_distributed_2"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "table_materialized_view_2"},
					ToTableKey:   table.Key{Database: "db", Name: "table_merge_tree_2"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "table_materialized_view_3"},
					ToTableKey:   table.Key{Database: "db", Name: "table_merge_tree_2"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "input_null_3"},
					ToTableKey:   table.Key{Database: "db", Name: "table_materialized_view_3"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "input_null"},
					ToTableKey:   table.Key{Database: "db", Name: "table_materialized_view_2"},
				},
			},
		},
		{
			name:            "Links of input_merge_tree_4",
			initialTableKey: table.Key{Database: "db", Name: "input_merge_tree_4"},
			wantLinks: []Link{
				{
					FromTableKey: table.Key{Database: "db", Name: "input_merge_tree_4"},
					ToTableKey:   table.Key{Database: "db", Name: "table_distributed_4"},
				},
			},
		},
		{
			name:            "Links of table_materialized_view_1",
			initialTableKey: table.Key{Database: "db", Name: "table_materialized_view_1"},
			wantLinks: []Link{
				{
					FromTableKey: table.Key{Database: "db", Name: "table_materialized_view_1"},
					ToTableKey:   table.Key{Database: "db", Name: "table_merge_tree_1"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "input_null"},
					ToTableKey:   table.Key{Database: "db", Name: "table_materialized_view_1"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "table_merge_tree_1"},
					ToTableKey:   table.Key{Database: "db", Name: "table_distributed_1"},
				},
			},
		},
		{
			name:            "Links of table_merge_tree_1",
			initialTableKey: table.Key{Database: "db", Name: "table_merge_tree_1"},
			wantLinks: []Link{
				{
					FromTableKey: table.Key{Database: "db", Name: "table_merge_tree_1"},
					ToTableKey:   table.Key{Database: "db", Name: "table_distributed_1"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "table_materialized_view_1"},
					ToTableKey:   table.Key{Database: "db", Name: "table_merge_tree_1"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "input_null"},
					ToTableKey:   table.Key{Database: "db", Name: "table_materialized_view_1"},
				},
			},
		},
		{
			name:            "Links of table_distributed_1",
			initialTableKey: table.Key{Database: "db", Name: "table_distributed_1"},
			wantLinks: []Link{
				{
					FromTableKey: table.Key{Database: "db", Name: "table_merge_tree_1"},
					ToTableKey:   table.Key{Database: "db", Name: "table_distributed_1"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "table_materialized_view_1"},
					ToTableKey:   table.Key{Database: "db", Name: "table_merge_tree_1"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "input_null"},
					ToTableKey:   table.Key{Database: "db", Name: "table_materialized_view_1"},
				},
			},
		},
		{
			name:            "Links of table_materialized_view_3",
			initialTableKey: table.Key{Database: "db", Name: "table_materialized_view_3"},
			wantLinks: []Link{
				{
					FromTableKey: table.Key{Database: "db", Name: "table_materialized_view_3"},
					ToTableKey:   table.Key{Database: "db", Name: "table_merge_tree_2"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "input_null_3"},
					ToTableKey:   table.Key{Database: "db", Name: "table_materialized_view_3"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "table_merge_tree_2"},
					ToTableKey:   table.Key{Database: "db", Name: "table_distributed_2"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "table_materialized_view_2"},
					ToTableKey:   table.Key{Database: "db", Name: "table_merge_tree_2"},
				},
				{
					FromTableKey: table.Key{Database: "db", Name: "input_null"},
					ToTableKey:   table.Key{Database: "db", Name: "table_materialized_view_2"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := New()
			for _, tableInfo := range tables {
				b.AddTable(tableInfo)
			}
			got, err := b.TableLinks(tt.initialTableKey)
			if err != nil {
				t.Errorf("LinksBuilder.TableLinks() error = %v", err)
				return
			}
			if !slices.Equal(got.Links, tt.wantLinks) {
				t.Errorf("LinksBuilder.TableLinks() =\n %v, \nWant =\n %v", got.Links, tt.wantLinks)
			}
		})

	}
}

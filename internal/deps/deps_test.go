package deps

import (
	"github.com/mbaksheev/clickhouse-table-graph/table"
	"testing"
)

func TestFromDistributedEngine(t *testing.T) {
	tests := []struct {
		name       string
		fullEngine string
		want       []table.Key
	}{
		{
			name:       "valid distributed engine",
			fullEngine: "Distributed('cluster', 'db', 'table')",
			want: []table.Key{
				{Database: "db", Name: "table"},
			},
		},
		{
			name:       "invalid distributed engine",
			fullEngine: "Distributed('cluster', 'db', 'table', 'sharding_key')",
			want: []table.Key{
				{Database: "db", Name: "table"},
			},
		},
		{
			name:       "invalid distributed engine",
			fullEngine: "Distributed('cluster', 'db', 'table', 'sharding_key', 'policy_name')",
			want: []table.Key{
				{Database: "db", Name: "table"},
			},
		},
		{
			name:       "invalid distributed engine",
			fullEngine: "Distributed('cluster', 'db')",
			want:       []table.Key{},
		},
		{
			name:       "empty string",
			fullEngine: "",
			want:       []table.Key{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FromDistributedEngine(tt.fullEngine); !equal(got, tt.want) {
				t.Errorf("FromDistributedEngine() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFromCreateQuery(t *testing.T) {
	tests := []struct {
		name        string
		createQuery string
		want        []table.Key
	}{
		{
			name:        "valid materialized view",
			createQuery: "CREATE MATERIALIZED VIEW view TO db.table AS SELECT * FROM source",
			want: []table.Key{
				{Database: "db", Name: "table"},
			},
		},
		{
			name:        "invalid materialized view",
			createQuery: "CREATE MATERIALIZED VIEW view TO db",
			want:        []table.Key{},
		},
		{
			name:        "empty string",
			createQuery: "",
			want:        []table.Key{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FromCreateQuery(tt.createQuery); !equal(got, tt.want) {
				t.Errorf("FromCreateQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJoinedTablesFromCreateQuery(t *testing.T) {
	tests := []struct {
		name        string
		createQuery string
		want        []table.Key
	}{
		{
			name:        "valid materialized view with JOIN and with AS",
			createQuery: "CREATE MATERIALIZED VIEW db.view TO tb.table AS SELECT * FROM db.table_a as a JOIN db.table_b as b ON a.id = b.id;",
			want: []table.Key{
				{Database: "db", Name: "table_b"},
			},
		},
		{
			name:        "valid materialized view with JOIN and without AS",
			createQuery: "CREATE MATERIALIZED VIEW db.view TO tb.table AS SELECT * FROM db.table_a JOIN db.table_b ON db.table_a.id =  db.table_b.id;",
			want: []table.Key{
				{Database: "db", Name: "table_b"},
			},
		},
		{
			name:        "valid materialized view with multiple JOINs and AS",
			createQuery: "CREATE MATERIALIZED VIEW db.view TO tb.table AS SELECT * FROM db.table_a as a JOIN db.table_b as b ON a.id = b.id JOIN db.table_c as c ON a.id = c.id;",
			want: []table.Key{
				{Database: "db", Name: "table_b"},
				{Database: "db", Name: "table_c"},
			},
		},
		{
			name:        "valid materialized view with multiple JOINs without AS",
			createQuery: "CREATE MATERIALIZED VIEW db.view TO tb.table AS SELECT * FROM db.table_a JOIN db.table_b ON db.table_a.id = db.table_b.id JOIN db.table_c ON db.table_a.id = db.table_c.id;",
			want: []table.Key{
				{Database: "db", Name: "table_b"},
				{Database: "db", Name: "table_c"},
			},
		},
		{
			name:        "invalid materialized view",
			createQuery: "CREATE MATERIALIZED VIEW view TO db.table AS SELECT * FROM source JOIN db2",
			want:        []table.Key{},
		},
		{
			name:        "empty string",
			createQuery: "",
			want:        []table.Key{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := JoinedTablesFromCreateQuery(tt.createQuery); !equal(got, tt.want) {
				t.Errorf("JoinedTablesFromCreateQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDictionariesFromCreateQuery(t *testing.T) {
	tests := []struct {
		name        string
		createQuery string
		want        []table.Key
	}{
		{
			name:        "materialized view with dictionaries",
			createQuery: "CREATE MATERIALIZED VIEW db.view TO db.table AS SELECT col_a, dictGet('dict_1', 'dict_key_1', t.col_b), dictGet('dict_db.dict_2', 'dict_key_1', t.col_b), dictGetOrNull('dict_3', 'dict_key_1', t.col_b) as col_c, dictGetOrDefault('dict_4', 'dict_key_1', t.col_b, 'default') as col_d, dictIsIn('dict_5', 'foo', 'bar') FROM db.table_a;",
			want: []table.Key{
				{Database: "default", Name: "dict_1"},
				{Database: "dict_db", Name: "dict_2"},
				{Database: "default", Name: "dict_3"},
				{Database: "default", Name: "dict_4"},
				{Database: "default", Name: "dict_5"},
			},
		},
		{
			name:        "empty string",
			createQuery: "",
			want:        []table.Key{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DictionariesFromCreateQuery(tt.createQuery); !equal(got, tt.want) {
				t.Errorf("DictionariesFromCreateQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFromDependencies(t *testing.T) {
	tests := []struct {
		name                 string
		dependenciesDatabase []string
		dependenciesTable    []string
		want                 []table.Key
	}{
		{
			name:                 "valid dependencies",
			dependenciesDatabase: []string{"db1", "db2"},
			dependenciesTable:    []string{"table1", "table2"},
			want: []table.Key{
				{Database: "db1", Name: "table1"},
				{Database: "db2", Name: "table2"},
			},
		},
		{
			name:                 "empty dependencies",
			dependenciesDatabase: []string{},
			dependenciesTable:    []string{},
			want:                 []table.Key{},
		},
		{
			name:                 "mismatched dependencies database",
			dependenciesDatabase: []string{"db1"},
			dependenciesTable:    []string{"table1", "table2"},
			want: []table.Key{
				{Database: "db1", Name: "table1"},
			},
		},
		{
			name:                 "mismatched dependencies table",
			dependenciesDatabase: []string{"db1", "db2"},
			dependenciesTable:    []string{"table1"},
			want: []table.Key{
				{Database: "db1", Name: "table1"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FromDependencies(tt.dependenciesDatabase, tt.dependenciesTable); !equal(got, tt.want) {
				t.Errorf("FromDependencies() = %v, want %v", got, tt.want)
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

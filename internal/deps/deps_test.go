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

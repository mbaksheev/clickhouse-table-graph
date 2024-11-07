package clickhouse

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/mbaksheev/clickhouse-table-graph/table"
)

type Server struct {
	Address  string
	Username string
	Password string
}

func (ch *Server) String() string {
	return fmt.Sprintf("Clickhouse server [Address=%s, Username=%s", ch.Address, ch.Username)
}

func (ch *Server) TableInfos() ([]table.Info, error) {
	const query = `
SELECT database, name, engine, engine_full, create_table_query, as_select, dependencies_database, dependencies_table 
FROM system.tables 
WHERE database NOT IN ('INFORMATION_SCHEMA','information_schema', 'system')`

	conn, err := connect(ch)
	if err != nil {
		return nil, fmt.Errorf("TableInfos: failed to connect to clickhouse server: %s, %w", ch.Address, err)
	}
	defer conn.Close()
	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("TableInfos: failed to execute query: %s, %w", query, err)
	}

	tables := make([]table.Info, 0, 100)

	for rows.Next() {
		t := table.Info{}
		err := rows.Scan(&t.Database, &t.Name, &t.Engine, &t.EngineFull, &t.CreateTableQuery, &t.AsSelect, &t.DependenciesDatabase, &t.DependenciesTable)
		if err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return tables, nil
}

func connect(ch *Server) (driver.Conn, error) {
	var ctx = context.Background()
	var conn, err = clickhouse.Open(&clickhouse.Options{
		Addr: []string{ch.Address},
		Auth: clickhouse.Auth{
			Database: "system",
			Username: ch.Username,
			Password: ch.Password,
		},
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "clickhouse-table-graph"},
			},
		},
	})

	if err != nil {
		return nil, err
	}
	if err = conn.Ping(ctx); err != nil {
		return nil, err
	}
	return conn, nil
}

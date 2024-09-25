package clickhouse

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/mbaksheev/clickhouse-table-graph/table"
)

type Server struct {
	Address string
}

func (ch *Server) QuerySystemTables() ([]table.Info, error) {
	const query = `
SELECT database, name, engine, engine_full, create_table_query, as_select, dependencies_database, dependencies_table 
FROM system.tables 
WHERE database NOT IN ('INFORMATION_SCHEMA','information_schema', 'system')`

	var tables []table.Info
	conn, err := connect(ch)
	if err != nil {
		return tables, err
	}
	defer conn.Close()
	tableCount, err := countTables(&conn)
	if err != nil {
		return tables, err
	}
	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return tables, err
	}

	tables = make([]table.Info, 0, tableCount)

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

func countTables(connection *driver.Conn) (uint64, error) {
	const query = "SELECT COUNT() FROM system.tables"
	row := (*connection).QueryRow(context.Background(), query)
	var count uint64
	if err := row.Scan(&count); err != nil {
		return 0, err
	}

	return count, nil
}

func connect(ch *Server) (driver.Conn, error) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{ch.Address},
			Auth: clickhouse.Auth{
				Database: "system",
				Username: "",
				Password: "",
			},
			ClientInfo: clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "an-example-go-client", Version: "0.1"},
				},
			},

			Debugf: func(format string, v ...interface{}) {
				fmt.Printf(format, v)
			},
		})
	)

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, nil
}

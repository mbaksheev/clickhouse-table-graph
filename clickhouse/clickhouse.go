// Package clickhouse provides a client to get tables information from Clickhouse server.
//
// Use [*Server.TableInfos] method to get the list of tables from the Clickhouse server.
package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/mbaksheev/clickhouse-table-graph/table"
)

// Server represents a Clickhouse server connection information.
type Server struct {
	// Address is the address of the Clickhouse server in format "host:port".
	Address string
	// Username is the username to connect to the Clickhouse server.
	Username string
	// Password is the password to connect to the Clickhouse server.
	Password string
	// Secure indicates whether to use TLS for the connection.
	Secure bool
	// SkipTLSVerify indicates whether to skip TLS verification.
	SkipTLSVerify bool
}

// String returns a string representation of the Clickhouse server.
func (ch *Server) String() string {
	return fmt.Sprintf("Clickhouse server [Address=%s, Username=%s", ch.Address, ch.Username)
}

// TableInfos returns the list of tables from the Clickhouse server.
// This function queries system.tables table to get the tables' information.
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

	chOptions := &clickhouse.Options{
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
	}

	// If Secure is true, use TLS
	if ch.Secure {
		chOptions.TLS = &tls.Config{
			InsecureSkipVerify: ch.SkipTLSVerify,
		}
	}

	var conn, err = clickhouse.Open(chOptions)

	if err != nil {
		return nil, err
	}
	if err = conn.Ping(ctx); err != nil {
		return nil, err
	}
	return conn, nil
}

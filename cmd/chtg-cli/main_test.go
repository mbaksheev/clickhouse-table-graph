package main

import (
	"context"
	"github.com/mbaksheev/clickhouse-table-graph/clickhouse"
	"github.com/testcontainers/testcontainers-go"
	tcClickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	"log"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreateTableGraph(t *testing.T) {
	ctx := context.Background()
	user := "integration_test"
	password := "password"

	clickHouseContainer, err := tcClickhouse.Run(ctx,
		"clickhouse/clickhouse-server:24.8.6.70-alpine",
		tcClickhouse.WithUsername(user),
		tcClickhouse.WithPassword(password),
		tcClickhouse.WithInitScripts(filepath.Join("main_test_data", "test-db.sql")),
	)
	defer func() {
		if err := testcontainers.TerminateContainer(clickHouseContainer); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		log.Printf("failed to start container: %s", err)
		return
	}
	t.Run("TestCreateTableGraph", func(t *testing.T) {
		chHost, err := clickHouseContainer.ConnectionHost(ctx)
		if err != nil {
			t.Errorf("failed to get container host: %s", err)
		}

		chServer := clickhouse.Server{
			Address:  chHost,
			Username: user,
			Password: password,
		}

		mermaid, err := createTableGraph(chServer, "test_db", "target_table_mv", MermaidMarkdown)
		if err != nil {
			t.Errorf("failed to create table graph: %s", err)
		}
		if !strings.Contains(mermaid, "flowchart") {
			t.Errorf("invalid mermaid result. Expected 'flowchart' in result")
		}
		expectedTables := []string{
			"test_db.input_table (Null)",
			"test_db.target_table_mv (MaterializedView)",
			"test_db.target_table (ReplacingMergeTree)",
		}
		for _, actualTable := range expectedTables {
			if !strings.Contains(mermaid, actualTable) {
				t.Errorf("expected table '%s' not found in mermaid result", actualTable)
			}
		}
	})
}

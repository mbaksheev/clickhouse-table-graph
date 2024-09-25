package main

import (
	"flag"
	"fmt"
	"github.com/mbaksheev/clickhouse-table-graph/clickhouse"
	"github.com/mbaksheev/clickhouse-table-graph/graph"
	"github.com/mbaksheev/clickhouse-table-graph/table"
	"log"
)

var chServer = flag.String("clickhouse-server", "localhost:9000", "Clickhouse URL")

func main() {
	flag.Parse()
	fmt.Println("Clickhouse table graph")
	fmt.Println("clickhouse-server:", *chServer)

	ch := clickhouse.Server{Address: *chServer}
	tables, err := ch.QuerySystemTables()
	if err != nil {
		log.Fatal(err)
	}

	myTableGraph := graph.New()

	for _, table := range tables {
		myTableGraph.AddTable(table)
	}

	fmt.Println("Graph:")
	links, err := myTableGraph.Graph(table.Key{Database: "tree", Name: "mid_table"})
	if err != nil {
		log.Fatal(err)
	}
	for _, link := range links {
		fmt.Printf("%s -> %s\n", link.FromTable.Key, link.ToTable)
	}
}

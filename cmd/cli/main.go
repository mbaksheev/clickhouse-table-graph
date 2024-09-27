package main

import (
	"flag"
	"fmt"
	"github.com/mbaksheev/clickhouse-table-graph/clickhouse"
	"github.com/mbaksheev/clickhouse-table-graph/graph"
	"github.com/mbaksheev/clickhouse-table-graph/mermaid"
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

	fmt.Println("Links:")
	tGraph, err := myTableGraph.Build(table.Key{Database: "tree", Name: "mid_table"})
	if err != nil {
		log.Fatal(err)
	}
	for _, link := range tGraph.Links {
		fmt.Printf("%s -> %s\n", link.FromTable.Key, link.ToTable)
	}

	mermaidFlowchart := mermaid.Flowchart(*tGraph, mermaid.FlowchartOptions{Orientation: mermaid.TB})
	fmt.Printf("Mermaid flowchart:\n%s", mermaidFlowchart)

	html := mermaid.Html(mermaidFlowchart, mermaid.HtmlOptions{})

	fmt.Printf("Mermaid html:\n%s", html)
}

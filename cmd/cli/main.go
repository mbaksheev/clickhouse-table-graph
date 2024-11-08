package main

import (
	"fmt"
	"github.com/mbaksheev/clickhouse-table-graph/graph"
	"github.com/mbaksheev/clickhouse-table-graph/mermaid"
	"github.com/mbaksheev/clickhouse-table-graph/table"
	"log"
	"os"
)

func main() {
	options, err := parseFlags()
	handleError(err)

	ch := options.clickhouseServer
	tables, err := ch.TableInfos()
	handleError(err)
	myTableGraph := graph.New()
	for _, t := range tables {
		myTableGraph.AddTable(t)
	}
	log.Printf("Creating graph for table %s.%s\n", options.clickhouseDatabase, options.clickhouseTable)
	tGraph, err := myTableGraph.Build(table.Key{Database: options.clickhouseDatabase, Name: options.clickhouseTable})
	handleError(err)

	mermaidFlowchart := mermaid.Flowchart(*tGraph, mermaid.FlowchartOptions{Orientation: mermaid.TB, IncludeEngine: true})
	var result string
	if options.outputFormat == MermaidMarkdown {
		result = mermaidFlowchart
	} else {
		result = mermaid.Html(mermaidFlowchart, mermaid.HtmlOptions{})
	}

	if options.outputMode == Stdout {
		log.Println("\n" + result)
	} else {
		handleError(saveToFile(options.outputFile, result))
	}
}

func saveToFile(fileName, result string) error {
	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("saveToFile: failed to create file: %s, %w", fileName, err)
	}
	defer file.Close()
	_, err = file.WriteString(result)
	if err != nil {
		return fmt.Errorf("saveToFile: failed to write to file: %s, %w", fileName, err)
	}
	return nil
}

func handleError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

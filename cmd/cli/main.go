package main

import (
	"fmt"
	"github.com/mbaksheev/clickhouse-table-graph/clickhouse"
	"github.com/mbaksheev/clickhouse-table-graph/graph"
	"github.com/mbaksheev/clickhouse-table-graph/mermaid"
	"github.com/mbaksheev/clickhouse-table-graph/table"
	"log"
	"os"
)

func main() {
	options, err := parseFlags()
	handleError(err)
	log.Printf("Creating graph for table %s.%s\n", options.clickhouseDatabase, options.clickhouseTable)
	result, err := createTableGraph(options.clickhouseServer, options.clickhouseDatabase, options.clickhouseTable, options.outputFormat)
	handleError(err)
	if options.outputMode == Stdout {
		log.Println("\n" + result)
	} else {
		handleError(saveToFile(options.outputFile, result))
	}
}

func createTableGraph(ch clickhouse.Server, chDatabase, chTable string, format outputFormat) (string, error) {
	tables, err := ch.TableInfos()
	if err != nil {
		return "", err
	}
	myTableGraph := graph.New()
	for _, t := range tables {
		myTableGraph.AddTable(t)
	}
	tGraph, err := myTableGraph.Build(table.Key{Database: chDatabase, Name: chTable})
	if err != nil {
		return "", err
	}

	mermaidFlowchart := mermaid.Flowchart(*tGraph, mermaid.FlowchartOptions{Orientation: mermaid.TB, IncludeEngine: true})
	var result string
	if format == MermaidMarkdown {
		result = mermaidFlowchart
	} else {
		result = mermaid.Html(mermaidFlowchart, mermaid.HtmlOptions{})
	}

	return result, nil
}

func saveToFile(fileName, result string) error {
	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("saveToFile: failed to create file: %s, %w", fileName, err)
	}
	defer file.Close()
	_, err = file.WriteString(result)
	log.Println("Graph saved to file: " + fileName)
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

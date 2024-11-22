// Package main provides the main entry point for the CLI application.
//
// The main function parses the command line arguments,
// creates a graph of tables, and saves it to the specified output file or outputs it to the console depending on the specified options:
//
//   - --clickhouse-host: the address of the ClickHouse server.
//   - --clickhouse-port: the port of the ClickHouse server.
//   - --clickhouse-user: the name of the ClickHouse user.
//   - --clickhouse-table: the name of the ClickHouse table in format database.table.
//   - --out-format: the output format. Possible options: "mermaid-html" - to generate full html document for displaying chart which can be opened in browser or "mermaid-md" - to generate only mermaid markdown diagram.
//   - --out-file: the name of the output file. Optional. If not specified, the output will be printed to the console.
//
// Note: The command will ask for the ClickHouse password for the specified user.
//
// For example command:
//
//	go run . --clickhouse-host=localhost --clickhouse-port=9000 --clickhouse-user=test_user --clickhouse-table=test_db.test_table --out-format=mermaid-html --out-file=output.html
//
// will do the following:
//  1. ask for ClickHouse password for the clickhouse server on localhost:9000;
//  2. connect to the ClickHouse server on localhost:9000 with the test_user and the provided password;
//  3. fetch the list of all tables (which can be accessed by the user) from the system.tables;
//  4. create a graph of tables connected to the specified table test_db.test_table;
//  5. export graph to the mermaid html format;
//  6. save the exported mermaid html to output.html file;
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
	tableLinks, err := myTableGraph.TableLinks(table.Key{Database: chDatabase, Name: chTable})
	if err != nil {
		return "", err
	}

	mermaidFlowchart := mermaid.Flowchart(*tableLinks, mermaid.FlowchartOptions{Orientation: mermaid.TB, IncludeEngine: true})
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
	log.Println("Links saved to file: " + fileName)
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

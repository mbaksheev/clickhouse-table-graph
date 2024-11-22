// Package main provides the main entry point for the CLI application.
//
// The main function parses the command line arguments,
// creates a graph of tables, and saves it to the specified output file or outputs it to the console depending on the specified options:
//
//   - --clickhouse-host string - Clickhouse host to get tables from. Optional. Default value is "localhost"
//   - --clickhouse-port string - Clickhouse port. Optional. Default value 9000
//   - --clickhouse-table string - Clickhouse full table name in format <database>.<table> to get dependencies for. Required.
//   - --clickhouse-user string - Clickhouse username. Optional. Default value is "" (empty string)
//   - --out-file string - Output file name. Optional. If not specified, the output will be printed to the console.
//   - --out-format string - Output format. Default value "mermaid-html". Possible values: "mermaid-html", "mermaid-md".
//   - --mermaid-theme - Mermaid theme. Optional. Default value is 'default'. See https://mermaid-js.github.io/mermaid/#/theming
//   - --table-highlight-color - Highlight color for the selected clickhouse table. E.g. '#ff5757' or 'red' Optional. If not specified, the table will not be highlighted. See https://mermaid.js.org/syntax/flowchart.html?id=flowcharts-basic-syntax#styling-a-node
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
	result, err := createTableGraph(options)
	handleError(err)
	if options.outputMode == Stdout {
		log.Println("\n" + result)
	} else {
		handleError(saveToFile(options.outputFile, result))
	}
}

func createTableGraph(options inputOptions) (string, error) {
	tables, err := options.clickhouseServer.TableInfos()
	if err != nil {
		return "", err
	}
	myTableGraph := graph.New()
	for _, t := range tables {
		myTableGraph.AddTable(t)
	}
	tableLinks, err := myTableGraph.TableLinks(table.Key{Database: options.clickhouseDatabase, Name: options.clickhouseTable})
	if err != nil {
		return "", err
	}

	mermaidFlowchart := mermaid.Flowchart(*tableLinks, mermaid.FlowchartOptions{
		Orientation:                mermaid.TB,
		IncludeEngine:              true,
		Theme:                      options.mermaidTheme,
		InitialTableHighlightColor: options.tableHighlightColor,
	})
	var result string
	if options.outputFormat == MermaidMarkdown {
		result = mermaidFlowchart
	} else {
		result = mermaid.Html(mermaidFlowchart, mermaid.HtmlOptions{
			Title: fmt.Sprintf("ClickHouse table dependencies graph for %s.%s", options.clickhouseDatabase, options.clickhouseTable),
		})
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

package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/mbaksheev/clickhouse-table-graph/clickhouse"
	"golang.org/x/crypto/ssh/terminal"
)

type outputFormat int

const (
	MermaidHtml outputFormat = iota
	MermaidMarkdown
)

type outputMode int

const (
	Stdout outputMode = iota
	File
)

var (
	chHost              = flag.String("clickhouse-host", "localhost", "ClickHouse host to get tables from. Optional.")
	chPort              = flag.String("clickhouse-port", "9000", "ClickHouse port. Optional.")
	chUsername          = flag.String("clickhouse-user", "", "ClickHouse username. Optional. If not provided, the default value is empty string.")
	chTable             = flag.String("clickhouse-table", "", "ClickHouse full table name in format <database>.<table> to get dependencies for. Required.")
	outFormat           = flag.String("out-format", "mermaid-html", "Output format. Possible options: 'mermaid-html' - to generate full html document for displaying chart which can be opened in browser or 'mermaid-md' - to generate only mermaid markdown diagram.")
	outFile             = flag.String("out-file", "", "Output file name. Optional. If not specified, the output will be printed to the console.")
	mermaidTheme        = flag.String("mermaid-theme", "", "Mermaid theme. Optional. Default value is 'default'. See https://mermaid-js.github.io/mermaid/#/theming")
	tableHighlightColor = flag.String("table-highlight-color", "", "Highlight color for the selected clickhouse table. E.g. '#ff5757' or 'red' Optional. If not specified, the table will not be highlighted. See https://mermaid.js.org/syntax/flowchart.html?id=flowcharts-basic-syntax#styling-a-node")
	chSecure            = flag.Bool("secure", false, "Use secure connection to ClickHouse. Optional. Default value is false.")
	chSkipTLSVerify     = flag.Bool("skip-tls-verify", false, "Skip TLS verification. Optional. Default value is false.")
)

type inputOptions struct {
	clickhouseServer    clickhouse.Server
	clickhouseTable     string
	clickhouseDatabase  string
	secure              string
	skipTLSVerify       string
	outputFormat        outputFormat
	outputMode          outputMode
	outputFile          string
	mermaidTheme        string
	tableHighlightColor string
}

func parseFlags() (inputOptions, error) {
	flag.Parse()
	password, err := askForPassword()
	if err != nil {
		return inputOptions{}, fmt.Errorf("parseFlags: Error while asking for password: %w", err)
	}
	chServer := clickhouse.Server{
		Address:       fmt.Sprintf("%s:%s", *chHost, *chPort),
		Username:      *chUsername,
		Password:      *password,
		Secure:        *chSecure,
		SkipTLSVerify: *chSkipTLSVerify,
	}

	var inputOpts inputOptions
	inputOpts.clickhouseServer = chServer
	if *chTable != "" {
		tableNameParts := strings.Split(*chTable, ".")
		if len(tableNameParts) != 2 {
			return inputOptions{}, fmt.Errorf("parseFlags: Incorrect table name format: '%s'. Clickhouse table should be in format <database>.<table>", *chTable)
		}
		inputOpts.clickhouseDatabase = tableNameParts[0]
		inputOpts.clickhouseTable = tableNameParts[1]
	} else {
		return inputOptions{}, fmt.Errorf("parseFlags: Incorrect table name. Clickhouse table is required")
	}

	switch *outFormat {
	case "mermaid-html":
		inputOpts.outputFormat = MermaidHtml
	case "mermaid-md":
		inputOpts.outputFormat = MermaidMarkdown
	default:
		return inputOptions{}, fmt.Errorf("parseFlags: unknown output format: %s", *outFormat)
	}
	if *outFile != "" {
		inputOpts.outputMode = File
		inputOpts.outputFile = *outFile
	} else {
		inputOpts.outputMode = Stdout
	}
	inputOpts.mermaidTheme = *mermaidTheme
	inputOpts.tableHighlightColor = *tableHighlightColor
	return inputOpts, nil
}

func askForPassword() (*string, error) {
	fmt.Print("Enter Password: ")
	bytePassword, err := terminal.ReadPassword(0)
	if err != nil {
		return nil, fmt.Errorf("askForPassword: Error while reading password: %w", err)
	}
	password := string(bytePassword)
	return &password, nil
}

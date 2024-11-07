package main

import (
	"flag"
	"fmt"
	"github.com/mbaksheev/clickhouse-table-graph/clickhouse"
	"strings"
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
	chHost     = flag.String("clickhouse-host", "localhost", "Clickhouse host to get tables from")
	chPort     = flag.String("clickhouse-port", "9000", "Clickhouse port")
	chUsername = flag.String("clickhouse-user", "", "Clickhouse username")
	chPassword = flag.String("clickhouse-password", "", "Clickhouse password")
	chTable    = flag.String("clickhouse-table", "", "Clickhouse full table name in format <database>.<table> to get dependencies for")
	outFormat  = flag.String("out-format", "mermaid-html", "Output format")
	outFile    = flag.String("out-file", "", "Output file name")
)

type inputOptions struct {
	clickhouseServer   clickhouse.Server
	clickhouseTable    string
	clickhouseDatabase string
	outputFormat       outputFormat
	outputMode         outputMode
	outputFile         string
}

func parseFlags() (inputOptions, error) {
	flag.Parse()
	chServer := clickhouse.Server{
		Address:  fmt.Sprintf("%s:%s", *chHost, *chPort),
		Username: *chUsername,
		Password: *chPassword,
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
	return inputOpts, nil
}

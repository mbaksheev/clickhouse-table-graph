package main

import (
	"flag"
	"fmt"
	"github.com/mbaksheev/clickhouse-table-graph/clickhouse"
	"golang.org/x/crypto/ssh/terminal"
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
	chHost     = flag.String("clickhouse-host", "localhost", "Clickhouse host to get tables from. Optional.")
	chPort     = flag.String("clickhouse-port", "9000", "Clickhouse port. Optional.")
	chUsername = flag.String("clickhouse-user", "", "Clickhouse username. Optional. If not provided, the default value is empty string.")
	chTable    = flag.String("clickhouse-table", "", "Clickhouse full table name in format <database>.<table> to get dependencies for. Required.")
	outFormat  = flag.String("out-format", "mermaid-html", "Output format. Possible options: 'mermaid-html' - to generate full html document for displaying chart which can be opened in browser or 'mermaid-md' - to generate only mermaid markdown diagram.")
	outFile    = flag.String("out-file", "", "Output file name. Optional. If not specified, the output will be printed to the console.")
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
	password, err := askForPassword()
	if err != nil {
		return inputOptions{}, fmt.Errorf("parseFlags: Error while asking for password: %w", err)
	}
	chServer := clickhouse.Server{
		Address:  fmt.Sprintf("%s:%s", *chHost, *chPort),
		Username: *chUsername,
		Password: *password,
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

func askForPassword() (*string, error) {
	fmt.Print("Enter Password: ")
	bytePassword, err := terminal.ReadPassword(0)
	if err != nil {
		return nil, fmt.Errorf("askForPassword: Error while reading password: %w", err)
	}
	password := string(bytePassword)
	return &password, nil
}

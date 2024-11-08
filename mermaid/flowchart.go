package mermaid

import (
	"github.com/mbaksheev/clickhouse-table-graph/graph"
	"github.com/mbaksheev/clickhouse-table-graph/table"
	"strings"
)

type Orientation int

const (
	TB Orientation = iota
	BT
	LR
	RL
)

func (o Orientation) name() string {
	return [...]string{"TB", "BT", "LR", "RL"}[o]
}

type nodeShape int

const (
	rectangle nodeShape = iota
	rounded
	stackedRectangle
	hexagon
	notchRectangle
)

func (ns nodeShape) name() string {
	return [...]string{"rect", "rounded", "st-rect", "hex", "notch-rect"}[ns]
}

type FlowchartOptions struct {
	Orientation   Orientation
	IncludeEngine bool
}

func Flowchart(tableGraph graph.Graph, options FlowchartOptions) string {
	orientation := options.Orientation.name()

	var mermaid strings.Builder
	mermaid.WriteString("flowchart " + orientation + "\n")
	mermaid.WriteString("%%{init: {'theme':'neutral'}}%%\n")
	for _, link := range tableGraph.Links {

		fromTableInfo, fromExists := tableGraph.TableInfo(link.FromTableKey)
		if !fromExists {
			writeInvalidNode(&mermaid, link.FromTableKey, options)
		} else {
			writeValidNode(&mermaid, fromTableInfo, options)
		}

		writeLink(&mermaid)

		toTableInfo, toExists := tableGraph.TableInfo(link.ToTableKey)
		if !toExists {
			writeInvalidNode(&mermaid, link.ToTableKey, options)
		} else {
			writeValidNode(&mermaid, toTableInfo, options)
		}
		mermaid.WriteString("\n")
	}
	return mermaid.String()
}

func writeValidNode(stringBuildr *strings.Builder, tableInfo table.Info, options FlowchartOptions) {
	stringBuildr.WriteString(tableInfo.Key.String())
	stringBuildr.WriteString("@{ shape: ")
	stringBuildr.WriteString(shapeOf(tableInfo))
	stringBuildr.WriteString(", label: \"")
	writeNodeLabel(stringBuildr, tableInfo, options)
	stringBuildr.WriteString("\" }")
}

func writeInvalidNode(stringBuildr *strings.Builder, tableKey table.Key, options FlowchartOptions) {
	stringBuildr.WriteString(tableKey.String())
	stringBuildr.WriteString("@{ shape: ")
	stringBuildr.WriteString(notchRectangle.name())
	stringBuildr.WriteString(", label: \"")
	stringBuildr.WriteString(tableKey.String())
	stringBuildr.WriteString(" (table does not exist)")
	stringBuildr.WriteString("\" }")
}

func shapeOf(tableInfo table.Info) string {
	var shape nodeShape
	switch tableInfo.Engine {
	case "MaterializedView":
		shape = hexagon
	case "Distributed":
		shape = stackedRectangle
	case "Null":
		shape = rounded
	default:
		shape = rectangle
	}
	return shape.name()
}

func writeNodeLabel(stringBuildr *strings.Builder, tableInfo table.Info, options FlowchartOptions) {
	stringBuildr.WriteString(tableInfo.Key.String())
	if options.IncludeEngine {
		stringBuildr.WriteString(" (")
		stringBuildr.WriteString(tableInfo.Engine)
		stringBuildr.WriteString(")")
	}
}

func writeLink(stringBuildr *strings.Builder) {
	stringBuildr.WriteString(" --> ")
}

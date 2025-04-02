// Package mermaid provides functionality to generate Mermaid flowchart diagrams.
//
// Use [Flowchart] function to generate a Mermaid flowchart diagram from the specified [graph.Links].
//
//	md := mermaid.Flowchart(*tableLinks, mermaid.FlowchartOptions{Orientation: mermaid.TB, IncludeEngine: true})
//
// Use [Html] function to generate a Mermaid HTML from the specified Mermaid string.
//
//	html := mermaid.Html(md, mermaid.HtmlOptions{})
package mermaid

import (
	"github.com/mbaksheev/clickhouse-table-graph/graph"
	"github.com/mbaksheev/clickhouse-table-graph/table"
	"strings"
)

// Orientation represents the orientation of the flowchart graph.
type Orientation int

// Possible values for the [Orientation] type.
const (
	// TB is a top to bottom orientation.
	TB Orientation = iota
	// BT is a bottom to top orientation.
	BT
	// LR is a left to right orientation.
	LR
	// RL is a right to left orientation.
	RL
)

// name returns the textual name of the [Orientation] in order to use it in the chart.
func (o Orientation) name() string {
	return [...]string{"TB", "BT", "LR", "RL"}[o]
}

// nodeShape represents the shape of the node in the flowchart.
type nodeShape int

// Possible values for the [nodeShape] type.
const (
	rectangle nodeShape = iota
	rounded
	stackedRectangle
	hexagon
	notchRectangle
	winPane
)

// name returns the textual name of the [nodeShape] in order to use it in the chart.
func (ns nodeShape) name() string {
	return [...]string{"rect", "rounded", "st-rect", "hex", "notch-rect", "win-pane"}[ns]
}

// FlowchartOptions represents the options for the flowchart diagram.
type FlowchartOptions struct {
	// Orientation is the orientation of the flowchart graph.
	Orientation Orientation
	// IncludeEngine is a flag to include the engine information in the node label. When true, the engine information is included.
	IncludeEngine bool
	// Theme is the theme of the flowchart diagram.
	// E.g. "neutral", "dark". The default value is "default". See https://mermaid.js.org/config/theming.html
	Theme string
	// InitialTableHighlightColor is the color of the node border for the initial table in the flowchart diagram.
	// E.g. "#ff8585", "red". If not specified, the node is not highlighted.
	InitialTableHighlightColor string
}

// Flowchart generates a Mermaid flowchart diagram from the specified [graph.Links].
func Flowchart(graphLinks graph.Links, options FlowchartOptions) string {
	orientation := options.Orientation.name()

	var mermaid strings.Builder
	mermaid.WriteString("flowchart " + orientation + "\n")
	mermaid.WriteString("%%{init: {'theme':'" + options.Theme + "'}}%%\n")
	for _, link := range graphLinks.Links {

		fromTableInfo, fromExists := graphLinks.TableInfo(link.FromTableKey)
		if !fromExists {
			writeInvalidNode(&mermaid, link.FromTableKey)
		} else {
			writeValidNode(&mermaid, fromTableInfo, options)
		}

		writeLink(&mermaid)

		toTableInfo, toExists := graphLinks.TableInfo(link.ToTableKey)
		if !toExists {
			writeInvalidNode(&mermaid, link.ToTableKey)
		} else {
			writeValidNode(&mermaid, toTableInfo, options)
		}
		mermaid.WriteString("\n")
	}
	if options.InitialTableHighlightColor != "" {
		writeStyleForHighlightedNode(&mermaid, graphLinks.InitialTable, options.InitialTableHighlightColor)
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

func writeInvalidNode(stringBuildr *strings.Builder, tableKey table.Key) {
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
	case "Dictionary":
		shape = winPane
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

func writeStyleForHighlightedNode(stringBuildr *strings.Builder, tableKey table.Key, color string) {
	stringBuildr.WriteString("style ")
	stringBuildr.WriteString(tableKey.String())
	stringBuildr.WriteString(" stroke:")
	stringBuildr.WriteString(color)
}

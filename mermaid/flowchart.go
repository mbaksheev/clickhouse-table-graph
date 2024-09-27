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

func (o Orientation) String() string {
	return [...]string{"TB", "BT", "LR", "RL"}[o]
}

type FlowchartOptions struct {
	Orientation Orientation
}

func Flowchart(tableGraph graph.Graph, options FlowchartOptions) string {
	orientation := options.Orientation.String()

	var mermaid strings.Builder
	mermaid.WriteString("flowchart " + orientation + "\n")
	for _, link := range tableGraph.Links {
		writeRoundEdgesNode(&mermaid, link.FromTable.Key)
		writeLink(&mermaid)
		writeRoundEdgesNode(&mermaid, link.ToTable.Key)
		mermaid.WriteString("\n")
	}
	return mermaid.String()
}
func writeRoundEdgesNode(stringBuildr *strings.Builder, tableKey table.Key) {
	stringBuildr.WriteString(tableKey.String())
	stringBuildr.WriteString("(")
	stringBuildr.WriteString(tableKey.String())
	stringBuildr.WriteString(")")
}
func writeLink(stringBuildr *strings.Builder) {
	stringBuildr.WriteString(" --> ")
}

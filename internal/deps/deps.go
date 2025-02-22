// Package deps provides functions to extract dependencies from ClickHouse table engine and create query statement.
package deps

import (
	"github.com/mbaksheev/clickhouse-table-graph/table"
	"regexp"
)

var (
	// distributedTableExtractorRegex is a regex to extract links from Distributed engine definition.
	distributedTableExtractorRegex = regexp.MustCompile(`Distributed\('.*?', '(.*?)', '(.*?)'.*?\)`)
	// materializedViewExtractorRegex is a regex to extract links from MaterializedView create query.
	materializedViewExtractorRegex = regexp.MustCompile(`CREATE MATERIALIZED VIEW .*? TO (\S+)\.(\S+) .*?`)
)

// FromDistributedEngine extracts links from Distributed engine definition.
func FromDistributedEngine(fullEngine string) []table.Key {
	links := make([]table.Key, 0)
	matches := distributedTableExtractorRegex.FindStringSubmatch(fullEngine)
	if len(matches) < 3 {
		return links
	} else {
		links = append(links, table.Key{
			Database: matches[1],
			Name:     matches[2],
		})
		return links
	}
}

// FromCreateQuery extracts links from MaterializedView create query.
func FromCreateQuery(createQuery string) []table.Key {
	links := make([]table.Key, 0)
	matches := materializedViewExtractorRegex.FindStringSubmatch(createQuery)
	if len(matches) < 3 {
		return links
	} else {
		links = append(links, table.Key{
			Database: matches[1],
			Name:     matches[2],
		})
		return links
	}
}

// FromDependencies extracts links from dependencies.
func FromDependencies(dependenciesDatabase []string, dependenciesTable []string) []table.Key {
	links := make([]table.Key, 0)
	for i, depTable := range dependenciesTable {
		if i >= len(dependenciesDatabase) {
			break
		}
		links = append(links, table.Key{
			Database: dependenciesDatabase[i],
			Name:     depTable,
		})
	}
	return links
}

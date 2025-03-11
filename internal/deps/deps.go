// Package deps provides functions to extract dependencies from ClickHouse table engine and create query statement.
package deps

import (
	"github.com/mbaksheev/clickhouse-table-graph/table"
	"regexp"
	"strings"
)

var (
	// distributedTableExtractorRegex is a regex to extract links from Distributed engine definition.
	distributedTableExtractorRegex = regexp.MustCompile(`Distributed\('.*?', '(.*?)', '(.*?)'.*?\)`)
	// materializedViewExtractorRegex is a regex to extract links from MaterializedView create query.
	materializedViewExtractorRegex = regexp.MustCompile(`CREATE MATERIALIZED VIEW .*? TO (\S+)\.(\S+) .*?`)
	// materializedViewJoinedTablesExtractorRegex is a regex to extract joined tables from MaterializedView create query.
	materializedViewJoinedTablesExtractorRegex = regexp.MustCompile(`JOIN\s+(\S+)\.(\S+)\s.*?`)
	// materializedViewDictionariesExtractorRegex is a regex to extract dictionaries from MaterializedView create query.
	materializedViewDictionariesExtractorRegex = regexp.MustCompile(`dict[A-Z]\w*\('([^']+)',\s*?`)
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

// JoinedTablesFromCreateQuery extracts all joined tables from MaterializedView create query
func JoinedTablesFromCreateQuery(createQuery string) []table.Key {
	links := make([]table.Key, 0)
	matches := materializedViewJoinedTablesExtractorRegex.FindAllStringSubmatch(createQuery, -1)
	for _, match := range matches {
		if len(match) < 3 {
			continue
		}
		links = append(links, table.Key{
			Database: match[1],
			Name:     match[2],
		})
	}
	return links
}

func DictionariesFromCreateQuery(createQuery string) []table.Key {
	links := make([]table.Key, 0)
	matches := materializedViewDictionariesExtractorRegex.FindAllStringSubmatch(createQuery, -1)
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		parts := strings.Split(match[1], ".")
		if len(parts) < 2 {
			links = append(links, table.Key{
				Database: "default",
				Name:     match[1],
			})
		} else {
			links = append(links, table.Key{
				Database: parts[0],
				Name:     parts[1],
			})
		}
	}
	return links
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

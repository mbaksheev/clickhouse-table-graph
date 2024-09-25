package deps

import (
	"github.com/mbaksheev/clickhouse-table-graph/table"
	"regexp"
)

var (
	distributedTableExtractorRegex = regexp.MustCompile(`Distributed\('.*?', '(.*?)', '(.*?)'\)`)
	materializedViewExtractorRegex = regexp.MustCompile(`CREATE MATERIALIZED VIEW .*? TO (\S+)\.(\S+) .*?`)
)

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
func FromDependencies(dependenciesDatabase []string, dependenciesTable []string) []table.Key {
	links := make([]table.Key, 0)
	for i, depTable := range dependenciesTable {
		links = append(links, table.Key{
			Database: dependenciesDatabase[i],
			Name:     depTable,
		})
	}
	return links
}

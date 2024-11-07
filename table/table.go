package table

import "fmt"

type Key struct {
	Database string
	Name     string
}

func (key Key) String() string {
	return fmt.Sprintf("%s.%s", key.Database, key.Name)
}

type Info struct {
	Key
	Engine               string
	EngineFull           string
	CreateTableQuery     string
	AsSelect             string
	DependenciesDatabase []string
	DependenciesTable    []string
}

type InfoProvider interface {
	TableInfos() ([]Info, error)
}

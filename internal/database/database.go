package database

import (
	"datasource-to-rds/internal/datasource"
)

type Database interface {
	DeleteOldData() error
	PutNewDatasources(datas []datasource.DataSourceDefinition) error
}

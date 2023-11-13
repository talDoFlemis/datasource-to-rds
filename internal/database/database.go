package database

import (
	"bigdatafor-datasource-ingest-to-rds/internal/datasource"
)

type Database interface {
	DeleteOldData() error
	PutNewDatasources(datas []datasource.DataSourceMetadataModel) error
}

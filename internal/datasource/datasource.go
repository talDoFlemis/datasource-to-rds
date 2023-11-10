package datasource

type DataSourceDefinition struct {
	AdministrativeDependency string `json:"administrative_dependency"`
	ExhibitionName           string `json:"exhibition_name"`
	LastDataCollection       string `json:"last_data_collection"`
	LastUpdate               string `json:"last_update"`
	Source                   string `json:"source"`
	TableName                string `json:"table_name"`
	UpdateFrequency          string `json:"update_frequency"`
}

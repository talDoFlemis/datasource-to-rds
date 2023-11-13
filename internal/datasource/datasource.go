package datasource

import (
	"fmt"
	"time"
)

type DataSourceDefinition struct {
	AdministrativeDependency string `json:"administrative_dependency"`
	ExhibitionName           string `json:"exhibition_name"`
	LastDataCollection       string `json:"last_data_collection"`
	LastUpdate               string `json:"last_update"`
	Source                   string `json:"source"`
	TableName                string `json:"table_name"`
	UpdateFrequency          string `json:"update_frequency"`
}

type DataSourceMetadataModel struct {
	AdministrativeDependency string
	ExhibitionName           string
	LastDataCollection       time.Time
	LastSourceUpdate         time.Time
	Source                   string
	TableName                string
	UpdateFrequency          string
	CreationDate             time.Time
	UpdateDate               time.Time
}

func (d *DataSourceDefinition) parseMultipleDataTypes(dateString string) (time.Time, error) {
	layouts := []string{
		"2006-01-02",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02 15:04:05",
		"02/01/2006",
	}

	for _, layout := range layouts {
		t, err := time.Parse(layout, dateString)
		if err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("no matching layout for date: %s", dateString)
}

func (d *DataSourceDefinition) ToDataSourceMetadateModel() (*DataSourceMetadataModel, error) {
	lastDataCollection, err := time.Parse("2006-01-02 15:04:05", d.LastDataCollection)
	if err != nil {
		return nil, err
	}
	lastUpdate, err := d.parseMultipleDataTypes(d.LastUpdate)
	if err != nil {
		return nil, err
	}

	return &DataSourceMetadataModel{
		AdministrativeDependency: d.AdministrativeDependency,
		ExhibitionName:           d.ExhibitionName,
		LastDataCollection:       lastDataCollection,
		LastSourceUpdate:         lastUpdate,
		Source:                   d.Source,
		TableName:                d.TableName,
		UpdateFrequency:          d.UpdateFrequency,
		CreationDate:             time.Now(),
		UpdateDate:               time.Now(),
	}, nil
}

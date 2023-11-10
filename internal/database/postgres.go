package database

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"datasource-to-rds/internal/datasource"
)

type PostgresManager struct {
	*pgxpool.Pool
}

func NewPostgresManager(connectionString string) (*PostgresManager, error) {
	pool, err := pgxpool.New(context.Background(), connectionString)
	if err != nil {
		log.Printf("Unable to connect to database: %v\n", err)
		return nil, err
	}

	query := `CREATE TABLE IF NOT EXISTS fonte_dados_metadados(
							table_name VARCHAR(255) PRIMARY KEY,
							administrative_dependency VARCHAR(255) NOT NULL,
							exhibition_name VARCHAR(255) NOT NULL,
							last_data_collection VARCHAR(255) NOT NULL,
							last_update VARCHAR(255) NOT NULL,
							source VARCHAR(255) NOT NULL,
							update_frequency VARCHAR(255) NOT NULL
						)
	`
	_, err = pool.Exec(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("unable to run a sql %v", err)
	}

	return &PostgresManager{
		Pool: pool,
	}, nil
}

func (p *PostgresManager) DeleteOldData() error {
	query := "DELETE FROM fonte_dados_metadados"

	_, err := p.Exec(context.Background(), query)
	if err != nil {
		log.Printf("Unable to delete old data: %v\n", err)
		return err
	}

	return nil
}

func (p *PostgresManager) PutNewDatasources(datas []datasource.DataSourceDefinition) error {
	query := `INSERT INTO fonte_dados_metadados(administrative_dependency, exhibition_name, last_data_collection, last_update, source, table_name, update_frequency)
						VALUES(@administrative_dependency, @exhibition_name, @last_data_collection, @last_update, @source, @table_name, @update_frequency)`

	tx, err := p.Begin(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())

	for _, data := range datas {
		namedParameters := pgx.NamedArgs{
			"administrative_dependency": data.AdministrativeDependency,
			"exhibition_name":           data.ExhibitionName,
			"last_data_collection":      data.LastDataCollection,
			"last_update":               data.LastUpdate,
			"source":                    data.Source,
			"table_name":                data.TableName,
			"update_frequency":          data.UpdateFrequency,
		}
		_, err = tx.Exec(context.Background(), query, namedParameters)

		if err != nil {
			return err
		}
	}
	err = tx.Commit(context.Background())
	return err
}

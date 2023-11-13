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

	query := `SELECT 1;`
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

func (p *PostgresManager) PutNewDatasources(datas []datasource.DataSourceMetadataModel) error {
	insert := `
						INSERT INTO
						fonte_dados_metadados (
							nome_tabela,
							dependencia_administrativa,
							nome_exibicao,
							ultima_coleta_dados,
							ultima_atualizacao_fonte,
							fonte,
							frequencia_atualizacao,
							data_atualizacao,
							data_criacao
						)
					VALUES
						(
							@table_name,
							@administrative_dependency,
							@exhibition_name,
							@last_data_collection,
							@last_update_data,
							@source,
							@update_frequency,
							@update_date,
							@creation_date
						)`

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
			"last_update_data":          data.LastSourceUpdate,
			"source":                    data.Source,
			"table_name":                data.TableName,
			"update_frequency":          data.UpdateFrequency,
			"update_date":               data.UpdateDate,
			"creation_date":             data.CreationDate,
		}
		_, err = tx.Exec(context.Background(), insert, namedParameters)

		if err != nil {
			return err
		}
	}
	err = tx.Commit(context.Background())
	return err
}

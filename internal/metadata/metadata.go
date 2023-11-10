package metadata

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"datasource-to-rds/internal/database"
	"datasource-to-rds/internal/datasource"
)

type MetadataService struct {
	bucket string
	db     database.Database
	*s3.Client
	*manager.Downloader
	*sync.WaitGroup
}

func NewMetadataService(
	db database.Database,
	cfg aws.Config,
	bucket string,
) (*MetadataService, error) {
	client := s3.NewFromConfig(cfg)
	downloader := manager.NewDownloader(client)

	service := &MetadataService{
		Client:     client,
		Downloader: downloader,
		WaitGroup:  &sync.WaitGroup{},
		bucket:     bucket,
		db:         db,
	}
	return service, nil
}

func (m *MetadataService) ListAllMetadata() ([]string, error) {
	output, err := m.ListObjectsV2(context.Background(),
		&s3.ListObjectsV2Input{
			Bucket: aws.String(m.bucket),
		},
	)

	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(output.Contents))
	for _, object := range output.Contents {
		result = append(result, *object.Key)
		log.Printf("Object name: %s, object size: %d", *object.Key, object.Size)
	}
	return result, nil
}

func (m *MetadataService) downloadObject(key string) (*datasource.DataSourceDefinition, error) {
	output, err := m.GetObject(
		context.TODO(),
		&s3.GetObjectInput{
			Bucket: aws.String(m.bucket),
			Key:    aws.String(key),
		})

	if err != nil {
		return nil, err
	}
	defer output.Body.Close()
	var datasource datasource.DataSourceDefinition

	body, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(body, &datasource)
	if err != nil {
		log.Printf("Error unmarshalling json: %s", err)
		return nil, err
	}

	return &datasource, nil
}

func (m *MetadataService) ProcessAllMetadata(
	metadatas []string,
) ([]datasource.DataSourceDefinition, error) {
	results := make([]datasource.DataSourceDefinition, 0, len(metadatas))
	m.Add(len(metadatas))

	for _, metadata := range metadatas {
		log.Printf("Processing metadata: %s", metadata)
		go func(metadata string) {
			log.Printf("Downloading object: %s", metadata)

			jsonData, err := m.downloadObject(metadata)
			if err != nil {
				m.Done()
				log.Printf("Error downloading object: %s", err)
				return
			}

			results = append(results, *jsonData)
			m.Done()
		}(metadata)
	}

	m.Wait()
	return results, nil
}

func (m *MetadataService) DeleteOldData() error {
	err := m.db.DeleteOldData()
	return err
}

func (m *MetadataService) InsertIntoDatabase(datas []datasource.DataSourceDefinition) error {
	err := m.db.PutNewDatasources(datas)
	return err
}

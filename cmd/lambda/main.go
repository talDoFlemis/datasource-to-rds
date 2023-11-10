package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"datasource-to-rds/internal/database"
	"datasource-to-rds/internal/datasource"
	"datasource-to-rds/internal/metadata"
)

var (
	bucket           string
	akid             string
	key              string
	connectionString string
)

func init() {
	bucket = os.Getenv("BUCKET")
	akid = os.Getenv("AKID")
	key = os.Getenv("KEY")
	connectionString = os.Getenv("CONNECTION_STRING")
}

type App struct {
	*metadata.MetadataService
}

type Request struct {
	Keys []string `json:"keys"`
}

type Response struct {
	Content []datasource.DataSourceDefinition `json:"content"`
}

func (a *App) Handler(ctx context.Context, event *Request) (*Response, error) {
	metadatas, err := a.ListAllMetadata()
	if err != nil {
		return nil, fmt.Errorf("error listing all metadata: %w", err)
	}
	resp, err := a.ProcessAllMetadata(metadatas)
	if err != nil {
		return nil, fmt.Errorf("error processing all metadata: %s", err)
	}
	if len(resp) != len(metadatas) {
		return nil, fmt.Errorf(
			"the response and the metadata differs: expected %d, got %d",
			len(metadatas),
			len(resp),
		)
	}
	err = a.DeleteOldData()
	if err != nil {
		return nil, err
	}

	err = a.InsertIntoDatabase(resp)
	if err != nil {
		return nil, err
	}

	return &Response{
		Content: resp,
	}, nil
}

func main() {
	creds := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(
		akid,
		key,
		"",
	))
	_, err := creds.Retrieve(context.TODO())
	if err != nil {
		log.Fatal("Error retrieving credentials: ", err)
	}
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("sa-east-1"),
		config.WithCredentialsProvider(creds),
	)

	if err != nil {
		log.Fatal("Error loading credentials: ", err)
	}

	postgresManager, err := database.NewPostgresManager(
		connectionString,
	)
	if err != nil {
		log.Fatal("Error creating postgres manager: ", err)
	}

	metadataService, err := metadata.NewMetadataService(postgresManager, cfg, bucket)
	if err != nil {
		log.Fatal("Error creating metadata service: ", err)
	}

	app := App{
		MetadataService: metadataService,
	}

	if err != nil {
		log.Fatal("Error doing the operation: ", err)
	}

	log.Println("Starting lambda function")
	lambda.Start(app.Handler)
}

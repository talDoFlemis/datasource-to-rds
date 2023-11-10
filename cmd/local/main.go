package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/joho/godotenv"

	"datasource-to-rds/internal/database"
	"datasource-to-rds/internal/metadata"
)

var (
	bucket           string
	akid             string
	key              string
	connectionString string
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Println("Error loading .env file: ", err)
		log.Fatal("Error loading .env file")
	}
	bucket = os.Getenv("BUCKET")
	akid = os.Getenv("AKID")
	key = os.Getenv("KEY")
	connectionString = os.Getenv("CONNECTION_STRING")
}

type App struct {
	*metadata.MetadataService
}

func (a *App) Run() error {
	metadatas, err := a.ListAllMetadata()
	if err != nil {
		return fmt.Errorf("error listing all metadata: %w", err)
	}
	resp, err := a.ProcessAllMetadata(metadatas)
	if err != nil {
		return fmt.Errorf("error processing all metadata: %s", err)
	}
	if len(resp) != len(metadatas) {
		return fmt.Errorf(
			"processed metadata differs from original metadata: expected %d, got %d",
			len(metadatas),
			len(resp),
		)
	}
	err = a.DeleteOldData()
	if err != nil {
		return err
	}

	err = a.InsertIntoDatabase(resp)
	return err
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

	err = app.Run()
	if err != nil {
		log.Fatal("Error doing the operation: ", err)
	}
	log.Println("Successfully processed all metadata")
}

package mongodb

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/malav4all/kafka-mongodb-consumer/models"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Database represents a MongoDB database
// Database represents a MongoDB database
type Database struct {
	Client   *mongo.Client
	Database *mongo.Database
}

// Connect establishes a connection to MongoDB
func Connect(uri, dbName string) Database {
	client, err := mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatalf("Failed to create MongoDB client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	// Ping MongoDB to ensure the connection is successful
	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatalf("Failed to ping MongoDB: %v", err)
	}

	log.Println("Successfully connected to MongoDB")

	return Database{
		Client:   client,
		Database: client.Database(dbName),
	}
}

// Disconnect closes the MongoDB connection
func (db Database) Disconnect() {
	if err := db.Client.Disconnect(context.Background()); err != nil {
		log.Printf("Error disconnecting from MongoDB: %v", err)
	} else {
		log.Println("MongoDB connection closed")
	}
}

// InsertBulk uses BulkWrite to insert multiple documents into a collection
func (db Database) InsertBulk(collectionName string, data []models.Data) error {
	collection := db.Database.Collection(collectionName)

	// Preallocate slice capacity to avoid resizing during append
	bulkOps := make([]mongo.WriteModel, 0, len(data))
	for _, d := range data {
		bulkOps = append(bulkOps, mongo.NewInsertOneModel().SetDocument(d))
	}

	// Perform BulkWrite operation
	bulkWriteOptions := options.BulkWrite().SetOrdered(false)
	result, err := collection.BulkWrite(context.Background(), bulkOps, bulkWriteOptions)
	if err != nil {
		return fmt.Errorf("failed to perform bulk write: %w", err)
	}

	log.Printf("Bulk write completed: %d inserted, %d modified", result.InsertedCount, result.ModifiedCount)
	return nil
}

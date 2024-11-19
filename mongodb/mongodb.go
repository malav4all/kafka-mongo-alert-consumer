package mongodb

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/malav4all/kafka-mongodb-consumer/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

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

	return Database{
		Client:   client,
		Database: client.Database(dbName),
	}
}

// Disconnect closes the MongoDB connection
func (db Database) Disconnect() {
	if err := db.Client.Disconnect(context.Background()); err != nil {
		log.Printf("Error disconnecting from MongoDB: %v", err)
	}
}

// Insert inserts a document into a collection
func (db Database) Insert(collectionName string, data models.Data) error {
	collection := db.Database.Collection(collectionName)
	_, err := collection.InsertOne(context.Background(), bson.M{
		"key":   data.Key,
		"value": data.Value,
	})
	if err != nil {
		return fmt.Errorf("failed to insert document: %w", err)
	}
	return nil
}

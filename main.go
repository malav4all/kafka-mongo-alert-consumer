package main

import (
	"log"

	"github.com/malav4all/kafka-mongo-alert-consumer/kafka"
	"github.com/malav4all/kafka-mongo-alert-consumer/mongodb"

	"github.com/malav4all/kafka-mongo-alert-consumer/config"
)

func main() {
	// Load configuration
	cfg := config.LoadConfig()

	// Connect to MongoDB
	db := mongodb.Connect(cfg.MongoURI, cfg.DatabaseName)
	defer db.Disconnect()

	// Start Kafka consumer
	err := kafka.StartConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, db)
	if err != nil {
		log.Fatalf("Error starting Kafka consumer: %v", err)
	}
}

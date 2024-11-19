package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/malav4all/kafka-mongodb-consumer/mongodb"

	"github.com/malav4all/kafka-mongodb-consumer/models"

	"github.com/segmentio/kafka-go"
)

// StartConsumer listens to a Kafka topic and processes incoming messages
func StartConsumer(brokers, topic string, db mongodb.Database) error {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokers},
		Topic:   topic,
		GroupID: "", // No GroupID needed as per user's requirement
	})

	defer r.Close()
	fmt.Printf("Listening to topic: %s\n", topic)

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		// Parse JSON message
		var data models.Data
		if err := json.Unmarshal(m.Value, &data); err != nil {
			log.Printf("Invalid JSON message: %s\n", string(m.Value))
			continue
		}

		// Save to MongoDB
		if err := db.Insert("collection_name", data); err != nil {
			log.Printf("Error saving to MongoDB: %v", err)
		}
	}
}

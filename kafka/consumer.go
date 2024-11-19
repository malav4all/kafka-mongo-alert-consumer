package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

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

	var (
		batch         []models.Data
		batchSize     = 10
		flushInterval = 5 * time.Second
		mutex         sync.Mutex
		flushTimer    = time.NewTimer(flushInterval)
	)

	// Flush function to handle bulk writing and memory cleanup
	flush := func() {
		mutex.Lock()
		defer mutex.Unlock()

		if len(batch) > 0 {
			// Perform bulk write to MongoDB
			if err := db.InsertBulk("collection_name", batch); err != nil {
				log.Printf("Error during bulk write: %v", err)
			}

			// Clear batch to release memory
			batch = nil
			log.Println("Batch flushed to MongoDB and memory released")
		}
		flushTimer.Reset(flushInterval)
	}

	// Goroutine to handle timer-based flushing
	go func() {
		for range flushTimer.C {
			flush()
		}
	}()

	for {
		// Read message from Kafka
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

		// Add to batch with thread safety
		mutex.Lock()
		batch = append(batch, data)
		mutex.Unlock()

		// If batch size reaches the limit, flush it
		if len(batch) >= batchSize {
			flush()
		}
	}
}

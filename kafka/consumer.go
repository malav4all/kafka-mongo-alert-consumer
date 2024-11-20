package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/malav4all/kafka-mongodb-consumer/mongodb"
	"github.com/segmentio/kafka-go"
)

// StartConsumer listens to a Kafka topic and processes incoming messages
func StartConsumer(brokers, topic string, db mongodb.Database) error {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokers},
		Topic:    topic,
		GroupID:  "socket310pconsumer",
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	defer func() {
		if err := r.Close(); err != nil {
			log.Printf("Error closing Kafka reader: %v", err)
		}
	}()
	fmt.Printf("Listening to topic: %s with GroupID: %s\n", topic, "socket301p")

	var (
		batch         []map[string]interface{}
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
			if err := db.InsertBulk("TRACK_DATA", batch); err != nil {
				log.Printf("Error during bulk write: %v", err)
			} else {
				log.Printf("Batch flushed to MongoDB with %d documents", len(batch))
			}

			// Clear batch to release memory
			batch = nil
			flushTimer.Reset(flushInterval)
		}
	}

	// Goroutine to handle timer-based flushing
	go func() {
		for range flushTimer.C {
			flush()
		}
	}()

	for {
		// Fetch message from Kafka
		m, err := r.FetchMessage(context.Background())
		if err != nil {
			log.Printf("Error fetching message: %v", err)
			continue
		}

		// Parse JSON message
		var data map[string]interface{}
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

			// Commit the offset for the batch
			if err := r.CommitMessages(context.Background(), m); err != nil {
				log.Printf("Error committing message offset: %v", err)
			} else {
				log.Printf("Offset committed successfully: %d", m.Offset)
			}
		}
	}
}

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
		Brokers:     []string{brokers},
		Topic:       topic,
		GroupID:     "socket310pconsumer",
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset, // Start from the earliest offset
	})

	defer func() {
		if err := r.Close(); err != nil {
			log.Printf("Error closing Kafka reader: %v", err)
		}
	}()
	fmt.Printf("Listening to topic: %s with GroupID: %s\n", topic, "socket310pconsumer")

	var (
		batch         []map[string]interface{}
		batchSize     = 10
		flushInterval = 5 * time.Second
		mutex         sync.Mutex
	)

	// Ticker to trigger flush at regular intervals
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	// Channels to handle messages and errors from Kafka
	messages := make(chan kafka.Message)
	errorsChan := make(chan error)

	// Goroutine to read messages from Kafka
	go func() {
		for {
			m, err := r.FetchMessage(context.Background())
			if err != nil {
				errorsChan <- err
				return
			}
			messages <- m
		}
	}()

	for {
		select {
		case m := <-messages:
			// Parse JSON message
			var data map[string]interface{}
			if err := json.Unmarshal(m.Value, &data); err != nil {
				log.Printf("Invalid JSON message: %s\n", string(m.Value))
				// Commit the offset even for invalid messages to prevent re-processing
				if err := r.CommitMessages(context.Background(), m); err != nil {
					log.Printf("Error committing message offset: %v", err)
				}
				continue
			}

			// Add to batch with thread safety
			mutex.Lock()
			batch = append(batch, data)
			mutex.Unlock()

			// If batch size reaches the limit, flush it
			if len(batch) >= batchSize {
				flush(&batch, db, &mutex)
			}

			// Commit the offset after processing
			if err := r.CommitMessages(context.Background(), m); err != nil {
				log.Printf("Error committing message offset: %v", err)
			} else {
				log.Printf("Offset committed successfully: %d", m.Offset)
			}

		case err := <-errorsChan:
			log.Printf("Error fetching message: %v", err)
			// Handle error as needed, possibly exit or attempt recovery
			return err

		case <-ticker.C:
			// Flush at regular intervals regardless of batch size
			flush(&batch, db, &mutex)
		}
	}
}

// flush handles bulk writing and memory cleanup
func flush(batch *[]map[string]interface{}, db mongodb.Database, mutex *sync.Mutex) {
	mutex.Lock()
	defer mutex.Unlock()

	if len(*batch) > 0 {
		// Perform bulk write to MongoDB
		if err := db.InsertBulk("TRACK_DATA", *batch); err != nil {
			log.Printf("Error during bulk write: %v", err)
		} else {
			log.Printf("Batch flushed to MongoDB with %d documents", len(*batch))
		}

		// Clear batch to release memory
		*batch = nil
	}
}

package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/malav4all/kafka-mongodb-consumer/mongodb"
	"github.com/segmentio/kafka-go"
)

// OffsetStore manages the storage and retrieval of offsets
type OffsetStore struct {
	FilePath string
	mutex    sync.Mutex
}

// GetOffset retrieves the last committed offset from the file
func (store *OffsetStore) GetOffset() int64 {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	data, err := ioutil.ReadFile(store.FilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return kafka.FirstOffset
		}
		log.Printf("Error reading offset file: %v", err)
		return kafka.FirstOffset
	}

	var offset int64
	if _, err := fmt.Sscanf(string(data), "%d", &offset); err != nil {
		log.Printf("Error parsing offset: %v", err)
		return kafka.FirstOffset
	}

	return offset
}

// SaveOffset saves the latest offset to the file
func (store *OffsetStore) SaveOffset(offset int64) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	data := []byte(fmt.Sprintf("%d", offset))
	if err := ioutil.WriteFile(store.FilePath, data, 0644); err != nil {
		log.Printf("Error writing offset file: %v", err)
	}
}

// StartConsumer listens to a Kafka topic and processes incoming messages
func StartConsumer(brokers, topic string, db mongodb.Database) error {
	offsetStore := &OffsetStore{FilePath: "offset.txt"}
	lastOffset := offsetStore.GetOffset()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{brokers},
		Topic:     topic,
		Partition: 0, // Specify the partition
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	r.SetOffset(lastOffset)

	defer r.Close()
	fmt.Printf("Listening to topic: %s from offset: %d\n", topic, lastOffset)

	var (
		batch         []map[string]interface{}
		batchOffsets  []int64
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

			// Commit the offset after successful processing
			latestOffset := batchOffsets[len(batchOffsets)-1] + 1
			offsetStore.SaveOffset(latestOffset)

			// Clear batch and offsets to release memory
			batch = nil
			batchOffsets = nil
			log.Printf("Batch flushed to MongoDB and offset %d committed", latestOffset)
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
		var data map[string]interface{}
		if err := json.Unmarshal(m.Value, &data); err != nil {
			log.Printf("Invalid JSON message: %s\n", string(m.Value))
			continue
		}

		// Add to batch with thread safety
		mutex.Lock()
		batch = append(batch, data)
		batchOffsets = append(batchOffsets, m.Offset)
		mutex.Unlock()

		// If batch size reaches the limit, flush it
		if len(batch) >= batchSize {
			flush()
		}
	}
}

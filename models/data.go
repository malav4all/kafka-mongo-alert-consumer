package models

// Data represents the JSON structure of incoming Kafka messages
type Data struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

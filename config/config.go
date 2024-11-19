package config

import "os"

// Config holds the application configuration
type Config struct {
	MongoURI     string
	DatabaseName string
	KafkaBrokers string
	KafkaTopic   string
}

// LoadConfig loads configuration from environment variables
func LoadConfig() Config {
	return Config{
		MongoURI:     getEnv("MONGO_URI", "mongodb://localhost:27017"),
		DatabaseName: getEnv("MONGO_DB", "kafka_db"),
		KafkaBrokers: getEnv("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:   getEnv("KAFKA_TOPIC", "socket_310p_jsonData"),
	}
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

package config

import (
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	Port               string
	DataSource         string // "csv" | "oracle" | "postgres"
	CSVPath            string
	EnrichedPath       string
	OracleDSN          string
	PostgresDSN        string
	MLServiceURL       string
	MLThreshold        float64
	PollingIntervalSec int
	SMTPHost           string
	SMTPPort           int
	SMTPUser           string
	SMTPPassword       string
	SMTPFrom           string
	AlertThreshold     float64
	AlertRecipients    string
}

func Load() *Config {
	_ = godotenv.Load(".env")
	return &Config{
		Port:               getEnv("PORT", "8080"),
		DataSource:         getEnv("DATA_SOURCE", "csv"),
		CSVPath:            getEnv("CSV_PATH", "data/access_log.csv"),
		EnrichedPath:       getEnv("ENRICHED_PATH", "data/enriched_log.csv"),
		OracleDSN:          getEnv("ORACLE_DSN", ""),
		PostgresDSN:        getEnv("POSTGRES_DSN", ""),
		MLServiceURL:       getEnv("ML_SERVICE_URL", "http://localhost:8000"),
		MLThreshold:        getEnvFloat("ML_THRESHOLD", 0.89),
		PollingIntervalSec: getEnvInt("POLLING_INTERVAL_SEC", 30),
		SMTPHost:           getEnv("SMTP_HOST", "smtp.gmail.com"),
		SMTPPort:           getEnvInt("SMTP_PORT", 587),
		SMTPUser:           getEnv("SMTP_USER", ""),
		SMTPPassword:       getEnv("SMTP_PASSWORD", ""),
		SMTPFrom:           getEnv("SMTP_FROM", ""),
		AlertThreshold:     getEnvFloat("ALERT_THRESHOLD", 0.89),
		AlertRecipients:    getEnv("ALERT_RECIPIENTS", ""),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

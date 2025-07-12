package env

import (
	"errors"
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	ServerPort string
	DB         PostgresConfig
}

type PostgresConfig struct {
	URL          string
	MaxOpenConns int
	MaxIdleConns int
	MaxIdleTime  string
}

var cfg *Config

func GetEnv() (*Config, error) {
	if cfg == nil {
		return nil, errors.New("config not initialized, call LoadEnv first")
	}

	return cfg, nil
}

func LoadEnv() (*Config, error) {
	if err := godotenv.Load(); err != nil {
		return nil, err
	}

	cfg = &Config{
		ServerPort: getString("SERVER_PORT"),
		DB: PostgresConfig{
			URL:          getString("DB_URL"),
			MaxOpenConns: getInt("DB_MAX_OPEN_CONNS"),
			MaxIdleConns: getInt("DB_MAX_IDLE_CONNS"),
			MaxIdleTime:  getString("DB_MAX_IDLE_TIME"),
		},
	}

	return cfg, nil
}

func getString(key string) string {
	val, ok := os.LookupEnv(key)
	if !ok {
		log.Fatalf("missing %s on .env file\n", key)
	}

	return val
}

func getInt(key string) int {
	val, ok := os.LookupEnv(key)
	if !ok {
		log.Fatalf("missing %s on .env file\n", key)
	}

	valAsInt, err := strconv.Atoi(val)
	if err != nil {
		log.Fatal(err)
	}

	return valAsInt
}

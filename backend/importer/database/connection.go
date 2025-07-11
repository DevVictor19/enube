package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

const (
	QueryDuration = time.Second * 5
	ParamsLimit   = 65535
	maxOpenConns  = 30
	maxIdleConns  = 30
	maxIdleTime   = "15m"
)

var dbRef *sql.DB

func Get() (*sql.DB, error) {
	if dbRef == nil {
		return nil, errors.New("calling Get before initialization")
	}
	return dbRef, nil
}

func Connect() (*sql.DB, error) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return nil, fmt.Errorf("could not get current file path")
	}

	currentDir := filepath.Dir(filename)
	projectRootPath := filepath.Join(currentDir, "..", "..")
	envPath := filepath.Join(projectRootPath, ".env")

	if err := godotenv.Load(envPath); err != nil {
		return nil, err
	}

	url := os.Getenv("DB_URL")
	if url == "" {
		return nil, fmt.Errorf("DB_URL env variable is empty")
	}

	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)

	duration, err := time.ParseDuration(maxIdleTime)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxIdleTime(duration)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err = db.PingContext(ctx); err != nil {
		return nil, err
	}

	dbRef = db

	return dbRef, nil
}

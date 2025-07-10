package migrate

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	migrateV4 "github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func MigrateUp() error {
	fmt.Println("Running up migrations...")

	cfg, err := loadPaths()
	if err != nil {
		return err
	}

	db, err := sql.Open("postgres", cfg.dbUrl)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return err
	}

	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return err
	}

	m, err := migrateV4.NewWithDatabaseInstance(cfg.migrationsUrl, "postgres", driver)
	if err != nil {
		return err
	}

	if err := m.Up(); err != nil {
		return err
	}

	fmt.Println("Up migrations completed!")

	return nil
}

func MigrateDown() error {
	fmt.Println("Running down migrations...")

	cfg, err := loadPaths()
	if err != nil {
		return err
	}

	db, err := sql.Open("postgres", cfg.dbUrl)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return err
	}

	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return err
	}

	m, err := migrateV4.NewWithDatabaseInstance(cfg.migrationsUrl, "postgres", driver)
	if err != nil {
		return err
	}

	if err := m.Down(); err != nil {
		return err
	}

	fmt.Println("Down migrations completed!")

	return nil
}

type pathConfig struct {
	dbUrl         string
	migrationsUrl string
}

func loadPaths() (*pathConfig, error) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return nil, fmt.Errorf("could not get current file path")
	}

	currentDir := filepath.Dir(filename)
	migrationsPath := "file://" + filepath.Join(currentDir, "migrations")
	projectRootPath := filepath.Join(currentDir, "..")
	envPath := filepath.Join(projectRootPath, ".env")

	if err := godotenv.Load(envPath); err != nil {
		return nil, err
	}

	dbUrl := os.Getenv("DB_URL")
	if dbUrl == "" {
		return nil, fmt.Errorf("DB_URL env variable is empty")
	}

	return &pathConfig{
		dbUrl,
		migrationsPath,
	}, nil
}

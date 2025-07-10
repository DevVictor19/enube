package main

import (
	"log"

	"github.com/DevVictor19/enube/backend/migrate"
)

func main() {
	if err := migrate.MigrateDown(); err != nil {
		log.Fatal(err)
	}
}

package importerV2

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/DevVictor19/enube/backend/importerV2/database"
	"github.com/DevVictor19/enube/backend/importerV2/helpers"
)

func batchInsertCSV(fp string, maxRows int) {
	pathParts := strings.Split(fp, string(os.PathSeparator))
	tableName := strings.TrimSuffix(pathParts[len(pathParts)-1], ".csv")

	cols, ok := tableCols[tableName]
	if !ok {
		log.Fatalf("unknown table name: %s", tableName)
	}

	insertQR, ok := insertQueries[tableName]
	if !ok {
		log.Fatalf("no insert query found for table: %s", tableName)
	}

	file, err := os.Open(fp)
	if err != nil {
		log.Fatalf("could not open file %s: %v", fp, err)
	}
	defer file.Close()

	var wg sync.WaitGroup
	reader := csv.NewReader(file)
	values := make([]string, 0, maxRows*cols)
	rowsProcessed := 0

	for {
		row, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			log.Fatal(err)
		}

		if rowsProcessed == maxRows {
			wg.Add(1)
			go func(rows int, vals []string) {
				defer wg.Done()
				fmt.Printf("Inserting %d rows into %s\n", rows, tableName)
				insert(insertQR + helpers.BuildInsertValues(vals, cols))
			}(rowsProcessed, values)

			rowsProcessed = 0
			values = make([]string, 0, maxRows*cols)
		}

		values = append(values, row...)
		rowsProcessed++
	}

	if rowsProcessed > 0 {
		wg.Add(1)
		go func(rows int, vals []string) {
			defer wg.Done()
			fmt.Printf("Inserting %d rows into %s\n", rows, tableName)
			insert(insertQR + helpers.BuildInsertValues(vals, cols))
		}(rowsProcessed, values)
		values = nil
	}

	wg.Wait()
}

func insert(query string) {
	db, err := database.Get()
	if err != nil {
		log.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	if _, err := tx.Exec(query); err != nil {
		tx.Rollback()
		log.Fatal("insert err:", err, "\n", query)
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
}

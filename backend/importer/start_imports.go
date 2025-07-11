package importer

import (
	"fmt"
	"log"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/DevVictor19/enube/backend/importer/database"
	"github.com/xuri/excelize/v2"
)

const batchSize = 1500

func StartImports() {
	start := time.Now()

	db, err := database.Connect()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	f, err := excelize.OpenFile(getExcelFilepath())
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	sheetName := f.GetSheetList()[0]

	rows, err := f.Rows(sheetName)
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup

	rowsToInsert := 0
	isFirstRow := true

	for rows.Next() {
		if isFirstRow {
			isFirstRow = false
			continue
		}

		row, err := rows.Columns()
		if err != nil {
			log.Fatal(err)
		}

		if rowsToInsert == batchSize {
			fmt.Printf("Starting batch insert of %d rows\n", rowsToInsert)
			wg.Add(1)
			rowsToInsert = 0
			go executeInsert(prepareInsert(), &wg)
		} else {
			rowsToInsert++
			consumeChargeFacts(row)
		}

	}

	if rowsToInsert > 0 {
		wg.Add(1)
		fmt.Printf("Starting batch insert of %d rows\n", rowsToInsert)
		go executeInsert(prepareInsert(), &wg)
	}

	wg.Wait()

	fmt.Println("File data import finished")

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	elapsed := time.Since(start)
	fmt.Printf("Execution Time: %v ms\n", elapsed.Milliseconds())

	fmt.Println("=== Go Runtime Memory Stats AFTER processing ===")
	fmt.Printf("Current Allocation: %v KB\n", memStats.Alloc/1024)
	fmt.Printf("Total Allocation: %v KB\n", memStats.TotalAlloc/1024)
	fmt.Printf("System Memory Used by Go: %v KB\n", memStats.Sys/1024)
	fmt.Printf("Number of GCs: %v\n", memStats.NumGC)
}

func getExcelFilepath() string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatal("could not get current file path")
	}
	currentDirPath := filepath.Dir(filename)
	dataFile := "reconfile-fornecedores.xlsx"
	return filepath.Join(currentDirPath, "files", dataFile)
}

package importerV2

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/DevVictor19/enube/backend/importerV2/database"
	"github.com/xuri/excelize/v2"
)

func StartImports() {
	start := time.Now()

	db, err := database.Connect()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("Starting imports...")

	chunks := getCSVFilepathChunks()

	var wg sync.WaitGroup
	for _, chunk := range chunks {
		wg.Add(1)
		go func(c string) {
			defer wg.Done()
			processCSV(c, 1500)
		}(chunk)
	}
	wg.Wait()

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

func SplitExcel() {
	start := time.Now()
	fmt.Println("Starting excel split...")

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
	defer rows.Close()

	var writer *csv.Writer
	var out *os.File

	chunks := 0
	chunkSize := 10000 // rows
	rowsProcessed := 0
	totalRowsProcessed := 0

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

		if chunks == 0 {
			chunks++

			fmt.Printf("Creating new chunk %d\n", chunks)
			out, err = os.Create(getCSVChunkFilepath(chunks))
			if err != nil {
				log.Fatal(err)
			}

			writer = csv.NewWriter(out)
		} else if rowsProcessed == chunkSize {
			writer.Flush()
			out.Close()
			chunks++
			totalRowsProcessed += rowsProcessed
			rowsProcessed = 0

			fmt.Printf("Creating new chunk %d\n", chunks)
			out, err = os.Create(getCSVChunkFilepath(chunks))
			if err != nil {
				log.Fatal(err)
			}

			writer = csv.NewWriter(out)
		}

		if err := writer.Write(row); err != nil {
			log.Fatal(err)
		}

		rowsProcessed++
	}

	if rowsProcessed > 0 {
		totalRowsProcessed += rowsProcessed
		writer.Flush()
		out.Close()
	}

	fmt.Printf("Inserted %d rows...\n", totalRowsProcessed)

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

func getCSVChunkFilepath(chunk int) string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatal("could not get current file path")
	}

	currentDirPath := filepath.Dir(filename)
	chunksDir := filepath.Join(currentDirPath, "files", "chunks")
	if err := os.MkdirAll(chunksDir, os.ModePerm); err != nil {
		log.Fatalf("could not create directory %s: %v", chunksDir, err)
	}

	dataFile := fmt.Sprintf("chunk_%d.csv", chunk)
	return filepath.Join(chunksDir, dataFile)
}

func getCSVFilepathChunks() []string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatal("could not get current file path")
	}
	currentDirPath := filepath.Dir(filename)
	chunksDir := filepath.Join(currentDirPath, "files", "chunks")

	entries, err := os.ReadDir(chunksDir)
	if err != nil {
		log.Fatalf("could not read directory %s: %v", chunksDir, err)
	}

	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".csv" {
			files = append(files, filepath.Join(chunksDir, entry.Name()))
		}
	}
	return files
}

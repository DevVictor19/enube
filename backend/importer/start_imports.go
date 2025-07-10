package importer

import (
	"fmt"
	"log"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/xuri/excelize/v2"
)

const batchSize = 2

func StartImports() {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatal("could not get current file path")
	}
	currentDirPath := filepath.Dir(filename)
	dataFile := "reconfile-fornecedores.xlsx"
	filepath := filepath.Join(currentDirPath, "files", dataFile)

	f, err := excelize.OpenFile(filepath)
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

	rowsToInsert := make([][]string, 0, batchSize)
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

		if len(rowsToInsert) == batchSize {
			wg.Add(1)
			fmt.Println("Starting batch insert")
			// antes de entrar na go routine eu já tenho que salvar no hashmaps os valores
			// das dimensões
			// insertRows vai receber apenas a query feita
			go insertRows(rowsToInsert, &wg)
			rowsToInsert = make([][]string, 0, batchSize)
			break
		} else {
			rowsToInsert = append(rowsToInsert, row)
		}

	}

	if len(rowsToInsert) > 0 {
		wg.Add(1)
		fmt.Println("Starting batch insert")
		go insertRows(rowsToInsert, &wg)
	}

	wg.Wait()

	fmt.Println("File data import finished")
}

func insertRows(rowsToInsert [][]string, wg *sync.WaitGroup) {
	fmt.Println(rowsToInsert)
	wg.Done()
}

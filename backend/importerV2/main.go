package importerV2

import (
	"bufio"
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

	filepaths := getCSVChunksFilepath()

	// insert dimension tables first
	var wg sync.WaitGroup
	for i := 0; i < len(filepaths)-1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batchInsertCSV(filepaths[i], 1500)
		}()
	}
	wg.Wait()

	// insert fact table last
	batchInsertCSV(filepaths[len(filepaths)-1], 1500)

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
	defer f.Close()

	sheetName := f.GetSheetList()[0]
	rows, err := f.Rows(sheetName)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	chargeFile, err := os.Create(getCSVFilepath("fact_charges"))
	if err != nil {
		log.Fatal(err)
	}
	defer chargeFile.Close()

	buffWriter := bufio.NewWriterSize(chargeFile, 1024*64) // 64KB buffer
	defer buffWriter.Flush()

	csvWriter := csv.NewWriter(buffWriter)
	defer csvWriter.Flush()

	dimCsvRows := make(map[string][][]string, 21)
	rowsProcessed := 0
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

		partnerSK, values := getPartnersSK(row)
		if values != nil {
			dimCsvRows[partnersTable] = append(dimCsvRows[partnersTable], values)
		}
		monthsChargeDatesSK, values := getMonthsChargeDatesSK(row)
		if values != nil {
			dimCsvRows[monthsChargeDatesTable] = append(dimCsvRows[monthsChargeDatesTable], values)
		}
		customersSK, values := getCustomersSK(row)
		if values != nil {
			dimCsvRows[customersTable] = append(dimCsvRows[customersTable], values)
		}
		metersSK, values := getMetersSK(row)
		if values != nil {
			dimCsvRows[metersTable] = append(dimCsvRows[metersTable], values)
		}
		productsSK, values := getProductsSK(row)
		if values != nil {
			dimCsvRows[productsTable] = append(dimCsvRows[productsTable], values)
		}
		skusSK, values := getSkusSK(row)
		if values != nil {
			dimCsvRows[skusTable] = append(dimCsvRows[skusTable], values)
		}
		publishersSK, values := getPublishersSK(row)
		if values != nil {
			dimCsvRows[publishersTable] = append(dimCsvRows[publishersTable], values)
		}
		subscriptionsSK, values := getSubscriptionsSK(row)
		if values != nil {
			dimCsvRows[subscriptionsTable] = append(dimCsvRows[subscriptionsTable], values)
		}
		resourceLocationsSK, values := getResourceLocationsSK(row)
		if values != nil {
			dimCsvRows[resourceLocationsTable] = append(dimCsvRows[resourceLocationsTable], values)
		}
		resourceGroupsSK, values := getResourceGroupsSK(row)
		if values != nil {
			dimCsvRows[resourceGroupsTable] = append(dimCsvRows[resourceGroupsTable], values)
		}
		servicesSK, values := getServicesSK(row)
		if values != nil {
			dimCsvRows[servicesTable] = append(dimCsvRows[servicesTable], values)
		}
		chargeTypesSK, values := getChargeTypesSK(row)
		if values != nil {
			dimCsvRows[chargeTypesTable] = append(dimCsvRows[chargeTypesTable], values)
		}
		unitTypesSK, values := getUnitTypesSK(row)
		if values != nil {
			dimCsvRows[unitTypesTable] = append(dimCsvRows[unitTypesTable], values)
		}
		entitlementsSK, values := getEntitlementsSK(row)
		if values != nil {
			dimCsvRows[entitlementsTable] = append(dimCsvRows[entitlementsTable], values)
		}
		partnerCreditsSK, values := getPartnerCreditsSK(row)
		if values != nil {
			dimCsvRows[partnerCreditsTable] = append(dimCsvRows[partnerCreditsTable], values)
		}
		benefitsSK, values := getBenefitsSK(row)
		if values != nil {
			dimCsvRows[benefitsTable] = append(dimCsvRows[benefitsTable], values)
		}
		benefitOrdersSK, values := getBenefitOrdersSK(row)
		if values != nil {
			dimCsvRows[benefitOrdersTable] = append(dimCsvRows[benefitOrdersTable], values)
		}
		availabilitySK, values := getAvailabilitySK(row)
		if values != nil {
			dimCsvRows[availabilitiesTable] = append(dimCsvRows[availabilitiesTable], values)
		}
		usageDatesSK, values := getUsageDatesSK(row)
		if values != nil {
			dimCsvRows[usageDatesTable] = append(dimCsvRows[usageDatesTable], values)
		}
		billingCurrenciesSK, values := getBillingCurrenciesSK(row)
		if values != nil {
			dimCsvRows[billingCurrenciesTable] = append(dimCsvRows[billingCurrenciesTable], values)
		}
		pricingCurrenciesSK, values := getPricingCurrenciesSK(row)
		if values != nil {
			dimCsvRows[pricingCurrenciesTable] = append(dimCsvRows[pricingCurrenciesTable], values)
		}

		resourceUri := row[resourceUriIndex]
		effectiveUnitPrice := row[effectiveUnitPriceIndex]
		unitPrice := row[unitPriceIndex]
		quantity := row[quantityIndex]
		billingPreTaxTotal := row[billingPreTaxTotalIndex]
		pricingPreTaxTotal := row[pricingPreTaxTotalIndex]
		pcToBcExchangeRate := row[pcToBcExchangeRateIndex]
		pcToBcExchangeRateDate := row[pcToBcExchangeRateDateIndex]
		serviceInfo1 := row[serviceInfo1Index]
		serviceInfo2 := row[serviceInfo2Index]
		tags := row[tagsIndex]
		additionalInfo := row[additionalInfoIndex]

		factChargesRow := []string{
			partnerSK,
			monthsChargeDatesSK,
			customersSK,
			metersSK,
			productsSK,
			skusSK,
			publishersSK,
			subscriptionsSK,
			resourceLocationsSK,
			resourceGroupsSK,
			servicesSK,
			chargeTypesSK,
			unitTypesSK,
			entitlementsSK,
			partnerCreditsSK,
			benefitsSK,
			benefitOrdersSK,
			availabilitySK,
			usageDatesSK,
			billingCurrenciesSK,
			pricingCurrenciesSK,
			resourceUri,
			effectiveUnitPrice,
			unitPrice,
			quantity,
			billingPreTaxTotal,
			pricingPreTaxTotal,
			pcToBcExchangeRate,
			pcToBcExchangeRateDate,
			serviceInfo1,
			serviceInfo2,
			tags,
			additionalInfo,
		}

		csvWriter.Write(factChargesRow)

		rowsProcessed++
	}

	var wg sync.WaitGroup
	wg.Add(len(dimCsvRows))
	for tableName, rows := range dimCsvRows {
		go func() {
			defer wg.Done()
			fmt.Printf("Writing %s with %d rows...\n", tableName, len(rows))
			dimFile, err := os.Create(getCSVFilepath(string(tableName)))
			if err != nil {
				log.Fatal(err)
			}
			defer dimFile.Close()

			csvWriter := csv.NewWriter(dimFile)
			defer csvWriter.Flush()

			csvWriter.WriteAll(rows)
		}()
	}
	wg.Wait()

	fmt.Printf("Inserted %d rows...\n", rowsProcessed)

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

func getCSVFilepath(name string) string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatal("could not get current file path")
	}

	currentDirPath := filepath.Dir(filename)
	chunksDir := filepath.Join(currentDirPath, "files", "chunks")
	if err := os.MkdirAll(chunksDir, os.ModePerm); err != nil {
		log.Fatalf("could not create directory %s: %v", chunksDir, err)
	}

	dataFile := fmt.Sprintf("%s.csv", name)
	return filepath.Join(chunksDir, dataFile)
}

func getCSVChunksFilepath() []string {
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

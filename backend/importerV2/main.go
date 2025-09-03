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

	// chunks := getCSVFilepathChunks()

	// var wg sync.WaitGroup
	// for _, chunk := range chunks {
	// 	wg.Add(1)
	// 	go func(c string) {
	// 		defer wg.Done()
	// 		processCSV(c, 1500)
	// 	}(chunk)
	// }
	// wg.Wait()

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

	chargeFile, err := os.Create(getCSVFilepath("fact_charges"))
	if err != nil {
		log.Fatal(err)
	}
	defer chargeFile.Close()

	csvWriter := csv.NewWriter(chargeFile)
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
			dimCsvRows["dim_partners"] = append(dimCsvRows["dim_partners"], values)
		}
		monthsChargeDatesSK, values := getMonthsChargeDatesSK(row)
		if values != nil {
			dimCsvRows["dim_months_charge_dates"] = append(dimCsvRows["dim_months_charge_dates"], values)
		}
		customersSK, values := getCustomersSK(row)
		if values != nil {
			dimCsvRows["dim_customers"] = append(dimCsvRows["dim_customers"], values)
		}
		metersSK, values := getMetersSK(row)
		if values != nil {
			dimCsvRows["dim_meters"] = append(dimCsvRows["dim_meters"], values)
		}
		productsSK, values := getProductsSK(row)
		if values != nil {
			dimCsvRows["dim_products"] = append(dimCsvRows["dim_products"], values)
		}
		skusSK, values := getSkusSK(row)
		if values != nil {
			dimCsvRows["dim_skus"] = append(dimCsvRows["dim_skus"], values)
		}
		publishersSK, values := getPublishersSK(row)
		if values != nil {
			dimCsvRows["dim_publishers"] = append(dimCsvRows["dim_publishers"], values)
		}
		subscriptionsSK, values := getSubscriptionsSK(row)
		if values != nil {
			dimCsvRows["dim_subscriptions"] = append(dimCsvRows["dim_subscriptions"], values)
		}
		resourceLocationsSK, values := getResourceLocationsSK(row)
		if values != nil {
			dimCsvRows["dim_resource_locations"] = append(dimCsvRows["dim_resource_locations"], values)
		}
		resourceGroupsSK, values := getResourceGroupsSK(row)
		if values != nil {
			dimCsvRows["dim_resource_groups"] = append(dimCsvRows["dim_resource_groups"], values)
		}
		servicesSK, values := getServicesSK(row)
		if values != nil {
			dimCsvRows["dim_services"] = append(dimCsvRows["dim_services"], values)
		}
		chargeTypesSK, values := getChargeTypesSK(row)
		if values != nil {
			dimCsvRows["dim_charge_types"] = append(dimCsvRows["dim_charge_types"], values)
		}
		unitTypesSK, values := getUnitTypesSK(row)
		if values != nil {
			dimCsvRows["dim_unit_types"] = append(dimCsvRows["dim_unit_types"], values)
		}
		entitlementsSK, values := getEntitlementsSK(row)
		if values != nil {
			dimCsvRows["dim_entitlements"] = append(dimCsvRows["dim_entitlements"], values)
		}
		partnerCreditsSK, values := getPartnerCreditsSK(row)
		if values != nil {
			dimCsvRows["dim_partner_credits"] = append(dimCsvRows["dim_partner_credits"], values)
		}
		benefitsSK, values := getBenefitsSK(row)
		if values != nil {
			dimCsvRows["dim_benefits"] = append(dimCsvRows["dim_benefits"], values)
		}
		benefitOrdersSK, values := getBenefitOrdersSK(row)
		if values != nil {
			dimCsvRows["dim_benefit_orders"] = append(dimCsvRows["dim_benefit_orders"], values)
		}
		availabilitySK, values := getAvailabilitySK(row)
		if values != nil {
			dimCsvRows["dim_availability"] = append(dimCsvRows["dim_availability"], values)
		}
		usageDatesSK, values := getUsageDatesSK(row)
		if values != nil {
			dimCsvRows["dim_usage_dates"] = append(dimCsvRows["dim_usage_dates"], values)
		}
		billingCurrenciesSK, values := getBillingCurrenciesSK(row)
		if values != nil {
			dimCsvRows["dim_billing_currencies"] = append(dimCsvRows["dim_billing_currencies"], values)
		}
		pricingCurrenciesSK, values := getPricingCurrenciesSK(row)
		if values != nil {
			dimCsvRows["dim_pricing_currencies"] = append(dimCsvRows["dim_pricing_currencies"], values)
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
	for tableName, rows := range dimCsvRows {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dimFile, err := os.Create(getCSVFilepath(tableName))
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

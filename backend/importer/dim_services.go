package importer

import (
	"database/sql"

	"github.com/DevVictor19/enube/backend/importer/helpers"
)

var (
	serviceSequence = 0
	services        map[string]int
	serviceValues   []any
)

func getServiceSk(row []string) sql.NullInt32 {
	if len(row) <= consumedServiceIndex {
		return sql.NullInt32{Valid: false}
	}

	service := row[consumedServiceIndex]

	if service == "" {
		return sql.NullInt32{Valid: false}
	}

	if services == nil {
		services = make(map[string]int)
	}

	existentSequence, ok := services[service]
	if !ok {
		serviceSequence++
		services[service] = serviceSequence

		serviceValues = append(
			serviceValues,
			serviceSequence,
			service,
		)

		return sql.NullInt32{
			Valid: true,
			Int32: int32(serviceSequence),
		}
	}

	return sql.NullInt32{
		Valid: true,
		Int32: int32(existentSequence),
	}
}

func getServiceStm() string {
	table := "dim_services"
	cols := []string{
		"service_sk",
		"service",
	}
	totalVals := len(serviceValues)
	return helpers.BuildBatchInsert(table, cols, totalVals)
}

func resetServiceValues() {
	serviceValues = nil
}

package importer

var (
	serviceSequence = 0
	services        map[string]int
	serviceValues   []any
)

func getServiceSk(row []string) int {
	if services == nil {
		services = make(map[string]int)
	}

	service := row[consumedServiceIndex]

	if service == "" {
		return 0
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

		return serviceSequence
	}

	return existentSequence
}

func getServiceStm() string {
	table := "dim_services"
	cols := []string{
		"service_sk",
		"service",
	}
	totalVals := len(serviceValues)
	return buildBatchInsert(table, cols, totalVals)
}

func resetServiceValues() {
	serviceValues = nil
}

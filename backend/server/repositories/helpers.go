package repositories

import (
	"fmt"
	"strings"
)

func getOffsetAndLimit(p PaginationParams) (int, int, error) {
	offset := (p.Page - 1) * p.Limit
	return offset, p.Limit, nil
}

func buildWhereClause(filters map[string]any) (string, []any) {
	var builder strings.Builder
	args := []any{}
	first := true

	startIndex := 1
	for key, value := range filters {
		if value == nil {
			continue
		}

		if !first {
			builder.WriteString(" AND ")
		}
		builder.WriteString(fmt.Sprintf("%s = $%d", key, startIndex))
		args = append(args, value)
		startIndex++
		first = false
	}

	return builder.String(), args
}

package utils

import (
	"fmt"
	"strings"
)

func BuildWhereClause(filters map[string]any) (string, []any) {
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

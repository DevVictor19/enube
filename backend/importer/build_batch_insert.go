package importer

import (
	"fmt"
	"strings"
)

func buildBatchInsert(tableName string, columns []string, totalValues int) string {
	if totalValues == 0 || len(columns) == 0 || totalValues%len(columns) != 0 {
		return ""
	}

	rows := totalValues / len(columns)
	var builder strings.Builder

	builder.WriteString("INSERT INTO ")
	builder.WriteString(tableName)
	builder.WriteString(fmt.Sprintf(" (%s) VALUES ", strings.Join(columns, ", ")))

	paramIndex := 1

	for i := 0; i < rows; i++ {
		builder.WriteString("(")
		for j := 0; j < len(columns); j++ {
			builder.WriteString(fmt.Sprintf("$%d", paramIndex))
			paramIndex++
			if j < len(columns)-1 {
				builder.WriteString(", ")
			}
		}
		builder.WriteString(")")
		if i < rows-1 {
			builder.WriteString(", ")
		}
	}

	builder.WriteString(";")
	return builder.String()
}

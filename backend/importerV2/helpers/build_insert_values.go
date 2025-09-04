package helpers

import (
	"strconv"
	"strings"
	"time"
)

func BuildInsertValues(values []string, cols int) string {
	if len(values) == 0 || cols <= 0 || len(values)%cols != 0 {
		return ""
	}

	var str strings.Builder
	rows := len(values) / cols
	paramIndex := 0

	for i := 0; i < rows; i++ {
		str.WriteString("(")
		for j := 0; j < cols; j++ {
			val := values[paramIndex]
			if val == "" {
				str.WriteString("NULL")
			} else if isNumeric(val) {
				str.WriteString(val)
			} else if t, ok := parseDate(val); ok {
				str.WriteString("'")
				str.WriteString(t.Format("2006-01-02 15:04:05"))
				str.WriteString("'")
			} else {
				str.WriteString("'" + escapeString(val) + "'")
			}

			if j != cols-1 {
				str.WriteString(", ")
			}
			paramIndex++
		}
		if i != rows-1 {
			str.WriteString("),\n")
		} else {
			str.WriteString(")")
		}
	}

	return str.String()
}

func isNumeric(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func parseDate(s string) (time.Time, bool) {
	layouts := []string{
		"2006-01-02",
		"2006-01-02 15:04:05",
		time.RFC3339,
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func escapeString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

package formatter

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/siglens/siglens/pkg/segment/structs"
)

// ProcessFormatResults formats the input records according to format command specifications
// and returns a single record with a "search" field containing the formatted search string
func ProcessFormatResults(records []map[string]interface{}, formatReq *structs.FormatResultsRequest) []map[string]interface{} {
	// Set defaults if not provided
	fmt.Printf("DEBUG: Processing format request: %+v\n", formatReq)

	if formatReq == nil {
		formatReq = getDefaultFormatResultsExpr()
	} else {
		// Fill in any missing options with defaults
		if formatReq.MVSeparator == "" {
			formatReq.MVSeparator = "OR"
		}
		if formatReq.EmptyString == "" {
			formatReq.EmptyString = "NOT()"
		}
		if formatReq.RowColOptions == nil {
			formatReq.RowColOptions = &structs.RowColOptions{
				RowPrefix:       "(",
				ColumnPrefix:    "(",
				ColumnSeparator: "AND",
				ColumnEnd:       ")",
				RowSeparator:    "OR",
				RowEnd:          ")",
			}
		}
		// Initialize NumFormatPatterns if nil
		if formatReq.NumFormatPatterns == nil {
			formatReq.NumFormatPatterns = make(map[string]string)
		}
	}

	// Limit results if MaxResults is specified
	if formatReq.MaxResults > 0 && uint64(len(records)) > formatReq.MaxResults {
		records = records[:formatReq.MaxResults]
	}

	// Check if there are any records to format
	if len(records) == 0 {
		// Return a single record containing the empty string
		return []map[string]interface{}{
			{"search": formatReq.EmptyString},
		}
	}

	// Format the records
	var formattedRows []string
	for _, record := range records {
		// Get sorted list of fields for consistent output order
		var fields []string
		for field := range record {
			fields = append(fields, field)
		}
		sort.Strings(fields)

		var formattedColumns []string
		for _, field := range fields {
			value := record[field]

			// Apply numeric formatting if applicable
			if formatPattern, exists := formatReq.NumFormatPatterns[field]; exists {
				if numValue, ok := value.(float64); ok {
					value = fmt.Sprintf(formatPattern, numValue)
				} else if numValue, ok := value.(int64); ok {
					value = fmt.Sprintf(formatPattern, float64(numValue))
				} else if numValue, ok := value.(int); ok {
					value = fmt.Sprintf(formatPattern, float64(numValue))
				}
			}

			// Handle multi-value fields
			if mvSlice, ok := value.([]interface{}); ok {
				// Format multiple values with the MVSeparator
				var mvValues []string
				for _, mvVal := range mvSlice {
					mvValues = append(mvValues, fmt.Sprintf("%s=\"%v\"", field, mvVal))
				}
				formattedColumns = append(formattedColumns, fmt.Sprintf("(%s)", strings.Join(mvValues, " "+formatReq.MVSeparator+" ")))
			} else {
				// Format single value
				formattedColumns = append(formattedColumns, fmt.Sprintf("%s=\"%v\"", field, value))
			}
		}

		// Format the row
		rowStr := formatReq.RowColOptions.ColumnPrefix +
			strings.Join(formattedColumns, " "+formatReq.RowColOptions.ColumnSeparator+" ") +
			formatReq.RowColOptions.ColumnEnd
		formattedRows = append(formattedRows, rowStr)
	}

	// Format all rows together
	searchStr := formatReq.RowColOptions.RowPrefix +
		strings.Join(formattedRows, " "+formatReq.RowColOptions.RowSeparator+" ") +
		formatReq.RowColOptions.RowEnd

	// Return the formatted string in a record with key "search"
	return []map[string]interface{}{
		{"search": searchStr},
	}
}

// ParseFormatCommand parses the format command string and returns a FormatResultsRequest
func ParseFormatCommand(formatCmd string) (*structs.FormatResultsRequest, error) {
	formatReq := getDefaultFormatResultsExpr()
	formatReq.NumFormatPatterns = make(map[string]string)

	// Remove leading and trailing whitespace
	formatCmd = strings.TrimSpace(formatCmd)

	// Parse numeric format patterns
	numRegex := regexp.MustCompile(`num\(([^)]+)\)\s*=\s*"([^"]+)"`)
	matches := numRegex.FindAllStringSubmatch(formatCmd, -1)

	if len(matches) > 0 {
		for _, match := range matches {
			if len(match) >= 3 {
				fieldName := strings.TrimSpace(match[1])
				formatPattern := match[2]
				formatReq.NumFormatPatterns[fieldName] = formatPattern
			}
		}
		return formatReq, nil
	}

	// Parse other format options
	parts := strings.Split(formatCmd, " ")
	for i := 0; i < len(parts); i++ {
		part := parts[i]
		switch part {
		case "maxresults":
			if i+1 < len(parts) {
				i++
				// Parse maxresults value
			}
		case "mvsep":
			if i+1 < len(parts) {
				i++
				formatReq.MVSeparator = parts[i]
			}
		case "emptystr":
			if i+1 < len(parts) {
				i++
				formatReq.EmptyString = parts[i]
			}
		default:
			// Unknown option
			return nil, fmt.Errorf("unknown format option: %s", part)
		}
	}

	return formatReq, nil
}

// getDefaultFormatResultsExpr returns default format command settings
func getDefaultFormatResultsExpr() *structs.FormatResultsRequest {
	return &structs.FormatResultsRequest{
		MVSeparator: "OR",
		MaxResults:  0,
		EmptyString: "NOT()",
		RowColOptions: &structs.RowColOptions{
			RowPrefix:       "(",
			ColumnPrefix:    "(",
			ColumnSeparator: "AND",
			ColumnEnd:       ")",
			RowSeparator:    "OR",
			RowEnd:          ")",
		},
		NumFormatPatterns: make(map[string]string),
	}
}

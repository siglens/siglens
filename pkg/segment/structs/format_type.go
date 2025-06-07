package structs

type FormatResultsRequest struct {
	MVSeparator       string
	MaxResults        uint64
	EmptyString       string
	RowColOptions     *RowColOptions
	NumFormatPatterns map[string]string // Add this field
}

type RowColOptions struct {
	RowPrefix       string
	ColumnPrefix    string
	ColumnSeparator string
	ColumnEnd       string
	RowSeparator    string
	RowEnd          string
}

package aggregations

import (
	"fmt"

	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
)

// EvalNullIf implements the nullif(expr1, expr2) SPL function.
// Returns nil when expr1 equals expr2, otherwise returns expr1.
// Supports string and numeric type comparison with proper type handling.
func EvalNullIf(args []*structs.ValueExpr, fieldToValue map[string]sutils.CValueEnclosure) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("nullif function requires exactly 2 arguments")
	}

	val1, err := args[0].EvaluateValueExpr(fieldToValue)
	if err != nil {
		return nil, err
	}

	val2, err := args[1].EvaluateValueExpr(fieldToValue)
	if err != nil {
		return nil, err
	}

	// Handle nil values first for efficiency
	if val1 == nil && val2 == nil {
		return nil, nil
	}
	if val1 == nil || val2 == nil {
		return val1, nil
	}

	// Try numeric comparison if both values can be converted to float64
	v1Float, v1IsFloat := tryConvertToFloat64(val1)
	v2Float, v2IsFloat := tryConvertToFloat64(val2)

	if v1IsFloat && v2IsFloat {
		if v1Float == v2Float {
			return nil, nil
		}
		return val1, nil
	}

	// Fall back to string comparison for non-numeric values
	v1Str := fmt.Sprintf("%v", val1)
	v2Str := fmt.Sprintf("%v", val2)
	if v1Str == v2Str {
		return nil, nil
	}

	return val1, nil
}

// tryConvertToFloat64 attempts to convert various data types to float64.
// Returns the numeric value and true if conversion was successful.
func tryConvertToFloat64(val interface{}) (float64, bool) {
	if val == nil {
		return 0, false
	}

	switch v := val.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint64:
		return float64(v), true
	case uint32:
		return float64(v), true
	case string:
		// Parse strings that contain numbers
		f, err := sutils.ParseStringToFloat64(v)
		if err == nil {
			return f, true
		}
	}

	return 0, false
}

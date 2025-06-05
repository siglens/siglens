package aggregations

import (
	"fmt"

	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
)

// EvalNullIf evaluates the nullif(expr1, expr2) function.
// Returns nil if both values are equal, otherwise returns the first value.
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

	// Handle nil values
	if val1 == nil && val2 == nil {
		return nil, nil
	}
	if val1 == nil || val2 == nil {
		return val1, nil
	}

	// Compare numeric values properly
	v1Float, v1IsFloat := tryConvertToFloat64(val1)
	v2Float, v2IsFloat := tryConvertToFloat64(val2)

	if v1IsFloat && v2IsFloat {
		// Both are numeric, compare as numbers
		if v1Float == v2Float {
			return nil, nil
		}
		return val1, nil
	}

	// Compare as strings as fallback
	v1Str := fmt.Sprintf("%v", val1)
	v2Str := fmt.Sprintf("%v", val2)
	if v1Str == v2Str {
		return nil, nil
	}

	return val1, nil
}

// tryConvertToFloat64 attempts to convert a value to float64
// Returns the converted value and a boolean indicating success
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
		// Try to parse the string as a number
		f, err := sutils.ParseStringToFloat64(v)
		if err == nil {
			return f, true
		}
	}

	return 0, false
}

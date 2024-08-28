package utils

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test_isInvalid tests if the number is invalid. It should return invalidType.
func Test_isInvalid(t *testing.T) {
	n := Number{}
	n.SetInvalidType()
	assert.True(t, n.ntype() == invalidType)
}

// Test_invalidTypeReturnsErrorForInt64AndFloat64 tests converting invalidType to Int64 and Float64.
func Test_invalidTypeReturnsErrorForInt64AndFloat64(t *testing.T) {
	n := Number{}
	n.SetInvalidType()
	if val, err := n.Int64(); err == nil || val != 0 {
		assert.Fail(t, "Expected 0, got %v with error %v", val, err)
	}
	if val, err := n.Float64(); err == nil || val != 0 {
		assert.Fail(t, "Expected 0, got %v with error %v", val, err)
	}
}

// Test_isBackfill tests if the number is a backfill. It should return 0 for both Int64 and Float64.
func Test_isBackfill(t *testing.T) {

	n := Number{}
	n.SetBackfillType()
	assert.True(t, n.ntype() == backfillType)

	intVal, err := n.Int64()
	assert.True(t, intVal == 0, nil)
	assert.True(t, err == nil)

	floatVal, err := n.Float64()
	assert.True(t, floatVal == 0, nil)
	assert.True(t, err == nil)
}

// Test_isInt64 tests if the number is an int64. It should return set value for Int64 and an error for Float64.
func Test_isInt64(t *testing.T) {

	n := Number{}
	n.SetInt64(42)
	assert.True(t, n.ntype() == int64Type)

	val, err := n.Int64()
	assert.True(t, val == 42)
	assert.True(t, err == nil)

	_, err = n.Float64()
	assert.True(t, err != nil)

}

// Test_isFloat64 tests if the number is a float64. It should return set value for Float64 and an error for Int64.
func Test_isFloat64(t *testing.T) {

	n := Number{}
	n.SetFloat64(42.0)
	assert.True(t, n.ntype() == float64Type)

	val, err := n.Float64()
	assert.True(t, val == 42.0)
	assert.True(t, err == nil)

	_, err = n.Int64()
	assert.True(t, err != nil)

}

// Test_reset tests resetting the number. It should return invalidType.
func Test_reset(t *testing.T) {

	n := Number{}
	n.SetInt64(42)
	assert.True(t, n.ntype() == int64Type)
	n.Reset()
	assert.True(t, n.ntype() == invalidType)

}

// Test_constructor tests the constructor of the number. It should return the correct type.
func Test_constructor(t *testing.T) {

	n := Number{}
	assert.True(t, n.ntype() == invalidType)

	n = Number{[9]byte{0, 0, 0, 0, 0, 0, 0, 0, backfillType}}
	assert.True(t, n.ntype() == backfillType)

	n = Number{[9]byte{0, 0, 0, 0, 0, 0, 0, 0, int64Type}}
	assert.True(t, n.ntype() == int64Type)

	n = Number{[9]byte{0, 0, 0, 0, 0, 0, 0, 0, float64Type}}
	assert.True(t, n.ntype() == float64Type)

}

// Test_numberSetAndGet tests setting and getting the number. It should return the correct value.
func Test_numberSetAndGet(t *testing.T) {
	n := Number{}

	// Test Int64
	n.SetInt64(42)
	if val, err := n.Int64(); err != nil || val != 42 {
		assert.Fail(t, "Expected 42, got %v with error %v", val, err)
	}

	// Test Float64
	n.SetFloat64(3.14)
	if val, err := n.Float64(); err != nil || math.Abs(val-3.14) > 1e-6 {
		assert.Fail(t, "Expected 3.14, got %v with error %v", val, err)
	}

	// Test Invalid Type
	n.SetInvalidType()
	if _, err := n.Int64(); err == nil {
		assert.Fail(t, "Expected error for invalid type")
	}
	if _, err := n.Float64(); err == nil {
		assert.Fail(t, "Expected error for invalid type")
	}

	// Test Backfill Type
	n.SetBackfillType()
	if val, err := n.Int64(); err != nil || val != 0 {
		assert.Fail(t, "Expected 0 for backfill, got %v with error %v", val, err)
	}
	if val, err := n.Float64(); err != nil || val != 0 {
		assert.Fail(t, "Expected 0 for backfill, got %v with error %v", val, err)
	}
}

// Test_numberConversion tests converting the number. It should return the correct value.
func TestNumberConversion(t *testing.T) {
	n := Number{}

	// Test Int64 to Float64 conversion
	n.SetInt64(42)
	if err := n.ConvertToFloat64(); err != nil {
		assert.Fail(t, "Conversion failed: %v", err)
	}
	if val, err := n.Float64(); err != nil || math.Abs(val-42.0) > 1e-6 {
		assert.Fail(t, "Expected 42.0, got %v with error %v", val, err)
	}

	// Test Float64 to Int64 conversion
	n.SetFloat64(3.14)
	if err := n.ConvertToInt64(); err != nil {
		assert.Fail(t, "Conversion failed: %v", err)
	}
	if val, err := n.Int64(); err != nil || val != 3 {
		assert.Fail(t, "Expected 3, got %v with error %v", val, err)
	}
}

// Test_numberCopyToBuffer tests copying the number to a buffer. It should return the correct value.
func Test_numberCopyToBuffer(t *testing.T) {
	n := Number{}
	n.SetFloat64(3.14)
	buf := make([]byte, 9)
	n.CopyToBuffer(buf)
	if buf[8] != float64Type {
		assert.Fail(t, "Expected float64Type, got %v", buf[8])
	}
}

type reduceTest struct {
	name     string
	n1       Number
	n2       Number
	fun      AggregateFunctions
	expected Number
}

func getTests() []reduceTest {
	return []reduceTest{
		{"Sum", Number{}, Number{}, Sum, Number{}},
		{"Min", Number{}, Number{}, Min, Number{}},
		{"Max", Number{}, Number{}, Max, Number{}},
		{"Count", Number{}, Number{}, Count, Number{}},
	}
}

// Test_numberReduceFast tests aggregating functions on Int64 numbers using ReduceFast.
func Test_numberReduceFastInt(t *testing.T) {
	tests := getTests()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.n1.SetInt64(10)
			tt.n2.SetInt64(20)
			tt.expected.SetInt64(30)
			if tt.name == "Min" {
				tt.expected.SetInt64(10)
			} else if tt.name == "Max" {
				tt.expected.SetInt64(20)
			}

			err := tt.n1.ReduceFast(&tt.n2, tt.fun)
			if err != nil {
				assert.Fail(t, "ReduceFast failed: %v", err)
			}

			if tt.n1.ntype() != tt.expected.ntype() {
				assert.Fail(t, "Expected %v, got %v", tt.expected.ntype(), tt.n1.ntype())
			}

			v1, _ := tt.n1.Int64()
			v2, _ := tt.expected.Int64()
			if v1 != v2 {
				assert.Fail(t, "Expected %v, got %v", v2, v1)
			}
		})
	}
}

// Test_numberReduceFastFloat tests aggregating functions on Float64 numbers using ReduceFast.
func Test_numberReduceFastFloat(t *testing.T) {
	tests := getTests()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.n1.SetFloat64(10.5)
			tt.n2.SetFloat64(20.5)
			tt.expected.SetFloat64(31.0)
			if tt.name == "Min" {
				tt.expected.SetFloat64(10.5)
			} else if tt.name == "Max" {
				tt.expected.SetFloat64(20.5)
			}

			err := tt.n1.ReduceFast(&tt.n2, tt.fun)
			if err != nil {
				assert.Fail(t, "ReduceFast failed: %v", err)
			}

			if tt.n1.ntype() != tt.expected.ntype() {
				assert.Fail(t, "Expected %v, got %v", tt.expected.ntype(), tt.n1.ntype())
			}

			v1, _ := tt.n1.Float64()
			v2, _ := tt.expected.Float64()
			if v1 != v2 {
				assert.Fail(t, "Expected %v, got %v", v2, v1)
			}
		})
	}
}

// Test_numberReduceFastInvalid tests aggregating functions on invalid numbers using ReduceFast.
func Test_numberReduceFastInvalid(t *testing.T) {
	tests := getTests()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.n1.SetInvalidType()
			tt.n2.SetInvalidType()
			tt.expected.SetInvalidType()
			if tt.name == "Min" {
				tt.expected.SetInvalidType()
			} else if tt.name == "Max" {
				tt.expected.SetInvalidType()
			}

			err := tt.n1.ReduceFast(&tt.n2, tt.fun)
			if err != nil {
				assert.Fail(t, "ReduceFast failed: %v", err)
			}

			if tt.n1.ntype() != tt.expected.ntype() {
				assert.Fail(t, "Expected %v, got %v", tt.expected.ntype(), tt.n1.ntype())
			}
		})
	}
}

// Test_numberReduceFastBackfill tests aggregating functions on backfill numbers using ReduceFast.
func Test_numberReduceFastBackfill(t *testing.T) {
	tests := getTests()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.n1.SetBackfillType()
			tt.n2.SetBackfillType()
			tt.expected.SetBackfillType()

			err := tt.n1.ReduceFast(&tt.n2, tt.fun)
			if err != nil {
				assert.Fail(t, "ReduceFast failed: %v", err)
			}

			if tt.n1.ntype() != tt.expected.ntype() {
				assert.Fail(t, "Expected %v, got %v", tt.expected.ntype(), tt.n1.ntype())
			}

			v1, _ := tt.n1.Int64()
			v2, _ := tt.expected.Int64()
			if v1 != v2 {
				assert.Fail(t, "Expected %v, got %v", v2, v1)
			}

		})
	}
}

// Test_numberReduceFastMixed tests aggregating functions on mixed numbers using ReduceFast.
func Test_numberReduceFastMixed(t *testing.T) {
	tests := getTests()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.n1.SetInt64(10)
			tt.n2.SetFloat64(20.5)
			tt.expected.SetFloat64(30.5)
			if tt.name == "Min" {
				tt.expected.SetFloat64(10.0)
			} else if tt.name == "Max" {
				tt.expected.SetFloat64(20.5)
			}

			err := tt.n1.ReduceFast(&tt.n2, tt.fun)
			if err != nil {
				assert.Fail(t, "ReduceFast failed: %v", err)
			}

			if tt.n1.ntype() != tt.expected.ntype() {
				assert.Fail(t, "Expected %v, got %v", tt.expected.ntype(), tt.n1.ntype())
			}

			if tt.n1.ntype() == int64Type {
				assert.Fail(t, "Expected float64Type, got int64Type")
			}

			v1, _ := tt.n1.Float64()
			v2, _ := tt.expected.Float64()
			if v1 != v2 {
				assert.Fail(t, "Expected %v, got %v", v2, v1)
			}
		})
	}
}

// Test_numberReduceFastUnsupported tests aggregating functions on unsupported numbers using ReduceFast.

func Test_numberReduceFastUnsupported(t *testing.T) {
	const unsupportedType = 0xFF
	n1 := Number{[9]byte{0, 0, 0, 0, 0, 0, 0, 0, unsupportedType}}
	n2 := Number{}
	n1.SetInvalidType()
	n2.SetInvalidType()
	err := n1.ReduceFast(&n2, Sum)
	if err != nil {
		assert.Fail(t, "ReduceFast failed: %v", err)
	}
}

func Test_convertBytesToNumber(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		intVal   int64
		floatVal float64
		dtype    SS_DTYPE
	}{
		{"InvalidType", make([]byte, 9), 0, 0, SS_INVALID},
		{"BackfillType", append(make([]byte, 8), backfillType), 0, 0, SS_DT_BACKFILL},
		{"Int64Type", nil, 42, 0, SS_DT_SIGNED_NUM},
		{"Float64Type", nil, 0, 3.14, SS_DT_UNSIGNED_NUM},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "Int64Type" {
				n := Number{}
				n.SetInt64(tt.intVal)
				tt.input = n.bytes[:]
			} else if tt.name == "Float64Type" {
				n := Number{}
				n.SetFloat64(tt.floatVal)
				tt.input = n.bytes[:]
			}

			intVal, floatVal, dtype := ConvertBytesToNumber(tt.input)
			if intVal != tt.intVal || math.Abs(floatVal-tt.floatVal) > 1e-6 || dtype != tt.dtype {
				assert.Fail(t, "Expected %v, %v, %v, got %v, %v, %v",
					tt.intVal, tt.floatVal, tt.dtype, intVal, floatVal, dtype)
			}
		})
	}
}

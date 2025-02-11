package utils

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Using LessOrEqual to compare float64 values to avoid floating point errors.

// Test_isInvalid tests if the number is invalid. It should return invalidType.
func Test_isInvalid(t *testing.T) {
	n := Number{}
	n.SetInvalidType()
	assert.Equal(t, n.ntype(), invalidType)
}

// Test_invalidTypeReturnsErrorForInt64AndFloat64 tests converting invalidType to Int64 and Float64.
func Test_invalidTypeReturnsErrorForInt64AndFloat64(t *testing.T) {
	n := Number{}
	n.SetInvalidType()

	_, err := n.Int64()
	assert.NotNil(t, err, "Expected error for invalidType while converting to int64")

	_, err = n.Float64()
	assert.NotNil(t, err, "Expected error for invalidType while converting to float64")

}

// Test_isBackfill tests if the number is a backfill. It should return 0 for both Int64 and Float64.
func Test_isBackfill(t *testing.T) {

	n := Number{}
	n.SetBackfillType()
	assert.Equal(t, n.ntype(), backfillType)

	intVal, err := n.Int64()
	assert.Equal(t, int64(0), intVal)
	assert.Nil(t, err, "No error expected for backfillType while converting to int64")

	floatVal, err := n.Float64()
	assert.LessOrEqual(t, math.Abs(floatVal-0.0), 1e-6)
	assert.Nil(t, err, "No error expected for backfillType while converting to float64")
}

// Test_isInt64 tests if the number is an int64. It should return set value for Int64 and an error for Float64.
func Test_isInt64(t *testing.T) {

	n := Number{}
	n.SetInt64(42)
	assert.Equal(t, int64Type, n.ntype())

	val, err := n.Int64()
	assert.Equal(t, int64(42), val)
	assert.Nil(t, err, "No error expected for int64Type while retrieving value as int64")

	_, err = n.Float64()
	assert.NotNil(t, err, "Expected error for int64Type while retrieving as float64")

}

// Test_isFloat64 tests if the number is a float64. It should return set value for Float64 and an error for Int64.
func Test_isFloat64(t *testing.T) {

	n := Number{}
	n.SetFloat64(42.0)
	assert.Equal(t, float64Type, n.ntype())

	val, err := n.Float64()
	assert.LessOrEqual(t, math.Abs(val-42.0), 1e-6)
	assert.Nil(t, err, "No error expected for float64Type while retrieving value as float64")

	_, err = n.Int64()
	assert.NotNil(t, err, "Expected error for float64Type while retrieving as int64")

}

// Test_reset tests resetting the number. It should return invalidType.
func Test_reset(t *testing.T) {

	n := Number{}
	n.SetInt64(42)
	assert.Equal(t, int64Type, n.ntype())
	n.Reset()
	assert.Equal(t, invalidType, n.ntype())

}

// Test_constructor tests the constructor of the number. It should return the correct type.
func Test_constructor(t *testing.T) {

	n := Number{}
	assert.Equal(t, invalidType, n.ntype())

}

// Test_numberConversion tests converting the number. It should return the correct value.
func TestNumberConversion(t *testing.T) {
	n := Number{}

	// Test Int64 to Float64 conversion
	n.SetInt64(42)
	assert.Equal(t, int64Type, n.ntype())

	err := n.ConvertToFloat64()
	assert.Nil(t, err, "No error expected for int64Type while converting to float64")
	assert.Equal(t, float64Type, n.ntype())

	floatFromInt, err := n.Float64()
	assert.LessOrEqual(t, math.Abs(floatFromInt-42.0), 1e-6)
	assert.Nil(t, err, "No error expected while retrieving as float64 after converting from int64")

	// Test Float64 to Int64 conversion
	n.SetFloat64(3.14)
	assert.Equal(t, float64Type, n.ntype())

	err = n.ConvertToInt64()
	assert.Nil(t, err, "Expected no error for float64Type while converting to int64")
	assert.Equal(t, int64Type, n.ntype())

	intFromFloat, err := n.Int64()
	assert.Equal(t, int64(3), intFromFloat)
	assert.Nil(t, err, "Expected no error while retrieving as int64 after converting from float64")

}

// Test_numberCopyToBuffer tests copying the number to a buffer. It should return the correct value.
func Test_numberCopyToBuffer(t *testing.T) {
	n := Number{}
	n.SetFloat64(3.14)
	buf := make([]byte, 9)
	n.CopyToBuffer(buf)

	assert.Equal(t, float64Type, buf[8])
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

// Test_numberReduceFast tests aggregating functions on numbers using ReduceFast.
func Test_numberReduceFast(t *testing.T) {
	tests := getTests()

	testCases := []struct {
		name  string
		setup func(*Number, *Number, *Number, string)
	}{
		{
			name: "Int64",
			setup: func(n1, n2, expected *Number, name string) {
				n1.SetInt64(10)
				n2.SetInt64(20)
				expected.SetInt64(30)

				if name == "Min" {
					expected.SetInt64(10)
				}
				if name == "Max" {
					expected.SetInt64(20)
				}
			},
		},
		{
			name: "Float64",
			setup: func(n1, n2, expected *Number, name string) {
				n1.SetFloat64(10.5)
				n2.SetFloat64(20.5)
				expected.SetFloat64(31.0)

				if name == "Min" {
					expected.SetFloat64(10.5)
				}
				if name == "Max" {
					expected.SetFloat64(20.5)
				}
			},
		},
		{
			name: "InvalidType",
			setup: func(n1, n2, expected *Number, name string) {
				n1.SetInvalidType()
				n2.SetInvalidType()
				expected.SetInvalidType()
			},
		},
		{
			name: "BackfillType",
			setup: func(n1, n2, expected *Number, name string) {
				n1.SetBackfillType()
				n2.SetBackfillType()
				expected.SetBackfillType()
			},
		},
		{
			name: "MixedIntWithFloat",
			setup: func(n1, n2, expected *Number, name string) {
				n1.SetInt64(10)
				n2.SetFloat64(20.5)
				expected.SetFloat64(30.5)

				if name == "Min" {
					expected.SetFloat64(10.0)
				}
				if name == "Max" {
					expected.SetFloat64(20.5)
				}
			},
		},
		{
			name: "MixedFloatWithInt",
			setup: func(n1, n2, expected *Number, name string) {
				n1.SetFloat64(10.5)
				n2.SetInt64(20)
				expected.SetFloat64(30.5)

				if name == "Min" {
					expected.SetFloat64(10.5)
				}
				if name == "Max" {
					expected.SetFloat64(20.0)
				}
			},
		},
	}

	for _, tt := range tests {
		for _, tc := range testCases {

			t.Run(tt.name+"_"+tc.name, func(t *testing.T) {
				tc.setup(&tt.n1, &tt.n2, &tt.expected, tt.name)

				err := tt.n1.ReduceFast(&tt.n2, tt.fun)
				assert.Nil(t, err, "Expected no error for ReduceFast")

				assert.Equal(t, tt.n1.ntype(), tt.expected.ntype())

				if tt.n1.ntype() == int64Type {
					v1, _ := tt.n1.Int64()
					v2, _ := tt.expected.Int64()
					assert.Equal(t, v1, v2)
				}

				if tt.n1.ntype() == float64Type {
					v1, _ := tt.n1.Float64()
					v2, _ := tt.expected.Float64()
					assert.LessOrEqual(t, math.Abs(v1-v2), 1e-6)
				}
			})

		}
	}
}

// Test_numberReduceFastUnsupported tests unsupported aggregating functions on numbers using ReduceFast.
func Test_numberReduceFastUnsupportedFunction(t *testing.T) {
	const unsupportedType = 0xFF
	n1 := Number{}
	n1.SetInt64(10)
	n2 := Number{}
	n2.SetInt64(20)
	err := n1.ReduceFast(&n2, unsupportedType)
	assert.NotNil(t, err, "Expected error for unsupported function type")
}

// Test_convertBytesToNumber tests converting bytes to number. It should return the correct value.
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

			assert.Equal(t, tt.intVal, intVal)
			assert.LessOrEqual(t, math.Abs(tt.floatVal-floatVal), 1e-6)
			assert.Equal(t, tt.dtype, dtype)

		})
	}
}

// Test_convertInvalidBytesToNumber tests converting invalid bytes to number. It should return 0 for both Int64 and Float64.
func Test_convertInvalidBytesToNumber(t *testing.T) {
	intVal, floatVal, dtype := ConvertBytesToNumber(nil)

	assert.Equal(t, int64(0), intVal)
	assert.LessOrEqual(t, math.Abs(floatVal-0.0), 1e-6)
	assert.Equal(t, SS_INVALID, dtype)
}

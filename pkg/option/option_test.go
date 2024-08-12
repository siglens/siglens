package option

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_isNil(t *testing.T) {
	// Non-nilable types.
	verifyValidOrNil(t, []int{0, 42}, nil)
	verifyValidOrNil(t, []string{"", "foo"}, nil)
	verifyValidOrNil(t, []bool{false, true}, nil)
	verifyValidOrNil(t, []float64{0.0, 42.0}, nil)
	verifyValidOrNil(t, []struct{ Value int }{{}, {42}}, nil)

	// Nilable types.
	verifyValidOrNil(t, []map[string]int{{}, {"foo": 42}}, []map[string]int{nil})
	verifyValidOrNil(t, [][]int{{}, {42}}, [][]int{nil})
	verifyValidOrNil(t, []func() int{func() int { return 42 }}, []func() int{nil})
	verifyValidOrNil(t, []func(x, y float32) float32{func(x, y float32) float32 { return 42 }}, []func(x, y float32) float32{nil})
	verifyValidOrNil(t, []chan int{make(chan int)}, []chan int{nil})
	verifyValidOrNil(t, []*int{new(int)}, []*int{nil})
	verifyValidOrNil(t, []interface{}{42, "foo"}, []interface{}{nil})
}

func verifyValidOrNil[U any](t *testing.T, nonNilValues []U, nilValues []U) {
	for _, value := range nonNilValues {
		assert.False(t, isNil(value), fmt.Sprintf("value %v should not be nil", value))
	}

	for _, value := range nilValues {
		assert.True(t, isNil(value), fmt.Sprintf("value %v should be nil", value))
	}
}

func Test_Constructor_NonNilableType(t *testing.T) {
	option := NewUnsetOption[int]()
	_, ok := option.Get()
	assert.False(t, ok)

	option = NewOptionWithValue(42)
	value, ok := option.Get()
	assert.True(t, ok)
	assert.Equal(t, 42, value)
}

func Test_Constructor_NilableType(t *testing.T) {
	option := NewUnsetOption[*int]()
	_, ok := option.Get()
	assert.False(t, ok)

	x := 42
	option = NewOptionWithValue(&x)
	value, ok := option.Get()
	assert.True(t, ok)
	assert.Equal(t, &x, value)

	option = NewOptionWithValue[*int](nil)
	_, ok = option.Get()
	assert.False(t, ok)
}

func Test_SetAndClear_NonNilableType(t *testing.T) {
	option := NewUnsetOption[int]()
	option.Set(42)
	value, ok := option.Get()
	assert.True(t, ok)
	assert.Equal(t, 42, value)

	option.Clear()
	_, ok = option.Get()
	assert.False(t, ok)
}

func Test_SetAndClear_NilableType(t *testing.T) {
	option := NewUnsetOption[map[string]int]()
	option.Set(map[string]int{"foo": 42})
	value, ok := option.Get()
	assert.True(t, ok)
	assert.Equal(t, map[string]int{"foo": 42}, value)

	option.Clear()
	_, ok = option.Get()
	assert.False(t, ok)

	option.Set(map[string]int{})
	value, ok = option.Get()
	assert.True(t, ok)
	assert.Equal(t, map[string]int{}, value)

	option.Clear()
	_, ok = option.Get()
	assert.False(t, ok)
}

func Test_Set_NilValue(t *testing.T) {
	verifyValueIsNone[map[string]int](t, nil)
	verifyValueIsNone[map[string]struct{}](t, nil)
	verifyValueIsNone[map[string]map[int]interface{}](t, nil)
	verifyValueIsNone[*int](t, nil)
	verifyValueIsNone[[]int](t, nil)
	verifyValueIsNone[func()](t, nil)
	verifyValueIsNone[func(x, y float32) float32](t, nil)
	verifyValueIsNone[chan int](t, nil)
	verifyValueIsNone[interface{}](t, nil)
}

func verifyValueIsNone[U any](t *testing.T, nilValue U) {
	option := NewUnsetOption[U]()
	option.Set(nilValue)
	_, ok := option.Get()
	assert.False(t, ok)
}

func Test_NilOptionStruct(t *testing.T) {
	optionPtr := (*Option[int])(nil)
	_, ok := optionPtr.Get()
	assert.False(t, ok)

	optionPtr.Set(42)
	_, ok = optionPtr.Get()
	assert.False(t, ok)

	optionPtr.Clear()
	_, ok = optionPtr.Get()
	assert.False(t, ok)
}

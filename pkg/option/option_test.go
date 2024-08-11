package option

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func verifyIsNone[T any](t *testing.T, nonNilValues []T, nilValues []T) {
	option := NewOption[T]()
	assert.True(t, option.isNone())

	for _, value := range nonNilValues {
		option.Set(value)
		assert.False(t, option.isNone(), fmt.Sprintf("value %v should not be None", value))
	}

	for _, value := range nilValues {
		option.Set(value)
		assert.True(t, option.isNone(), fmt.Sprintf("value %v should be None", value))
	}
}

func Test_isNone(t *testing.T) {
	// Non-nilable types.
	verifyIsNone(t, []int{0, 42}, nil)
	verifyIsNone(t, []string{"", "foo"}, nil)
	verifyIsNone(t, []bool{false, true}, nil)
	verifyIsNone(t, []float64{0.0, 42.0}, nil)

	// Nilable types.
	verifyIsNone(t, []map[string]int{{}, {"foo": 42}}, []map[string]int{nil})
	verifyIsNone(t, [][]int{{}, {42}}, [][]int{nil})
	verifyIsNone(t, []func() int{func() int { return 42 }}, []func() int{nil})
	verifyIsNone(t, []func(x, y float32) float32{func(x, y float32) float32 { return 42 }}, []func(x, y float32) float32{nil})
	verifyIsNone(t, []chan int{make(chan int)}, []chan int{nil})
	verifyIsNone(t, []*int{new(int)}, []*int{nil})
	verifyIsNone(t, []struct{ Value int }{{}, {42}}, []struct{ Value int }{})
}

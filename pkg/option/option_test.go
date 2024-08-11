package option

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func verifyIsNil[U any](t *testing.T, nonNilValues []U, nilValues []U) {
	for _, value := range nonNilValues {
		assert.False(t, isNil(value), fmt.Sprintf("value %v should not be nil", value))
	}

	for _, value := range nilValues {
		assert.True(t, isNil(value), fmt.Sprintf("value %v should be nil", value))
	}
}

func Test_isNone(t *testing.T) {
	// Non-nilable types.
	verifyIsNil(t, []int{0, 42}, nil)
	verifyIsNil(t, []string{"", "foo"}, nil)
	verifyIsNil(t, []bool{false, true}, nil)
	verifyIsNil(t, []float64{0.0, 42.0}, nil)

	// Nilable types.
	verifyIsNil(t, []map[string]int{{}, {"foo": 42}}, []map[string]int{nil})
	verifyIsNil(t, [][]int{{}, {42}}, [][]int{nil})
	verifyIsNil(t, []func() int{func() int { return 42 }}, []func() int{nil})
	verifyIsNil(t, []func(x, y float32) float32{func(x, y float32) float32 { return 42 }}, []func(x, y float32) float32{nil})
	verifyIsNil(t, []chan int{make(chan int)}, []chan int{nil})
	verifyIsNil(t, []*int{new(int)}, []*int{nil})
	verifyIsNil(t, []struct{ Value int }{{}, {42}}, []struct{ Value int }{})
}

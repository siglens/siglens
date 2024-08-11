package option

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_isNone_int(t *testing.T) {
	option := NewOption[int]()
	assert.True(t, option.isNone())

	option.Set(42)
	assert.False(t, option.isNone())
}

func Test_isNone_map(t *testing.T) {
	option := NewOption[map[string]int]()
	assert.True(t, option.isNone())

	option.Set(map[string]int{"foo": 42})
	assert.False(t, option.isNone())

	option.Set(nil)
	assert.True(t, option.isNone())
}

func Test_isNone_slice(t *testing.T) {
	option := NewOption[[]int]()
	assert.True(t, option.isNone())

	option.Set([]int{42})
	assert.False(t, option.isNone())

	option.Set(nil)
	assert.True(t, option.isNone())
}

func Test_isNone_func(t *testing.T) {
	option := NewOption[func() int]()
	assert.True(t, option.isNone())

	option.Set(func() int { return 42 })
	assert.False(t, option.isNone())

	option.Set(nil)
	assert.True(t, option.isNone())
}

func Test_isNone_chan(t *testing.T) {
	option := NewOption[chan int]()
	assert.True(t, option.isNone())

	option.Set(make(chan int))
	assert.False(t, option.isNone())

	option.Set(nil)
	assert.True(t, option.isNone())
}

func Test_isNone_pointer(t *testing.T) {
	option := NewOption[*int]()
	assert.True(t, option.isNone())

	i := 42
	option.Set(&i)
	assert.False(t, option.isNone())

	option.Set(nil)
	assert.True(t, option.isNone())
}

func Test_isNone_struct(t *testing.T) {
	type Foo struct {
		Value int
	}

	option := NewOption[Foo]()
	assert.True(t, option.isNone())

	option.Set(Foo{Value: 42})
	assert.False(t, option.isNone())

	// A zero-value struct is not None.
	option.Set(Foo{})
	assert.False(t, option.isNone())
}

func Test_isNone_string(t *testing.T) {
	option := NewOption[string]()
	assert.True(t, option.isNone())

	option.Set("foo")
	assert.False(t, option.isNone())

	option.Set("")
	assert.False(t, option.isNone())
}

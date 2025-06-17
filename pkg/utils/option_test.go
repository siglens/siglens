// Copyright (c) 2021-2024 SigScalr, Inc.
//
// # This file is part of SigLens Observability Solution
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package utils

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

func Test_Option_EncodeDecode_newOption(t *testing.T) {
	option := NewOptionWithValue(42)
	encoded, err := option.GobEncode()
	assert.NoError(t, err)

	decoded := NewUnsetOption[int]()
	err = decoded.GobDecode(encoded)
	assert.NoError(t, err)

	value, ok := decoded.Get()
	assert.True(t, ok)
	assert.Equal(t, 42, value)
}

func Test_Option_EncodeDecode_replaceOption(t *testing.T) {
	option := NewOptionWithValue(42)
	encoded, err := option.GobEncode()
	assert.NoError(t, err)

	decoded := NewOptionWithValue(1)
	err = decoded.GobDecode(encoded)
	assert.NoError(t, err)

	value, ok := decoded.Get()
	assert.True(t, ok)
	assert.Equal(t, 42, value)
}

func Test_Option_EncodeDecode_badEncoding(t *testing.T) {
	encoded := []byte("bad encoding")
	decoded := NewOptionWithValue(42)
	err := decoded.GobDecode(encoded)
	assert.Error(t, err)
}

func Test_Option_EncodeDecode_nilOption(t *testing.T) {
	option := NewUnsetOption[int]()
	encoded, err := option.GobEncode()
	assert.NoError(t, err)

	decoded := NewOptionWithValue(1)
	err = decoded.GobDecode(encoded)
	assert.NoError(t, err)

	_, ok := decoded.Get()
	assert.False(t, ok)
}

func Test_EqualOptions(t *testing.T) {
	option1 := NewOptionWithValue(42)
	option2 := NewOptionWithValue(42)
	assert.True(t, EqualOptions(option1, option2))

	option2.Set(43)
	assert.False(t, EqualOptions(option1, option2))
	option1.Set(43)
	assert.True(t, EqualOptions(option1, option2))

	option2.Clear()
	assert.False(t, EqualOptions(option1, option2))
	option1.Clear()
	assert.True(t, EqualOptions(option1, option2))
}

package option

import (
	"reflect"
)

type Option[T any] struct {
	value    T
	hasValue bool // This will never be true if `value` is nil.
}

func NewUnsetOption[T any]() Option[T] {
	return Option[T]{hasValue: false}
}

func NewOptionWithValue[T any](value T) Option[T] {
	option := NewUnsetOption[T]()
	option.Set(value)

	return option
}

func (o *Option[T]) Set(value T) {
	if o == nil {
		return
	}

	if isNil(value) {
		o.Clear()
		return
	}

	o.value = value
	o.hasValue = true
}

func (o *Option[T]) Clear() {
	if o == nil {
		return
	}

	var defaultValue T
	o.value = defaultValue
	o.hasValue = false
}

func (o *Option[T]) Get() (T, bool) {
	if o == nil {
		var defaultValue T
		return defaultValue, false
	}

	return o.value, o.hasValue
}

func isNil(value interface{}) bool {
	if value == nil {
		return true
	}

	reflectValue := reflect.ValueOf(value)
	switch reflectValue.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return reflectValue.IsNil()
	default:
		return false
	}
}

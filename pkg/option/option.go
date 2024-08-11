package option

import (
	"reflect"
)

type Option[T any] struct {
	value    T
	hasValue bool
}

func NewOption[T any]() Option[T] {
	return Option[T]{hasValue: false}
}

func (o *Option[T]) Set(value T) {
	o.value = value
	o.hasValue = true
}

func (o *Option[T]) SetNone() {
	o.hasValue = false
}

func (o *Option[T]) Get() (T, bool) {
	return o.value, o.hasValue
}

func (o *Option[T]) isNone() bool {
	value := reflect.ValueOf(o.value)

	switch value.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return value.IsNil()
	default:
		return !o.hasValue
	}
}

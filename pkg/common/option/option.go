package option

type Option[T any] struct {
    Valid bool
    Value T
}

func Some[T any](val T) Option[T] {
    return Option[T]{Valid: true, Value: val}
}

func None[T any]() Option[T] {
    var zero T
    return Option[T]{Valid: false, Value: zero}
}

func (opt Option[T]) IsSome() bool {
    return opt.Valid
}

func (opt Option[T]) IsNone() bool {
    return !opt.Valid
}

func (opt Option[T]) UnwrapOr(defaultVal T) T {
    if opt.Valid {
        return opt.Value
    }
    return defaultVal
}

package utils

import (
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/valyala/fastrand"
)

func Benchmark_randomBody(b *testing.B) {
	m := make(map[string]interface{})
	f := gofakeit.NewUnlocked(int64(fastrand.Uint32n(1_000)))
	for i := 0; i < b.N; i++ {
		randomizeBody(f, m, true)
	}
}

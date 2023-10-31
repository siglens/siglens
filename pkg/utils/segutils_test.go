/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"fmt"
	"testing"
	"time"

	"github.com/cespare/xxhash"
	"github.com/google/uuid"
	"github.com/rogpeppe/fastuuid"
)

func Test_IsSubWordPresent(t *testing.T) {
	type args struct {
		haystack []byte
		needle   []byte
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Either haystack or needle is empty",
			args: args{
				haystack: []byte(""),
				needle:   []byte("abc"),
			},
			want: false,
		},
		{
			name: "When haystack and needle are the same length",
			args: args{
				haystack: []byte("abc"),
				needle:   []byte("abc"),
			},
			want: true,
		},
		{
			name: "When needle is bigger than haystack",
			args: args{
				haystack: []byte("abc"),
				needle:   []byte("abcd"),
			},
			want: false,
		},
		{
			name: "When needle present in haystack",
			args: args{
				haystack: []byte("abc"),
				needle:   []byte("ab"),
			},
			want: false,
		},
		{
			name: "When needle is not present in haystack",
			args: args{
				haystack: []byte("abc"),
				needle:   []byte("ef"),
			},
			want: false,
		},
		{
			name: "complex words 1",
			args: args{
				haystack: []byte("abc def hij"),
				needle:   []byte("ef"),
			},
			want: false,
		},
		{
			name: "complex words 2",
			args: args{
				haystack: []byte("abc def hij"),
				needle:   []byte("ij"),
			},
			want: false,
		},
		{
			name: "complex words 3",
			args: args{
				haystack: []byte("abc def hij"),
				needle:   []byte("ab"),
			},
			want: false,
		},
		{
			name: "complex words 4",
			args: args{
				haystack: []byte("abc def hij"),
				needle:   []byte("abc"),
			},
			want: true,
		},
		{
			name: "complex words 5",
			args: args{
				haystack: []byte("abc def hij"),
				needle:   []byte("def"),
			},
			want: true,
		},
		{
			name: "complex words 6",
			args: args{
				haystack: []byte("abc def hij"),
				needle:   []byte("hij"),
			},
			want: true,
		},
		{
			name: "complex phrase 1",
			args: args{
				haystack: []byte("abc def hij"),
				needle:   []byte("abc def"),
			},
			want: true,
		},
		{
			name: "complex phrase 2",
			args: args{
				haystack: []byte("abc def hij"),
				needle:   []byte("def hij"),
			},
			want: true,
		},
		{
			name: "complex phrase 3",
			args: args{
				haystack: []byte("abc def hij"),
				needle:   []byte("abc def hij"),
			},
			want: true,
		},
		{
			name: "complex phrase 4",
			args: args{
				haystack: []byte("batch-777"),
				needle:   []byte("batch-77"),
			},
			want: false,
		},
		{
			name: "complex phrase 5",
			args: args{
				haystack: []byte("test1 batch-777"),
				needle:   []byte("batch-77"),
			},
			want: false,
		},
		{
			name: "complex phrase 6",
			args: args{
				haystack: []byte("test1 batch-777"),
				needle:   []byte("batch-777"),
			},
			want: true,
		},
		{
			name: "complex phrase 6",
			args: args{
				haystack: []byte("batch-777 test1 "),
				needle:   []byte("batch-777"),
			},
			want: true,
		},
		{
			name: "complex phrase 7",
			args: args{
				haystack: []byte("batch-777"),
				needle:   []byte("batch-777"),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsSubWordPresent(tt.args.haystack, tt.args.needle); got != tt.want {
				t.Errorf("IsSubWordPresent() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Benchmark_UUIDNew(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = uuid.New().String()
	}
}

func Benchmark_UUIDRandPool(b *testing.B) {
	uuid.EnableRandPool()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = uuid.New().String()
	}
}

func Benchmark_FastUUID(b *testing.B) {
	g, _ := fastuuid.NewGenerator()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = g.Hex128()
	}
}

func Benchmark_StringCreate(b *testing.B) {
	ts_millis := uint64(time.Now().UTC().UnixNano()) / uint64(time.Millisecond)
	sizeBytes := uint64(100000)
	indexNameIn := "abcs"
	hostname := "localhost"
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tmpStr := fmt.Sprintf("%s-%d-%d-%s", hostname, ts_millis, sizeBytes, indexNameIn)
		_ = fmt.Sprintf("%d", xxhash.Sum64String(tmpStr))
	}
}

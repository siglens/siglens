// Copyright (c) 2021-2024 SigScalr, Inc.
//
// This file is part of SigLens Observability Solution
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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cespare/xxhash"
	"github.com/google/uuid"
	"github.com/rogpeppe/fastuuid"
	"github.com/stretchr/testify/assert"
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
			if got := IsSubWordPresent(tt.args.haystack, tt.args.needle, false); got != tt.want {
				t.Errorf("IsSubWordPresent() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_GetSegBaseDirFromFilename(t *testing.T) {
	expectedDir := "data/admins-MacBook-Air.local.ckrkh9bzWSR6B6BniNyjsV/final/ind-0/0-0-3544697602014606120/3/"

	segKey := "data/admins-MacBook-Air.local.ckrkh9bzWSR6B6BniNyjsV/final/ind-0/0-0-3544697602014606120/3/3/abc.txt"
	dir, err := GetSegBaseDirFromFilename(segKey)
	assert.NoError(t, err)
	assert.Equal(t, expectedDir, dir)

	segKey = "data/admins-MacBook-Air.local.ckrkh9bzWSR6B6BniNyjsV/final/ind-0/0-0-3544697602014606120/3/3"
	dir, err = GetSegBaseDirFromFilename(segKey)
	assert.NoError(t, err)
	assert.Equal(t, expectedDir, dir)

	segKey = "data/admins-MacBook-Air.local.ckrkh9bzWSR6B6BniNyjsV/final/ind-0/0-0-3544697602014606120/3/"
	dir, err = GetSegBaseDirFromFilename(segKey)
	assert.NoError(t, err)
	assert.Equal(t, expectedDir, dir)

	segKey = "data/admins-MacBook-Air.local.ckrkh9bzWSR6B6BniNyjsV/final/ind-0/0-0-3544697602014606120/3"
	_, err = GetSegBaseDirFromFilename(segKey)
	assert.Error(t, err)
}

func Test_SegmentValidityFile_SimpleBase(t *testing.T) {
	t.Cleanup(func() { os.RemoveAll("data") })

	dataDir := "data"
	segBaseDir := filepath.Join(dataDir, "hostid/final/index/streamid/suffix/")
	filename := filepath.Join(segBaseDir, "foo/bar.txt")
	assert.False(t, IsFileForRotatedSegment(filename))

	err := WriteValidityFile(segBaseDir)
	assert.NoError(t, err)
	assert.True(t, IsFileForRotatedSegment(filename))
}

func Test_SegmentValidityFile_NestedBase(t *testing.T) {
	t.Cleanup(func() { os.RemoveAll("foo") })

	dataDir := "foo/data/baz"
	segBaseDir := filepath.Join(dataDir, "hostid/final/index/streamid/suffix/")
	filename := filepath.Join(segBaseDir, "foo/bar.txt")
	assert.False(t, IsFileForRotatedSegment(filename))

	err := WriteValidityFile(segBaseDir)
	assert.NoError(t, err)
	assert.True(t, IsFileForRotatedSegment(filename))
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

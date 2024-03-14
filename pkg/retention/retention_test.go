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

package retention

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/siglens/siglens/pkg/config"

	"github.com/stretchr/testify/assert"
)

func Test_IsDirEmpty(t *testing.T) {
	type args struct {
		name    string
		dirName string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Test Empty Directory",
			args: args{
				name:    "data",
				dirName: "data",
			},
			want: true,
		},
		{
			name: "Test Empty Directory",
			args: args{
				name:    "data",
				dirName: "data/test",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := os.MkdirAll(tt.args.dirName, 0755)
			if err != nil {
				panic("error creating directory")
			}
			if got := IsDirEmpty(tt.args.name); got != tt.want {
				t.Errorf("IsDirEmpty() = %v, want %v", got, tt.want)
			}
			os.RemoveAll("data")
		})
	}
}

func Test_RecursivelyDeleteParentDirectories(t *testing.T) {
	config.InitializeDefaultConfig()
	type args struct {
		filePath   string
		testFile1  string
		testFile2  string
		fileExists string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Test positive scenario till final directory",
			args: args{
				filePath:   "data/test_host/final/2022/01/01",
				testFile1:  "data/test_host/final/2023/01/01",
				testFile2:  "data/test_host/final/2023/02/02",
				fileExists: "data/test_host/final/2022",
			},
		},
		{
			name: "Test positive scenario month folder",
			args: args{
				filePath:   "data/test_host/final/2022/01/01",
				testFile1:  "data/test_host/final/2022/01/02",
				testFile2:  "data/test_host/final/2022/01/03",
				fileExists: "data/test_host/final/2022/01/01",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.args.filePath != "" {
				err := os.MkdirAll(tt.args.filePath, 0755)
				fmt.Println(err)
			}
			if tt.args.testFile1 != "" {
				err := os.MkdirAll(tt.args.testFile1, 0755)
				fmt.Println(err)
			}
			if tt.args.testFile2 != "" {
				err := os.MkdirAll(tt.args.testFile2, 0755)
				fmt.Println(err)
			}
			RecursivelyDeleteParentDirectories(tt.args.filePath + "/t.txt")
			assert.NoDirExists(t, tt.args.fileExists, "Failed to backtrack cleanup")
			os.RemoveAll("data")
		})
	}
}

func Test_GetRetentionTimeMs(t *testing.T) {
	currTime := time.Now()
	oneHourAgo := time.Now().Add(-time.Duration(time.Hour))
	retentionInMs := GetRetentionTimeMs(1, currTime)
	assert.Equal(t, uint64(oneHourAgo.UnixMilli()), retentionInMs)
}

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

package otsdbquery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_MetricsQueryParsing(t *testing.T) {
	queries := []struct {
		startStr  string
		endStr    string
		m         string
		expectErr bool
	}{
		{
			startStr:  "",
			endStr:    "2022-01-01",
			m:         "cpu.usage{host=server1}",
			expectErr: true,
		},
		{
			startStr:  "2022-01-01 00:00:00",
			endStr:    "",
			m:         "cpu.usage{host=*}",
			expectErr: false,
		},
		{
			startStr:  "1h-ago",
			endStr:    "",
			m:         "cpu.usage{host=\"server1\"}",
			expectErr: false,
		},
		{
			startStr:  "2022-01-01 00:00:00",
			endStr:    "2022-01-02 00:00:00",
			m:         "",
			expectErr: true,
		},
		{
			startStr:  "2022-01-01 00:00:00",
			endStr:    "2022-01-02 00:00:00",
			m:         "sum:1h-avg:cpu.usage{host=server1}",
			expectErr: false,
		},
		{
			startStr:  "2022-01-01 00:00:00",
			endStr:    "2022-01-02 00:00:00",
			m:         "1h-avg:cpu.usage{host=server1}",
			expectErr: true,
		},
		{
			startStr:  "1d-ago",
			endStr:    "1h-ago",
			m:         "1h-avg:cpu.usage{host=server1,color=*}",
			expectErr: true,
		},
		{
			startStr:  "1d-ago",
			endStr:    "",
			m:         "cpu.usage{host='server1', color=*}",
			expectErr: false,
		},
	}
	for _, query := range queries {
		result, err := ParseRequest(query.startStr, query.endStr, query.m, 0)
		if query.expectErr {
			assert.Error(t, err)
			assert.Nil(t, result)
		} else {
			assert.NoError(t, err)
			assert.NotNil(t, result)
		}
	}
}

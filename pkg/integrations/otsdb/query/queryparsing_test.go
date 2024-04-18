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

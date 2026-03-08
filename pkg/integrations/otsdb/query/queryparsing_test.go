// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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

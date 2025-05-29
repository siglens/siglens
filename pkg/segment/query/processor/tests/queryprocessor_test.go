// Copyright (c) 2021-2025 SigScalr, Inc.
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

package tests

import (
	"testing"

	"github.com/siglens/siglens/pkg/ast/pipesearch"
	"github.com/siglens/siglens/pkg/segment/query/processor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertCanParallelSearch(t *testing.T, expectedOk bool, expectedSplitIndex int, query string) {
	t.Helper()

	_, agg, _, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	require.NoError(t, err)

	dataProcessors := make([]*processor.DataProcessor, 0)
	for agg != nil {
		dataProcessors = append(dataProcessors, processor.AsDataProcessor(agg, nil))
		agg = agg.Next
	}

	ok, splitIndex := processor.CanParallelSearch(dataProcessors)
	require.Equal(t, expectedOk, ok)
	if expectedOk {
		assert.Equal(t, expectedSplitIndex, splitIndex)
	}
}

func Test_CanParallelSearch(t *testing.T) {
	assertCanParallelSearch(t, false, 0, `*`)
	assertCanParallelSearch(t, false, 0, `foo=bar`)
	assertCanParallelSearch(t, true, 2, `* | rex field=foo "(?<bar>.*)" | sort bar`)
}

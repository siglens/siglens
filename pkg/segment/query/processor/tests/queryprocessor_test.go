// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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

	dataProcessors := processor.AggsToDataProcessors(agg, nil)
	ok, splitIndex := processor.CanParallelSearch(dataProcessors)
	require.Equal(t, expectedOk, ok)
	if expectedOk {
		assert.Equal(t, expectedSplitIndex, splitIndex)
	}
}

func Test_CanParallelSearch(t *testing.T) {
	assertCanParallelSearch(t, false, 0, `*`)
	assertCanParallelSearch(t, false, 0, `foo=bar`)
	assertCanParallelSearch(t, true, 1, `* | rex field=foo "(?<bar>.*)" | sort bar`)
	assertCanParallelSearch(t, true, 1, `* | rex field=foo "(?<bar>.*)" | stats avg(bar) as avg | sort avg`)
	assertCanParallelSearch(t, true, 2, `* | rex field=foo "(?<bar>.*)" | eval x=bar*10 | stats avg(x)`)
	assertCanParallelSearch(t, false, 0, `* | dedup latency | rex field=foo "(?<bar>.*)" | eval x=bar*10 | stats avg(x)`)
	assertCanParallelSearch(t, false, 0, `* | rex field=foo "(?<bar>.*)" | eval x=bar*10 | where x>1000`)
	assertCanParallelSearch(t, true, 0, `* | stats count | eval x=1`)

	// TODO: change the expected results once we support parallel search for data generators
	assertCanParallelSearch(t, false, 0, `| inputlookup test_lookup.csv | stats count`)
	assertCanParallelSearch(t, false, 0, `| gentimes start=-30 end=-20 increment=7s | stats count`)
}

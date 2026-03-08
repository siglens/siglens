// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package pipesearch

import (
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/stretchr/testify/assert"
)

func Test_shouldRunTimechartQuery(t *testing.T) {
	assertShouldRunTimechart(t, true, "*")
	assertShouldRunTimechart(t, false, "* | stats count")
	assertShouldRunTimechart(t, false, "* | streamstats count")
	assertShouldRunTimechart(t, false, "* | timechart count")
	assertShouldRunTimechart(t, true, "* | eval x=latency | where x > 100")
	assertShouldRunTimechart(t, false, "* | eval x=latency | stats count as Count by x | where Count > 100")
}

func parseSPL(t *testing.T, query string) (*structs.ASTNode, *structs.QueryAggregators) {
	astNode, aggs, _, err := ParseQuery(query, 0, "Splunk QL")
	assert.NoError(t, err)
	return astNode, aggs
}

func assertShouldRunTimechart(t *testing.T, expectedValue bool, query string) {
	_, aggs := parseSPL(t, query)
	runTimechart := shouldRunTimechartQuery(aggs)
	assert.Equal(t, expectedValue, runTimechart)
}

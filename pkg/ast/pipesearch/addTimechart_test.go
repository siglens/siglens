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

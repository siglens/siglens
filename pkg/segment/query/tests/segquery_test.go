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

	"github.com/siglens/siglens/pkg/ast"
	"github.com/siglens/siglens/pkg/ast/spl"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/stretchr/testify/assert"
)

func Test_getQueryType(t *testing.T) {
	assertQueryType(t, `latency<10000 | where city="Boston"`, structs.RRCCmd)
	assertQueryType(t, `latency<10000 | where city="Boston" | stats count`, structs.SegmentStatsCmd)
	assertQueryType(t, `latency<10000 | where city="Boston" | stats count by weekday`, structs.GroupByCmd)
}

func assertQueryType(t *testing.T, splQuery string, expectedType structs.QueryType) {
	t.Helper()

	queryBytes := []byte(splQuery)
	res, err := spl.Parse("", queryBytes)
	assert.Nil(t, err)
	filter := res.(ast.QueryStruct)
	qType := query.GetQueryTypeFromAggs(filter.PipeCommands)
	assert.Equal(t, expectedType, qType, "Expected %s but got %s for query %s", expectedType, qType, splQuery)
}

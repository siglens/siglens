// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0
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
	qType := query.GetQueryTypeOfFullChain(filter.PipeCommands)
	assert.Equal(t, expectedType, qType, "Expected %s but got %s for query %s", expectedType, qType, splQuery)
}

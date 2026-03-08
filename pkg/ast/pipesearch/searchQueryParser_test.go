// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package pipesearch

import (
	"testing"

	"github.com/siglens/siglens/pkg/ast"
	"github.com/siglens/siglens/pkg/ast/spl"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/stretchr/testify/assert"
)

func splToUnoptimizedNodes(t *testing.T, query string) (*ast.Node, *structs.QueryAggregators) {
	queryStructAsAny, err := spl.Parse("", []byte(query))
	assert.NoError(t, err)

	queryStruct, ok := queryStructAsAny.(ast.QueryStruct)
	assert.True(t, ok)

	return queryStruct.SearchFilter, queryStruct.PipeCommands
}

func verifyEquivalentSplQueries(t *testing.T,
	optimizationFunction func(*ast.Node, *structs.QueryAggregators) (*ast.Node, *structs.QueryAggregators),
	unoptomizedQuery string, optimizedQuery string) {

	astNode1, aggs1 := splToUnoptimizedNodes(t, unoptomizedQuery)
	astNode2, aggs2 := splToUnoptimizedNodes(t, optimizedQuery)

	astNode1, aggs1 = optimizationFunction(astNode1, aggs1)

	assert.Equal(t, astNode1, astNode2)
	assert.Equal(t, aggs1, aggs2)
}

func Test_optimizeStatsEvalQueries(t *testing.T) {
	verifyEquivalentSplQueries(t, optimizeStatsEvalQueries,
		`* | stats count(eval(foo=42))`,
		`* AND foo=42 | stats count(eval(foo=42))`,
	)
	verifyEquivalentSplQueries(t, optimizeStatsEvalQueries,
		`* | stats count(eval(foo=42)), sum(eval(bar="baz"))`,
		`* AND (foo=42 OR bar=CASE("baz")) | stats count(eval(foo=42)), sum(eval(bar="baz"))`,
	)
	verifyEquivalentSplQueries(t, optimizeStatsEvalQueries,
		`A=1 OR NOT B=2 | stats count(eval(foo=42))`,
		`(A=1 OR NOT B=2) AND foo=42 | stats count(eval(foo=42))`,
	)
}

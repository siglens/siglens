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

	"github.com/siglens/siglens/pkg/ast"
	"github.com/siglens/siglens/pkg/ast/spl"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/stretchr/testify/assert"
)

func convertAstNodeToCaseSensitive(node *ast.Node) {
	if node == nil {
		return
	}

	if node.Comparison.Values == `"*"` {
		return
	}

	node.Comparison.CaseInsensitive = false
	node.Comparison.OriginalValues = nil
	convertAstNodeToCaseSensitive(node.Left)
	convertAstNodeToCaseSensitive(node.Right)
}

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

	// We don't want to do a case-insentive search for a command that is with stats.
	// But by default, the search node returns a case-insensitive node. So we need to convert it to case-sensitive.
	// But also please not this conversion will only work for these specific example queries as the filter search node is number type
	// So there is no concept of case-insensitive search for numbers.
	// If the filter type is a string, then the output of this function will not match with output of astNode1.
	convertAstNodeToCaseSensitive(astNode2)

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
		`* AND (foo=42 OR bar="baz") | stats count(eval(foo=42)), sum(eval(bar="baz"))`,
	)
	verifyEquivalentSplQueries(t, optimizeStatsEvalQueries,
		`A=1 OR NOT B=2 | stats count(eval(foo=42))`,
		`(A=1 OR NOT B=2) AND foo=42 | stats count(eval(foo=42))`,
	)
}

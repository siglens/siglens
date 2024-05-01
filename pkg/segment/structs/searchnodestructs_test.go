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

package structs

import (
	"testing"

	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_extractSearchNodeType(t *testing.T) {

	batch0, err := utils.CreateDtypeEnclosure("batch-0", 0)
	assert.Nil(t, err)
	batchOneAllCols := &SearchExpression{
		LeftSearchInput:  &SearchExpressionInput{ColumnName: "*"},
		FilterOp:         utils.Equals,
		RightSearchInput: &SearchExpressionInput{ColumnValue: batch0},
	}

	query := &SearchQuery{
		ExpressionFilter: &SearchExpression{
			LeftSearchInput:  &SearchExpressionInput{ColumnName: "col3"},
			FilterOp:         utils.Equals,
			RightSearchInput: &SearchExpressionInput{ColumnValue: batch0},
		},
		SearchType: SimpleExpression,
	}

	node := &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{query},
		},
	}
	node.AddQueryInfoForNode()
	assert.Equal(t, node.NodeType, ColumnValueQuery)

	node = &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{{
				ExpressionFilter: batchOneAllCols,
				SearchType:       SimpleExpression,
			}},
		},
	}
	node.AddQueryInfoForNode()
	assert.Equal(t, node.NodeType, ColumnValueQuery)

	wildcard, err := utils.CreateDtypeEnclosure("*", 0)
	assert.Nil(t, err)
	node = &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{{
				ExpressionFilter: &SearchExpression{
					LeftSearchInput:  &SearchExpressionInput{ColumnName: "*"},
					FilterOp:         utils.Equals,
					RightSearchInput: &SearchExpressionInput{ColumnValue: wildcard},
				},
				SearchType: SimpleExpression,
			}},
		},
	}
	node.AddQueryInfoForNode()
	assert.Equal(t, node.NodeType, MatchAllQuery)

	assert.Nil(t, err)
	mf := &MatchFilter{
		MatchColumn:   "*",
		MatchWords:    [][]byte{[]byte("*")},
		MatchOperator: utils.And,
	}

	node = &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{{
				MatchFilter: mf,
				SearchType:  SimpleExpression,
			}},
		},
	}
	node.AddQueryInfoForNode()
	assert.Equal(t, node.NodeType, MatchAllQuery)

	assert.Nil(t, err)
	mf = &MatchFilter{
		MatchColumn:   "*",
		MatchWords:    [][]byte{[]byte("*"), []byte("abc")},
		MatchOperator: utils.And,
	}

	node = &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{{
				MatchFilter: mf,
				SearchType:  SimpleExpression,
			}},
		},
	}
	node.AddQueryInfoForNode()
	assert.Equal(t, node.NodeType, ColumnValueQuery, "no longer match all")

	wordMatch := &MatchFilter{
		MatchColumn:   "*",
		MatchWords:    [][]byte{[]byte("def"), []byte("abc")},
		MatchOperator: utils.And,
	}

	node = &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{{
				MatchFilter: wordMatch,
				SearchType:  SimpleExpression,
			}},
		},
	}
	node.AddQueryInfoForNode()
	assert.Equal(t, node.NodeType, ColumnValueQuery, "no longer match all")

	mf = &MatchFilter{
		MatchColumn:   "*",
		MatchWords:    [][]byte{[]byte("*"), []byte("abc")},
		MatchOperator: utils.Or,
	}

	node = &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{{
				MatchFilter: mf,
				SearchType:  SimpleExpression,
			}},
		},
	}
	node.AddQueryInfoForNode()
	assert.Equal(t, node.NodeType, MatchAllQuery, "no longer match all")

	nestNode := &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{
				{
					MatchFilter: mf,
					SearchType:  SimpleExpression,
				},
				{
					ExpressionFilter: batchOneAllCols,
					SearchType:       SimpleExpression,
				},
			},
		},
	}

	nestNode.AddQueryInfoForNode()
	assert.Equal(t, nestNode.NodeType, ColumnValueQuery, "has a nested non match all")

	matchAllExp := &SearchExpression{
		LeftSearchInput:  &SearchExpressionInput{ColumnName: "*"},
		FilterOp:         utils.Equals,
		RightSearchInput: &SearchExpressionInput{ColumnValue: wildcard},
	}
	parentNode := &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchNode: []*SearchNode{nestNode},
			SearchQueries: []*SearchQuery{
				{
					ExpressionFilter: matchAllExp,
					SearchType:       SimpleExpression,
				},
			},
		},
	}
	parentNode.AddQueryInfoForNode()
	assert.Equal(t, parentNode.NodeType, ColumnValueQuery, "has a nested non match all")
}

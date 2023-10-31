/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

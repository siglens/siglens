// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package structs

import (
	"testing"

	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_extractSearchNodeType(t *testing.T) {

	batch0, err := sutils.CreateDtypeEnclosure("batch-0", 0)
	assert.Nil(t, err)
	batchOneAllCols := &SearchExpression{
		LeftSearchInput:  &SearchExpressionInput{ColumnName: "*"},
		FilterOp:         sutils.Equals,
		RightSearchInput: &SearchExpressionInput{ColumnValue: batch0},
	}

	query := &SearchQuery{
		ExpressionFilter: &SearchExpression{
			LeftSearchInput:  &SearchExpressionInput{ColumnName: "col3"},
			FilterOp:         sutils.Equals,
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

	wildcard, err := sutils.CreateDtypeEnclosure("*", 0)
	assert.Nil(t, err)
	node = &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{{
				ExpressionFilter: &SearchExpression{
					LeftSearchInput:  &SearchExpressionInput{ColumnName: "*"},
					FilterOp:         sutils.Equals,
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
		MatchOperator: sutils.And,
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
		MatchOperator: sutils.And,
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
		MatchOperator: sutils.And,
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
		MatchOperator: sutils.Or,
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
		FilterOp:         sutils.Equals,
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

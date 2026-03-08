// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package querytracker

import (
	"testing"

	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"

	. "github.com/siglens/siglens/pkg/segment/structs"
)

func Test_HashSearchNode(t *testing.T) {

	qVal, err := sutils.CreateDtypeEnclosure("iOS", 0)
	assert.Nil(t, err)

	sNode := &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{
				{
					ExpressionFilter: &SearchExpression{
						LeftSearchInput:  &SearchExpressionInput{ColumnName: "os"},
						FilterOp:         sutils.Equals,
						RightSearchInput: &SearchExpressionInput{ColumnValue: qVal},
					},
					SearchType: SimpleExpression,
				},
			},
		},
		NodeType: ColumnValueQuery,
	}

	hid1 := GetHashForQuery(sNode)
	expected := "15281968501864336114" // pre-computed hashid to compare against for above query
	assert.Equal(t, expected, hid1, "hid1=%v, not equal to expected=%v", hid1, expected)

	hid2 := GetHashForQuery(sNode)
	assert.Equal(t, hid1, hid2, "hid2=%v, not equal to expected=%v", hid2, hid1)

	for i := 0; i < 10; i++ {
		newId := GetHashForQuery(sNode)
		assert.Equal(t, expected, newId, "hid2=%v, not equal to expected=%v", newId, expected)
	}
}

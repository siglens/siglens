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

package querytracker

import (
	"testing"

	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"

	. "github.com/siglens/siglens/pkg/segment/structs"
)

func Test_HashSearchNode(t *testing.T) {

	qVal, err := utils.CreateDtypeEnclosure("iOS", 0)
	assert.Nil(t, err)

	sNode := &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{
				{
					ExpressionFilter: &SearchExpression{
						LeftSearchInput:  &SearchExpressionInput{ColumnName: "os"},
						FilterOp:         utils.Equals,
						RightSearchInput: &SearchExpressionInput{ColumnValue: qVal},
					},
					SearchType: SimpleExpression,
				},
			},
		},
		NodeType: ColumnValueQuery,
	}

	hid1 := GetHashForQuery(sNode)
	expected := "11481340929163441556" // pre-computed hashid to compare against for above query
	assert.Equal(t, expected, hid1, "hid1=%v, not equal to expected=%v", hid1, expected)

	hid2 := GetHashForQuery(sNode)
	assert.Equal(t, hid1, hid2, "hid2=%v, not equal to expected=%v", hid2, hid1)

	for i := 0; i < 10; i++ {
		newId := GetHashForQuery(sNode)
		assert.Equal(t, expected, newId, "hid2=%v, not equal to expected=%v", newId, expected)
	}
}

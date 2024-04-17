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

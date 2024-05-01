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

	. "github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_getSearchInputFromFilter(t *testing.T) {

	simpleFilter := &FilterInput{
		SubtreeResult: "literal1",
	}

	search := getSearchInputFromFilterInput(simpleFilter, 0)
	log.Info(search)
	assert.Equal(t, search.ColumnValue.StringVal, "literal1")

	expInput := &ExpressionInput{
		ColumnName: "key1",
	}
	exp := &Expression{
		LeftInput: expInput,
	}
	expressionColumnFilter := &FilterInput{
		Expression: exp,
	}

	search = getSearchInputFromFilterInput(expressionColumnFilter, 0)
	log.Info(search)
	assert.Nil(t, search.ColumnValue)
	assert.Equal(t, 1, len(search.getAllColumnsInSearch()))
	assert.Equal(t, "key1", search.ColumnName)
	assert.Nil(t, search.ComplexRelation)

	leftExpInput := &ExpressionInput{
		ColumnName: "key1",
	}
	rightExpInput := &ExpressionInput{
		ColumnName: "key2",
	}
	exp = &Expression{
		LeftInput:    leftExpInput,
		ExpressionOp: Add,
		RightInput:   rightExpInput,
	}
	expressionComplexFilter := &FilterInput{
		Expression: exp,
	}
	search = getSearchInputFromFilterInput(expressionComplexFilter, 0)
	assert.Nil(t, search.ColumnValue)
	assert.Equal(t, 0, len(search.ColumnName))
	assert.Equal(t, 2, len(search.getAllColumnsInSearch()))
	assert.NotNil(t, search.ComplexRelation)
}

func Test_extractBlockBloomTokens(t *testing.T) {
	numLiteral, _ := CreateDtypeEnclosure(1.0, 0)
	leftNumberInput := &SearchExpressionInput{
		ColumnValue: numLiteral,
	}

	strLiteral, _ := CreateDtypeEnclosure("abc", 0)
	leftLiteralInput := &SearchExpressionInput{
		ColumnValue: strLiteral,
	}

	strWildcardLiteral, _ := CreateDtypeEnclosure("abc*", 0)
	leftWildCardInput := &SearchExpressionInput{
		ColumnValue: strWildcardLiteral,
	}

	rightInput := &SearchExpressionInput{
		ColumnName: "col1",
	}

	query := &SearchQuery{
		ExpressionFilter: &SearchExpression{
			LeftSearchInput:  leftNumberInput,
			FilterOp:         Equals,
			RightSearchInput: rightInput,
		},
	}
	allKeys, wildcard, op := query.GetAllBlockBloomKeysToSearch()
	assert.Len(t, allKeys, 1, "only 1 key")
	_, ok := allKeys["1"]
	assert.True(t, ok, "value exists")
	assert.False(t, wildcard)
	assert.Equal(t, And, op)

	query.ExpressionFilter.LeftSearchInput = leftLiteralInput
	allKeys, wildcard, op = query.GetAllBlockBloomKeysToSearch()
	assert.Len(t, allKeys, 1, "only 1 key")
	_, ok = allKeys["abc"]
	assert.True(t, ok, "abc key exists")
	assert.False(t, wildcard)
	assert.Equal(t, And, op)

	query.ExpressionFilter.LeftSearchInput = leftWildCardInput
	allKeys, wildcard, op = query.GetAllBlockBloomKeysToSearch()
	assert.Len(t, allKeys, 0, "no keys")
	_, ok = allKeys["abc*"]
	assert.False(t, ok, "abc* should not exist bc of wildcard")
	assert.True(t, wildcard)
	assert.Equal(t, And, op)

	matchTest := &SearchQuery{
		MatchFilter: &MatchFilter{
			MatchColumn:   "*",
			MatchWords:    [][]byte{[]byte("a"), []byte("b"), STAR_BYTE},
			MatchOperator: Or,
		},
	}
	allKeys, wildcard, op = matchTest.GetAllBlockBloomKeysToSearch()
	assert.True(t, wildcard)
	assert.Len(t, allKeys, 2, "2 keys")
	_, ok = allKeys["a"]
	assert.True(t, ok, "key a exists")

	_, ok = allKeys["b"]
	assert.True(t, ok, "key b exists")

	_, ok = allKeys["*"]
	assert.False(t, ok, "key * does not exists")
	assert.Equal(t, Or, op)
}

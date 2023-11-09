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

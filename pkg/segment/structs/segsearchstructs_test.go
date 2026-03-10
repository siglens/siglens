// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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

	search := getSearchInputFromFilterInput(simpleFilter, false, false, 0)
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

	search = getSearchInputFromFilterInput(expressionColumnFilter, false, false, 0)
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
	search = getSearchInputFromFilterInput(expressionComplexFilter, false, false, 0)
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
	allKeys, _, wildcard, op := query.GetAllBlockBloomKeysToSearch()
	assert.Len(t, allKeys, 1, "only 1 key")
	_, ok := allKeys["1"]
	assert.True(t, ok, "value exists")
	assert.False(t, wildcard)
	assert.Equal(t, And, op)

	query.ExpressionFilter.LeftSearchInput = leftLiteralInput
	allKeys, _, wildcard, op = query.GetAllBlockBloomKeysToSearch()
	assert.Len(t, allKeys, 1, "only 1 key")
	_, ok = allKeys["abc"]
	assert.True(t, ok, "abc key exists")
	assert.False(t, wildcard)
	assert.Equal(t, And, op)

	query.ExpressionFilter.LeftSearchInput = leftWildCardInput
	allKeys, _, wildcard, op = query.GetAllBlockBloomKeysToSearch()
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
	allKeys, _, wildcard, op = matchTest.GetAllBlockBloomKeysToSearch()
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

func Test_GetAllBlockBloomKeysToSearch_MatchPhrase(t *testing.T) {
	matchFilterNoWildcard := &MatchFilter{
		MatchColumn:   "*",
		MatchWords:    [][]byte{[]byte("foo"), []byte("bar"), STAR_BYTE},
		MatchPhrase:   []byte("foo bar"),
		MatchOperator: And,
		MatchType:     MATCH_PHRASE,
	}

	allKeys, _, wildcard, op := matchFilterNoWildcard.GetAllBlockBloomKeysToSearch(false)
	assert.Equal(t, 1, len(allKeys))
	_, ok := allKeys["foo bar"]
	assert.True(t, ok)
	assert.False(t, wildcard)
	assert.Equal(t, And, op)

	matchFilterWithWildcard := &MatchFilter{
		MatchColumn:   "*",
		MatchWords:    [][]byte{[]byte("foo*"), []byte("bar"), STAR_BYTE},
		MatchPhrase:   []byte("foo* bar"),
		MatchOperator: And,
		MatchType:     MATCH_PHRASE,
	}

	allKeys, _, wildcard, op = matchFilterWithWildcard.GetAllBlockBloomKeysToSearch(false)
	assert.Equal(t, 0, len(allKeys))
	assert.True(t, wildcard)
	assert.Equal(t, And, op)
}

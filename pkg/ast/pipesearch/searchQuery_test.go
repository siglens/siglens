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

package pipesearch

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/siglens/siglens/pkg/ast"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	. "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func testParseQuery(query string, want *ast.Node) func(t *testing.T) {
	return func(t *testing.T) {
		t.Helper()
		t.Parallel()
		n, err := Parse("", []byte(query))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(want, n.(ast.QueryStruct).SearchFilter) {
			wb, _ := json.MarshalIndent(want, "", " ")
			nb, _ := json.MarshalIndent(n, "", " ")
			t.Fatalf("expected:\n%s\ngot:\n%s", string(wb), string(nb))
		}
	}
}

func TestAST_simpleEqual(t *testing.T) {
	json_body := []byte(`something="another"`)
	res, _, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "something")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "another")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
}

func TestAST_simpleAnd(t *testing.T) {
	json_body := []byte(`name="t1" AND surname="t2"`)
	res, _, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 2)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "name")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t1")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "surname")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t2")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, Equals)
}

func TestAST_simpleOr(t *testing.T) {
	json_body := []byte(`name="t1" OR surname="t2"`)
	res, _, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.OrFilterCondition.FilterCriteria)
	assert.Len(t, res.OrFilterCondition.FilterCriteria, 2)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "name")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t1")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "surname")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t2")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, Equals)
}

func TestAST_AND_Or(t *testing.T) {
	json_body := []byte(`name="t1" OR name="t2" AND age=100`)
	res, _, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.OrFilterCondition.FilterCriteria)
	assert.Len(t, res.OrFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "name")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t1")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "name")
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t2")
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "age")
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "100")
}

func TestAST_AND_Or_paren(t *testing.T) {
	json_body := []byte(`(name="t1" OR name="t2") AND age=100`)
	res, _, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "age")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "100")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "name")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t1")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "name")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t2")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, Equals)

}
func TestAST_AND_Or_paren_around_condition(t *testing.T) {
	json_body := []byte(`name="t1" OR (name="t2")`)
	res, _, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.OrFilterCondition.FilterCriteria)
	assert.Len(t, res.OrFilterCondition.FilterCriteria, 2)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "name")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t1")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "name")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t2")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, Equals)
}

func TestAST_AND_Or_multiple(t *testing.T) {
	json_body := []byte(`name="t1" OR name="t2" OR age=100 AND surname=t3`)
	res, _, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "name")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t1")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "name")
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t2")
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)

	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "age")
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "100")
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)

	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "surname")
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t3")
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, Equals)

}

func TestAST_freeText_simple(t *testing.T) {
	json_body := []byte(`test`)
	res, _, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, And)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("test")})
	assert.NotEqual(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchType, structs.MATCH_PHRASE)
}

func TestAST_freeText_simple_matchWords(t *testing.T) {
	json_body := []byte(`"test this ."`)
	res, _, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, And)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("test"), []byte("this"), []byte(".")})
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchType, structs.MATCH_PHRASE)
}

func TestAST_freeText_simple_wildcard(t *testing.T) {
	json_body := []byte(`test*`)
	res, _, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)

	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "test*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)

}

func TestAST_freeText_simpleAnd(t *testing.T) {
	json_body := []byte(`another AND test`)
	res, _, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 2)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, And)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("another")})
	assert.NotEqual(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchType, structs.MATCH_PHRASE)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].MatchFilter.MatchOperator, And)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].MatchFilter.MatchWords, [][]byte{[]byte("test")})
	assert.NotEqual(t, res.AndFilterCondition.FilterCriteria[1].MatchFilter.MatchType, structs.MATCH_PHRASE)
}

func TestAST_freeText_simpleAnd_wildcard(t *testing.T) {
	json_body := []byte(`another* AND test*`)
	res, _, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 2)

	assert.Len(t, res.AndFilterCondition.FilterCriteria, 2)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "another*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "test*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, Equals)
}

func TestAST_freeText_complexOrAnd(t *testing.T) {
	json_body := []byte(`"t2" OR ("t1" AND t3)`)
	res, _, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.OrFilterCondition.FilterCriteria)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, And)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].MatchFilter.MatchPhrase, []byte("t2"))
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].MatchFilter.MatchType, structs.MATCH_PHRASE)

	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, And)
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].MatchFilter.MatchPhrase, []byte("t1"))
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].MatchFilter.MatchType, structs.MATCH_PHRASE)

	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].MatchFilter.MatchOperator, And)
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].MatchFilter.MatchWords, [][]byte{[]byte("t3")})
	assert.NotEqual(t, res.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].MatchFilter.MatchType, structs.MATCH_PHRASE)
}

func TestAST_freeText_complexOrAnd_wildcard(t *testing.T) {
	json_body := []byte(`t2* OR (t1* AND t3*)`)
	res, _, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.OrFilterCondition.FilterCriteria)
	assert.Len(t, res.OrFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "*")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t2*")
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "*")
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t1*")
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "*")
	assert.Equal(t, res.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t3*")

}

func TestAST_freeText_complexAndOr(t *testing.T) {
	json_body := []byte(`"t2" AND ("t1" OR t3)`)
	res, _, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)

	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, And)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchPhrase, []byte("t2"))
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchType, structs.MATCH_PHRASE)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].MatchFilter.MatchPhrase, []byte("t1"))
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].MatchFilter.MatchWords, [][]byte{[]byte("t3")})
}

func TestAST_columnValue_complexString(t *testing.T) {
	json_body := []byte(`click_id = "627af0a5-aeb4-4471-889f-6e850958d98e"`)
	res, _, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)

	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "click_id")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "627af0a5-aeb4-4471-889f-6e850958d98e")
}

func TestAST_columnValue_multiWordValue(t *testing.T) {
	json_body := []byte(`utm_campaign = "Town apply record."`)
	res, _, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)

	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "utm_campaign")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "Town apply record.")
}

func TestSimpleEqual(t *testing.T) {
	t.Run("simple minimal condition", testParseQuery(`something="another"`, &ast.Node{
		NodeType: ast.NodeTerminal,
		Comparison: ast.Comparison{
			Op:     "=",
			Field:  "something",
			Values: "\"another\"",
		},
		Left:  nil,
		Right: nil,
	}))

	t.Run("multi part field name", testParseQuery(`name.something = another`, &ast.Node{
		NodeType: ast.NodeTerminal,
		Comparison: ast.Comparison{
			Op:     "=",
			Field:  "name.something",
			Values: "another",
		},
		Left:  nil,
		Right: nil,
	}))

	t.Run("simple AND clause", testParseQuery(`name="t1" AND surname="t2"`, &ast.Node{
		NodeType: ast.NodeAnd,
		Left: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "name",
				Values: "\"t1\"",
			},
		},
		Right: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "surname",
				Values: "\"t2\"",
			},
		},
	}))

	t.Run("simple AND clause with parenthesis", testParseQuery(`(name="t1" AND surname="t2")`, &ast.Node{
		NodeType: ast.NodeAnd,
		Left: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "name",
				Values: "\"t1\"",
			},
		},
		Right: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "surname",
				Values: "\"t2\"",
			},
		},
	}))

	t.Run("simple OR clause", testParseQuery(`name="t1" OR surname="t2"`, &ast.Node{
		NodeType: ast.NodeOr,
		Left: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "name",
				Values: "\"t1\"",
			},
		},
		Right: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "surname",
				Values: "\"t2\"",
			},
		},
	}))

	t.Run("simple OR clause with parenthesis", testParseQuery(`(name="t1" OR surname="t2")`, &ast.Node{
		NodeType: ast.NodeOr,
		Left: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "name",
				Values: "\"t1\"",
			},
		},
		Right: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "surname",
				Values: "\"t2\"",
			},
		},
	}))

	t.Run("simple OR clause with parenthesis around condition", testParseQuery(`name="t1" OR (name="t2")`, &ast.Node{
		NodeType: ast.NodeOr,
		Left: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "name",
				Values: "\"t1\"",
			},
		},
		Right: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "name",
				Values: "\"t2\"",
			},
		},
	}))

	t.Run("OR / AND clauses", testParseQuery(`name="t1" OR name="t2" AND age=100`, &ast.Node{
		NodeType: ast.NodeOr,
		Left: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "name",
				Values: "\"t1\"",
			},
		},
		Right: &ast.Node{
			NodeType: ast.NodeAnd,
			Left: &ast.Node{
				NodeType: ast.NodeTerminal,
				Comparison: ast.Comparison{
					Op:     "=",
					Field:  "name",
					Values: "\"t2\"",
				},
			},
			Right: &ast.Node{
				NodeType: ast.NodeTerminal,
				Comparison: ast.Comparison{
					Op:     "=",
					Field:  "age",
					Values: json.Number("100"),
				},
			},
		},
	}))

	t.Run("OR / AND clauses with paren precedence", testParseQuery(`(name="t1" AND age=100) OR name="t2"`, &ast.Node{
		NodeType: ast.NodeOr,
		Left: &ast.Node{
			NodeType: ast.NodeAnd,
			Left: &ast.Node{
				NodeType: ast.NodeTerminal,
				Comparison: ast.Comparison{
					Op:     "=",
					Field:  "name",
					Values: "\"t1\"",
				},
			},
			Right: &ast.Node{
				NodeType: ast.NodeTerminal,
				Comparison: ast.Comparison{
					Op:     "=",
					Field:  "age",
					Values: json.Number("100"),
				},
			},
		},
		Right: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "name",
				Values: "\"t2\"",
			},
		},
	}))

	t.Run("OR / AND clauses with paren precedence variation", testParseQuery(`name="t2" OR (name="t1" AND age=100) `, &ast.Node{
		NodeType: ast.NodeOr,
		Left: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "name",
				Values: "\"t2\"",
			},
		},
		Right: &ast.Node{
			NodeType: ast.NodeAnd,
			Left: &ast.Node{
				NodeType: ast.NodeTerminal,
				Comparison: ast.Comparison{
					Op:     "=",
					Field:  "name",
					Values: "\"t1\"",
				},
			},
			Right: &ast.Node{
				NodeType: ast.NodeTerminal,
				Comparison: ast.Comparison{
					Op:     "=",
					Field:  "age",
					Values: json.Number("100"),
				},
			},
		},
	}))

	t.Run("OR / AND clauses", testParseQuery(`name="t1" OR name="t2" OR age=100`, &ast.Node{
		NodeType: ast.NodeOr,
		Left: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "name",
				Values: "\"t1\"",
			},
		},
		Right: &ast.Node{
			NodeType: ast.NodeOr,
			Left: &ast.Node{
				NodeType: ast.NodeTerminal,
				Comparison: ast.Comparison{
					Op:     "=",
					Field:  "name",
					Values: "\"t2\"",
				},
			},
			Right: &ast.Node{
				NodeType: ast.NodeTerminal,
				Comparison: ast.Comparison{
					Op:     "=",
					Field:  "age",
					Values: json.Number("100"),
				},
			},
		},
	}))

	t.Run("OR / AND clauses", testParseQuery(`name="t1" OR name="t2" OR age=100 AND surname=t3`, &ast.Node{
		NodeType: ast.NodeOr,

		Left: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "name",
				Values: "\"t1\"",
			},
		},
		Right: &ast.Node{
			NodeType: ast.NodeOr,
			Left: &ast.Node{
				NodeType: ast.NodeTerminal,
				Comparison: ast.Comparison{
					Op:     "=",
					Field:  "name",
					Values: "\"t2\"",
				},
			},
			Right: &ast.Node{
				NodeType: ast.NodeAnd,
				Left: &ast.Node{
					NodeType: ast.NodeTerminal,
					Comparison: ast.Comparison{
						Op:     "=",
						Field:  "age",
						Values: json.Number("100"),
					},
				},
				Right: &ast.Node{
					NodeType: ast.NodeTerminal,
					Comparison: ast.Comparison{
						Op:     "=",
						Field:  "surname",
						Values: "t3",
					},
				},
			},
		},
	}))

	t.Run("multiple OR / AND clauses", testParseQuery(`name="t1" OR name="t2" OR age=100 AND surname=t3`, &ast.Node{
		NodeType: ast.NodeOr,

		Left: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "name",
				Values: "\"t1\"",
			},
		},
		Right: &ast.Node{
			NodeType: ast.NodeOr,
			Left: &ast.Node{
				NodeType: ast.NodeTerminal,
				Comparison: ast.Comparison{
					Op:     "=",
					Field:  "name",
					Values: "\"t2\"",
				},
			},
			Right: &ast.Node{
				NodeType: ast.NodeAnd,
				Left: &ast.Node{
					NodeType: ast.NodeTerminal,
					Comparison: ast.Comparison{
						Op:     "=",
						Field:  "age",
						Values: json.Number("100"),
					},
				},
				Right: &ast.Node{
					NodeType: ast.NodeTerminal,
					Comparison: ast.Comparison{
						Op:     "=",
						Field:  "surname",
						Values: "t3",
					},
				},
			},
		},
	}))

	t.Run(" multiple AND clauses", testParseQuery(`name="t1" AND name="t2" AND age=100 AND surname=t3`, &ast.Node{
		NodeType: ast.NodeAnd,

		Left: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "name",
				Values: "\"t1\"",
			},
		},
		Right: &ast.Node{
			NodeType: ast.NodeAnd,
			Left: &ast.Node{
				NodeType: ast.NodeTerminal,
				Comparison: ast.Comparison{
					Op:     "=",
					Field:  "name",
					Values: "\"t2\"",
				},
			},
			Right: &ast.Node{
				NodeType: ast.NodeAnd,
				Left: &ast.Node{
					NodeType: ast.NodeTerminal,
					Comparison: ast.Comparison{
						Op:     "=",
						Field:  "age",
						Values: json.Number("100"),
					},
				},
				Right: &ast.Node{
					NodeType: ast.NodeTerminal,
					Comparison: ast.Comparison{
						Op:     "=",
						Field:  "surname",
						Values: "t3",
					},
				},
			},
		},
	}))

	t.Run("float value", testParseQuery(`avg=1.4`, &ast.Node{
		NodeType: ast.NodeTerminal,
		Comparison: ast.Comparison{
			Op:     "=",
			Field:  "avg",
			Values: json.Number("1.4"),
		},
		Left:  nil,
		Right: nil,
	}))

	t.Run("negative int value", testParseQuery(`total=-132`, &ast.Node{
		NodeType: ast.NodeTerminal,
		Comparison: ast.Comparison{
			Op:     "=",
			Field:  "total",
			Values: json.Number("-132"),
		},
		Left:  nil,
		Right: nil,
	}))

	t.Run("comparator = ", testParseQuery(`answer=42`, &ast.Node{
		NodeType: ast.NodeTerminal,
		Comparison: ast.Comparison{
			Op:     "=",
			Field:  "answer",
			Values: json.Number("42"),
		},
		Left:  nil,
		Right: nil,
	}))

	t.Run("comparator >", testParseQuery(`answer>42`, &ast.Node{
		NodeType: ast.NodeTerminal,
		Comparison: ast.Comparison{
			Op:     ">",
			Field:  "answer",
			Values: json.Number("42"),
		},
		Left:  nil,
		Right: nil,
	}))

	t.Run("comparator >=", testParseQuery(`answer>=42`, &ast.Node{
		NodeType: ast.NodeTerminal,
		Comparison: ast.Comparison{
			Op:     ">=",
			Field:  "answer",
			Values: json.Number("42"),
		},
		Left:  nil,
		Right: nil,
	}))

	t.Run("comparator <", testParseQuery(`answer<42`, &ast.Node{
		NodeType: ast.NodeTerminal,
		Comparison: ast.Comparison{
			Op:     "<",
			Field:  "answer",
			Values: json.Number("42"),
		},
		Left:  nil,
		Right: nil,
	}))

	t.Run("comparator <", testParseQuery(`answer<=42`, &ast.Node{
		NodeType: ast.NodeTerminal,
		Comparison: ast.Comparison{
			Op:     "<=",
			Field:  "answer",
			Values: json.Number("42"),
		},
		Left:  nil,
		Right: nil,
	}))

	t.Run("comparator <", testParseQuery(`answer!=42`, &ast.Node{
		NodeType: ast.NodeTerminal,
		Comparison: ast.Comparison{
			Op:     "!=",
			Field:  "answer",
			Values: json.Number("42"),
		},
		Left:  nil,
		Right: nil,
	}))

	t.Run("allow dash in field name", testParseQuery(`na-me= "t1-t2"`, &ast.Node{
		NodeType: ast.NodeTerminal,
		Comparison: ast.Comparison{
			Op:     "=",
			Field:  "na-me",
			Values: "\"t1-t2\"",
		},
		Left:  nil,
		Right: nil,
	}))

	t.Run("allow dash and slash in field name", testParseQuery(`na-/me= "t1/t2"`, &ast.Node{
		NodeType: ast.NodeTerminal,
		Comparison: ast.Comparison{
			Op:     "=",
			Field:  "na-/me",
			Values: "\"t1/t2\"",
		},
		Left:  nil,
		Right: nil,
	}))

	t.Run("free text search", testParseQuery(`"another"`, &ast.Node{
		NodeType: ast.NodeTerminal,
		Comparison: ast.Comparison{
			Op:     "=",
			Field:  "*",
			Values: "\"another\"",
		},
		Left:  nil,
		Right: nil,
	}))

	t.Run("search with *", testParseQuery(`*="another"`, &ast.Node{
		NodeType: ast.NodeTerminal,
		Comparison: ast.Comparison{
			Op:     "=",
			Field:  "*",
			Values: "\"another\"",
		},
		Left:  nil,
		Right: nil,
	}))

	t.Run("free text search AND", testParseQuery(`another AND test`, &ast.Node{
		NodeType: ast.NodeAnd,
		Left: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "*",
				Values: "another",
			},
		},
		Right: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "*",
				Values: "test",
			},
		},
	}))

	t.Run("free text search OR", testParseQuery(`another OR test`, &ast.Node{
		NodeType: ast.NodeOr,
		Left: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "*",
				Values: "another",
			},
		},
		Right: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "*",
				Values: "test",
			},
		},
	}))

	t.Run("free text search OR / AND clauses with paren precedence variation", testParseQuery(`"t2" OR ("t1" AND t3) `, &ast.Node{
		NodeType: ast.NodeOr,
		Left: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "*",
				Values: "\"t2\"",
			},
		},
		Right: &ast.Node{
			NodeType: ast.NodeAnd,
			Left: &ast.Node{
				NodeType: ast.NodeTerminal,
				Comparison: ast.Comparison{
					Op:     "=",
					Field:  "*",
					Values: "\"t1\"",
				},
			},
			Right: &ast.Node{
				NodeType: ast.NodeTerminal,
				Comparison: ast.Comparison{
					Op:     "=",
					Field:  "*",
					Values: "t3",
				},
			},
		},
	}))

	t.Run("free text search OR / AND clauses with paren precedence variation", testParseQuery(`"t2" AND ("t1" OR t3)  `, &ast.Node{
		NodeType: ast.NodeAnd,
		Left: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "*",
				Values: "\"t2\"",
			},
		},
		Right: &ast.Node{
			NodeType: ast.NodeOr,
			Left: &ast.Node{
				NodeType: ast.NodeTerminal,
				Comparison: ast.Comparison{
					Op:     "=",
					Field:  "*",
					Values: "\"t1\"",
				},
			},
			Right: &ast.Node{
				NodeType: ast.NodeTerminal,
				Comparison: ast.Comparison{
					Op:     "=",
					Field:  "*",
					Values: "t3",
				},
			},
		},
	}))

	t.Run("free text search - OR / AND clauses with paren precedence", testParseQuery(`("t1" AND t2) OR name="t3"`, &ast.Node{
		NodeType: ast.NodeOr,
		Left: &ast.Node{
			NodeType: ast.NodeAnd,
			Left: &ast.Node{
				NodeType: ast.NodeTerminal,
				Comparison: ast.Comparison{
					Op:     "=",
					Field:  "*",
					Values: "\"t1\"",
				},
			},
			Right: &ast.Node{
				NodeType: ast.NodeTerminal,
				Comparison: ast.Comparison{
					Op:     "=",
					Field:  "*",
					Values: "t2",
				},
			},
		},
		Right: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "name",
				Values: "\"t3\"",
			},
		},
	}))

	t.Run("wildcard - simple minimal condition", testParseQuery(`something="another*"`, &ast.Node{
		NodeType: ast.NodeTerminal,
		Comparison: ast.Comparison{
			Op:     "=",
			Field:  "something",
			Values: "\"another*\"",
		},
		Left:  nil,
		Right: nil,
	}))

	t.Run("wildcard - simple minimal condition", testParseQuery(`"another*"`, &ast.Node{
		NodeType: ast.NodeTerminal,
		Comparison: ast.Comparison{
			Op:     "=",
			Field:  "*",
			Values: "\"another*\"",
		},
		Left:  nil,
		Right: nil,
	}))

	t.Run("wildcard - simple minimal condition-no quotes", testParseQuery(`another*`, &ast.Node{
		NodeType: ast.NodeTerminal,
		Comparison: ast.Comparison{
			Op:     "=",
			Field:  "*",
			Values: "another*",
		},
		Left:  nil,
		Right: nil,
	}))

	t.Run("wildcard - simple minimal condition-no quotes", testParseQuery(`another* AND test*`, &ast.Node{
		NodeType: ast.NodeAnd,
		Left: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "*",
				Values: "another*",
			},
		},
		Right: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "*",
				Values: "test*",
			},
		},
	}))

	t.Run("wildcard - simple minimal condition-no quotes", testParseQuery(`another* OR test*`, &ast.Node{
		NodeType: ast.NodeOr,
		Left: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "*",
				Values: "another*",
			},
		},
		Right: &ast.Node{
			NodeType: ast.NodeTerminal,
			Comparison: ast.Comparison{
				Op:     "=",
				Field:  "*",
				Values: "test*",
			},
		},
	}))

	t.Run("column complex value condition", testParseQuery(`something="phrase-with-special-characters"`, &ast.Node{
		NodeType: ast.NodeTerminal,
		Comparison: ast.Comparison{
			Op:     "=",
			Field:  "something",
			Values: "\"phrase-with-special-characters\"",
		},
		Left:  nil,
		Right: nil,
	}))

	t.Run("column multi word condition", testParseQuery(`something="phrase with multiple words"`, &ast.Node{
		NodeType: ast.NodeTerminal,
		Comparison: ast.Comparison{
			Op:     "=",
			Field:  "something",
			Values: "\"phrase with multiple words\"",
		},
		Left:  nil,
		Right: nil,
	}))

	t.Run("special characters", testParseQuery(`something=AppleWebKit/532.47.1KHTML,likeGecko`, &ast.Node{
		NodeType: ast.NodeTerminal,
		Comparison: ast.Comparison{
			Op:     "=",
			Field:  "something",
			Values: "AppleWebKit/532.47.1KHTML,likeGecko",
		},
		Left:  nil,
		Right: nil,
	}))

	t.Run("special characters free text", testParseQuery(`https://peterson.info/`, &ast.Node{
		NodeType: ast.NodeTerminal,
		Comparison: ast.Comparison{
			Op:     "=",
			Field:  "*",
			Values: "https://peterson.info/",
		},
		Left:  nil,
		Right: nil,
	}))
}

func TestAST_simpleAnd_CommandsAgg(t *testing.T) {
	json_body := []byte(`name="t1" AND surname="t2" | columns name`)
	res, aggNode, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 2)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "name")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t1")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "surname")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t2")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, Equals)
	assert.NotNil(t, aggNode.OutputTransforms)
	assert.Len(t, aggNode.OutputTransforms.OutputColumns.IncludeColumns, 1)
	assert.Equal(t, aggNode.OutputTransforms.OutputColumns.IncludeColumns[0], "name")
}

func TestAST_blankSearchFilter_CommandsAgg(t *testing.T) {
	json_body := []byte(`| columns name`)
	res, aggNode, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.NotNil(t, aggNode.OutputTransforms)
	assert.Len(t, aggNode.OutputTransforms.OutputColumns.IncludeColumns, 1)
	assert.Equal(t, aggNode.OutputTransforms.OutputColumns.IncludeColumns[0], "name")
}

func TestAST_blankSearchFilter_ColumnAgg_List(t *testing.T) {
	json_body := []byte(`| columns name,test`)
	res, aggNode, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.NotNil(t, aggNode)
	assert.NotNil(t, aggNode.OutputTransforms)
	assert.Len(t, aggNode.OutputTransforms.OutputColumns.IncludeColumns, 2)
	assert.Equal(t, aggNode.OutputTransforms.OutputColumns.IncludeColumns[0], "name")
	assert.Equal(t, aggNode.OutputTransforms.OutputColumns.IncludeColumns[1], "test")
}

func TestAST_simpleAnd_SegLevelStats(t *testing.T) {
	json_body := []byte(`name="t1" AND surname="t2" | min(latency)`)
	res, aggNode, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 2)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "name")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t1")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "surname")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t2")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, Equals)
	assert.NotNil(t, aggNode)
	assert.NotNil(t, aggNode.MeasureOperations)
	assert.Equal(t, aggNode.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, aggNode.MeasureOperations[0].MeasureFunc, utils.Min)
}

func TestAST_blankSearchFilter_SegLevelStats(t *testing.T) {
	json_body := []byte(`| max(cnt)`)
	res, aggNode, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.NotNil(t, aggNode)
	assert.NotNil(t, aggNode.MeasureOperations)
	assert.Equal(t, aggNode.MeasureOperations[0].MeasureCol, "cnt")
	assert.Equal(t, aggNode.MeasureOperations[0].MeasureFunc, utils.Max)
}

func TestAST_blankSearchFilter_ColumnAgg_ExcludeList(t *testing.T) {
	json_body := []byte(`| columns - name,test`)
	res, aggNode, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.NotNil(t, aggNode)
	assert.NotNil(t, aggNode.OutputTransforms)
	assert.Len(t, aggNode.OutputTransforms.OutputColumns.ExcludeColumns, 2)
	assert.Equal(t, aggNode.OutputTransforms.OutputColumns.ExcludeColumns[0], "name")
	assert.Equal(t, aggNode.OutputTransforms.OutputColumns.ExcludeColumns[1], "test")
}

func TestAST_simpleAnd_ColumnAgg_Rename(t *testing.T) {
	json_body := []byte(`name="t1" AND surname="t2" | columns newname = name`)
	res, aggNode, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 2)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "name")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t1")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "surname")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t2")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, Equals)
	assert.NotNil(t, aggNode.OutputTransforms)
	assert.Len(t, aggNode.OutputTransforms.OutputColumns.RenameColumns, 1)
	assert.Equal(t, aggNode.OutputTransforms.OutputColumns.RenameColumns["name"], "newname")
}

func TestAST_simpleAnd_LetAgg_Single(t *testing.T) {
	json_body := []byte(`name="t1" AND surname="t2" | let isError=(status >= 399)`)
	res, aggNode, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 2)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "name")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t1")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "surname")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t2")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, Equals)
	assert.NotNil(t, aggNode.OutputTransforms)
	assert.NotNil(t, aggNode.OutputTransforms.LetColumns)
	assert.Equal(t, aggNode.OutputTransforms.LetColumns.NewColName, "isError")
	assert.NotNil(t, aggNode.OutputTransforms.LetColumns.SingleColRequest)
	assert.Equal(t, aggNode.OutputTransforms.LetColumns.SingleColRequest.CName, "status")
	assert.Equal(t, aggNode.OutputTransforms.LetColumns.SingleColRequest.Oper, utils.LogicalAndArithmeticOperator(10))
	assert.Equal(t, aggNode.OutputTransforms.LetColumns.SingleColRequest.Value.UnsignedVal, uint64(399))

}

func TestAST_simpleAnd_seglevelStats_commaSeparated(t *testing.T) {
	json_body := []byte(`name="t1" AND surname="t2" | min(latency),max(latency)`)
	res, aggNode, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 2)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "name")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t1")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "surname")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "t2")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, Equals)
	assert.NotNil(t, aggNode.MeasureOperations)
	assert.Equal(t, aggNode.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, aggNode.MeasureOperations[0].MeasureFunc, utils.Min)
	assert.Equal(t, aggNode.MeasureOperations[1].MeasureCol, "latency")
	assert.Equal(t, aggNode.MeasureOperations[1].MeasureFunc, utils.Max)
}

func TestAST_GroupByseglevelStats_commaSeparated(t *testing.T) {
	json_body := []byte(` min(latency),max(latency) groupby region`)
	res, aggNode, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.NotNil(t, aggNode.GroupByRequest)
	assert.NotNil(t, aggNode.GroupByRequest.MeasureOperations)
	assert.Equal(t, aggNode.GroupByRequest.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, aggNode.GroupByRequest.MeasureOperations[0].MeasureFunc, utils.Min)
	assert.Equal(t, aggNode.GroupByRequest.MeasureOperations[1].MeasureCol, "latency")
	assert.Equal(t, aggNode.GroupByRequest.MeasureOperations[1].MeasureFunc, utils.Max)
	assert.NotNil(t, aggNode.GroupByRequest.GroupByColumns)
	assert.Equal(t, aggNode.GroupByRequest.GroupByColumns[0], "region")
}

func TestAST_GroupByseglevelStats_commaSeparated_multipleGroups(t *testing.T) {
	json_body := []byte(` min(latency),max(latency) groupby region,os_name`)
	res, aggNode, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.NotNil(t, aggNode.GroupByRequest)
	assert.NotNil(t, aggNode.GroupByRequest.MeasureOperations)
	assert.Equal(t, aggNode.GroupByRequest.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, aggNode.GroupByRequest.MeasureOperations[0].MeasureFunc, utils.Min)
	assert.Equal(t, aggNode.GroupByRequest.MeasureOperations[1].MeasureCol, "latency")
	assert.Equal(t, aggNode.GroupByRequest.MeasureOperations[1].MeasureFunc, utils.Max)
	assert.NotNil(t, aggNode.GroupByRequest.GroupByColumns)
	assert.Equal(t, aggNode.GroupByRequest.GroupByColumns[0], "region")
	assert.Equal(t, aggNode.GroupByRequest.GroupByColumns[1], "os_name")
}

func TestAST_GroupByseglevelStats_commaSeparated_multipleGroups_withPipe(t *testing.T) {
	json_body := []byte(`| min(latency),max(latency) groupby region,os_name`)
	res, aggNode, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.Nil(t, err)
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.NotNil(t, aggNode.GroupByRequest)
	assert.NotNil(t, aggNode.GroupByRequest.MeasureOperations)
	assert.Equal(t, aggNode.GroupByRequest.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, aggNode.GroupByRequest.MeasureOperations[0].MeasureFunc, utils.Min)
	assert.Equal(t, aggNode.GroupByRequest.MeasureOperations[1].MeasureCol, "latency")
	assert.Equal(t, aggNode.GroupByRequest.MeasureOperations[1].MeasureFunc, utils.Max)
	assert.NotNil(t, aggNode.GroupByRequest.GroupByColumns)
	assert.Equal(t, aggNode.GroupByRequest.GroupByColumns[0], "region")
	assert.Equal(t, aggNode.GroupByRequest.GroupByColumns[1], "os_name")
}

func TestAST_invalidSearch_SegLevelStats(t *testing.T) {
	json_body := []byte(`| max(cnt).`)
	res, _, err := parsePipeSearch(string(json_body), "Pipe QL", 1)
	assert.NotNil(t, err)
	assert.Nil(t, res)
}

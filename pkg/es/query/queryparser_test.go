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

package query

import (
	"strconv"
	"testing"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_ParseRequest(t *testing.T) {
	json_body := []byte(`{
	"size": 0,
	"query":{
	"bool": {"must" :
			{"term" : { "user.id" : "kimchy" }}
	}}}`)
	res, aggs, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, aggs, structs.InitDefaultQueryAggregations())
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "kimchy")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
}

func Test_parseQuery_matchall(t *testing.T) {
	json_body := []byte(`{
		"query": {
			"match_all": {}
		}
	}`)
	res, aggs, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, aggs, structs.InitDefaultQueryAggregations())
	assert.IsType(t, res, &ASTNode{})
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "*")
}

func Test_parseQuery_bool(t *testing.T) {
	json_body := []byte(`{
		"bool" :{}
	}`)
	_, err := parseBool(json_body, 0, false)
	assert.NotNil(t, err)
}

func Test_parseQuery_must(t *testing.T) {
	json_body := []byte(`{
	"query":{
	"bool": {"must" :
			{"term" : { "user.id" : "kimchy" }}
	}}}`)
	res, aggs, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, aggs, structs.InitDefaultQueryAggregations())
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "kimchy")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
}

func Test_ParseRequest_must_term_array(t *testing.T) {
	json_body := []byte(`{
	"query":{
	"bool": {"must" :
			[{"term" : { "user.id" : "kimchy" }},
			 {"term" : { "id" : 123 }}
			]
	}}}`)
	res, aggs, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, aggs, structs.InitDefaultQueryAggregations())
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 2)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "kimchy")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "id")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(123))
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, Equals)
}

func Test_ParseRequest_match_noQueryClause(t *testing.T) {

	json_body := []byte(`{
	"query":{
		"bool": {"must" :
				{"match": {"message": "this is a test"}}
		}}}`)

	res, aggs, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, aggs, structs.InitDefaultQueryAggregations())
	assert.IsType(t, res, &ASTNode{})
	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "message")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, Or)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("this"), []byte("is"), []byte("a"), []byte("test")})
}

func Test_ParseRequest_match(t *testing.T) {
	json_body := []byte(`{
	"query":{
			"bool": {"must" :
					{"match": {"message": {
									  "query": "test",
									  "operator": "and"
									}}}
			}}}`)
	res, aggs, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, aggs, structs.InitDefaultQueryAggregations())
	assert.IsType(t, res, &ASTNode{})
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "message")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, And)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("test")})
}

func Test_ParseRequest_must_range_lte_Condition(t *testing.T) {
	json_body := []byte(`
				 {
				 "query":{
				 "bool": {"must" :
						   {"range" : { "age" : {
												 "lte": 10
												 }}}}}}`)
	res, aggs, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, aggs, structs.InitDefaultQueryAggregations())
	assert.IsType(t, res, &ASTNode{})
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "age")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "10")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, LessThanOrEqualTo)

}

func Test_ParseRequest_must_range_gte_Condition(t *testing.T) {
	json_body := []byte(`
					 {"query":{
					 "bool": {"must" :
							   {"range" : { "age" : {
													 "gte": 10
													 }}}}}}`)
	res, aggs, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, aggs, structs.InitDefaultQueryAggregations())
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "age")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "10")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, GreaterThanOrEqualTo)

}

func Test_ParseRequest_must_range_gt_lt_Condition(t *testing.T) {
	json_body := []byte(`
					 {"query":{
					 "bool": {"must" :
							   {"range" : { "age" : {
													 "gte": 10,
													 "lte": 20
													 }}}}}}`)
	res, aggs, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, aggs, structs.InitDefaultQueryAggregations())
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "age")
	//		assert.Equal(t, res.andFilterConditions[0].expressionFilter.rightInput.expression.leftInput.literal, "10")
	//		assert.Equal(t, res.andFilterConditions[0].expressionFilter.filterOperator, GreaterThanOrEqualTo)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "age")
	//		assert.Equal(t, res.andFilterConditions[1].expressionFilter.rightInput.expression.leftInput.literal, "20")
	//		assert.Equal(t, res.andFilterConditions[1].expressionFilter.filterOperator, LessThanOrEqualTo)

}

func Test_ParseRequest_must_range_timerange(t *testing.T) {
	config.InitializeDefaultConfig()
	json_body := []byte(`
					 {"query":{
					   "bool": {
						 "must": [{
						   "range": {
							 "timestamp": {
							   "gte": 1633000284000,
							   "lte": 1633000304000
							 }
						   }
						 },
						 {"term": {"user.id" : "kimchy"}}]
					   }
					 }}`)
	res, aggs, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, aggs, structs.InitDefaultQueryAggregations())
	assert.IsType(t, res, &ASTNode{})
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "kimchy")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)

	assert.Equal(t, strconv.FormatUint(res.TimeRange.StartEpochMs, 10), "1633000284000")
	assert.Equal(t, strconv.FormatUint(res.TimeRange.EndEpochMs, 10), "1633000304000")

}

func Test_ParseRequest_must_Terms(t *testing.T) {
	json_body := []byte(`{"query":{
		"bool": {"must" :
				{"terms" : { "user.id" : [ "kimchy", "elkbee" ] }}
		}}}`)
	res, aggs, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, aggs, structs.InitDefaultQueryAggregations())
	assert.IsType(t, res, &ASTNode{})
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "user.id")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, Or)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("kimchy"), []byte("elkbee")})
}

func Test_ParseRequest_must_Terms_invalid_empty(t *testing.T) {
	json_body := []byte(`{"query":{
			"bool": {"must" :
					{"terms" : { "user.id" : [] }}
			}}}`)
	_, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.NotNil(t, err)
}

func Test_ParseRequest_must_Terms_invalid_nested(t *testing.T) {
	json_body := []byte(`{"query":{
			"bool": {"must" :
					{"terms" : {"message": {
									   "query": "test"
									 }}}
			}}}`)
	_, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.NotNil(t, err)
}

func Test_ParseRequest_must_Prefix(t *testing.T) {
	json_body := []byte(`{"query":{
			"bool": {"must" :
					{"prefix" : {"user.id": "ki"}}
			}}}`)
	res, aggs, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, aggs, structs.InitDefaultQueryAggregations())
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "ki*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)

}

func Test_ParseRequest_must_Prefix_nested(t *testing.T) {
	json_body := []byte(`{"query":{
			"bool": {"must" :
					{"prefix" :{ "user.name": {
										"value": "SK"
									   }}}
			}}}`)
	res, aggs, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, aggs, structs.InitDefaultQueryAggregations())
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.name")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "SK*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
}
func Test_ParseRequest_must_Prefix_invalid(t *testing.T) {
	json_body := []byte(`{"query":{
			"bool": {"must" :
					{"prefix" :{ "user.name":{
										"value": ["SK"]
									   }}}
			}}}`)
	_, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.NotNil(t, err)

}

func Test_ParseRequest_must_Regexp(t *testing.T) {
	json_body := []byte(`{"query":{
			"bool": {"must" :
					{"regexp" :{"user.id": {"value": "k.*y"}}
									   }
			}}}`)
	res, aggs, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, aggs, structs.InitDefaultQueryAggregations())
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "k.*y")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)

}

func Test_ParseRequest_must_Wildcard(t *testing.T) {
	json_body := []byte(`{"query":{
			"bool": {"must" :
					{"wildcard" :{"user.id": {"value": "k.*y"}}
									   }
			}}}`)
	res, aggs, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, aggs, structs.InitDefaultQueryAggregations())
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "k.*y")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)

}

func Test_ParseRequest_query_string(t *testing.T) {
	json_body := []byte(`{"query":{
			"bool": {"must" :
					{"query_string": {
								 "query": "customer_full_name :\"Gwen Powell\""

							   }}
			}}}`)
	res, aggs, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, aggs, structs.InitDefaultQueryAggregations())
	assert.IsType(t, res, &ASTNode{})
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "customer_full_name")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "Gwen Powell")
}

func Test_ParseRequest_filter(t *testing.T) {
	json_body := []byte(`{
	"query":{
	"bool": {"filter" :
			{"term" : { "user.id" : "kimchy" }}
	}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "kimchy")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
}

func Test_ParseRequest_filter_term_array(t *testing.T) {
	json_body := []byte(`{
	"query":{
	"bool": {"filter" :
			[{"term" : { "user.id" : "kimchy" }},
			 {"term" : { "id" : 123 }}
			]
	}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "kimchy")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "id")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(123))
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, Equals)
}

func Test_ParseRequest_filter_term_with_bool(t *testing.T) {
	json_body := []byte(`{
	"query":{
	"bool": {"filter" :
			[{"term" : { "user.id" : "kimchy" }},
			 {"term" : { "device_is_mobile" : false }}
			]
	}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "kimchy")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "device_is_mobile")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.BoolVal, uint8(0))
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, Equals)
}

func Test_ParseRequest_filter_term_array_invalid(t *testing.T) {
	json_body := []byte(`{
	"query":{
	"bool": {"filter" :
			[{"term" : { "user.id" : "kimchy" }},
			 "123"
			]
	}}}`)
	_, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.NotNil(t, err)
}

func Test_ParseRequest_filter_match_noQueryClause(t *testing.T) {

	json_body := []byte(`{
	"query":{
		"bool": {"filter" :
				{"match": {"message": "this is a test"}}
		}}}`)

	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "message")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, Or)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("this"), []byte("is"), []byte("a"), []byte("test")})
}

func Test_ParseRequest_filter_match(t *testing.T) {
	json_body := []byte(`{
	"query":{
			"bool": {"filter" :
					{"match": {"message": {
									  "query": "test",
									  "operator": "and"
									}}}
			}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "message")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, And)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("test")})

}

func Test_ParseRequest_filter_range_lte_Condition(t *testing.T) {
	json_body := []byte(`
				 {
				 "query":{
				 "bool": {"filter" :
						   {"range" : { "age" : {
												 "lte": 10
												 }}}}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "age")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "10")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, LessThanOrEqualTo)

}

func Test_ParseRequest_filter_range_gte_Condition(t *testing.T) {
	json_body := []byte(`
					 {"query":{
					 "bool": {"filter" :
							   {"range" : { "age" : {
													 "gte": 10
													 }}}}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "age")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "10")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, GreaterThanOrEqualTo)

}

func Test_ParseRequest_filter_range_gt_lt_Condition(t *testing.T) {
	json_body := []byte(`
					 {"query":{
					 "bool": {"filter" :
							   {"range" : { "age" : {
													 "gte": 10,
													 "lte": 20
													 }}}}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "age")
	//		assert.Equal(t, res.andFilterConditions[0].expressionFilter.rightInput.expression.leftInput.literal, "10")
	//		assert.Equal(t, res.andFilterConditions[0].expressionFilter.filterOperator, GreaterThanOrEqualTo)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "age")
	//		assert.Equal(t, res.andFilterConditions[1].expressionFilter.rightInput.expression.leftInput.literal, "20")
	//		assert.Equal(t, res.andFilterConditions[1].expressionFilter.filterOperator, LessThanOrEqualTo)

}

func Test_ParseRequest_filter_range_timerange(t *testing.T) {
	config.InitializeDefaultConfig()
	json_body := []byte(`
					 {"query":{
					   "bool": {
						 "filter": [{
						   "range": {
							 "timestamp": {
							   "gte": 1633000284000,
							   "lte": 1633000304000,
							   "format": "epoch_millis"
							 }
						   }
						 },
						 {"term": {"user.id" : "kimchy"}}]
					   }
					 }}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "kimchy")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)

	assert.Equal(t, strconv.FormatUint(res.TimeRange.StartEpochMs, 10), "1633000284000")
	assert.Equal(t, strconv.FormatUint(res.TimeRange.EndEpochMs, 10), "1633000304000")

}

func Test_ParseRequest_filter_Terms(t *testing.T) {
	json_body := []byte(`{"query":{
		"bool": {"filter" :
				{"terms" : { "user.id" : [ "kimchy", "elkbee" ] }}
		}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "user.id")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, Or)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("kimchy"), []byte("elkbee")})
}

func Test_ParseRequest_filter_Terms_invalid_empty(t *testing.T) {
	json_body := []byte(`{"query":{
			"bool": {"filter" :
					{"terms" : { "user.id" : [] }}
			}}}`)
	_, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.NotNil(t, err)
}

func Test_ParseRequest_filter_Terms_invalid_nested(t *testing.T) {
	json_body := []byte(`{"query":{
			"bool": {"filter" :
					{"terms" : {"message": {
									   "query": "test"
									 }}}
			}}}`)
	_, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.NotNil(t, err)
}

func Test_ParseRequest_filter_Prefix(t *testing.T) {
	json_body := []byte(`{"query":{
			"bool": {"filter" :
					{"prefix" : {"user.id": "ki"}}
			}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)

	assert.Nil(t, err)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "ki*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)

}

func Test_ParseRequest_filter_Prefix_nested(t *testing.T) {
	json_body := []byte(`{"query":{
			"bool": {"filter" :
					{"prefix" :{ "user.name": {
										"value": "SK"
									   }}}
			}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.name")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "SK*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
}

func Test_ParseRequest_filter_Prefix_invalid(t *testing.T) {
	json_body := []byte(`{"query":{
			"bool": {"filter" :
					{"prefix" :{ "user.name":{
										"value": ["SK"]
									   }}}
			}}}`)
	_, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.NotNil(t, err)

}

func Test_ParseRequest_filter_Regexp_multipleParams(t *testing.T) {
	json_body := []byte(`{
		"query": {
			"bool": {
				"filter": [
					{
						"regexp": {
							"metric.raw": {
								"value": "ConnectionPool-name1-.*\\.NumWaitingForConnections",
								"flags_value": 255,
								"max_determinized_states": 10000,
								"boost": 1.0
							}
						}
					},
					{
						"regexp": {
							"scope.raw": {
								"value": "core\\.PH2\\..*\\.(na|eu|um|ap|cs|gs).*",
								"flags_value": 255,
								"max_determinized_states": 10000,
								"boost": 1.0
							}
						}
					},
					{
						"regexp": {
							"tagk.raw": {
								"value": "device",
								"flags_value": 255,
								"max_determinized_states": 10000,
								"boost": 1.0
							}
						}
					},
					{
						"regexp": {
							"tagv.raw": {
								"value": ".*-app.*-.*",
								"flags_value": 255,
								"max_determinized_states": 10000,
								"boost": 1.0
							}
						}
					},
					{
						"range": {
							"mts": {
								"from": 1638627863984,
								"to": 1639491866710,
								"include_lower": true,
								"include_upper": true,
								"boost": 1.0
							}
						}
					}
				],
				"adjust_pure_negative": true,
				"boost": 1.0
			}
		}
	}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)

	assert.Nil(t, err)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "metric")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "ConnectionPool-name1-.*\\.NumWaitingForConnections")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
}

func Test_ParseRequest_filter_Regexp(t *testing.T) {
	json_body := []byte(`{"query":{
			"bool": {"filter" :
					{"regexp" :{"user.id": {"value": "k.*y"}}
									   }
			}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)

	assert.Nil(t, err)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "k.*y")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)

}

func Test_ParseRequest_filter_Wildcard(t *testing.T) {
	json_body := []byte(`{"query":{
			"bool": {"filter" :
					{"wildcard" :{"user.id": {"value": "k.*y"}}
									   }
			}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "k.*y")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)

}

func Test_ParseRequest_filter_query_string(t *testing.T) {
	json_body := []byte(`{"query":{
			"bool": {"filter" :
					{"query_string": {
								  "analyze_wildcard": true,
								  "query": "col1:abc"
							   }}
			}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "col1")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "abc")
}

func Test_ParseRequest_should(t *testing.T) {
	json_body := []byte(`{
	"query":{
	"bool": {"should" :
			{"term" : { "user.id" : "kimchy" }}
	}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "kimchy")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
}

func Test_ParseRequest_should_term_array(t *testing.T) {
	json_body := []byte(`{
	"query":{
	"bool": {"should" :
			[{"term" : { "user.id" : "kimchy" }},
			 {"term" : { "id" : 123 }}
			]
	}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "kimchy")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "id")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(123))
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, Equals)

}

func Test_ParseRequest_should_term_array_invalid(t *testing.T) {
	json_body := []byte(`{
	"query":{
	"bool": {"should" :
			[{"term" : { "user.id" : "kimchy" }},
			 "123"
			]
	}}}`)
	_, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.NotNil(t, err)
}

func Test_ParseRequest_should_match_noQueryClause(t *testing.T) {

	json_body := []byte(`{
	"query":{
    	"bool": {"should" :
    			{"match": {"message": "this is a test"}}
    	}}}`)

	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "message")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, Or)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("this"), []byte("is"), []byte("a"), []byte("test")})
}

func Test_ParseRequest_should_match(t *testing.T) {
	json_body := []byte(`{
	"query":{
        	"bool": {"should" :
        			{"match": {"message": {
                                      "query": "test",
                                      "operator": "and"
                                    }}}
        	}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "message")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, And)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("test")})
}

func Test_ParseRequest_should_range_lte_Condition(t *testing.T) {
	json_body := []byte(`
                 {
                 "query":{
                 "bool": {"should" :
                           {"range" : { "age" : {
                                                 "lte": 10
                                                 }}}}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "age")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "10")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, LessThanOrEqualTo)

}

func Test_ParseRequest_should_range_gte_Condition(t *testing.T) {
	json_body := []byte(`
                     {"query":{
                     "bool": {"should" :
                               {"range" : { "age" : {
                                                     "gte": 10
                                                     }}}}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "age")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "10")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, GreaterThanOrEqualTo)

}

func Test_ParseRequest_should_range_gt_lt_Condition(t *testing.T) {
	json_body := []byte(`
                     {"query":{
                     "bool": {"should" :
                               {"range" : { "age" : {
                                                     "gte": 10,
        						                     "lte": 20
                                                     }}}}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "age")
	// 		assert.Equal(t, res.andFilterConditions[0].expressionFilter.rightInput.expression.leftInput.literal, "10")
	// 		assert.Equal(t, res.andFilterConditions[0].expressionFilter.filterOperator, GreaterThanOrEqualTo)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "age")
	// 		assert.Equal(t, res.andFilterConditions[1].expressionFilter.rightInput.expression.leftInput.literal, "20")
	// 		assert.Equal(t, res.andFilterConditions[1].expressionFilter.filterOperator, LessThanOrEqualTo)

}

func Test_ParseRequest_should_range_timerange(t *testing.T) {
	config.InitializeDefaultConfig()
	json_body := []byte(`
                     {"query":{
                       "bool": {
                         "should": [{
                           "range": {
                             "timestamp": {
                               "gte": 1633000284000,
                               "lte": 1633000304000,
                               "format": "epoch_millis"
                             }
                           }
                         },
                         {"term": {"user.id" : "kimchy"}}]
                       }
                     }}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "kimchy")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)

	assert.Equal(t, strconv.FormatUint(res.TimeRange.StartEpochMs, 10), "1633000284000")
	assert.Equal(t, strconv.FormatUint(res.TimeRange.EndEpochMs, 10), "1633000304000")

}

func Test_ParseRequest_should_Terms(t *testing.T) {
	json_body := []byte(`{"query":{
    	"bool": {"should" :
    			{"terms" : { "user.id" : [ "kimchy", "elkbee" ] }}
    	}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "user.id")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, Or)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("kimchy"), []byte("elkbee")})

}

func Test_ParseRequest_should_Terms_invalid_empty(t *testing.T) {
	json_body := []byte(`{"query":{
        	"bool": {"should" :
        			{"terms" : { "user.id" : [] }}
        	}}}`)
	_, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.NotNil(t, err)
}

func Test_ParseRequest_should_Terms_invalid_nested(t *testing.T) {
	json_body := []byte(`{"query":{
           	"bool": {"should" :
           			{"terms" : {"message": {
                                       "query": "test"
                                     }}}
           	}}}`)
	_, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.NotNil(t, err)
}

func Test_ParseRequest_should_Prefix(t *testing.T) {
	json_body := []byte(`{"query":{
           	"bool": {"should" :
           			{"prefix" : {"user.id": "ki"}}
           	}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)

	assert.Nil(t, err)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "ki*")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)

}

func Test_ParseRequest_should_Prefix_nested(t *testing.T) {
	json_body := []byte(`{"query":{
           	"bool": {"should" :
           			{"prefix" :{ "user.name": {
                                        "value": "SK"
                                       }}}
           	}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.name")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "SK*")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
}

func Test_ParseRequest_should_Prefix_invalid(t *testing.T) {
	json_body := []byte(`{"query":{
           	"bool": {"should" :
           			{"prefix" :{ "user.name":{
                                        "value": ["SK"]
                                       }}}
           	}}}`)
	_, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.NotNil(t, err)

}

func Test_ParseRequest_should_Regexp(t *testing.T) {
	json_body := []byte(`{"query":{
           	"bool": {"should" :
           			{"regexp" :{"user.id": {"value": "k.*y"}}
                                       }
           	}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)

	assert.Nil(t, err)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "k.*y")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)

}

func Test_ParseRequest_should_Wildcard(t *testing.T) {
	json_body := []byte(`{"query":{
           	"bool": {"should" :
           			{"wildcard" :{"user.id": {"value": "k.*y"}}
                                       }
           	}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "k.*y")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)

}

func Test_ParseRequest_should_query_string(t *testing.T) {
	json_body := []byte(`{"query":{
           	"bool": {"should" :
           			{"query_string": {
                                  "analyze_wildcard": true,
                                  "query": "col1:abc"
                               }}
           	}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Len(t, res.OrFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "col1")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "abc")
}

func Test_parseQuery_multiple(t *testing.T) {
	json_body := []byte(`{"query":{
	"bool":{
                  "must":
                    { "term": { "title":   "Search"        }},
                  "should":
                    { "term":  { "status": "published" }}

                }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "title")
	assert.Equal(t, res.OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "status")
}

func Test_aggregationParsing(t *testing.T) {
	json_body := []byte(`{"aggs": {
		"2": {
			"date_histogram": {
				"interval": "2h",
				"field": "timestamp",
				"min_doc_count": 0,
				"extended_bounds": {
					"min": 0,
					"max": 10
				},
				"format": "epoch_millis"
			},
			"aggs": {}
		}
	}}`)
	_, agg, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, uint64(7_200_000), agg.TimeHistogram.IntervalMillis)
	assert.Equal(t, "2", agg.TimeHistogram.AggName)
	assert.Equal(t, uint64(0), agg.TimeHistogram.StartTime)
	assert.Equal(t, uint64(10_000), agg.TimeHistogram.EndTime, "test conversion to millis")
}

func Test_ParseRequest_query_aggs(t *testing.T) {
	json_body := []byte(`{"query":{
			"bool": {"filter" :
					{"wildcard" :{"user.id": {"value": "k.*y"}}
									   }
			}},"aggs": {
				"2": {
					"date_histogram": {
						"interval": "2h",
						"field": "timestamp",
						"min_doc_count": 0,
						"extended_bounds": {
							"min": 0,
							"max": 10
						},
						"format": "epoch_millis"
					},
					"aggs": {}
				}
			}}`)
	res, agg, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "k.*y")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)

	assert.NotNil(t, agg.TimeHistogram)
	assert.Equal(t, uint64(7_200_000), agg.TimeHistogram.IntervalMillis)
	assert.Equal(t, "2", agg.TimeHistogram.AggName)
	assert.Equal(t, uint64(0), agg.TimeHistogram.StartTime)
	assert.Equal(t, uint64(10_000), agg.TimeHistogram.EndTime)

}

func Test_ParseRequest_GroupByTerms(t *testing.T) {
	json_body := []byte(`{"aggs":{"2":{"terms":{"field":"a","size":10,"order":{"_term":"desc"},"min_doc_count":1},"aggs":{}}}}`)
	_, agg, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.NotNil(t, agg.GroupByRequest)
	assert.Equal(t, "a", agg.GroupByRequest.GroupByColumns[0])
	assert.Equal(t, Count, agg.GroupByRequest.MeasureOperations[0].MeasureFunc)
	assert.Equal(t, "a", agg.GroupByRequest.MeasureOperations[0].MeasureCol)
	assert.Equal(t, "2", agg.GroupByRequest.AggName)

}

func Test_ParseRequest_SimpleNestedAgg(t *testing.T) {
	json_body := []byte(`{"aggs":{"2":{"aggs":{"agg1":{"terms":{"field": "vpcName"}},"3":{"avg":{"field":"a"}}}}}}`)
	_, agg, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.NotNil(t, agg.GroupByRequest)
	assert.Equal(t, "vpcName", agg.GroupByRequest.GroupByColumns[0])
	assert.Equal(t, Avg, agg.GroupByRequest.MeasureOperations[0].MeasureFunc)
	assert.Equal(t, "a", agg.GroupByRequest.MeasureOperations[0].MeasureCol)
	assert.Equal(t, "2", agg.GroupByRequest.AggName)
}

func Test_ParseRequest_MultipleGroupByTerms(t *testing.T) {
	json_body := []byte(`{"aggs":{"2":{"aggs":{"agg1":{"terms":{"field": "vpcName"}},"3":{"avg":{"field":"a"}}},"terms":{"field": "vpcID"}}}}`)
	_, agg, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.NotNil(t, agg.GroupByRequest)
	assert.Contains(t, agg.GroupByRequest.GroupByColumns, "vpcName")
	assert.Contains(t, agg.GroupByRequest.GroupByColumns, "vpcID")
	assert.Len(t, agg.GroupByRequest.GroupByColumns, 2)
	assert.Equal(t, Avg, agg.GroupByRequest.MeasureOperations[0].MeasureFunc)
	assert.Equal(t, "a", agg.GroupByRequest.MeasureOperations[0].MeasureCol)
	assert.Equal(t, "2", agg.GroupByRequest.AggName)

}

func Test_ParseRequest_MultipleGroupByTerms_NoMeasureAgg(t *testing.T) {
	json_body := []byte(`{"aggs":{"2":{"aggs":{"agg1":{"terms":{"field": "vpcName"}}},"terms":{"field": "vpcID"}}}}`)
	_, agg, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.NotNil(t, agg.GroupByRequest)
	assert.Contains(t, agg.GroupByRequest.GroupByColumns, "vpcName")
	assert.Contains(t, agg.GroupByRequest.GroupByColumns, "vpcID")
	assert.Len(t, agg.GroupByRequest.GroupByColumns, 2)
	assert.Len(t, agg.GroupByRequest.MeasureOperations, 2)
	if agg.GroupByRequest.MeasureOperations[0].MeasureCol == "vpcName" && agg.GroupByRequest.MeasureOperations[0].MeasureFunc == Count {
		if agg.GroupByRequest.MeasureOperations[1].MeasureCol == "vpcID" && agg.GroupByRequest.MeasureOperations[1].MeasureFunc == Count {
		} else {
			assert.Fail(t, "wrong measure ops %+v", agg.GroupByRequest.MeasureOperations)
		}
	} else if agg.GroupByRequest.MeasureOperations[0].MeasureCol == "vpcID" && agg.GroupByRequest.MeasureOperations[0].MeasureFunc == Count {
		if agg.GroupByRequest.MeasureOperations[1].MeasureCol == "vpcName" && agg.GroupByRequest.MeasureOperations[1].MeasureFunc == Count {

		} else {
			assert.Fail(t, "wrong measure ops %+v", agg.GroupByRequest.MeasureOperations)
		}
	} else {
		assert.Fail(t, "wrong measure ops %+v", agg.GroupByRequest.MeasureOperations)
	}
	assert.Equal(t, "2", agg.GroupByRequest.AggName)
}

func Test_ParseRequest_SimpleAgg(t *testing.T) {
	json_body := []byte(`{"aggs":{"2":{"terms":{"field": "vpcName"}}}}`)
	_, agg, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.NotNil(t, agg.GroupByRequest)
	assert.Equal(t, "vpcName", agg.GroupByRequest.GroupByColumns[0])
	assert.Equal(t, Count, agg.GroupByRequest.MeasureOperations[0].MeasureFunc)
	assert.Equal(t, "vpcName", agg.GroupByRequest.MeasureOperations[0].MeasureCol)
	assert.Equal(t, "2", agg.GroupByRequest.AggName)
}

func Test_ParseRequest_SimpleDateHistogram(t *testing.T) {
	json_body := []byte(`{
		"aggs": {
			"2": {
				"date_histogram": {
					"interval": "2h",
					"field": "timestamp",
					"min_doc_count": 0,
					"extended_bounds": {
						"min": 0,
						"max": 10
					},
					"format": "epoch_millis"
				}
			}
		}
	}`)
	_, agg, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.NotNil(t, agg.TimeHistogram)
	assert.Equal(t, uint64(7_200_000), agg.TimeHistogram.IntervalMillis)
	assert.Equal(t, "2", agg.TimeHistogram.AggName)
	assert.Equal(t, uint64(0), agg.TimeHistogram.StartTime)
	assert.Equal(t, uint64(10_000), agg.TimeHistogram.EndTime)
}
func Test_ParseRequest_FilterHistogram(t *testing.T) {
	json_body := []byte(`{"aggs":{"2":{"filters":{"filters":{"internet":{"query_string":{"query":"col1: abc","analyze_wildcard":true}}}},"aggs":{}}}}`)
	_, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.NotNil(t, err)

}

func Test_ParseRequest_FilterAgg(t *testing.T) {
	json_body := []byte(`{"aggs":{"2":{"filters":{"filters":{"internet":{"query_string":{"query":"col1: abc","analyze_wildcard":true}}}},"aggs":{"3":{"avg":{"field":"a"}}}}}}`)
	_, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.NotNil(t, err)
}

func Test_ParseSize(t *testing.T) {
	json_body := []byte(`{"size":20,"query":{"bool": {"should" :{"terms" : { "user.id" : [ "kimchy", "elkbee" ] }}}}}`)
	_, _, size, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, uint64(20), size)
}

func Test_ParseRequest_must_not(t *testing.T) {
	json_body := []byte(`{
    "query":{
    "bool": {"must_not" :
            {"term" : { "user.id" : "kimchy" }}
    }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "kimchy")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
}

func Test_ParseRequest_must_not_term_array(t *testing.T) {
	json_body := []byte(`{
    "query":{
    "bool": {"must_not" :
            [{"term" : { "user.id" : "kimchy" }},
             {"term" : { "id" : 123 }}
            ]
    }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "kimchy")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "id")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(123))
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, Equals)

}

func Test_ParseRequest_must_not_term_array_invalid(t *testing.T) {
	json_body := []byte(`{
    "query":{
    "bool": {"must_not" :
            [{"term" : { "user.id" : "kimchy" }},
             "123"
            ]
    }}}`)
	_, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.NotNil(t, err)
}

func Test_ParseRequest_must_not_match_noQueryClause(t *testing.T) {

	json_body := []byte(`{
    "query":{
        "bool": {"must_not" :
                {"match": {"message": "this is a test"}}
        }}}`)

	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "message")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, Or)
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("this"), []byte("is"), []byte("a"), []byte("test")})
}

func Test_ParseRequest_must_not_match(t *testing.T) {
	json_body := []byte(`{
    "query":{
            "bool": {"must_not" :
                    {"match": {"message": {
                                      "query": "test",
                                      "operator": "and"
                                    }}}
            }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "message")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, And)
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("test")})
}

func Test_ParseRequest_must_not_range_lte_Condition(t *testing.T) {
	json_body := []byte(`
                 {
                 "query":{
                 "bool": {"must_not" :
                           {"range" : { "age" : {
                                                 "lte": 10
                                                 }}}}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "age")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "10")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, LessThanOrEqualTo)

}

func Test_ParseRequest_must_not_range_gte_Condition(t *testing.T) {
	json_body := []byte(`
                     {"query":{
                     "bool": {"must_not" :
                               {"range" : { "age" : {
                                                     "gte": 10
                                                     }}}}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "age")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "10")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, GreaterThanOrEqualTo)
}

func Test_ParseRequest_must_not_range_gt_lt_Condition(t *testing.T) {
	json_body := []byte(`
                     {"query":{
                     "bool": {"must_not" :
                               {"range" : { "age" : {
                                                     "gte": 10,
                                                     "lte": 20
                                                     }}}}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "age")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "age")
}

func Test_ParseRequest_must_not_range_timerange(t *testing.T) {
	config.InitializeDefaultConfig()
	json_body := []byte(`
                     {"query":{
                       "bool": {
                         "must_not": [{
                           "range": {
                             "timestamp": {
                               "gte": 1633000284000,
                               "lte": 1633000304000,
                               "format": "epoch_millis"
                             }
                           }
                         },
                         {"term": {"user.id" : "kimchy"}}]
                       }
                     }}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "kimchy")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)

	assert.Equal(t, strconv.FormatUint(res.TimeRange.StartEpochMs, 10), "1633000284000")
	assert.Equal(t, strconv.FormatUint(res.TimeRange.EndEpochMs, 10), "1633000304000")

}

func Test_ParseRequest_must_not_Terms(t *testing.T) {
	json_body := []byte(`{"query":{
        "bool": {"must_not" :
                {"terms" : { "user.id" : [ "kimchy", "elkbee" ] }}
        }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "user.id")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, Or)
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("kimchy"), []byte("elkbee")})
}

func Test_ParseRequest_must_not_Terms_invalid_empty(t *testing.T) {
	json_body := []byte(`{"query":{
            "bool": {"must_not" :
                    {"terms" : { "user.id" : [] }}
            }}}`)
	_, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.NotNil(t, err)
}

func Test_ParseRequest_must_not_Terms_invalid_nested(t *testing.T) {
	json_body := []byte(`{"query":{
               "bool": {"must_not" :
                       {"terms" : {"message": {
                                       "query": "test"
                                     }}}
               }}}`)
	_, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.NotNil(t, err)
}

func Test_ParseRequest_must_not_Prefix(t *testing.T) {
	json_body := []byte(`{"query":{
               "bool": {"must_not" :
                       {"prefix" : {"user.id": "ki"}}
               }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)

	assert.Nil(t, err)
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "ki*")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
}

func Test_ParseRequest_must_not_Prefix_nested(t *testing.T) {
	json_body := []byte(`{"query":{
               "bool": {"must_not" :
                       {"prefix" :{ "user.name": {
                                        "value": "SK"
                                       }}}
               }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.name")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "SK*")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
}

func Test_ParseRequest_must_not_Prefix_invalid(t *testing.T) {
	json_body := []byte(`{"query":{
               "bool": {"must_not" :
                       {"prefix" :{ "user.name":{
                                        "value": ["SK"]
                                       }}}
               }}}`)
	_, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.NotNil(t, err)

}

func Test_ParseRequest_must_not_Regexp(t *testing.T) {
	json_body := []byte(`{"query":{
               "bool": {"must_not" :
                       {"regexp" :{"user.id": {"value": "k.*y"}}
                                       }
               }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)

	assert.Nil(t, err)
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "k.*y")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
}

func Test_ParseRequest_must_not_Wildcard(t *testing.T) {
	json_body := []byte(`{"query":{
               "bool": {"must_not" :
                       {"wildcard" :{"user.id": {"value": "k.*y"}}
                                       }
               }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user.id")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "k.*y")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)

}

func Test_ParseRequest_must_not_query_string(t *testing.T) {
	json_body := []byte(`{"query":{
               "bool": {"must_not" :
                       {"query_string": {
                                  "analyze_wildcard": true,
                                  "query": "col1:abc"
                               }}
               }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "col1")
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.ExclusionFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "abc")
}

func Test_ParseRequest_must_query_string_AND(t *testing.T) {
	json_body := []byte(`{"query":{
               "bool": {"must" :
                       {"query_string": {
                                  "analyze_wildcard": true,
								  "default_field": "*",
                                  "query": "col1:abc AND col2:abcd"
                               }}
               }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Len(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria, 2)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "col1")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "abc")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "col2")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "abcd")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, Equals)

}

func Test_ParseRequest_must_query_string_OR(t *testing.T) {
	json_body := []byte(`{"query":{
               "bool": {"must" :
                       {"query_string": {
                                  "analyze_wildcard": true,
								  "default_field": "*",
                                  "query": "col1:abc OR col2:abcd"
                               }}
               }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Len(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria, 2)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "col1")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "abc")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "col2")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "abcd")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, Equals)

}

func Test_ParseRequest_must_query_string_Key_value_OR(t *testing.T) {
	json_body := []byte(`{"query":{
               "bool": {"must" :
                       {"query_string": {
                                  "analyze_wildcard": true,
								  "default_field": "*",
                                  "query": "col1:(abc OR abcd)"
                               }}
               }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Len(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria, 2)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "col1")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "abc")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "col1")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "abcd")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, Equals)

}

func Test_ParseRequest_must_query_string_AND_OR(t *testing.T) {
	json_body := []byte(`{"query":{
               "bool": {"must" :
                       {"query_string": {
                                  "analyze_wildcard": true,
								  "default_field": "*",
                                  "query": "(col1:abc AND col2:abcd) OR col3 : eee"
                               }}
               }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Len(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria, 1)
	assert.Len(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria, 2)

	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "col1")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "abc")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "col2")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "abcd")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, Equals)

	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "col3")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "eee")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)

}

func Test_ParseRequest_must_query_string_AND_values(t *testing.T) {
	json_body := []byte(`{"query":{
               "bool": {"must" :
                       {"query_string": {
                                  "analyze_wildcard": true,
								  "default_field": "*",
                                  "query": "val1 AND val2"
                               }}
               }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Len(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria, 2)

	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("val1")})
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, Or)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].MatchFilter.MatchWords, [][]byte{[]byte("val2")})
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].MatchFilter.MatchOperator, Or)

}

func Test_ParseRequest_must_query_string_OR_values(t *testing.T) {
	json_body := []byte(`{"query":{
               "bool": {"must" :
                       {"query_string": {
                                  "analyze_wildcard": true,
								  "default_field": "*",
                                  "query": "val1 OR val2"
                               }}
               }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})

	assert.Len(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria, 2)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("val1")})
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, Or)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].MatchFilter.MatchWords, [][]byte{[]byte("val2")})
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].MatchFilter.MatchOperator, Or)

}

func Test_ParseRequest_must_query_string_AND_OR_values(t *testing.T) {
	json_body := []byte(`{"query":{
               "bool": {"must" :
                       {"query_string": {
                                  "analyze_wildcard": true,
								  "default_field": "*",
                                  "query": "(abc AND abcd) OR eee"
                               }}
               }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Len(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria, 1)
	assert.Len(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria, 2)

	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("abc")})
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, Or)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].MatchFilter.MatchWords, [][]byte{[]byte("abcd")})
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].MatchFilter.MatchOperator, Or)

	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("eee")})
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, Or)

}

func Test_ParseRequest_NestedQuery(t *testing.T) {

	// (col1=a or col1=b) and col2=c
	json_body := []byte(`
	{
		"query":{
			"bool": {
				"must" : [
					{"term" : { "col2" : "c" }},
					{"bool": {
						"should" : [
							{"term" : { "col1" : "a" }},
							{"term" : { "col1" : "b" }}
						]
					}}
				]
			}
		}
	}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "col2")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "c")
	assert.Len(t, res.AndFilterCondition.NestedNodes, 1)
	assert.Len(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria, 2)
	assert.Len(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.NestedNodes, 0)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "col1")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "a")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "col1")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "b")
}

func Test_parseExists(t *testing.T) {
	json_body := []byte(`
	{
		"query": {
			"bool": {
				"must": {
					"exists": {
						"field": "user"
					}
				}
			}
		}
	}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "user")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, IsNotNull)
}

func Test_parseSort(t *testing.T) {
	json_body := []byte(`
	{
		"sort": [
		{
		"timestamp": {
			"order": "asc",
			"unmapped_type": "boolean"
		}
		}
	]
	}`)
	_, agg, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, agg, &QueryAggregators{})
	assert.NotNil(t, agg.Sort)
	assert.Equal(t, agg.Sort.ColName, "timestamp")
	assert.Equal(t, agg.Sort.Ascending, true)

	json_body = []byte(`
	{
		"sort": [
		{
		"timestamp": {
			"order": "desc",
			"unmapped_type": "boolean"
		}
		}
	]
	}`)
	_, agg, _, _, err = ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, agg, &QueryAggregators{})
	assert.NotNil(t, agg.Sort)
	assert.Equal(t, agg.Sort.ColName, "timestamp")
	assert.Equal(t, agg.Sort.Ascending, false)
}

func Test_ParseRequest_match_phrase(t *testing.T) {
	json_body := []byte(`{
		"query": {
		  "match_phrase": {
			"message": "this is a test"
		  }
		}
	  }`)
	res, aggs, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, aggs, structs.InitDefaultQueryAggregations())
	assert.IsType(t, res, &ASTNode{})

	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "message")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, And)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("this"), []byte("is"), []byte("a"), []byte("test")})
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchPhrase, []uint8([]byte{0x74, 0x68, 0x69, 0x73, 0x20, 0x69, 0x73, 0x20, 0x61, 0x20, 0x74, 0x65, 0x73, 0x74}))
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchType, MATCH_PHRASE)

}

func Test_ParseRequest_must_query_string_free_text(t *testing.T) {
	json_body := []byte(`{"query":{
               "bool": {"must" :
                       {"query_string": {
                                  "analyze_wildcard": true,
								  "default_field": "*",
                                  "query": "val1"
                               }}
               }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})

	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("val1")})
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, Or)
}

func Test_ParseRequest_must_query_string_match_phrase_with_AND(t *testing.T) {
	json_body := []byte(`{"query":{
               "bool": {"must" :
                       {"query_string": {
                                  "analyze_wildcard": true,
								  "default_field": "*",
                                  "query": "\"val1\" AND \"val2\""
                               }}
               }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})

	assert.Len(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria, 2)

	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("val1")})
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].MatchFilter.MatchType, MATCH_PHRASE)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].MatchFilter.MatchWords, [][]byte{[]byte("val2")})
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].MatchFilter.MatchType, MATCH_PHRASE)
}

func Test_ParseRequest_must_query_string_match_phrase_with_OR(t *testing.T) {
	json_body := []byte(`{"query":{
               "bool": {"must" :
                       {"query_string": {
                                  "analyze_wildcard": true,
								  "default_field": "*",
                                  "query": "\"val1\" OR \"val2\""
                               }}
               }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})

	assert.Len(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria, 2)

	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("val1")})
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].MatchFilter.MatchType, MATCH_PHRASE)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].MatchFilter.MatchWords, [][]byte{[]byte("val2")})
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].MatchFilter.MatchType, MATCH_PHRASE)
}

func Test_ParseRequest_must_query_string_match_word_with_AND(t *testing.T) {
	json_body := []byte(`{"query":{
               "bool": {"must" :
                       {"query_string": {
                                  "analyze_wildcard": true,
								  "default_field": "*",
                                  "query": "val1 AND val2"
                               }}
               }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})

	assert.Len(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria, 2)

	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("val1")})
	assert.NotEqual(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].MatchFilter.MatchType, MATCH_PHRASE)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].MatchFilter.MatchWords, [][]byte{[]byte("val2")})
	assert.NotEqual(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].MatchFilter.MatchType, MATCH_PHRASE)
}

func Test_ParseRequest_must_query_string_match_word_with_OR(t *testing.T) {
	json_body := []byte(`{"query":{
               "bool": {"must" :
                       {"query_string": {
                                  "analyze_wildcard": true,
								  "default_field": "*",
                                  "query": "val1 OR val2"
                               }}
               }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})

	assert.Len(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria, 2)

	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("val1")})
	assert.NotEqual(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].MatchFilter.MatchType, MATCH_PHRASE)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].MatchFilter.MatchWords, [][]byte{[]byte("val2")})
	assert.NotEqual(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].MatchFilter.MatchType, MATCH_PHRASE)
}

func Test_ParseRequest_filter_query_string_col_val(t *testing.T) {
	json_body := []byte(`{"query":{
			"bool": {"filter" :
					{"query_string": {
								  "analyze_wildcard": true,
								  "query": "*:abc"
							   }}
			}}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "abc")
}

func Test_ParseRequest_must_query_string_col_val_AND_words(t *testing.T) {
	json_body := []byte(`{"query":{
               "bool": {"must" :
                       {"query_string": {
                                  "analyze_wildcard": true,
								  "default_field": "*",
                                  "query": "col1:val1 AND val2"
                               }}
               }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Len(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria, 2)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "col1")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "val1")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].MatchFilter.MatchWords, [][]byte{[]byte("val2")})
	assert.NotEqual(t, res.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].MatchFilter.MatchType, MATCH_PHRASE)
}

func Test_ParseRequest_must_query_string_col_val_OR_words(t *testing.T) {
	json_body := []byte(`{"query":{
               "bool": {"must" :
                       {"query_string": {
                                  "analyze_wildcard": true,
								  "default_field": "*",
                                  "query": "col1:val1 OR val2"
                               }}
               }}}`)
	res, _, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.IsType(t, res, &ASTNode{})
	assert.Len(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria, 2)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "col1")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "val1")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].MatchFilter.MatchWords, [][]byte{[]byte("val2")})
	assert.NotEqual(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].MatchFilter.MatchType, MATCH_PHRASE)
}
func Test_ParseRequest_match_multi_match_no_fields(t *testing.T) {
	json_body := []byte(`{
		"query": {
			"bool": {
				"must": {
					"multi_match": {
						"query":      "quick brown",
						"type":       "phrase",
						"operator": 	"and"
					}
				}
			}
		}
	  }`)
	res, aggs, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, aggs, structs.InitDefaultQueryAggregations())
	assert.IsType(t, res, &ASTNode{})

	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, And)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("quick"), []byte("brown")})
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchType, MATCH_PHRASE)

}

func Test_ParseRequest_match_multi_match_with_fields(t *testing.T) {
	json_body := []byte(`{
		"query": {
			"bool": {
				"must": {
					"multi_match": {
						"query":      "quick brown",
						"type":       "phrase",
						"operator": 	"and",
						"fields": [ "subject", "message" ]
					}
				}
			}
		}
	}`)
	res, aggs, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, aggs, structs.InitDefaultQueryAggregations())
	assert.IsType(t, res, &ASTNode{})

	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 2)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "subject")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, And)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("quick"), []byte("brown")})
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].MatchFilter.MatchType, MATCH_PHRASE)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].MatchFilter.MatchColumn, "message")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].MatchFilter.MatchOperator, And)
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].MatchFilter.MatchWords, [][]byte{[]byte("quick"), []byte("brown")})
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].MatchFilter.MatchType, MATCH_PHRASE)

}

func Test_ParseRequest_match_multi_match_with_fields_phrase_prefix(t *testing.T) {
	json_body := []byte(`{
		"query": {
			"bool": {
				"must": {
					"multi_match": {
						"query":      "quick brown f",
						"type":       "phrase_prefix",
						"operator": 	"and",
						"fields": [ "subject", "message" ]
					}
				}
			}
		}
	}`)
	res, aggs, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, aggs, structs.InitDefaultQueryAggregations())
	assert.IsType(t, res, &ASTNode{})

	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 2)

	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "subject")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "quick brown f.*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)

	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "message")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "quick brown f.*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, Equals)

}

func Test_ParseRequest_match_multi_match_no_fields_phrase_prefix(t *testing.T) {
	json_body := []byte(`{
		"query": {
			"bool": {
				"must": {
					"multi_match": {
						"query":      "quick brown f",
						"type":       "phrase_prefix",
						"operator": 	"and"
					}
				}
			}
		}
	}`)
	res, aggs, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, aggs, structs.InitDefaultQueryAggregations())
	assert.IsType(t, res, &ASTNode{})

	assert.NotNil(t, res.AndFilterCondition.FilterCriteria)
	assert.Len(t, res.AndFilterCondition.FilterCriteria, 1)

	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "quick brown f.*")
	assert.Equal(t, res.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, Equals)

}

func Test_ParseRequest_match_multi_best_fields(t *testing.T) {
	json_body := []byte(`{
		"query": {
			"bool": {
				"must": {
					"multi_match": {
						"type": "best_fields",
						"query": "match words",
						"lenient": true
					}
				}
			}
		}
	}`)
	res, aggs, _, _, err := ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, aggs, structs.InitDefaultQueryAggregations())
	assert.IsType(t, res, &ASTNode{})

	log.Infof("result %+v", res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0])
	assert.NotNil(t, res.AndFilterCondition.NestedNodes)
	assert.Len(t, res.AndFilterCondition.NestedNodes, 1)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "*")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, Or)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("match"), []byte("words")})
	assert.NotEqual(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].MatchFilter.MatchType, MATCH_PHRASE)

	json_body = []byte(`{
		"query": {
			"bool": {
				"must": {
					"multi_match" : {
						"query":      "brown fox",
						"type":       "best_fields",
						"fields":     [ "subject", "message" ],
						"tie_breaker": 0.3
					  }
				}
			}
		}
	}`)
	res, aggs, _, _, err = ParseRequest(json_body, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, aggs, structs.InitDefaultQueryAggregations())
	assert.IsType(t, res, &ASTNode{})
	assert.NotNil(t, res.AndFilterCondition.NestedNodes)
	assert.Len(t, res.AndFilterCondition.NestedNodes, 1)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].MatchFilter.MatchColumn, "subject")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].MatchFilter.MatchOperator, Or)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].MatchFilter.MatchWords, [][]byte{[]byte("brown"), []byte("fox")})
	assert.NotEqual(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].MatchFilter.MatchType, MATCH_PHRASE)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].MatchFilter.MatchColumn, "message")
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].MatchFilter.MatchOperator, Or)
	assert.Equal(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].MatchFilter.MatchWords, [][]byte{[]byte("brown"), []byte("fox")})
	assert.NotEqual(t, res.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].MatchFilter.MatchType, MATCH_PHRASE)
}

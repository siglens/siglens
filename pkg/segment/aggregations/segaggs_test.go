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

package aggregations

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

type SimpleSearchExpr struct {
	Op             string
	Field          string
	Values         interface{}
	ValueIsRegex   bool
	ExprType       utils.SS_DTYPE
	DtypeEnclosure *utils.DtypeEnclosure
}

func Test_conditionMatch(t *testing.T) {
	tests := []struct {
		name         string
		fieldValue   interface{}
		op           string
		searchValue  interface{}
		expectedBool bool
	}{
		{"EqualStringTrue", "test", "=", "test", true},
		{"EqualStringFalse", "test", "=", "fail", false},
		{"NotEqualStringTrue", "test", "!=", "fail", true},
		{"NotEqualStringFalse", "test", "!=", "test", false},
		{"GreaterThanTrue", 10, ">", 5, true},
		{"GreaterThanFalse", 3, ">", 5, false},
		{"GreaterThanOrEqualTrue", 5, ">=", 5, true},
		{"GreaterThanOrEqualFalse", 3, ">=", 5, false},
		{"LessThanTrue", 3, "<", 5, true},
		{"LessThanFalse", 10, "<", 5, false},
		{"LessThanOrEqualTrue", 5, "<=", 5, true},
		{"LessThanOrEqualFalse", 10, "<=", 5, false},
		{"InvalidOperator", 10, "invalid", 5, false},
		{"InvalidFieldValue", "invalid", ">", 5, false},
		{"InvalidSearchValue", 5, ">", "invalid", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := conditionMatch(test.fieldValue, test.op, test.searchValue)
			assert.Equal(t, test.expectedBool, result)
		})
	}
}

var cities = []string{
	"Hyderabad",
	"New York",
	"Los Angeles",
	"Chicago",
	"Houston",
	"Phoenix",
	"Philadelphia",
	"San Antonio",
	"San Diego",
	"Dallas",
	"San Jose",
}

var countries = []string{
	"India",
	"United States",
	"Canada",
	"United Kingdom",
	"Australia",
	"Germany",
	"France",
	"Spain",
	"Italy",
	"Japan",
}

func generateTestRecords(numRecords int) map[string]map[string]interface{} {
	records := make(map[string]map[string]interface{}, numRecords)

	for i := 0; i < numRecords; i++ {
		record := make(map[string]interface{})

		record["timestamp"] = uint64(1659874108987)
		record["city"] = cities[rand.Intn(len(cities))]
		record["gender"] = []string{"male", "female"}[rand.Intn(2)]
		record["country"] = countries[rand.Intn(len(countries))]
		record["http_method"] = []string{"GET", "POST", "PUT", "DELETE"}[rand.Intn(4)]
		record["http_status"] = []int{200, 201, 301, 302, 404}[rand.Intn(5)]

		records[fmt.Sprint(i)] = record
	}

	return records
}

// Test Cases for processTransactionsOnRecords
func All_TestCasesForTransactionCommands() (map[int]bool, []*structs.TransactionArguments, map[int]map[string]interface{}) {
	matchesSomeRecords := make(map[int]bool)
	searchResults := make(map[int]map[string]interface{})

	// CASE 1: Only Fields
	txnArgs1 := &structs.TransactionArguments{
		Fields:     []string{"gender", "city"},
		StartsWith: nil,
		EndsWith:   nil,
	}
	matchesSomeRecords[1] = true

	// CASE 2: Only EndsWith
	txnArgs2 := &structs.TransactionArguments{
		EndsWith:   &structs.FilterStringExpr{StringValue: "DELETE"},
		StartsWith: &structs.FilterStringExpr{StringValue: "GET"},
		Fields:     []string{},
	}
	matchesSomeRecords[2] = true

	// CASE 3: Only StartsWith
	txnArgs3 := &structs.TransactionArguments{
		StartsWith: &structs.FilterStringExpr{StringValue: "GET"},
		EndsWith:   nil,
		Fields:     []string{},
	}
	matchesSomeRecords[3] = true

	// CASE 4: StartsWith and EndsWith
	txnArgs4 := &structs.TransactionArguments{
		StartsWith: &structs.FilterStringExpr{StringValue: "GET"},
		EndsWith:   &structs.FilterStringExpr{StringValue: "DELETE"},
		Fields:     []string{},
	}
	matchesSomeRecords[4] = true

	// CASE 5: StartsWith and EndsWith and one Field
	txnArgs5 := &structs.TransactionArguments{
		StartsWith: &structs.FilterStringExpr{StringValue: "GET"},
		EndsWith:   &structs.FilterStringExpr{StringValue: "DELETE"},
		Fields:     []string{"gender"},
	}
	matchesSomeRecords[5] = true

	// CASE 6: StartsWith and EndsWith and two Fields
	txnArgs6 := &structs.TransactionArguments{
		StartsWith: &structs.FilterStringExpr{StringValue: "GET"},
		EndsWith:   &structs.FilterStringExpr{StringValue: "DELETE"},
		Fields:     []string{"gender", "country"},
	}
	matchesSomeRecords[6] = true

	// CASE 7: StartsWith and EndsWith with String Clauses only OR: startswith=("GET" OR "POST1") endswith=("DELETE" OR "POST2")
	txnArgs7 := &structs.TransactionArguments{
		StartsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				OrFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							MatchFilter: &structs.MatchFilter{
								MatchColumn: "*",
								MatchWords: [][]byte{
									[]byte("GET"),
								},
								MatchOperator: utils.And,
								MatchPhrase:   []byte("GET"),
								MatchType:     structs.MATCH_PHRASE,
							},
						},
						{
							MatchFilter: &structs.MatchFilter{
								MatchColumn: "*",
								MatchWords: [][]byte{
									[]byte("POST1"),
								},
								MatchOperator: utils.And,
								MatchPhrase:   []byte("POST1"),
								MatchType:     structs.MATCH_PHRASE,
							},
						},
					},
				},
			},
		},
		EndsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				OrFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							MatchFilter: &structs.MatchFilter{
								MatchColumn: "*",
								MatchWords: [][]byte{
									[]byte("DELETE"),
								},
								MatchOperator: utils.And,
								MatchPhrase:   []byte("DELETE"),
								MatchType:     structs.MATCH_PHRASE,
							},
						},
						{
							MatchFilter: &structs.MatchFilter{
								MatchColumn: "*",
								MatchWords: [][]byte{
									[]byte("POST2"),
								},
								MatchOperator: utils.And,
								MatchPhrase:   []byte("POST2"),
								MatchType:     structs.MATCH_PHRASE,
							},
						},
					},
				},
			},
		},
		Fields: []string{"gender", "country"},
	}
	matchesSomeRecords[7] = true
	searchResults[7] = map[string]interface{}{
		"startswith": [][]*SimpleSearchExpr{
			{
				{
					Op:           "=",
					Field:        "http_method",
					Values:       "GET",
					ValueIsRegex: false,
					ExprType:     utils.SS_DT_STRING,
					DtypeEnclosure: &utils.DtypeEnclosure{
						Dtype:     utils.SS_DT_STRING,
						StringVal: "GET",
					},
				},
				{
					Op:           "=",
					Field:        "http_method",
					Values:       "POST1",
					ValueIsRegex: false,
					ExprType:     utils.SS_DT_STRING,
					DtypeEnclosure: &utils.DtypeEnclosure{
						Dtype:     utils.SS_DT_STRING,
						StringVal: "POST1",
					},
				},
			},
		},
		"endswith": [][]*SimpleSearchExpr{
			{
				{
					Op:           "=",
					Field:        "http_method",
					Values:       "DELETE",
					ValueIsRegex: false,
					ExprType:     utils.SS_DT_STRING,
					DtypeEnclosure: &utils.DtypeEnclosure{
						Dtype:     utils.SS_DT_STRING,
						StringVal: "DELETE",
					},
				},
				{
					Op:           "=",
					Field:        "http_method",
					Values:       "POST2",
					ValueIsRegex: false,
					ExprType:     utils.SS_DT_STRING,
					DtypeEnclosure: &utils.DtypeEnclosure{
						Dtype:     utils.SS_DT_STRING,
						StringVal: "POST2",
					},
				},
			},
		},
	}

	// CASE 8: StartsWith and EndsWith with String Clauses only AND (Negative Case): startswith=("GET" AND "POST2") endswith=("POST")
	txnArgs8 := &structs.TransactionArguments{
		StartsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				AndFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							MatchFilter: &structs.MatchFilter{
								MatchColumn: "*",
								MatchWords: [][]byte{
									[]byte("GET"),
								},
								MatchOperator: utils.And,
								MatchPhrase:   []byte("GET"),
								MatchType:     structs.MATCH_PHRASE,
							},
						},
						{
							MatchFilter: &structs.MatchFilter{
								MatchColumn: "*",
								MatchWords: [][]byte{
									[]byte("POST2"),
								},
								MatchOperator: utils.And,
								MatchPhrase:   []byte("POST2"),
								MatchType:     structs.MATCH_PHRASE,
							},
						},
					},
				},
			},
		},
		EndsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				AndFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							MatchFilter: &structs.MatchFilter{
								MatchColumn: "*",
								MatchWords: [][]byte{
									[]byte("DELETE"),
								},
								MatchOperator: utils.And,
								MatchPhrase:   []byte("DELETE"),
								MatchType:     structs.MATCH_PHRASE,
							},
						},
					},
				},
			},
		},
		Fields: []string{"gender", "country"},
	}
	matchesSomeRecords[8] = false

	// CASE 9: StartsWith and EndsWith with String Clauses only AND (Positive Case): startswith=("GET" AND "male") endswith=("DELETE")
	txnArgs9 := &structs.TransactionArguments{
		Fields: []string{"gender", "country"},
		StartsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				AndFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							MatchFilter: &structs.MatchFilter{
								MatchColumn: "*",
								MatchWords: [][]byte{
									[]byte("GET"),
								},
								MatchOperator: utils.And,
								MatchPhrase:   []byte("GET"),
								MatchType:     structs.MATCH_PHRASE,
							},
						},
						{
							MatchFilter: &structs.MatchFilter{
								MatchColumn: "*",
								MatchWords: [][]byte{
									[]byte("male"),
								},
								MatchOperator: utils.And,
								MatchPhrase:   []byte("male"),
								MatchType:     structs.MATCH_PHRASE,
							},
						},
					},
				},
			},
		},
		EndsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				AndFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							MatchFilter: &structs.MatchFilter{
								MatchColumn: "*",
								MatchWords: [][]byte{
									[]byte("DELETE"),
								},
								MatchOperator: utils.And,
								MatchPhrase:   []byte("DELETE"),
								MatchType:     structs.MATCH_PHRASE,
							},
						},
					},
				},
			},
		},
	}
	matchesSomeRecords[9] = true
	searchResults[9] = map[string]interface{}{
		"startswith": [][]*SimpleSearchExpr{
			{
				{
					Op:           "=",
					Field:        "http_method",
					Values:       "GET",
					ValueIsRegex: false,
					ExprType:     utils.SS_DT_STRING,
					DtypeEnclosure: &utils.DtypeEnclosure{
						Dtype:          utils.SS_DT_STRING,
						StringVal:      "GET",
						StringValBytes: []byte("GET"),
					},
				},
			},
			{
				{
					Op:           "=",
					Field:        "gender",
					Values:       "male",
					ValueIsRegex: false,
					ExprType:     utils.SS_DT_STRING,
					DtypeEnclosure: &utils.DtypeEnclosure{
						Dtype:          utils.SS_DT_STRING,
						StringVal:      "male",
						StringValBytes: []byte("male"),
					},
				},
			},
		},
		"endswith": [][]*SimpleSearchExpr{
			{
				{
					Op:           "=",
					Field:        "http_method",
					Values:       "DELETE",
					ValueIsRegex: false,
					ExprType:     utils.SS_DT_STRING,
					DtypeEnclosure: &utils.DtypeEnclosure{
						Dtype:     utils.SS_DT_STRING,
						StringVal: "DELETE",
					},
				},
			},
		},
	}

	// CASE 10: StartsWith is a Valid Search Expr and EndsWith is String Value: startswith=status>=300 endswith="DELETE"
	txnArgs10 := &structs.TransactionArguments{
		StartsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				AndFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							ExpressionFilter: &structs.ExpressionFilter{
								LeftInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: nil,
											ColumnName:  "http_status",
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
								FilterOperator: utils.GreaterThanOrEqualTo,
								RightInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: &utils.DtypeEnclosure{
												Dtype:       utils.SS_DT_UNSIGNED_NUM,
												UnsignedVal: uint64(300),
												SignedVal:   int64(300),
												FloatVal:    float64(300),
												StringVal:   "300",
											},
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
							},
						},
					},
				},
			},
		},
		EndsWith: &structs.FilterStringExpr{StringValue: "DELETE"},
	}
	matchesSomeRecords[10] = true
	searchResults[10] = map[string]interface{}{
		"startswith": [][]*SimpleSearchExpr{
			{
				{
					Op:           ">=",
					Field:        "http_status",
					Values:       json.Number("300"),
					ValueIsRegex: false,
					ExprType:     utils.SS_DT_SIGNED_NUM,
					DtypeEnclosure: &utils.DtypeEnclosure{
						Dtype:       utils.SS_DT_SIGNED_NUM,
						FloatVal:    float64(300),
						UnsignedVal: uint64(300),
						SignedVal:   int64(300),
						StringVal:   "300",
					},
				},
			},
		},
	}

	// CASE 11: StartsWith is not a Valid Search Term (comparing between two string fields) and EndsWith is String value: startswith=city>"Hyderabad" endswith="DELETE"
	txnArgs11 := &structs.TransactionArguments{
		StartsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				AndFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							ExpressionFilter: &structs.ExpressionFilter{
								LeftInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: nil,
											ColumnName:  "city",
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
								FilterOperator: utils.GreaterThan,
								RightInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: &utils.DtypeEnclosure{
												Dtype:     utils.SS_DT_STRING,
												StringVal: "Hyderabad",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		EndsWith: &structs.FilterStringExpr{StringValue: "DELETE"},
	}
	matchesSomeRecords[11] = false

	// CASE 12: StartsWith is not a Valid Search Term (comparing between string and number fields) and EndsWith is String Clause: startswith=city>300 endswith=("DELETE")
	txnArgs12 := &structs.TransactionArguments{
		StartsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				AndFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							ExpressionFilter: &structs.ExpressionFilter{
								LeftInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: nil,
											ColumnName:  "city",
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
								FilterOperator: utils.GreaterThan,
								RightInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: &utils.DtypeEnclosure{
												Dtype:       utils.SS_DT_UNSIGNED_NUM,
												StringVal:   "300",
												UnsignedVal: uint64(300),
												SignedVal:   int64(300),
												FloatVal:    float64(300),
											},
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
							},
						},
					},
				},
			},
		},
		EndsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				AndFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							MatchFilter: &structs.MatchFilter{
								MatchColumn: "*",
								MatchWords: [][]byte{
									[]byte("DELETE"),
								},
								MatchOperator: utils.And,
								MatchPhrase:   []byte("DELETE"),
								MatchType:     structs.MATCH_PHRASE,
							},
						},
					},
				},
			},
		},
	}
	matchesSomeRecords[12] = false

	// CASE 13: StartsWith is a Valid Search Term (String1 = String2) and EndsWith is String Value: startswith=city="Hyderabad" endswith="DELETE"
	txnArgs13 := &structs.TransactionArguments{
		StartsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				AndFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							ExpressionFilter: &structs.ExpressionFilter{
								LeftInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: nil,
											ColumnName:  "city",
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
								FilterOperator: utils.Equals,
								RightInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: &utils.DtypeEnclosure{
												Dtype:     utils.SS_DT_STRING,
												StringVal: "Hyderabad",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		EndsWith: &structs.FilterStringExpr{StringValue: "DELETE"},
	}
	matchesSomeRecords[13] = true
	searchResults[13] = map[string]interface{}{
		"startswith": [][]*SimpleSearchExpr{
			{
				{
					Op:           "=",
					Field:        "city",
					Values:       "Hyderabad",
					ValueIsRegex: false,
					ExprType:     utils.SS_DT_STRING,
					DtypeEnclosure: &utils.DtypeEnclosure{
						Dtype:     utils.SS_DT_STRING,
						StringVal: "Hyderabad",
					},
				},
			},
		},
	}

	// CASE 14: Eval Expression:  transaction gender startswith=eval(status > 300 AND http_method="POST" OR http_method="PUT")
	txnArgs14 := &structs.TransactionArguments{
		Fields: []string{"gender"},
		StartsWith: &structs.FilterStringExpr{
			EvalBoolExpr: &structs.BoolExpr{
				IsTerminal: false,
				LeftBool: &structs.BoolExpr{
					IsTerminal: true,
					LeftValue: &structs.ValueExpr{
						ValueExprMode: structs.VEMNumericExpr,
						NumericExpr: &structs.NumericExpr{
							NumericExprMode: structs.NEMNumberField,
							IsTerminal:      true,
							ValueIsField:    true,
							Value:           "http_status",
						},
					},
					RightValue: &structs.ValueExpr{
						NumericExpr: &structs.NumericExpr{
							NumericExprMode: structs.NEMNumber,
							IsTerminal:      true,
							ValueIsField:    false,
							Value:           "300",
						},
					},
					ValueOp: ">",
				},
				RightBool: &structs.BoolExpr{
					IsTerminal: false,
					LeftBool: &structs.BoolExpr{
						IsTerminal: true,
						LeftValue: &structs.ValueExpr{
							ValueExprMode: structs.VEMNumericExpr,
							NumericExpr: &structs.NumericExpr{
								NumericExprMode: structs.NEMNumberField,
								IsTerminal:      true,
								ValueIsField:    true,
								Value:           "http_method",
							},
						},
						RightValue: &structs.ValueExpr{
							ValueExprMode: structs.VEMStringExpr,
							StringExpr: &structs.StringExpr{
								StringExprMode: structs.SEMRawString,
								RawString:      "POST",
							},
						},
						ValueOp: "=",
					},
					RightBool: &structs.BoolExpr{
						IsTerminal: true,
						LeftValue: &structs.ValueExpr{
							ValueExprMode: structs.VEMNumericExpr,
							NumericExpr: &structs.NumericExpr{
								NumericExprMode: structs.NEMNumberField,
								IsTerminal:      true,
								ValueIsField:    true,
								Value:           "http_method",
							},
						},
						RightValue: &structs.ValueExpr{
							ValueExprMode: structs.VEMStringExpr,
							StringExpr: &structs.StringExpr{
								StringExprMode: structs.SEMRawString,
								RawString:      "PUT",
							},
						},
						ValueOp: "=",
					},
					BoolOp: structs.BoolOpOr,
				},
				BoolOp: structs.BoolOpAnd,
			},
		},
	}
	matchesSomeRecords[14] = true
	searchResults[14] = map[string]interface{}{
		"startswith": [][]*SimpleSearchExpr{
			{
				{
					Op:           ">",
					Field:        "http_status",
					Values:       json.Number("300"),
					ValueIsRegex: false,
					ExprType:     utils.SS_DT_SIGNED_NUM,
					DtypeEnclosure: &utils.DtypeEnclosure{
						Dtype:       utils.SS_DT_SIGNED_NUM,
						FloatVal:    float64(300),
						UnsignedVal: uint64(300),
						SignedVal:   int64(300),
						StringVal:   "300",
					},
				},
			},
			{
				{
					Op:           "=",
					Field:        "http_method",
					Values:       "POST",
					ValueIsRegex: false,
					ExprType:     utils.SS_DT_STRING,
					DtypeEnclosure: &utils.DtypeEnclosure{
						Dtype:     utils.SS_DT_STRING,
						StringVal: "POST",
					},
				},
				{
					Op:           "=",
					Field:        "http_method",
					Values:       "PUT",
					ValueIsRegex: false,
					ExprType:     utils.SS_DT_STRING,
					DtypeEnclosure: &utils.DtypeEnclosure{
						Dtype:     utils.SS_DT_STRING,
						StringVal: "PUT",
					},
				},
			},
		},
	}

	// CASE 15: String Search Expr: transaction gender startswith="status>300 OR status=201 AND http_method=POST" endswith=eval(status<400)
	txnArgs15 := &structs.TransactionArguments{
		Fields: []string{"gender"},
		StartsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				AndFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							ExpressionFilter: &structs.ExpressionFilter{
								LeftInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: nil,
											ColumnName:  "http_method",
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
								RightInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: &utils.DtypeEnclosure{
												Dtype:     utils.SS_DT_STRING,
												StringVal: "POST",
											},
											ColumnName: "",
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
								FilterOperator: utils.Equals,
							},
						},
					},
					NestedNodes: []*structs.ASTNode{
						{
							OrFilterCondition: &structs.Condition{
								FilterCriteria: []*structs.FilterCriteria{
									{
										ExpressionFilter: &structs.ExpressionFilter{
											LeftInput: &structs.FilterInput{
												Expression: &structs.Expression{
													LeftInput: &structs.ExpressionInput{
														ColumnValue: nil,
														ColumnName:  "http_status",
													},
													ExpressionOp: utils.Add,
													RightInput:   nil,
												},
											},
											RightInput: &structs.FilterInput{
												Expression: &structs.Expression{
													LeftInput: &structs.ExpressionInput{
														ColumnValue: &utils.DtypeEnclosure{
															Dtype:       utils.SS_DT_UNSIGNED_NUM,
															UnsignedVal: uint64(300),
															SignedVal:   int64(300),
															FloatVal:    float64(300),
															StringVal:   "300",
														},
														ColumnName: "",
													},
													ExpressionOp: utils.Add,
													RightInput:   nil,
												},
											},
											FilterOperator: utils.GreaterThan,
										},
									},
									{
										ExpressionFilter: &structs.ExpressionFilter{
											LeftInput: &structs.FilterInput{
												Expression: &structs.Expression{
													LeftInput: &structs.ExpressionInput{
														ColumnValue: nil,
														ColumnName:  "http_status",
													},
													ExpressionOp: utils.Add,
													RightInput:   nil,
												},
											},
											RightInput: &structs.FilterInput{
												Expression: &structs.Expression{
													LeftInput: &structs.ExpressionInput{
														ColumnValue: &utils.DtypeEnclosure{
															Dtype:       utils.SS_DT_UNSIGNED_NUM,
															UnsignedVal: uint64(201),
															SignedVal:   int64(201),
															FloatVal:    float64(201),
															StringVal:   "201",
														},
														ColumnName: "",
													},
													ExpressionOp: utils.Add,
													RightInput:   nil,
												},
											},
											FilterOperator: utils.Equals,
										},
									},
								},
								NestedNodes: nil,
							},
						},
					},
				},
			},
		},
		EndsWith: &structs.FilterStringExpr{
			EvalBoolExpr: &structs.BoolExpr{
				IsTerminal: true,
				LeftValue: &structs.ValueExpr{
					NumericExpr: &structs.NumericExpr{
						IsTerminal:      true,
						NumericExprMode: structs.NEMNumberField,
						ValueIsField:    true,
						Value:           "http_status",
					},
				},
				RightValue: &structs.ValueExpr{
					NumericExpr: &structs.NumericExpr{
						IsTerminal:      true,
						NumericExprMode: structs.NEMNumber,
						ValueIsField:    false,
						Value:           "400",
					},
				},
				ValueOp: "<",
			},
		},
	}
	matchesSomeRecords[15] = true
	searchResults[15] = map[string]interface{}{
		"endswith": [][]*SimpleSearchExpr{
			{
				{
					Op:           "<",
					Field:        "http_status",
					Values:       json.Number("400"),
					ValueIsRegex: false,
					ExprType:     utils.SS_DT_SIGNED_NUM,
					DtypeEnclosure: &utils.DtypeEnclosure{
						Dtype:       utils.SS_DT_SIGNED_NUM,
						FloatVal:    float64(400),
						UnsignedVal: uint64(400),
						SignedVal:   int64(400),
						StringVal:   "400",
					},
				},
			},
		},
		"startswith": [][]*SimpleSearchExpr{
			{
				{
					Op:           ">",
					Field:        "http_status",
					Values:       json.Number("300"),
					ValueIsRegex: false,
					ExprType:     utils.SS_DT_SIGNED_NUM,
					DtypeEnclosure: &utils.DtypeEnclosure{
						Dtype:       utils.SS_DT_SIGNED_NUM,
						FloatVal:    float64(300),
						UnsignedVal: uint64(300),
						SignedVal:   int64(300),
						StringVal:   "300",
					},
				},
				{
					Op:           "=",
					Field:        "http_status",
					Values:       json.Number("201"),
					ValueIsRegex: false,
					ExprType:     utils.SS_DT_SIGNED_NUM,
					DtypeEnclosure: &utils.DtypeEnclosure{
						Dtype:       utils.SS_DT_SIGNED_NUM,
						FloatVal:    float64(201),
						UnsignedVal: uint64(201),
						SignedVal:   int64(201),
						StringVal:   "201",
					},
				},
			},
			{
				{
					Op:           "=",
					Field:        "http_method",
					Values:       "POST",
					ValueIsRegex: false,
					ExprType:     utils.SS_DT_STRING,
					DtypeEnclosure: &utils.DtypeEnclosure{
						Dtype:     utils.SS_DT_STRING,
						StringVal: "POST",
					},
				},
			},
		},
	}

	// CASE 16: String Search Expr: transaction city startswith="status>300 OR status=201" endswith=eval(status<400)
	txnArgs16 := &structs.TransactionArguments{
		Fields: []string{"city"},
		StartsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				OrFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							ExpressionFilter: &structs.ExpressionFilter{
								LeftInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: nil,
											ColumnName:  "http_status",
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
								RightInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: &utils.DtypeEnclosure{
												Dtype:       utils.SS_DT_UNSIGNED_NUM,
												UnsignedVal: uint64(300),
												SignedVal:   int64(300),
												FloatVal:    float64(300),
												StringVal:   "300",
											},
											ColumnName: "",
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
								FilterOperator: utils.GreaterThan,
							},
						},
						{
							ExpressionFilter: &structs.ExpressionFilter{
								LeftInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: nil,
											ColumnName:  "http_status",
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
								RightInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: &utils.DtypeEnclosure{
												Dtype:       utils.SS_DT_UNSIGNED_NUM,
												UnsignedVal: uint64(201),
												SignedVal:   int64(201),
												FloatVal:    float64(201),
												StringVal:   "201",
											},
											ColumnName: "",
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
								FilterOperator: utils.Equals,
							},
						},
					},
					NestedNodes: nil,
				},
			},
		},
		EndsWith: &structs.FilterStringExpr{
			EvalBoolExpr: &structs.BoolExpr{
				IsTerminal: true,
				LeftValue: &structs.ValueExpr{
					NumericExpr: &structs.NumericExpr{
						IsTerminal:      true,
						NumericExprMode: structs.NEMNumberField,
						ValueIsField:    true,
						Value:           "http_status",
					},
				},
				RightValue: &structs.ValueExpr{
					NumericExpr: &structs.NumericExpr{
						IsTerminal:      true,
						NumericExprMode: structs.NEMNumber,
						ValueIsField:    false,
						Value:           "400",
					},
				},
				ValueOp: "<",
			},
		},
	}
	matchesSomeRecords[16] = true
	searchResults[16] = map[string]interface{}{
		"endswith": [][]*SimpleSearchExpr{
			{
				{
					Op:           "<",
					Field:        "http_status",
					Values:       json.Number("400"),
					ValueIsRegex: false,
					ExprType:     utils.SS_DT_SIGNED_NUM,
					DtypeEnclosure: &utils.DtypeEnclosure{
						Dtype:       utils.SS_DT_SIGNED_NUM,
						FloatVal:    float64(400),
						UnsignedVal: uint64(400),
						SignedVal:   int64(400),
						StringVal:   "400",
					},
				},
			},
		},
		"startswith": [][]*SimpleSearchExpr{
			{
				{
					Op:           ">",
					Field:        "http_status",
					Values:       json.Number("300"),
					ValueIsRegex: false,
					ExprType:     utils.SS_DT_SIGNED_NUM,
					DtypeEnclosure: &utils.DtypeEnclosure{
						Dtype:       utils.SS_DT_SIGNED_NUM,
						FloatVal:    float64(300),
						UnsignedVal: uint64(300),
						SignedVal:   int64(300),
						StringVal:   "300",
					},
				},
				{
					Op:           "=",
					Field:        "http_status",
					Values:       json.Number("201"),
					ValueIsRegex: false,
					ExprType:     utils.SS_DT_SIGNED_NUM,
					DtypeEnclosure: &utils.DtypeEnclosure{
						Dtype:       utils.SS_DT_SIGNED_NUM,
						FloatVal:    float64(201),
						UnsignedVal: uint64(201),
						SignedVal:   int64(201),
						StringVal:   "201",
					},
				},
			},
		},
	}

	return matchesSomeRecords, []*structs.TransactionArguments{txnArgs1, txnArgs2, txnArgs3, txnArgs4, txnArgs5, txnArgs6, txnArgs7, txnArgs8, txnArgs9,
		txnArgs10, txnArgs11, txnArgs12, txnArgs13, txnArgs14, txnArgs15, txnArgs16}, searchResults
}

func Test_processTransactionsOnRecords(t *testing.T) {

	allCols := map[string]bool{"city": true, "gender": true}

	matchesSomeRecords, allCasesTxnArgs, searchResults := All_TestCasesForTransactionCommands()

	for index, txnArgs := range allCasesTxnArgs {
		records := generateTestRecords(500)
		// Process Transactions
		performTransactionCommandRequest(&structs.NodeResult{}, &structs.QueryAggregators{TransactionArguments: txnArgs}, records, allCols)

		assert.Equal(t, allCols, map[string]bool{"timestamp": true, "duration": true, "eventcount": true, "event": true})

		// Check if the number of records is positive or negative
		assert.Equal(t, matchesSomeRecords[index+1], len(records) > 0)

		for _, record := range records {
			assert.Equal(t, record["timestamp"], uint64(1659874108987))
			assert.Equal(t, record["duration"], uint64(0))

			events := record["event"].([]map[string]interface{})

			initFields := []string{}

			for ind, eventMap := range events {
				fields := []string{}

				for _, field := range txnArgs.Fields {
					fields = append(fields, eventMap[field].(string))
				}

				// Check if the fields are same for all events by assigning the first event's fields to initFields
				if ind == 0 {
					initFields = fields
				}

				assert.Equal(t, fields, initFields)

				if txnArgs.StartsWith != nil {
					if ind == 0 {
						resultData, exists := getResultData(index+1, "startswith", searchResults)
						if txnArgs.StartsWith.StringValue != "" {
							assert.Equal(t, eventMap["http_method"], txnArgs.StartsWith.StringValue)
						} else if txnArgs.StartsWith.EvalBoolExpr != nil {
							if exists {
								valid := validateSearchExpr(eventMap, resultData.([][]*SimpleSearchExpr))
								assert.True(t, valid)
							}
						} else if txnArgs.StartsWith.SearchNode != nil {
							if exists {
								valid := validateSearchExpr(eventMap, resultData.([][]*SimpleSearchExpr))
								assert.True(t, valid)
							}
						}
					}
				}

				if txnArgs.EndsWith != nil {
					if ind == len(events)-1 {
						resultData, exists := getResultData(index+1, "endswith", searchResults)
						if txnArgs.EndsWith.StringValue != "" {
							assert.Equal(t, eventMap["http_method"], txnArgs.EndsWith.StringValue)
						} else if txnArgs.EndsWith.EvalBoolExpr != nil {
							if exists {
								valid := validateSearchExpr(eventMap, resultData.([][]*SimpleSearchExpr))
								assert.True(t, valid)
							}
						} else if txnArgs.EndsWith.SearchNode != nil {
							if exists {
								valid := validateSearchExpr(eventMap, resultData.([][]*SimpleSearchExpr))
								assert.True(t, valid)
							}
						}
					}
				}

			}
		}
	}

}

func getResultData(resultIndex int, resultType string, resultData map[int]map[string]interface{}) (interface{}, bool) {
	resultDataMap, exists := resultData[resultIndex]
	if exists {
		data, exists := resultDataMap[resultType]
		return data, exists
	} else {
		return nil, false
	}
}

func validateSearchString(searchTerm *SimpleSearchExpr, eventMap map[string]interface{}) bool {
	fieldValue, exists := eventMap[searchTerm.Field]
	if !exists {
		return false
	}

	return conditionMatch(fieldValue, searchTerm.Op, searchTerm.Values)
}

func validateSearchExpr(eventMap map[string]interface{}, resultData [][]*SimpleSearchExpr) bool {
	for _, resultAnd := range resultData {
		valid := false
		for _, resultOr := range resultAnd {
			if validateSearchString(resultOr, eventMap) {
				valid = true
				break
			}
		}
		if !valid {
			return false
		}
	}
	return true
}

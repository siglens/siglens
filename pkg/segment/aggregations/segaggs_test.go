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

package aggregations

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
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
		record["http_status"] = []int64{200, 201, 301, 302, 404}[rand.Intn(5)]
		record["latitude"] = rand.Float64() * 180
		record["longitude"] = rand.Float64() * 180

		records[fmt.Sprint(i)] = record
	}

	return records
}

func getFinalColsForGeneratedTestRecords() map[string]bool {
	return map[string]bool{"timestamp": true, "city": true, "gender": true, "country": true, "http_method": true, "http_status": true, "latitude": true, "longitude": true}
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
		performTransactionCommandRequest(&structs.NodeResult{}, &structs.QueryAggregators{TransactionArguments: txnArgs}, records, allCols, 1, true)

		expectedCols := map[string]bool{"duration": true, "event": true, "eventcount": true, "timestamp": true}

		for _, field := range txnArgs.Fields {
			expectedCols[field] = true
		}

		assert.Equal(t, expectedCols, allCols)

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

func Test_performValueColRequestWithoutGroupBy_VEMNumericExpr(t *testing.T) {

	// Query 1: * | eval rlatitude=round(latitude, 2)
	letColReq := &structs.LetColumnsRequest{
		ValueColRequest: &structs.ValueExpr{
			ValueExprMode: structs.VEMNumericExpr,
			NumericExpr: &structs.NumericExpr{
				NumericExprMode: structs.NEMNumericExpr,
				Op:              "round",
				Left: &structs.NumericExpr{
					NumericExprMode: structs.NEMNumberField,
					IsTerminal:      true,
					ValueIsField:    true,
					Value:           "latitude",
				},
				Right: &structs.NumericExpr{
					NumericExprMode: structs.NEMNumber,
					IsTerminal:      true,
					ValueIsField:    false,
					Value:           "2",
				},
			},
		},
		NewColName: "rlatitude",
	}

	records := generateTestRecords(500)
	finalCols := getFinalColsForGeneratedTestRecords()

	// Perform the value column request
	err := performValueColRequest(&structs.NodeResult{}, &structs.QueryAggregators{}, letColReq, records, finalCols)
	assert.Nil(t, err)

	// Check if the new column is added to the records
	assert.True(t, finalCols["rlatitude"])

	for _, record := range records {
		assert.True(t, record["rlatitude"] != nil)
		valueStr := fmt.Sprintf("%.2f", record["latitude"].(float64))
		splitValue := strings.Split(valueStr, ".")
		if len(splitValue) > 1 {
			assert.Equal(t, len(splitValue[1]), 2)
		}
	}

}

func Test_performValueColRequestWithoutGroupBy_VEMConditionExpr(t *testing.T) {
	// Query: * |  eval http_status_mod=if(in(http_status, 400,  500), "Failure", http_status)
	letColReq := &structs.LetColumnsRequest{
		ValueColRequest: &structs.ValueExpr{
			ValueExprMode: structs.VEMConditionExpr,
			ConditionExpr: &structs.ConditionExpr{
				Op: "if",
				BoolExpr: &structs.BoolExpr{
					IsTerminal: true,
					LeftValue: &structs.ValueExpr{
						NumericExpr: &structs.NumericExpr{
							NumericExprMode: structs.NEMNumberField,
							IsTerminal:      true,
							ValueIsField:    true,
							Value:           "http_status",
						},
					},
					ValueOp: "in",
					ValueList: []*structs.ValueExpr{
						{
							NumericExpr: &structs.NumericExpr{
								NumericExprMode: structs.NEMNumber,
								IsTerminal:      true,
								ValueIsField:    false,
								Value:           "400",
							},
						},
						{
							NumericExpr: &structs.NumericExpr{
								NumericExprMode: structs.NEMNumber,
								IsTerminal:      true,
								ValueIsField:    false,
								Value:           "500",
							},
						},
					},
				},
				TrueValue: &structs.ValueExpr{
					ValueExprMode: structs.VEMStringExpr,
					StringExpr: &structs.StringExpr{
						RawString: "Failure",
					},
				},
				FalseValue: &structs.ValueExpr{
					NumericExpr: &structs.NumericExpr{
						NumericExprMode: structs.NEMNumberField,
						IsTerminal:      true,
						ValueIsField:    true,
						Value:           "http_status",
					},
				},
			},
		},
		NewColName: "http_status_mod",
	}

	records := generateTestRecords(500)
	finalCols := getFinalColsForGeneratedTestRecords()

	// Perform the value column request
	err := performValueColRequest(&structs.NodeResult{}, &structs.QueryAggregators{}, letColReq, records, finalCols)
	assert.Nil(t, err)

	// Check if the new column is added to the records
	assert.True(t, finalCols["http_status_mod"])

	for _, record := range records {
		assert.True(t, record["http_status_mod"] != nil)
		httpStatus := record["http_status"].(int64)
		if httpStatus == 400 || httpStatus == 500 {
			assert.Equal(t, "Failure", record["http_status_mod"])
		} else {
			assert.Equal(t, fmt.Sprint(httpStatus), record["http_status_mod"])
		}
	}
}

func Test_performValueColRequestWithoutGroupBy_VEMStringExpr(t *testing.T) {
	// Query: * | eval country_city=country.":-".city
	letColReq := &structs.LetColumnsRequest{
		ValueColRequest: &structs.ValueExpr{
			ValueExprMode: structs.VEMStringExpr,
			StringExpr: &structs.StringExpr{
				StringExprMode: structs.SEMConcatExpr,
				ConcatExpr: &structs.ConcatExpr{
					Atoms: []*structs.ConcatAtom{
						{
							IsField: true,
							Value:   "country",
						},
						{
							IsField: false,
							Value:   ":-",
						},
						{
							IsField: true,
							Value:   "city",
						},
					},
				},
			},
		},
		NewColName: "country_city",
	}

	records := generateTestRecords(500)
	finalCols := getFinalColsForGeneratedTestRecords()

	// Perform the value column request
	err := performValueColRequest(&structs.NodeResult{}, &structs.QueryAggregators{}, letColReq, records, finalCols)

	assert.Nil(t, err)

	// Check if the new column is added to the records
	assert.True(t, finalCols["country_city"])

	for _, record := range records {
		assert.True(t, record["country_city"] != nil)
		country := record["country"].(string)
		city := record["city"].(string)
		assert.Equal(t, country+":-"+city, record["country_city"])
	}
}

func Test_getColumnsToKeepAndRemove(t *testing.T) {
	tests := []struct {
		name             string
		cols             []string
		wildcardCols     []string
		keepMatches      bool
		wantIndices      []int
		wantColsToKeep   []string
		wantColsToRemove []string
	}{
		{
			name:             "No wildcards, keepMatches true",
			cols:             []string{"id", "name", "email"},
			wildcardCols:     []string{},
			keepMatches:      true,
			wantIndices:      []int{},
			wantColsToKeep:   []string{},
			wantColsToRemove: []string{"id", "name", "email"},
		},
		{
			name:             "No wildcards, keepMatches false",
			cols:             []string{"id", "name", "email"},
			wildcardCols:     []string{},
			keepMatches:      false,
			wantIndices:      []int{0, 1, 2},
			wantColsToKeep:   []string{"id", "name", "email"},
			wantColsToRemove: []string{},
		},
		{
			name:             "Exact match one wildcard, keepMatches true",
			cols:             []string{"id", "name", "email"},
			wildcardCols:     []string{"name"},
			keepMatches:      true,
			wantIndices:      []int{1},
			wantColsToKeep:   []string{"name"},
			wantColsToRemove: []string{"id", "email"},
		},
		{
			name:             "Wildcard matches multiple columns, keepMatches true",
			cols:             []string{"user_id", "username", "user_email", "age"},
			wildcardCols:     []string{"user_*"},
			keepMatches:      true,
			wantIndices:      []int{0, 2},
			wantColsToKeep:   []string{"user_id", "user_email"},
			wantColsToRemove: []string{"username", "age"},
		},
		{
			name:             "Wildcard matches none, keepMatches false",
			cols:             []string{"id", "name", "email"},
			wildcardCols:     []string{"user_*"},
			keepMatches:      false,
			wantIndices:      []int{0, 1, 2},
			wantColsToKeep:   []string{"id", "name", "email"},
			wantColsToRemove: []string{},
		},
		{
			name:             "Multiple wildcards with overlaps, keepMatches true",
			cols:             []string{"user_id", "admin_id", "username", "email"},
			wildcardCols:     []string{"user_*", "*_id"},
			keepMatches:      true,
			wantIndices:      []int{0, 1},
			wantColsToKeep:   []string{"user_id", "admin_id"},
			wantColsToRemove: []string{"username", "email"},
		},
		{
			name:             "Empty cols, keepMatches true",
			cols:             []string{},
			wildcardCols:     []string{"user_*"},
			keepMatches:      true,
			wantIndices:      []int{},
			wantColsToKeep:   []string{},
			wantColsToRemove: []string{},
		},
		{
			name:             "Wildcard matches all, keepMatches false",
			cols:             []string{"user_id", "user_name", "user_email"},
			wildcardCols:     []string{"user_*"},
			keepMatches:      false,
			wantIndices:      []int{},
			wantColsToKeep:   []string{},
			wantColsToRemove: []string{"user_id", "user_name", "user_email"},
		},
		{
			name:             "Complex wildcards, partial matches",
			cols:             []string{"user_id", "admin_id", "username", "user_profile", "user_email", "age"},
			wildcardCols:     []string{"user_*", "*name"},
			keepMatches:      true,
			wantIndices:      []int{0, 2, 3, 4},
			wantColsToKeep:   []string{"user_id", "username", "user_profile", "user_email"},
			wantColsToRemove: []string{"admin_id", "age"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIndices, gotColsToKeep, gotColsToRemove := getColumnsToKeepAndRemove(tt.cols, tt.wildcardCols, tt.keepMatches)

			assert.Equal(t, tt.wantIndices, gotIndices)
			assert.Equal(t, tt.wantColsToKeep, gotColsToKeep)
			assert.Equal(t, tt.wantColsToRemove, gotColsToRemove)
		})
	}
}

func Test_performArithmeticOperation_Addition(t *testing.T) {
	tests := []struct {
		name    string
		left    interface{}
		right   interface{}
		want    interface{}
		wantErr bool
		errMsg  string
	}{
		{
			name:  "Addition of numbers",
			left:  5,
			right: 3,
			want:  float64(8),
		},
		{
			name:  "Add a string and a number",
			left:  2,
			right: "3",
			want:  float64(5),
		},
		{
			name:  "Concatenation of strings",
			left:  "Hello, ",
			right: "World!",
			want:  "Hello, World!",
		},
		{
			name:    "Adding a string and a number",
			left:    "Hello, ",
			right:   3,
			wantErr: true,
			errMsg:  "rightValue is not a string",
		},
		{
			name:    "Adding a number and a string",
			left:    3,
			right:   "World!",
			wantErr: true,
			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := performArithmeticOperation(tt.left, tt.right, utils.Add)
			if !tt.wantErr {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
			if tt.wantErr {
				assert.Equal(t, tt.errMsg, err.Error())
			}
		})
	}
}

func Test_performArithmeticOperation_Subtraction(t *testing.T) {
	tests := []struct {
		name    string
		left    interface{}
		right   interface{}
		want    float64
		wantErr bool
		errMsg  string
	}{
		{
			name:  "10-5",
			left:  10,
			right: 5,
			want:  5,
		},
		{
			name:  "-5-(-2)",
			left:  -5,
			right: -2,
			want:  -3,
		},
		{
			name:  "5.5-2.2",
			left:  5.5,
			right: 2.2,
			want:  3.3,
		},
		{
			name:  "0-5",
			left:  0,
			right: 5,
			want:  -5,
		},
		{
			name:  "Subtracting a string(number) from a string(number)",
			left:  "2",
			right: "5",
			want:  -3,
		},
		{
			name:  "Subtracting a number from a string(number)",
			left:  "2",
			right: 5,
			want:  -3,
		},
		{
			name:  "Subtracting a string from a number",
			left:  5,
			right: "2",
			want:  3,
		},
		{
			name:    "Subtracting a number from a string",
			left:    "Hello,",
			right:   5,
			wantErr: true,
			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
		},
		{
			name:    "Subtracting a string from a number",
			left:    5,
			right:   "World!",
			wantErr: true,
			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
		},
		{
			name:    "Subtracting a string from a string",
			left:    "Hello,",
			right:   "World!",
			wantErr: true,
			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := performArithmeticOperation(tt.left, tt.right, utils.Subtract)
			if !tt.wantErr {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
			if tt.wantErr {
				assert.Equal(t, tt.errMsg, err.Error())
			}
		})
	}
}

func Test_performArithmeticOperation_Multiplication(t *testing.T) {
	tests := []struct {
		name    string
		left    interface{}
		right   interface{}
		want    float64
		wantErr bool
		errMsg  string
	}{
		{
			name:  "3*4",
			left:  3,
			right: 4,
			want:  12,
		},
		{
			name:  "-2*3",
			left:  -2,
			right: 3,
			want:  -6,
		},
		{
			name:  "5.5*2",
			left:  5.5,
			right: 2,
			want:  11,
		},
		{
			name:  "2*(-5)",
			left:  2,
			right: -5,
			want:  -10,
		},
		{
			name:    "Multiplying a string with a number",
			left:    "Hello,",
			right:   5,
			wantErr: true,
			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
		},
		{
			name:    "Multiplying a number with a string",
			left:    5,
			right:   "World!",
			wantErr: true,
			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
		},
		{
			name:    "Multiplying a string with a string",
			left:    "Hello,",
			right:   "World!",
			wantErr: true,
			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := performArithmeticOperation(tt.left, tt.right, utils.Multiply)
			if !tt.wantErr {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
			if tt.wantErr {
				assert.Equal(t, tt.errMsg, err.Error())
			}
		})
	}
}

func Test_performArithmeticOperation_Division(t *testing.T) {
	tests := []struct {
		name    string
		left    interface{}
		right   interface{}
		want    float64
		wantErr bool
		errMsg  string
	}{
		{
			name:  "10/2",
			left:  10,
			right: 2,
			want:  5,
		},
		{
			name:  "5/(-1)",
			left:  5,
			right: -1,
			want:  -5,
		},
		{
			name:  "(-6)/(-3)",
			left:  -6,
			right: -3,
			want:  2,
		},
		{
			name:  "7.5/2.5",
			left:  7.5,
			right: 2.5,
			want:  3,
		},
		{
			name:    "Dividing by zero",
			left:    5,
			right:   0,
			wantErr: true,
			errMsg:  "performArithmeticOperation: cannot divide by zero",
		},
		{
			name:    "Dividing a string by a number",
			left:    "Hello,",
			right:   5,
			wantErr: true,
			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
		},
		{
			name:    "Dividing a number by a string",
			left:    5,
			right:   "World!",
			wantErr: true,
			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
		},
		{
			name:    "Dividing a string by a string",
			left:    "Hello,",
			right:   "World!",
			wantErr: true,
			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := performArithmeticOperation(tt.left, tt.right, utils.Divide)
			if !tt.wantErr {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
			if tt.wantErr {
				assert.Equal(t, tt.errMsg, err.Error())
			}
		})
	}
}

func Test_performArithmeticOperation_Modulo(t *testing.T) {
	tests := []struct {
		name    string
		left    interface{}
		right   interface{}
		want    int64
		wantErr bool
		errMsg  string
	}{
		{
			name:  "10%3",
			left:  10,
			right: 3,
			want:  1,
		},
		{
			name:  "18%7",
			left:  18,
			right: -7,
			want:  4,
		},
		{
			name:  "(-4)%3",
			left:  -4,
			right: 3,
			want:  -1,
		},
		{
			name:  "(-4)%(-3)",
			left:  -4,
			right: -3,
			want:  -1,
		},
		{
			name:    "Modulo a string by a number",
			left:    "Hello,",
			right:   5,
			wantErr: true,
			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
		},
		{
			name:    "Modulo a number by a string",
			left:    5,
			right:   "World!",
			wantErr: true,
			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
		},
		{
			name:    "Modulo a string by a string",
			left:    "Hello,",
			right:   "World!",
			wantErr: true,
			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := performArithmeticOperation(tt.left, tt.right, utils.Modulo)
			if !tt.wantErr {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
			if tt.wantErr {
				assert.Equal(t, tt.errMsg, err.Error())
			}
		})
	}
}

func Test_performArithmeticOperation_BitwiseAnd(t *testing.T) {
	tests := []struct {
		name    string
		left    interface{}
		right   interface{}
		want    int64
		wantErr bool
		errMsg  string
	}{
		{
			name:  "6 & 3, expect 2",
			left:  6,
			right: 3,
			want:  2,
		},
		{
			name:  "10 & 8, expect 8",
			left:  10,
			right: 8,
			want:  8,
		},
		{
			name:    "bitwise a string by a number",
			left:    "a",
			right:   5,
			wantErr: true,
			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
		},
		{
			name:    "bitwise a number by a string",
			left:    5,
			right:   "b",
			wantErr: true,
			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
		},
		{
			name:    "bitwise a string by a string",
			left:    "a",
			right:   "b",
			wantErr: true,
			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := performArithmeticOperation(tt.left, tt.right, utils.BitwiseAnd)
			if !tt.wantErr {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
			if tt.wantErr {
				assert.Equal(t, tt.errMsg, err.Error())
			}
		})
	}
}

func Test_performArithmeticOperation_BitwiseOr(t *testing.T) {
	tests := []struct {
		name    string
		left    interface{}
		right   interface{}
		want    int64
		wantErr bool
		errMsg  string
	}{
		{
			name:  "10|2",
			left:  10,
			right: 2,
			want:  10,
		},
		{
			name:  "1|4",
			left:  1,
			right: 4,
			want:  5,
		},
		{
			name:  "1|2",
			left:  1,
			right: 2,
			want:  3,
		},
		{
			name:    "bitwise or a string by a number",
			left:    "a",
			right:   5,
			wantErr: true,
			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
		},
		{
			name:    "bitwise or a number by a string",
			left:    5,
			right:   "b",
			wantErr: true,
			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
		},
		{
			name:    "bitwise or a string by a string",
			left:    "a",
			right:   "b",
			wantErr: true,
			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := performArithmeticOperation(tt.left, tt.right, utils.BitwiseOr)
			if !tt.wantErr {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
			if tt.wantErr {
				assert.Equal(t, tt.errMsg, err.Error())
			}
		})
	}
}

func Test_performArithmeticOperation_BitwiseExclusiveOr(t *testing.T) {
	tests := []struct {
		name    string
		left    interface{}
		right   interface{}
		want    int64
		wantErr bool
		errMsg  string
	}{
		{
			name:  "10^5",
			left:  10,
			right: 5,
			want:  15,
		},
		{
			name:  "3^4",
			left:  3,
			right: 4,
			want:  7,
		},
		{
			name:  "2^3",
			left:  2,
			right: 3,
			want:  1,
		},
		{
			name:    "bitwise exclusive or a string by a number",
			left:    "a",
			right:   5,
			wantErr: true,
			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
		},
		{
			name:    "bitwise exclusive or a number by a string",
			left:    5,
			right:   "b",
			wantErr: true,
			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
		},
		{
			name:    "bitwise exclusive or a string by a string",
			left:    "a",
			right:   "b",
			wantErr: true,
			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := performArithmeticOperation(tt.left, tt.right, utils.BitwiseExclusiveOr)
			if !tt.wantErr {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
			if tt.wantErr {
				assert.Equal(t, tt.errMsg, err.Error())
			}
		})
	}
}

func Test_performArithmeticOperation_WrongOp(t *testing.T) {
	_, err := performArithmeticOperation(5, 3, 100)
	assert.Equal(t, errors.New("performArithmeticOperation: invalid arithmetic operator"), err)
}

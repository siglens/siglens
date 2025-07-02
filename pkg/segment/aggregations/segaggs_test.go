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

import "testing"

// import (
// 	"encoding/json"
// 	"errors"
// 	"fmt"
// 	"math/rand"
// 	"sort"
// 	"strconv"
// 	"strings"
// 	"testing"
// 	"time"

// 	"github.com/siglens/siglens/pkg/segment/structs"
// 	sutils "github.com/siglens/siglens/pkg/segment/utils"
// 	"github.com/siglens/siglens/pkg/utils"
// 	"github.com/stretchr/testify/assert"
// )

// type SimpleSearchExpr struct {
// 	Op             string
// 	Field          string
// 	Values         interface{}
// 	ValueIsRegex   bool
// 	ExprType       sutils.SS_DTYPE
// 	DtypeEnclosure *sutils.DtypeEnclosure
// }

// func Test_conditionMatch(t *testing.T) {
// 	tests := []struct {
// 		name         string
// 		fieldValue   interface{}
// 		op           string
// 		searchValue  interface{}
// 		expectedBool bool
// 	}{
// 		{"EqualStringTrue", "test", "=", "test", true},
// 		{"EqualStringFalse", "test", "=", "fail", false},
// 		{"NotEqualStringTrue", "test", "!=", "fail", true},
// 		{"NotEqualStringFalse", "test", "!=", "test", false},
// 		{"GreaterThanTrue", 10, ">", 5, true},
// 		{"GreaterThanFalse", 3, ">", 5, false},
// 		{"GreaterThanOrEqualTrue", 5, ">=", 5, true},
// 		{"GreaterThanOrEqualFalse", 3, ">=", 5, false},
// 		{"LessThanTrue", 3, "<", 5, true},
// 		{"LessThanFalse", 10, "<", 5, false},
// 		{"LessThanOrEqualTrue", 5, "<=", 5, true},
// 		{"LessThanOrEqualFalse", 10, "<=", 5, false},
// 		{"InvalidOperator", 10, "invalid", 5, false},
// 		{"InvalidFieldValue", "invalid", ">", 5, false},
// 		{"InvalidSearchValue", 5, ">", "invalid", false},
// 	}

// 	for _, test := range tests {
// 		t.Run(test.name, func(t *testing.T) {
// 			result := conditionMatch(test.fieldValue, test.op, test.searchValue)
// 			assert.Equal(t, test.expectedBool, result)
// 		})
// 	}
// }

// var cities = []string{
// 	"Hyderabad",
// 	"New York",
// 	"Los Angeles",
// 	"Chicago",
// 	"Houston",
// 	"Phoenix",
// 	"Philadelphia",
// 	"San Antonio",
// 	"San Diego",
// 	"Dallas",
// 	"San Jose",
// }

// var countries = []string{
// 	"India",
// 	"United States",
// 	"Canada",
// 	"United Kingdom",
// 	"Australia",
// 	"Germany",
// 	"France",
// 	"Spain",
// 	"Italy",
// 	"Japan",
// }

// func generateTestRecords(numRecords int) map[string]map[string]interface{} {
// 	records := make(map[string]map[string]interface{}, numRecords)

// 	for i := 0; i < numRecords; i++ {
// 		record := make(map[string]interface{})

// 		record["timestamp"] = uint64(1659874108987)
// 		record["city"] = cities[rand.Intn(len(cities))]
// 		record["gender"] = []string{"male", "female"}[rand.Intn(2)]
// 		record["country"] = countries[rand.Intn(len(countries))]
// 		record["http_method"] = []string{"GET", "POST", "PUT", "DELETE"}[rand.Intn(4)]
// 		record["http_status"] = []int64{200, 201, 301, 302, 404}[rand.Intn(5)]
// 		record["latitude"] = rand.Float64() * 180
// 		record["longitude"] = rand.Float64() * 180

// 		records[fmt.Sprint(i)] = record
// 	}

// 	return records
// }

// func getFinalColsForGeneratedTestRecords() map[string]bool {
// 	return map[string]bool{"timestamp": true, "city": true, "gender": true, "country": true, "http_method": true, "http_status": true, "latitude": true, "longitude": true}
// }

// // Test Cases for processTransactionsOnRecords
// func All_TestCasesForTransactionCommands() (map[int]bool, []*structs.TransactionArguments, map[int]map[string]interface{}) {
// 	matchesSomeRecords := make(map[int]bool)
// 	searchResults := make(map[int]map[string]interface{})

// 	// CASE 1: Only Fields
// 	txnArgs1 := &structs.TransactionArguments{
// 		Fields:     []string{"gender", "city"},
// 		StartsWith: nil,
// 		EndsWith:   nil,
// 	}
// 	matchesSomeRecords[1] = true

// 	// CASE 2: Only EndsWith
// 	txnArgs2 := &structs.TransactionArguments{
// 		EndsWith:   &structs.FilterStringExpr{StringValue: "DELETE"},
// 		StartsWith: &structs.FilterStringExpr{StringValue: "GET"},
// 		Fields:     []string{},
// 	}
// 	matchesSomeRecords[2] = true

// 	// CASE 3: Only StartsWith
// 	txnArgs3 := &structs.TransactionArguments{
// 		StartsWith: &structs.FilterStringExpr{StringValue: "GET"},
// 		EndsWith:   nil,
// 		Fields:     []string{},
// 	}
// 	matchesSomeRecords[3] = true

// 	// CASE 4: StartsWith and EndsWith
// 	txnArgs4 := &structs.TransactionArguments{
// 		StartsWith: &structs.FilterStringExpr{StringValue: "GET"},
// 		EndsWith:   &structs.FilterStringExpr{StringValue: "DELETE"},
// 		Fields:     []string{},
// 	}
// 	matchesSomeRecords[4] = true

// 	// CASE 5: StartsWith and EndsWith and one Field
// 	txnArgs5 := &structs.TransactionArguments{
// 		StartsWith: &structs.FilterStringExpr{StringValue: "GET"},
// 		EndsWith:   &structs.FilterStringExpr{StringValue: "DELETE"},
// 		Fields:     []string{"gender"},
// 	}
// 	matchesSomeRecords[5] = true

// 	// CASE 6: StartsWith and EndsWith and two Fields
// 	txnArgs6 := &structs.TransactionArguments{
// 		StartsWith: &structs.FilterStringExpr{StringValue: "GET"},
// 		EndsWith:   &structs.FilterStringExpr{StringValue: "DELETE"},
// 		Fields:     []string{"gender", "country"},
// 	}
// 	matchesSomeRecords[6] = true

// 	// CASE 7: StartsWith and EndsWith with String Clauses only OR: startswith=("GET" OR "POST1") endswith=("DELETE" OR "POST2")
// 	txnArgs7 := &structs.TransactionArguments{
// 		StartsWith: &structs.FilterStringExpr{
// 			SearchNode: &structs.ASTNode{
// 				OrFilterCondition: &structs.Condition{
// 					FilterCriteria: []*structs.FilterCriteria{
// 						{
// 							MatchFilter: &structs.MatchFilter{
// 								MatchColumn: "*",
// 								MatchWords: [][]byte{
// 									[]byte("GET"),
// 								},
// 								MatchOperator: sutils.And,
// 								MatchPhrase:   []byte("GET"),
// 								MatchType:     structs.MATCH_PHRASE,
// 							},
// 						},
// 						{
// 							MatchFilter: &structs.MatchFilter{
// 								MatchColumn: "*",
// 								MatchWords: [][]byte{
// 									[]byte("POST1"),
// 								},
// 								MatchOperator: sutils.And,
// 								MatchPhrase:   []byte("POST1"),
// 								MatchType:     structs.MATCH_PHRASE,
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 		EndsWith: &structs.FilterStringExpr{
// 			SearchNode: &structs.ASTNode{
// 				OrFilterCondition: &structs.Condition{
// 					FilterCriteria: []*structs.FilterCriteria{
// 						{
// 							MatchFilter: &structs.MatchFilter{
// 								MatchColumn: "*",
// 								MatchWords: [][]byte{
// 									[]byte("DELETE"),
// 								},
// 								MatchOperator: sutils.And,
// 								MatchPhrase:   []byte("DELETE"),
// 								MatchType:     structs.MATCH_PHRASE,
// 							},
// 						},
// 						{
// 							MatchFilter: &structs.MatchFilter{
// 								MatchColumn: "*",
// 								MatchWords: [][]byte{
// 									[]byte("POST2"),
// 								},
// 								MatchOperator: sutils.And,
// 								MatchPhrase:   []byte("POST2"),
// 								MatchType:     structs.MATCH_PHRASE,
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 		Fields: []string{"gender", "country"},
// 	}
// 	matchesSomeRecords[7] = true
// 	searchResults[7] = map[string]interface{}{
// 		"startswith": [][]*SimpleSearchExpr{
// 			{
// 				{
// 					Op:           "=",
// 					Field:        "http_method",
// 					Values:       "GET",
// 					ValueIsRegex: false,
// 					ExprType:     sutils.SS_DT_STRING,
// 					DtypeEnclosure: &sutils.DtypeEnclosure{
// 						Dtype:     sutils.SS_DT_STRING,
// 						StringVal: "GET",
// 					},
// 				},
// 				{
// 					Op:           "=",
// 					Field:        "http_method",
// 					Values:       "POST1",
// 					ValueIsRegex: false,
// 					ExprType:     sutils.SS_DT_STRING,
// 					DtypeEnclosure: &sutils.DtypeEnclosure{
// 						Dtype:     sutils.SS_DT_STRING,
// 						StringVal: "POST1",
// 					},
// 				},
// 			},
// 		},
// 		"endswith": [][]*SimpleSearchExpr{
// 			{
// 				{
// 					Op:           "=",
// 					Field:        "http_method",
// 					Values:       "DELETE",
// 					ValueIsRegex: false,
// 					ExprType:     sutils.SS_DT_STRING,
// 					DtypeEnclosure: &sutils.DtypeEnclosure{
// 						Dtype:     sutils.SS_DT_STRING,
// 						StringVal: "DELETE",
// 					},
// 				},
// 				{
// 					Op:           "=",
// 					Field:        "http_method",
// 					Values:       "POST2",
// 					ValueIsRegex: false,
// 					ExprType:     sutils.SS_DT_STRING,
// 					DtypeEnclosure: &sutils.DtypeEnclosure{
// 						Dtype:     sutils.SS_DT_STRING,
// 						StringVal: "POST2",
// 					},
// 				},
// 			},
// 		},
// 	}

// 	// CASE 8: StartsWith and EndsWith with String Clauses only AND (Negative Case): startswith=("GET" AND "POST2") endswith=("POST")
// 	txnArgs8 := &structs.TransactionArguments{
// 		StartsWith: &structs.FilterStringExpr{
// 			SearchNode: &structs.ASTNode{
// 				AndFilterCondition: &structs.Condition{
// 					FilterCriteria: []*structs.FilterCriteria{
// 						{
// 							MatchFilter: &structs.MatchFilter{
// 								MatchColumn: "*",
// 								MatchWords: [][]byte{
// 									[]byte("GET"),
// 								},
// 								MatchOperator: sutils.And,
// 								MatchPhrase:   []byte("GET"),
// 								MatchType:     structs.MATCH_PHRASE,
// 							},
// 						},
// 						{
// 							MatchFilter: &structs.MatchFilter{
// 								MatchColumn: "*",
// 								MatchWords: [][]byte{
// 									[]byte("POST2"),
// 								},
// 								MatchOperator: sutils.And,
// 								MatchPhrase:   []byte("POST2"),
// 								MatchType:     structs.MATCH_PHRASE,
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 		EndsWith: &structs.FilterStringExpr{
// 			SearchNode: &structs.ASTNode{
// 				AndFilterCondition: &structs.Condition{
// 					FilterCriteria: []*structs.FilterCriteria{
// 						{
// 							MatchFilter: &structs.MatchFilter{
// 								MatchColumn: "*",
// 								MatchWords: [][]byte{
// 									[]byte("DELETE"),
// 								},
// 								MatchOperator: sutils.And,
// 								MatchPhrase:   []byte("DELETE"),
// 								MatchType:     structs.MATCH_PHRASE,
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 		Fields: []string{"gender", "country"},
// 	}
// 	matchesSomeRecords[8] = false

// 	// CASE 9: StartsWith and EndsWith with String Clauses only AND (Positive Case): startswith=("GET" AND "male") endswith=("DELETE")
// 	txnArgs9 := &structs.TransactionArguments{
// 		Fields: []string{"gender", "country"},
// 		StartsWith: &structs.FilterStringExpr{
// 			SearchNode: &structs.ASTNode{
// 				AndFilterCondition: &structs.Condition{
// 					FilterCriteria: []*structs.FilterCriteria{
// 						{
// 							MatchFilter: &structs.MatchFilter{
// 								MatchColumn: "*",
// 								MatchWords: [][]byte{
// 									[]byte("GET"),
// 								},
// 								MatchOperator: sutils.And,
// 								MatchPhrase:   []byte("GET"),
// 								MatchType:     structs.MATCH_PHRASE,
// 							},
// 						},
// 						{
// 							MatchFilter: &structs.MatchFilter{
// 								MatchColumn: "*",
// 								MatchWords: [][]byte{
// 									[]byte("male"),
// 								},
// 								MatchOperator: sutils.And,
// 								MatchPhrase:   []byte("male"),
// 								MatchType:     structs.MATCH_PHRASE,
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 		EndsWith: &structs.FilterStringExpr{
// 			SearchNode: &structs.ASTNode{
// 				AndFilterCondition: &structs.Condition{
// 					FilterCriteria: []*structs.FilterCriteria{
// 						{
// 							MatchFilter: &structs.MatchFilter{
// 								MatchColumn: "*",
// 								MatchWords: [][]byte{
// 									[]byte("DELETE"),
// 								},
// 								MatchOperator: sutils.And,
// 								MatchPhrase:   []byte("DELETE"),
// 								MatchType:     structs.MATCH_PHRASE,
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 	}
// 	matchesSomeRecords[9] = true
// 	searchResults[9] = map[string]interface{}{
// 		"startswith": [][]*SimpleSearchExpr{
// 			{
// 				{
// 					Op:           "=",
// 					Field:        "http_method",
// 					Values:       "GET",
// 					ValueIsRegex: false,
// 					ExprType:     sutils.SS_DT_STRING,
// 					DtypeEnclosure: &sutils.DtypeEnclosure{
// 						Dtype:          sutils.SS_DT_STRING,
// 						StringVal:      "GET",
// 						StringValBytes: []byte("GET"),
// 					},
// 				},
// 			},
// 			{
// 				{
// 					Op:           "=",
// 					Field:        "gender",
// 					Values:       "male",
// 					ValueIsRegex: false,
// 					ExprType:     sutils.SS_DT_STRING,
// 					DtypeEnclosure: &sutils.DtypeEnclosure{
// 						Dtype:          sutils.SS_DT_STRING,
// 						StringVal:      "male",
// 						StringValBytes: []byte("male"),
// 					},
// 				},
// 			},
// 		},
// 		"endswith": [][]*SimpleSearchExpr{
// 			{
// 				{
// 					Op:           "=",
// 					Field:        "http_method",
// 					Values:       "DELETE",
// 					ValueIsRegex: false,
// 					ExprType:     sutils.SS_DT_STRING,
// 					DtypeEnclosure: &sutils.DtypeEnclosure{
// 						Dtype:     sutils.SS_DT_STRING,
// 						StringVal: "DELETE",
// 					},
// 				},
// 			},
// 		},
// 	}

// 	// CASE 10: StartsWith is a Valid Search Expr and EndsWith is String Value: startswith=status>=300 endswith="DELETE"
// 	txnArgs10 := &structs.TransactionArguments{
// 		StartsWith: &structs.FilterStringExpr{
// 			SearchNode: &structs.ASTNode{
// 				AndFilterCondition: &structs.Condition{
// 					FilterCriteria: []*structs.FilterCriteria{
// 						{
// 							ExpressionFilter: &structs.ExpressionFilter{
// 								LeftInput: &structs.FilterInput{
// 									Expression: &structs.Expression{
// 										LeftInput: &structs.ExpressionInput{
// 											ColumnValue: nil,
// 											ColumnName:  "http_status",
// 										},
// 										ExpressionOp: sutils.Add,
// 										RightInput:   nil,
// 									},
// 								},
// 								FilterOperator: sutils.GreaterThanOrEqualTo,
// 								RightInput: &structs.FilterInput{
// 									Expression: &structs.Expression{
// 										LeftInput: &structs.ExpressionInput{
// 											ColumnValue: &sutils.DtypeEnclosure{
// 												Dtype:       sutils.SS_DT_UNSIGNED_NUM,
// 												UnsignedVal: uint64(300),
// 												SignedVal:   int64(300),
// 												FloatVal:    float64(300),
// 												StringVal:   "300",
// 											},
// 										},
// 										ExpressionOp: sutils.Add,
// 										RightInput:   nil,
// 									},
// 								},
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 		EndsWith: &structs.FilterStringExpr{StringValue: "DELETE"},
// 	}
// 	matchesSomeRecords[10] = true
// 	searchResults[10] = map[string]interface{}{
// 		"startswith": [][]*SimpleSearchExpr{
// 			{
// 				{
// 					Op:           ">=",
// 					Field:        "http_status",
// 					Values:       json.Number("300"),
// 					ValueIsRegex: false,
// 					ExprType:     sutils.SS_DT_SIGNED_NUM,
// 					DtypeEnclosure: &sutils.DtypeEnclosure{
// 						Dtype:       sutils.SS_DT_SIGNED_NUM,
// 						FloatVal:    float64(300),
// 						UnsignedVal: uint64(300),
// 						SignedVal:   int64(300),
// 						StringVal:   "300",
// 					},
// 				},
// 			},
// 		},
// 	}

// 	// CASE 11: StartsWith is not a Valid Search Term (comparing between two string fields) and EndsWith is String value: startswith=city>"Hyderabad" endswith="DELETE"
// 	txnArgs11 := &structs.TransactionArguments{
// 		StartsWith: &structs.FilterStringExpr{
// 			SearchNode: &structs.ASTNode{
// 				AndFilterCondition: &structs.Condition{
// 					FilterCriteria: []*structs.FilterCriteria{
// 						{
// 							ExpressionFilter: &structs.ExpressionFilter{
// 								LeftInput: &structs.FilterInput{
// 									Expression: &structs.Expression{
// 										LeftInput: &structs.ExpressionInput{
// 											ColumnValue: nil,
// 											ColumnName:  "city",
// 										},
// 										ExpressionOp: sutils.Add,
// 										RightInput:   nil,
// 									},
// 								},
// 								FilterOperator: sutils.GreaterThan,
// 								RightInput: &structs.FilterInput{
// 									Expression: &structs.Expression{
// 										LeftInput: &structs.ExpressionInput{
// 											ColumnValue: &sutils.DtypeEnclosure{
// 												Dtype:     sutils.SS_DT_STRING,
// 												StringVal: "Hyderabad",
// 											},
// 										},
// 									},
// 								},
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 		EndsWith: &structs.FilterStringExpr{StringValue: "DELETE"},
// 	}
// 	matchesSomeRecords[11] = false

// 	// CASE 12: StartsWith is not a Valid Search Term (comparing between string and number fields) and EndsWith is String Clause: startswith=city>300 endswith=("DELETE")
// 	txnArgs12 := &structs.TransactionArguments{
// 		StartsWith: &structs.FilterStringExpr{
// 			SearchNode: &structs.ASTNode{
// 				AndFilterCondition: &structs.Condition{
// 					FilterCriteria: []*structs.FilterCriteria{
// 						{
// 							ExpressionFilter: &structs.ExpressionFilter{
// 								LeftInput: &structs.FilterInput{
// 									Expression: &structs.Expression{
// 										LeftInput: &structs.ExpressionInput{
// 											ColumnValue: nil,
// 											ColumnName:  "city",
// 										},
// 										ExpressionOp: sutils.Add,
// 										RightInput:   nil,
// 									},
// 								},
// 								FilterOperator: sutils.GreaterThan,
// 								RightInput: &structs.FilterInput{
// 									Expression: &structs.Expression{
// 										LeftInput: &structs.ExpressionInput{
// 											ColumnValue: &sutils.DtypeEnclosure{
// 												Dtype:       sutils.SS_DT_UNSIGNED_NUM,
// 												StringVal:   "300",
// 												UnsignedVal: uint64(300),
// 												SignedVal:   int64(300),
// 												FloatVal:    float64(300),
// 											},
// 										},
// 										ExpressionOp: sutils.Add,
// 										RightInput:   nil,
// 									},
// 								},
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 		EndsWith: &structs.FilterStringExpr{
// 			SearchNode: &structs.ASTNode{
// 				AndFilterCondition: &structs.Condition{
// 					FilterCriteria: []*structs.FilterCriteria{
// 						{
// 							MatchFilter: &structs.MatchFilter{
// 								MatchColumn: "*",
// 								MatchWords: [][]byte{
// 									[]byte("DELETE"),
// 								},
// 								MatchOperator: sutils.And,
// 								MatchPhrase:   []byte("DELETE"),
// 								MatchType:     structs.MATCH_PHRASE,
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 	}
// 	matchesSomeRecords[12] = false

// 	// CASE 13: StartsWith is a Valid Search Term (String1 = String2) and EndsWith is String Value: startswith=city="Hyderabad" endswith="DELETE"
// 	txnArgs13 := &structs.TransactionArguments{
// 		StartsWith: &structs.FilterStringExpr{
// 			SearchNode: &structs.ASTNode{
// 				AndFilterCondition: &structs.Condition{
// 					FilterCriteria: []*structs.FilterCriteria{
// 						{
// 							ExpressionFilter: &structs.ExpressionFilter{
// 								LeftInput: &structs.FilterInput{
// 									Expression: &structs.Expression{
// 										LeftInput: &structs.ExpressionInput{
// 											ColumnValue: nil,
// 											ColumnName:  "city",
// 										},
// 										ExpressionOp: sutils.Add,
// 										RightInput:   nil,
// 									},
// 								},
// 								FilterOperator: sutils.Equals,
// 								RightInput: &structs.FilterInput{
// 									Expression: &structs.Expression{
// 										LeftInput: &structs.ExpressionInput{
// 											ColumnValue: &sutils.DtypeEnclosure{
// 												Dtype:     sutils.SS_DT_STRING,
// 												StringVal: "Hyderabad",
// 											},
// 										},
// 									},
// 								},
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 		EndsWith: &structs.FilterStringExpr{StringValue: "DELETE"},
// 	}
// 	matchesSomeRecords[13] = true
// 	searchResults[13] = map[string]interface{}{
// 		"startswith": [][]*SimpleSearchExpr{
// 			{
// 				{
// 					Op:           "=",
// 					Field:        "city",
// 					Values:       "Hyderabad",
// 					ValueIsRegex: false,
// 					ExprType:     sutils.SS_DT_STRING,
// 					DtypeEnclosure: &sutils.DtypeEnclosure{
// 						Dtype:     sutils.SS_DT_STRING,
// 						StringVal: "Hyderabad",
// 					},
// 				},
// 			},
// 		},
// 	}

// 	// CASE 14: Eval Expression:  transaction gender startswith=eval(status > 300 AND http_method="POST" OR http_method="PUT")
// 	txnArgs14 := &structs.TransactionArguments{
// 		Fields: []string{"gender"},
// 		StartsWith: &structs.FilterStringExpr{
// 			EvalBoolExpr: &structs.BoolExpr{
// 				IsTerminal: false,
// 				LeftBool: &structs.BoolExpr{
// 					IsTerminal: true,
// 					LeftValue: &structs.ValueExpr{
// 						ValueExprMode: structs.VEMNumericExpr,
// 						NumericExpr: &structs.NumericExpr{
// 							NumericExprMode: structs.NEMNumberField,
// 							IsTerminal:      true,
// 							ValueIsField:    true,
// 							Value:           "http_status",
// 						},
// 					},
// 					RightValue: &structs.ValueExpr{
// 						NumericExpr: &structs.NumericExpr{
// 							NumericExprMode: structs.NEMNumber,
// 							IsTerminal:      true,
// 							ValueIsField:    false,
// 							Value:           "300",
// 						},
// 					},
// 					ValueOp: ">",
// 				},
// 				RightBool: &structs.BoolExpr{
// 					IsTerminal: false,
// 					LeftBool: &structs.BoolExpr{
// 						IsTerminal: true,
// 						LeftValue: &structs.ValueExpr{
// 							ValueExprMode: structs.VEMNumericExpr,
// 							NumericExpr: &structs.NumericExpr{
// 								NumericExprMode: structs.NEMNumberField,
// 								IsTerminal:      true,
// 								ValueIsField:    true,
// 								Value:           "http_method",
// 							},
// 						},
// 						RightValue: &structs.ValueExpr{
// 							ValueExprMode: structs.VEMStringExpr,
// 							StringExpr: &structs.StringExpr{
// 								StringExprMode: structs.SEMRawString,
// 								RawString:      "POST",
// 							},
// 						},
// 						ValueOp: "=",
// 					},
// 					RightBool: &structs.BoolExpr{
// 						IsTerminal: true,
// 						LeftValue: &structs.ValueExpr{
// 							ValueExprMode: structs.VEMNumericExpr,
// 							NumericExpr: &structs.NumericExpr{
// 								NumericExprMode: structs.NEMNumberField,
// 								IsTerminal:      true,
// 								ValueIsField:    true,
// 								Value:           "http_method",
// 							},
// 						},
// 						RightValue: &structs.ValueExpr{
// 							ValueExprMode: structs.VEMStringExpr,
// 							StringExpr: &structs.StringExpr{
// 								StringExprMode: structs.SEMRawString,
// 								RawString:      "PUT",
// 							},
// 						},
// 						ValueOp: "=",
// 					},
// 					BoolOp: structs.BoolOpOr,
// 				},
// 				BoolOp: structs.BoolOpAnd,
// 			},
// 		},
// 	}
// 	matchesSomeRecords[14] = true
// 	searchResults[14] = map[string]interface{}{
// 		"startswith": [][]*SimpleSearchExpr{
// 			{
// 				{
// 					Op:           ">",
// 					Field:        "http_status",
// 					Values:       json.Number("300"),
// 					ValueIsRegex: false,
// 					ExprType:     sutils.SS_DT_SIGNED_NUM,
// 					DtypeEnclosure: &sutils.DtypeEnclosure{
// 						Dtype:       sutils.SS_DT_SIGNED_NUM,
// 						FloatVal:    float64(300),
// 						UnsignedVal: uint64(300),
// 						SignedVal:   int64(300),
// 						StringVal:   "300",
// 					},
// 				},
// 			},
// 			{
// 				{
// 					Op:           "=",
// 					Field:        "http_method",
// 					Values:       "POST",
// 					ValueIsRegex: false,
// 					ExprType:     sutils.SS_DT_STRING,
// 					DtypeEnclosure: &sutils.DtypeEnclosure{
// 						Dtype:     sutils.SS_DT_STRING,
// 						StringVal: "POST",
// 					},
// 				},
// 				{
// 					Op:           "=",
// 					Field:        "http_method",
// 					Values:       "PUT",
// 					ValueIsRegex: false,
// 					ExprType:     sutils.SS_DT_STRING,
// 					DtypeEnclosure: &sutils.DtypeEnclosure{
// 						Dtype:     sutils.SS_DT_STRING,
// 						StringVal: "PUT",
// 					},
// 				},
// 			},
// 		},
// 	}

// 	// CASE 15: String Search Expr: transaction gender startswith="status>300 OR status=201 AND http_method=POST" endswith=eval(status<400)
// 	txnArgs15 := &structs.TransactionArguments{
// 		Fields: []string{"gender"},
// 		StartsWith: &structs.FilterStringExpr{
// 			SearchNode: &structs.ASTNode{
// 				AndFilterCondition: &structs.Condition{
// 					FilterCriteria: []*structs.FilterCriteria{
// 						{
// 							ExpressionFilter: &structs.ExpressionFilter{
// 								LeftInput: &structs.FilterInput{
// 									Expression: &structs.Expression{
// 										LeftInput: &structs.ExpressionInput{
// 											ColumnValue: nil,
// 											ColumnName:  "http_method",
// 										},
// 										ExpressionOp: sutils.Add,
// 										RightInput:   nil,
// 									},
// 								},
// 								RightInput: &structs.FilterInput{
// 									Expression: &structs.Expression{
// 										LeftInput: &structs.ExpressionInput{
// 											ColumnValue: &sutils.DtypeEnclosure{
// 												Dtype:     sutils.SS_DT_STRING,
// 												StringVal: "POST",
// 											},
// 											ColumnName: "",
// 										},
// 										ExpressionOp: sutils.Add,
// 										RightInput:   nil,
// 									},
// 								},
// 								FilterOperator: sutils.Equals,
// 							},
// 						},
// 					},
// 					NestedNodes: []*structs.ASTNode{
// 						{
// 							OrFilterCondition: &structs.Condition{
// 								FilterCriteria: []*structs.FilterCriteria{
// 									{
// 										ExpressionFilter: &structs.ExpressionFilter{
// 											LeftInput: &structs.FilterInput{
// 												Expression: &structs.Expression{
// 													LeftInput: &structs.ExpressionInput{
// 														ColumnValue: nil,
// 														ColumnName:  "http_status",
// 													},
// 													ExpressionOp: sutils.Add,
// 													RightInput:   nil,
// 												},
// 											},
// 											RightInput: &structs.FilterInput{
// 												Expression: &structs.Expression{
// 													LeftInput: &structs.ExpressionInput{
// 														ColumnValue: &sutils.DtypeEnclosure{
// 															Dtype:       sutils.SS_DT_UNSIGNED_NUM,
// 															UnsignedVal: uint64(300),
// 															SignedVal:   int64(300),
// 															FloatVal:    float64(300),
// 															StringVal:   "300",
// 														},
// 														ColumnName: "",
// 													},
// 													ExpressionOp: sutils.Add,
// 													RightInput:   nil,
// 												},
// 											},
// 											FilterOperator: sutils.GreaterThan,
// 										},
// 									},
// 									{
// 										ExpressionFilter: &structs.ExpressionFilter{
// 											LeftInput: &structs.FilterInput{
// 												Expression: &structs.Expression{
// 													LeftInput: &structs.ExpressionInput{
// 														ColumnValue: nil,
// 														ColumnName:  "http_status",
// 													},
// 													ExpressionOp: sutils.Add,
// 													RightInput:   nil,
// 												},
// 											},
// 											RightInput: &structs.FilterInput{
// 												Expression: &structs.Expression{
// 													LeftInput: &structs.ExpressionInput{
// 														ColumnValue: &sutils.DtypeEnclosure{
// 															Dtype:       sutils.SS_DT_UNSIGNED_NUM,
// 															UnsignedVal: uint64(201),
// 															SignedVal:   int64(201),
// 															FloatVal:    float64(201),
// 															StringVal:   "201",
// 														},
// 														ColumnName: "",
// 													},
// 													ExpressionOp: sutils.Add,
// 													RightInput:   nil,
// 												},
// 											},
// 											FilterOperator: sutils.Equals,
// 										},
// 									},
// 								},
// 								NestedNodes: nil,
// 							},
// 						},
// 					},
// 				},
// 			},
// 		},
// 		EndsWith: &structs.FilterStringExpr{
// 			EvalBoolExpr: &structs.BoolExpr{
// 				IsTerminal: true,
// 				LeftValue: &structs.ValueExpr{
// 					NumericExpr: &structs.NumericExpr{
// 						IsTerminal:      true,
// 						NumericExprMode: structs.NEMNumberField,
// 						ValueIsField:    true,
// 						Value:           "http_status",
// 					},
// 				},
// 				RightValue: &structs.ValueExpr{
// 					NumericExpr: &structs.NumericExpr{
// 						IsTerminal:      true,
// 						NumericExprMode: structs.NEMNumber,
// 						ValueIsField:    false,
// 						Value:           "400",
// 					},
// 				},
// 				ValueOp: "<",
// 			},
// 		},
// 	}
// 	matchesSomeRecords[15] = true
// 	searchResults[15] = map[string]interface{}{
// 		"endswith": [][]*SimpleSearchExpr{
// 			{
// 				{
// 					Op:           "<",
// 					Field:        "http_status",
// 					Values:       json.Number("400"),
// 					ValueIsRegex: false,
// 					ExprType:     sutils.SS_DT_SIGNED_NUM,
// 					DtypeEnclosure: &sutils.DtypeEnclosure{
// 						Dtype:       sutils.SS_DT_SIGNED_NUM,
// 						FloatVal:    float64(400),
// 						UnsignedVal: uint64(400),
// 						SignedVal:   int64(400),
// 						StringVal:   "400",
// 					},
// 				},
// 			},
// 		},
// 		"startswith": [][]*SimpleSearchExpr{
// 			{
// 				{
// 					Op:           ">",
// 					Field:        "http_status",
// 					Values:       json.Number("300"),
// 					ValueIsRegex: false,
// 					ExprType:     sutils.SS_DT_SIGNED_NUM,
// 					DtypeEnclosure: &sutils.DtypeEnclosure{
// 						Dtype:       sutils.SS_DT_SIGNED_NUM,
// 						FloatVal:    float64(300),
// 						UnsignedVal: uint64(300),
// 						SignedVal:   int64(300),
// 						StringVal:   "300",
// 					},
// 				},
// 				{
// 					Op:           "=",
// 					Field:        "http_status",
// 					Values:       json.Number("201"),
// 					ValueIsRegex: false,
// 					ExprType:     sutils.SS_DT_SIGNED_NUM,
// 					DtypeEnclosure: &sutils.DtypeEnclosure{
// 						Dtype:       sutils.SS_DT_SIGNED_NUM,
// 						FloatVal:    float64(201),
// 						UnsignedVal: uint64(201),
// 						SignedVal:   int64(201),
// 						StringVal:   "201",
// 					},
// 				},
// 			},
// 			{
// 				{
// 					Op:           "=",
// 					Field:        "http_method",
// 					Values:       "POST",
// 					ValueIsRegex: false,
// 					ExprType:     sutils.SS_DT_STRING,
// 					DtypeEnclosure: &sutils.DtypeEnclosure{
// 						Dtype:     sutils.SS_DT_STRING,
// 						StringVal: "POST",
// 					},
// 				},
// 			},
// 		},
// 	}

// 	// CASE 16: String Search Expr: transaction city startswith="status>300 OR status=201" endswith=eval(status<400)
// 	txnArgs16 := &structs.TransactionArguments{
// 		Fields: []string{"city"},
// 		StartsWith: &structs.FilterStringExpr{
// 			SearchNode: &structs.ASTNode{
// 				OrFilterCondition: &structs.Condition{
// 					FilterCriteria: []*structs.FilterCriteria{
// 						{
// 							ExpressionFilter: &structs.ExpressionFilter{
// 								LeftInput: &structs.FilterInput{
// 									Expression: &structs.Expression{
// 										LeftInput: &structs.ExpressionInput{
// 											ColumnValue: nil,
// 											ColumnName:  "http_status",
// 										},
// 										ExpressionOp: sutils.Add,
// 										RightInput:   nil,
// 									},
// 								},
// 								RightInput: &structs.FilterInput{
// 									Expression: &structs.Expression{
// 										LeftInput: &structs.ExpressionInput{
// 											ColumnValue: &sutils.DtypeEnclosure{
// 												Dtype:       sutils.SS_DT_UNSIGNED_NUM,
// 												UnsignedVal: uint64(300),
// 												SignedVal:   int64(300),
// 												FloatVal:    float64(300),
// 												StringVal:   "300",
// 											},
// 											ColumnName: "",
// 										},
// 										ExpressionOp: sutils.Add,
// 										RightInput:   nil,
// 									},
// 								},
// 								FilterOperator: sutils.GreaterThan,
// 							},
// 						},
// 						{
// 							ExpressionFilter: &structs.ExpressionFilter{
// 								LeftInput: &structs.FilterInput{
// 									Expression: &structs.Expression{
// 										LeftInput: &structs.ExpressionInput{
// 											ColumnValue: nil,
// 											ColumnName:  "http_status",
// 										},
// 										ExpressionOp: sutils.Add,
// 										RightInput:   nil,
// 									},
// 								},
// 								RightInput: &structs.FilterInput{
// 									Expression: &structs.Expression{
// 										LeftInput: &structs.ExpressionInput{
// 											ColumnValue: &sutils.DtypeEnclosure{
// 												Dtype:       sutils.SS_DT_UNSIGNED_NUM,
// 												UnsignedVal: uint64(201),
// 												SignedVal:   int64(201),
// 												FloatVal:    float64(201),
// 												StringVal:   "201",
// 											},
// 											ColumnName: "",
// 										},
// 										ExpressionOp: sutils.Add,
// 										RightInput:   nil,
// 									},
// 								},
// 								FilterOperator: sutils.Equals,
// 							},
// 						},
// 					},
// 					NestedNodes: nil,
// 				},
// 			},
// 		},
// 		EndsWith: &structs.FilterStringExpr{
// 			EvalBoolExpr: &structs.BoolExpr{
// 				IsTerminal: true,
// 				LeftValue: &structs.ValueExpr{
// 					NumericExpr: &structs.NumericExpr{
// 						IsTerminal:      true,
// 						NumericExprMode: structs.NEMNumberField,
// 						ValueIsField:    true,
// 						Value:           "http_status",
// 					},
// 				},
// 				RightValue: &structs.ValueExpr{
// 					NumericExpr: &structs.NumericExpr{
// 						IsTerminal:      true,
// 						NumericExprMode: structs.NEMNumber,
// 						ValueIsField:    false,
// 						Value:           "400",
// 					},
// 				},
// 				ValueOp: "<",
// 			},
// 		},
// 	}
// 	matchesSomeRecords[16] = true
// 	searchResults[16] = map[string]interface{}{
// 		"endswith": [][]*SimpleSearchExpr{
// 			{
// 				{
// 					Op:           "<",
// 					Field:        "http_status",
// 					Values:       json.Number("400"),
// 					ValueIsRegex: false,
// 					ExprType:     sutils.SS_DT_SIGNED_NUM,
// 					DtypeEnclosure: &sutils.DtypeEnclosure{
// 						Dtype:       sutils.SS_DT_SIGNED_NUM,
// 						FloatVal:    float64(400),
// 						UnsignedVal: uint64(400),
// 						SignedVal:   int64(400),
// 						StringVal:   "400",
// 					},
// 				},
// 			},
// 		},
// 		"startswith": [][]*SimpleSearchExpr{
// 			{
// 				{
// 					Op:           ">",
// 					Field:        "http_status",
// 					Values:       json.Number("300"),
// 					ValueIsRegex: false,
// 					ExprType:     sutils.SS_DT_SIGNED_NUM,
// 					DtypeEnclosure: &sutils.DtypeEnclosure{
// 						Dtype:       sutils.SS_DT_SIGNED_NUM,
// 						FloatVal:    float64(300),
// 						UnsignedVal: uint64(300),
// 						SignedVal:   int64(300),
// 						StringVal:   "300",
// 					},
// 				},
// 				{
// 					Op:           "=",
// 					Field:        "http_status",
// 					Values:       json.Number("201"),
// 					ValueIsRegex: false,
// 					ExprType:     sutils.SS_DT_SIGNED_NUM,
// 					DtypeEnclosure: &sutils.DtypeEnclosure{
// 						Dtype:       sutils.SS_DT_SIGNED_NUM,
// 						FloatVal:    float64(201),
// 						UnsignedVal: uint64(201),
// 						SignedVal:   int64(201),
// 						StringVal:   "201",
// 					},
// 				},
// 			},
// 		},
// 	}

// 	return matchesSomeRecords, []*structs.TransactionArguments{txnArgs1, txnArgs2, txnArgs3, txnArgs4, txnArgs5, txnArgs6, txnArgs7, txnArgs8, txnArgs9,
// 		txnArgs10, txnArgs11, txnArgs12, txnArgs13, txnArgs14, txnArgs15, txnArgs16}, searchResults
// }

// func Test_processTransactionsOnRecords(t *testing.T) {

// 	allCols := map[string]bool{"city": true, "gender": true}

// 	matchesSomeRecords, allCasesTxnArgs, searchResults := All_TestCasesForTransactionCommands()

// 	for index, txnArgs := range allCasesTxnArgs {
// 		records := generateTestRecords(500)
// 		// Process Transactions
// 		performTransactionCommandRequest(&structs.NodeResult{}, &structs.QueryAggregators{TransactionArguments: txnArgs}, records, allCols, 1, true)

// 		expectedCols := map[string]bool{"duration": true, "event": true, "eventcount": true, "timestamp": true}

// 		for _, field := range txnArgs.Fields {
// 			expectedCols[field] = true
// 		}

// 		assert.Equal(t, expectedCols, allCols)

// 		// Check if the number of records is positive or negative
// 		assert.Equal(t, matchesSomeRecords[index+1], len(records) > 0)

// 		for _, record := range records {
// 			assert.Equal(t, record["timestamp"], uint64(1659874108987))
// 			assert.Equal(t, record["duration"], uint64(0))

// 			events := record["event"].([]map[string]interface{})

// 			initFields := []string{}

// 			for ind, eventMap := range events {
// 				fields := []string{}

// 				for _, field := range txnArgs.Fields {
// 					fields = append(fields, eventMap[field].(string))
// 				}

// 				// Check if the fields are same for all events by assigning the first event's fields to initFields
// 				if ind == 0 {
// 					initFields = fields
// 				}

// 				assert.Equal(t, fields, initFields)

// 				if txnArgs.StartsWith != nil {
// 					if ind == 0 {
// 						resultData, exists := getResultData(index+1, "startswith", searchResults)
// 						if txnArgs.StartsWith.StringValue != "" {
// 							assert.Equal(t, eventMap["http_method"], txnArgs.StartsWith.StringValue)
// 						} else if txnArgs.StartsWith.EvalBoolExpr != nil {
// 							if exists {
// 								valid := validateSearchExpr(eventMap, resultData.([][]*SimpleSearchExpr))
// 								assert.True(t, valid)
// 							}
// 						} else if txnArgs.StartsWith.SearchNode != nil {
// 							if exists {
// 								valid := validateSearchExpr(eventMap, resultData.([][]*SimpleSearchExpr))
// 								assert.True(t, valid)
// 							}
// 						}
// 					}
// 				}

// 				if txnArgs.EndsWith != nil {
// 					if ind == len(events)-1 {
// 						resultData, exists := getResultData(index+1, "endswith", searchResults)
// 						if txnArgs.EndsWith.StringValue != "" {
// 							assert.Equal(t, eventMap["http_method"], txnArgs.EndsWith.StringValue)
// 						} else if txnArgs.EndsWith.EvalBoolExpr != nil {
// 							if exists {
// 								valid := validateSearchExpr(eventMap, resultData.([][]*SimpleSearchExpr))
// 								assert.True(t, valid)
// 							}
// 						} else if txnArgs.EndsWith.SearchNode != nil {
// 							if exists {
// 								valid := validateSearchExpr(eventMap, resultData.([][]*SimpleSearchExpr))
// 								assert.True(t, valid)
// 							}
// 						}
// 					}
// 				}

// 			}
// 		}
// 	}

// }

// func getResultData(resultIndex int, resultType string, resultData map[int]map[string]interface{}) (interface{}, bool) {
// 	resultDataMap, exists := resultData[resultIndex]
// 	if exists {
// 		data, exists := resultDataMap[resultType]
// 		return data, exists
// 	} else {
// 		return nil, false
// 	}
// }

// func validateSearchString(searchTerm *SimpleSearchExpr, eventMap map[string]interface{}) bool {
// 	fieldValue, exists := eventMap[searchTerm.Field]
// 	if !exists {
// 		return false
// 	}

// 	return conditionMatch(fieldValue, searchTerm.Op, searchTerm.Values)
// }

// func validateSearchExpr(eventMap map[string]interface{}, resultData [][]*SimpleSearchExpr) bool {
// 	for _, resultAnd := range resultData {
// 		valid := false
// 		for _, resultOr := range resultAnd {
// 			if validateSearchString(resultOr, eventMap) {
// 				valid = true
// 				break
// 			}
// 		}
// 		if !valid {
// 			return false
// 		}
// 	}
// 	return true
// }

// func Test_performValueColRequestWithoutGroupBy_VEMNumericExpr(t *testing.T) {

// 	// Query 1: * | eval rlatitude=round(latitude, 2)
// 	letColReq := &structs.LetColumnsRequest{
// 		ValueColRequest: &structs.ValueExpr{
// 			ValueExprMode: structs.VEMNumericExpr,
// 			NumericExpr: &structs.NumericExpr{
// 				NumericExprMode: structs.NEMNumericExpr,
// 				Op:              "round",
// 				Left: &structs.NumericExpr{
// 					NumericExprMode: structs.NEMNumberField,
// 					IsTerminal:      true,
// 					ValueIsField:    true,
// 					Value:           "latitude",
// 				},
// 				Right: &structs.NumericExpr{
// 					NumericExprMode: structs.NEMNumber,
// 					IsTerminal:      true,
// 					ValueIsField:    false,
// 					Value:           "2",
// 				},
// 			},
// 		},
// 		NewColName: "rlatitude",
// 	}

// 	records := generateTestRecords(500)
// 	finalCols := getFinalColsForGeneratedTestRecords()

// 	// Perform the value column request
// 	err := performValueColRequest(&structs.NodeResult{}, &structs.QueryAggregators{}, letColReq, records, finalCols)
// 	assert.Nil(t, err)

// 	// Check if the new column is added to the records
// 	assert.True(t, finalCols["rlatitude"])

// 	for _, record := range records {
// 		assert.True(t, record["rlatitude"] != nil)
// 		valueStr := fmt.Sprintf("%.2f", record["latitude"].(float64))
// 		splitValue := strings.Split(valueStr, ".")
// 		if len(splitValue) > 1 {
// 			assert.Equal(t, len(splitValue[1]), 2)
// 		}
// 	}

// }

// func Test_performValueColRequestWithoutGroupBy_VEMConditionExpr(t *testing.T) {
// 	// Query: * |  eval http_status_mod=if(in(http_status, 400,  500), "Failure", http_status)
// 	letColReq := &structs.LetColumnsRequest{
// 		ValueColRequest: &structs.ValueExpr{
// 			ValueExprMode: structs.VEMConditionExpr,
// 			ConditionExpr: &structs.ConditionExpr{
// 				Op: "if",
// 				BoolExpr: &structs.BoolExpr{
// 					IsTerminal: true,
// 					LeftValue: &structs.ValueExpr{
// 						NumericExpr: &structs.NumericExpr{
// 							NumericExprMode: structs.NEMNumberField,
// 							IsTerminal:      true,
// 							ValueIsField:    true,
// 							Value:           "http_status",
// 						},
// 					},
// 					ValueOp: "in",
// 					ValueList: []*structs.ValueExpr{
// 						{
// 							NumericExpr: &structs.NumericExpr{
// 								NumericExprMode: structs.NEMNumber,
// 								IsTerminal:      true,
// 								ValueIsField:    false,
// 								Value:           "400",
// 							},
// 						},
// 						{
// 							NumericExpr: &structs.NumericExpr{
// 								NumericExprMode: structs.NEMNumber,
// 								IsTerminal:      true,
// 								ValueIsField:    false,
// 								Value:           "500",
// 							},
// 						},
// 					},
// 				},
// 				TrueValue: &structs.ValueExpr{
// 					ValueExprMode: structs.VEMStringExpr,
// 					StringExpr: &structs.StringExpr{
// 						RawString: "Failure",
// 					},
// 				},
// 				FalseValue: &structs.ValueExpr{
// 					NumericExpr: &structs.NumericExpr{
// 						NumericExprMode: structs.NEMNumberField,
// 						IsTerminal:      true,
// 						ValueIsField:    true,
// 						Value:           "http_status",
// 					},
// 				},
// 			},
// 		},
// 		NewColName: "http_status_mod",
// 	}

// 	records := generateTestRecords(500)
// 	finalCols := getFinalColsForGeneratedTestRecords()

// 	// Perform the value column request
// 	err := performValueColRequest(&structs.NodeResult{}, &structs.QueryAggregators{}, letColReq, records, finalCols)
// 	assert.Nil(t, err)

// 	// Check if the new column is added to the records
// 	assert.True(t, finalCols["http_status_mod"])

// 	for _, record := range records {
// 		assert.True(t, record["http_status_mod"] != nil)
// 		httpStatus := record["http_status"].(int64)
// 		if httpStatus == 400 || httpStatus == 500 {
// 			assert.Equal(t, "Failure", record["http_status_mod"])
// 		} else {
// 			assert.Equal(t, int64(httpStatus), record["http_status_mod"])
// 		}
// 	}
// }

// func Test_performValueColRequestWithoutGroupBy_VEMStringExpr(t *testing.T) {
// 	// Query: * | eval country_city=country.":-".city
// 	letColReq := &structs.LetColumnsRequest{
// 		ValueColRequest: &structs.ValueExpr{
// 			ValueExprMode: structs.VEMStringExpr,
// 			StringExpr: &structs.StringExpr{
// 				StringExprMode: structs.SEMConcatExpr,
// 				ConcatExpr: &structs.ConcatExpr{
// 					Atoms: []*structs.ConcatAtom{
// 						{
// 							IsField: true,
// 							Value:   "country",
// 						},
// 						{
// 							IsField: false,
// 							Value:   ":-",
// 						},
// 						{
// 							IsField: true,
// 							Value:   "city",
// 						},
// 					},
// 				},
// 			},
// 		},
// 		NewColName: "country_city",
// 	}

// 	records := generateTestRecords(500)
// 	finalCols := getFinalColsForGeneratedTestRecords()

// 	// Perform the value column request
// 	err := performValueColRequest(&structs.NodeResult{}, &structs.QueryAggregators{}, letColReq, records, finalCols)

// 	assert.Nil(t, err)

// 	// Check if the new column is added to the records
// 	assert.True(t, finalCols["country_city"])

// 	for _, record := range records {
// 		assert.True(t, record["country_city"] != nil)
// 		country := record["country"].(string)
// 		city := record["city"].(string)
// 		assert.Equal(t, country+":-"+city, record["country_city"])
// 	}
// }

// func Test_getColumnsToKeepAndRemove(t *testing.T) {
// 	tests := []struct {
// 		name             string
// 		cols             []string
// 		wildcardCols     []string
// 		keepMatches      bool
// 		wantIndices      []int
// 		wantColsToKeep   []string
// 		wantColsToRemove []string
// 	}{
// 		{
// 			name:             "No wildcards, keepMatches true",
// 			cols:             []string{"id", "name", "email"},
// 			wildcardCols:     []string{},
// 			keepMatches:      true,
// 			wantIndices:      []int{},
// 			wantColsToKeep:   []string{},
// 			wantColsToRemove: []string{"id", "name", "email"},
// 		},
// 		{
// 			name:             "No wildcards, keepMatches false",
// 			cols:             []string{"id", "name", "email"},
// 			wildcardCols:     []string{},
// 			keepMatches:      false,
// 			wantIndices:      []int{0, 1, 2},
// 			wantColsToKeep:   []string{"id", "name", "email"},
// 			wantColsToRemove: []string{},
// 		},
// 		{
// 			name:             "Exact match one wildcard, keepMatches true",
// 			cols:             []string{"id", "name", "email"},
// 			wildcardCols:     []string{"name"},
// 			keepMatches:      true,
// 			wantIndices:      []int{1},
// 			wantColsToKeep:   []string{"name"},
// 			wantColsToRemove: []string{"id", "email"},
// 		},
// 		{
// 			name:             "Wildcard matches multiple columns, keepMatches true",
// 			cols:             []string{"user_id", "username", "user_email", "age"},
// 			wildcardCols:     []string{"user_*"},
// 			keepMatches:      true,
// 			wantIndices:      []int{0, 2},
// 			wantColsToKeep:   []string{"user_id", "user_email"},
// 			wantColsToRemove: []string{"username", "age"},
// 		},
// 		{
// 			name:             "Wildcard matches none, keepMatches false",
// 			cols:             []string{"id", "name", "email"},
// 			wildcardCols:     []string{"user_*"},
// 			keepMatches:      false,
// 			wantIndices:      []int{0, 1, 2},
// 			wantColsToKeep:   []string{"id", "name", "email"},
// 			wantColsToRemove: []string{},
// 		},
// 		{
// 			name:             "Multiple wildcards with overlaps, keepMatches true",
// 			cols:             []string{"user_id", "admin_id", "username", "email"},
// 			wildcardCols:     []string{"user_*", "*_id"},
// 			keepMatches:      true,
// 			wantIndices:      []int{0, 1},
// 			wantColsToKeep:   []string{"user_id", "admin_id"},
// 			wantColsToRemove: []string{"username", "email"},
// 		},
// 		{
// 			name:             "Empty cols, keepMatches true",
// 			cols:             []string{},
// 			wildcardCols:     []string{"user_*"},
// 			keepMatches:      true,
// 			wantIndices:      []int{},
// 			wantColsToKeep:   []string{},
// 			wantColsToRemove: []string{},
// 		},
// 		{
// 			name:             "Wildcard matches all, keepMatches false",
// 			cols:             []string{"user_id", "user_name", "user_email"},
// 			wildcardCols:     []string{"user_*"},
// 			keepMatches:      false,
// 			wantIndices:      []int{},
// 			wantColsToKeep:   []string{},
// 			wantColsToRemove: []string{"user_id", "user_name", "user_email"},
// 		},
// 		{
// 			name:             "Complex wildcards, partial matches",
// 			cols:             []string{"user_id", "admin_id", "username", "user_profile", "user_email", "age"},
// 			wildcardCols:     []string{"user_*", "*name"},
// 			keepMatches:      true,
// 			wantIndices:      []int{0, 2, 3, 4},
// 			wantColsToKeep:   []string{"user_id", "username", "user_profile", "user_email"},
// 			wantColsToRemove: []string{"admin_id", "age"},
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			gotIndices, gotColsToKeep, gotColsToRemove := getColumnsToKeepAndRemove(tt.cols, tt.wildcardCols, tt.keepMatches)

// 			assert.Equal(t, tt.wantIndices, gotIndices)
// 			assert.Equal(t, tt.wantColsToKeep, gotColsToKeep)
// 			assert.Equal(t, tt.wantColsToRemove, gotColsToRemove)
// 		})
// 	}
// }

// func Test_performArithmeticOperation_Addition(t *testing.T) {
// 	tests := []struct {
// 		name    string
// 		left    interface{}
// 		right   interface{}
// 		want    interface{}
// 		wantErr bool
// 		errMsg  string
// 	}{
// 		{
// 			name:  "Addition of numbers",
// 			left:  5,
// 			right: 3,
// 			want:  float64(8),
// 		},
// 		{
// 			name:  "Add a string and a number",
// 			left:  2,
// 			right: "3",
// 			want:  float64(5),
// 		},
// 		{
// 			name:  "Concatenation of strings",
// 			left:  "Hello, ",
// 			right: "World!",
// 			want:  "Hello, World!",
// 		},
// 		{
// 			name:    "Adding a string and a number",
// 			left:    "Hello, ",
// 			right:   3,
// 			wantErr: true,
// 			errMsg:  "rightValue is not a string",
// 		},
// 		{
// 			name:    "Adding a number and a string",
// 			left:    3,
// 			right:   "World!",
// 			wantErr: true,
// 			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			got, err := performArithmeticOperation(tt.left, tt.right, sutils.Add)
// 			if !tt.wantErr {
// 				assert.NoError(t, err)
// 				assert.Equal(t, tt.want, got)
// 			}
// 			if tt.wantErr {
// 				assert.Equal(t, tt.errMsg, err.Error())
// 			}
// 		})
// 	}
// }

// func Test_performArithmeticOperation_Subtraction(t *testing.T) {
// 	tests := []struct {
// 		name    string
// 		left    interface{}
// 		right   interface{}
// 		want    float64
// 		wantErr bool
// 		errMsg  string
// 	}{
// 		{
// 			name:  "10-5",
// 			left:  10,
// 			right: 5,
// 			want:  5,
// 		},
// 		{
// 			name:  "-5-(-2)",
// 			left:  -5,
// 			right: -2,
// 			want:  -3,
// 		},
// 		{
// 			name:  "5.5-2.2",
// 			left:  5.5,
// 			right: 2.2,
// 			want:  3.3,
// 		},
// 		{
// 			name:  "0-5",
// 			left:  0,
// 			right: 5,
// 			want:  -5,
// 		},
// 		{
// 			name:  "Subtracting a string(number) from a string(number)",
// 			left:  "2",
// 			right: "5",
// 			want:  -3,
// 		},
// 		{
// 			name:  "Subtracting a number from a string(number)",
// 			left:  "2",
// 			right: 5,
// 			want:  -3,
// 		},
// 		{
// 			name:  "Subtracting a string from a number",
// 			left:  5,
// 			right: "2",
// 			want:  3,
// 		},
// 		{
// 			name:    "Subtracting a number from a string",
// 			left:    "Hello,",
// 			right:   5,
// 			wantErr: true,
// 			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
// 		},
// 		{
// 			name:    "Subtracting a string from a number",
// 			left:    5,
// 			right:   "World!",
// 			wantErr: true,
// 			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
// 		},
// 		{
// 			name:    "Subtracting a string from a string",
// 			left:    "Hello,",
// 			right:   "World!",
// 			wantErr: true,
// 			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			got, err := performArithmeticOperation(tt.left, tt.right, sutils.Subtract)
// 			if !tt.wantErr {
// 				assert.NoError(t, err)
// 				assert.Equal(t, tt.want, got)
// 			}
// 			if tt.wantErr {
// 				assert.Equal(t, tt.errMsg, err.Error())
// 			}
// 		})
// 	}
// }

// func Test_performArithmeticOperation_Multiplication(t *testing.T) {
// 	tests := []struct {
// 		name    string
// 		left    interface{}
// 		right   interface{}
// 		want    float64
// 		wantErr bool
// 		errMsg  string
// 	}{
// 		{
// 			name:  "3*4",
// 			left:  3,
// 			right: 4,
// 			want:  12,
// 		},
// 		{
// 			name:  "-2*3",
// 			left:  -2,
// 			right: 3,
// 			want:  -6,
// 		},
// 		{
// 			name:  "5.5*2",
// 			left:  5.5,
// 			right: 2,
// 			want:  11,
// 		},
// 		{
// 			name:  "2*(-5)",
// 			left:  2,
// 			right: -5,
// 			want:  -10,
// 		},
// 		{
// 			name:    "Multiplying a string with a number",
// 			left:    "Hello,",
// 			right:   5,
// 			wantErr: true,
// 			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
// 		},
// 		{
// 			name:    "Multiplying a number with a string",
// 			left:    5,
// 			right:   "World!",
// 			wantErr: true,
// 			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
// 		},
// 		{
// 			name:    "Multiplying a string with a string",
// 			left:    "Hello,",
// 			right:   "World!",
// 			wantErr: true,
// 			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			got, err := performArithmeticOperation(tt.left, tt.right, sutils.Multiply)
// 			if !tt.wantErr {
// 				assert.NoError(t, err)
// 				assert.Equal(t, tt.want, got)
// 			}
// 			if tt.wantErr {
// 				assert.Equal(t, tt.errMsg, err.Error())
// 			}
// 		})
// 	}
// }

// func Test_performArithmeticOperation_Division(t *testing.T) {
// 	tests := []struct {
// 		name    string
// 		left    interface{}
// 		right   interface{}
// 		want    float64
// 		wantErr bool
// 		errMsg  string
// 	}{
// 		{
// 			name:  "10/2",
// 			left:  10,
// 			right: 2,
// 			want:  5,
// 		},
// 		{
// 			name:  "5/(-1)",
// 			left:  5,
// 			right: -1,
// 			want:  -5,
// 		},
// 		{
// 			name:  "(-6)/(-3)",
// 			left:  -6,
// 			right: -3,
// 			want:  2,
// 		},
// 		{
// 			name:  "7.5/2.5",
// 			left:  7.5,
// 			right: 2.5,
// 			want:  3,
// 		},
// 		{
// 			name:    "Dividing by zero",
// 			left:    5,
// 			right:   0,
// 			wantErr: true,
// 			errMsg:  "performArithmeticOperation: cannot divide by zero",
// 		},
// 		{
// 			name:    "Dividing a string by a number",
// 			left:    "Hello,",
// 			right:   5,
// 			wantErr: true,
// 			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
// 		},
// 		{
// 			name:    "Dividing a number by a string",
// 			left:    5,
// 			right:   "World!",
// 			wantErr: true,
// 			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
// 		},
// 		{
// 			name:    "Dividing a string by a string",
// 			left:    "Hello,",
// 			right:   "World!",
// 			wantErr: true,
// 			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			got, err := performArithmeticOperation(tt.left, tt.right, sutils.Divide)
// 			if !tt.wantErr {
// 				assert.NoError(t, err)
// 				assert.Equal(t, tt.want, got)
// 			}
// 			if tt.wantErr {
// 				assert.Equal(t, tt.errMsg, err.Error())
// 			}
// 		})
// 	}
// }

// func Test_performArithmeticOperation_Modulo(t *testing.T) {
// 	tests := []struct {
// 		name    string
// 		left    interface{}
// 		right   interface{}
// 		want    int64
// 		wantErr bool
// 		errMsg  string
// 	}{
// 		{
// 			name:  "10%3",
// 			left:  10,
// 			right: 3,
// 			want:  1,
// 		},
// 		{
// 			name:  "18%7",
// 			left:  18,
// 			right: -7,
// 			want:  4,
// 		},
// 		{
// 			name:  "(-4)%3",
// 			left:  -4,
// 			right: 3,
// 			want:  -1,
// 		},
// 		{
// 			name:  "(-4)%(-3)",
// 			left:  -4,
// 			right: -3,
// 			want:  -1,
// 		},
// 		{
// 			name:    "Modulo a string by a number",
// 			left:    "Hello,",
// 			right:   5,
// 			wantErr: true,
// 			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
// 		},
// 		{
// 			name:    "Modulo a number by a string",
// 			left:    5,
// 			right:   "World!",
// 			wantErr: true,
// 			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
// 		},
// 		{
// 			name:    "Modulo a string by a string",
// 			left:    "Hello,",
// 			right:   "World!",
// 			wantErr: true,
// 			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			got, err := performArithmeticOperation(tt.left, tt.right, sutils.Modulo)
// 			if !tt.wantErr {
// 				assert.NoError(t, err)
// 				assert.Equal(t, tt.want, got)
// 			}
// 			if tt.wantErr {
// 				assert.Equal(t, tt.errMsg, err.Error())
// 			}
// 		})
// 	}
// }

// func Test_performArithmeticOperation_BitwiseAnd(t *testing.T) {
// 	tests := []struct {
// 		name    string
// 		left    interface{}
// 		right   interface{}
// 		want    int64
// 		wantErr bool
// 		errMsg  string
// 	}{
// 		{
// 			name:  "6 & 3, expect 2",
// 			left:  6,
// 			right: 3,
// 			want:  2,
// 		},
// 		{
// 			name:  "10 & 8, expect 8",
// 			left:  10,
// 			right: 8,
// 			want:  8,
// 		},
// 		{
// 			name:    "bitwise a string by a number",
// 			left:    "a",
// 			right:   5,
// 			wantErr: true,
// 			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
// 		},
// 		{
// 			name:    "bitwise a number by a string",
// 			left:    5,
// 			right:   "b",
// 			wantErr: true,
// 			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
// 		},
// 		{
// 			name:    "bitwise a string by a string",
// 			left:    "a",
// 			right:   "b",
// 			wantErr: true,
// 			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			got, err := performArithmeticOperation(tt.left, tt.right, sutils.BitwiseAnd)
// 			if !tt.wantErr {
// 				assert.NoError(t, err)
// 				assert.Equal(t, tt.want, got)
// 			}
// 			if tt.wantErr {
// 				assert.Equal(t, tt.errMsg, err.Error())
// 			}
// 		})
// 	}
// }

// func Test_performArithmeticOperation_BitwiseOr(t *testing.T) {
// 	tests := []struct {
// 		name    string
// 		left    interface{}
// 		right   interface{}
// 		want    int64
// 		wantErr bool
// 		errMsg  string
// 	}{
// 		{
// 			name:  "10|2",
// 			left:  10,
// 			right: 2,
// 			want:  10,
// 		},
// 		{
// 			name:  "1|4",
// 			left:  1,
// 			right: 4,
// 			want:  5,
// 		},
// 		{
// 			name:  "1|2",
// 			left:  1,
// 			right: 2,
// 			want:  3,
// 		},
// 		{
// 			name:    "bitwise or a string by a number",
// 			left:    "a",
// 			right:   5,
// 			wantErr: true,
// 			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
// 		},
// 		{
// 			name:    "bitwise or a number by a string",
// 			left:    5,
// 			right:   "b",
// 			wantErr: true,
// 			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
// 		},
// 		{
// 			name:    "bitwise or a string by a string",
// 			left:    "a",
// 			right:   "b",
// 			wantErr: true,
// 			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			got, err := performArithmeticOperation(tt.left, tt.right, sutils.BitwiseOr)
// 			if !tt.wantErr {
// 				assert.NoError(t, err)
// 				assert.Equal(t, tt.want, got)
// 			}
// 			if tt.wantErr {
// 				assert.Equal(t, tt.errMsg, err.Error())
// 			}
// 		})
// 	}
// }

// func Test_performArithmeticOperation_BitwiseExclusiveOr(t *testing.T) {
// 	tests := []struct {
// 		name    string
// 		left    interface{}
// 		right   interface{}
// 		want    int64
// 		wantErr bool
// 		errMsg  string
// 	}{
// 		{
// 			name:  "10^5",
// 			left:  10,
// 			right: 5,
// 			want:  15,
// 		},
// 		{
// 			name:  "3^4",
// 			left:  3,
// 			right: 4,
// 			want:  7,
// 		},
// 		{
// 			name:  "2^3",
// 			left:  2,
// 			right: 3,
// 			want:  1,
// 		},
// 		{
// 			name:    "bitwise exclusive or a string by a number",
// 			left:    "a",
// 			right:   5,
// 			wantErr: true,
// 			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
// 		},
// 		{
// 			name:    "bitwise exclusive or a number by a string",
// 			left:    5,
// 			right:   "b",
// 			wantErr: true,
// 			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
// 		},
// 		{
// 			name:    "bitwise exclusive or a string by a string",
// 			left:    "a",
// 			right:   "b",
// 			wantErr: true,
// 			errMsg:  "performArithmeticOperation: leftValue or rightValue is not a number",
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			got, err := performArithmeticOperation(tt.left, tt.right, sutils.BitwiseExclusiveOr)
// 			if !tt.wantErr {
// 				assert.NoError(t, err)
// 				assert.Equal(t, tt.want, got)
// 			}
// 			if tt.wantErr {
// 				assert.Equal(t, tt.errMsg, err.Error())
// 			}
// 		})
// 	}
// }

// func Test_performArithmeticOperation_WrongOp(t *testing.T) {
// 	_, err := performArithmeticOperation(5, 3, 100)
// 	assert.Equal(t, errors.New("performArithmeticOperation: invalid arithmetic operator"), err)
// }

// func Test_performMultiValueColRequestWithoutGroupby_OnlyDelimiter(t *testing.T) {
// 	recs := map[string]map[string]interface{}{
// 		"1": {"senders": "john@example.com,jane@example.com,doe@example.com"},
// 		"2": {"senders": "foo@example.com,,bar@example.com"},
// 	}

// 	letColReq := &structs.LetColumnsRequest{
// 		MultiValueColRequest: &structs.MultiValueColLetRequest{
// 			Command:         "makemv",
// 			ColName:         "senders",
// 			DelimiterString: ",",
// 			IsRegex:         false,
// 			AllowEmpty:      false,
// 			Setsv:           false,
// 		},
// 	}

// 	expectedResults := make(map[string]map[string][]string)
// 	expectedResults["1"] = map[string][]string{
// 		"senders": {"john@example.com", "jane@example.com", "doe@example.com"},
// 	}
// 	expectedResults["2"] = map[string][]string{
// 		"senders": {"foo@example.com", "bar@example.com"},
// 	}

// 	err := performMultiValueColRequestWithoutGroupby(letColReq, recs)
// 	assert.Nil(t, err)

// 	for recNum, rec := range recs {
// 		for colName, values := range rec {
// 			assert.Equal(t, expectedResults[recNum][colName], values)
// 		}
// 	}
// }

// func Test_performMultiValueColRequestWithoutGroupby_OnlyRegexDelimiter(t *testing.T) {
// 	recs := map[string]map[string]interface{}{
// 		"1": {"senders": "john@example.com|jane@example.com|doe@example.com"},
// 		"2": {"senders": "foo@example.com||bar@example.com"},
// 	}

// 	letColReq := &structs.LetColumnsRequest{
// 		MultiValueColRequest: &structs.MultiValueColLetRequest{
// 			Command:         "makemv",
// 			ColName:         "senders",
// 			DelimiterString: `([^|]+)\|?`,
// 			IsRegex:         true,
// 			AllowEmpty:      false,
// 			Setsv:           false,
// 		},
// 	}

// 	expectedResults := map[string]map[string][]string{
// 		"1": {"senders": {"john@example.com", "jane@example.com", "doe@example.com"}},
// 		"2": {"senders": {"foo@example.com", "bar@example.com"}},
// 	}

// 	err := performMultiValueColRequestWithoutGroupby(letColReq, recs)
// 	assert.Nil(t, err)

// 	// Verify the results
// 	for recNum, rec := range recs {
// 		for colName, values := range rec {
// 			assert.Equal(t, expectedResults[recNum][colName], values)
// 		}
// 	}
// }

// func Test_performMultiValueColRequestWithoutGroupby_DelimiterWithAllowEmptyValues(t *testing.T) {
// 	recs := map[string]map[string]interface{}{
// 		"1": {"senders": "john@example.com,jane@example.com,,doe@example.com"},
// 		"2": {"senders": ",foo@example.com,,bar@example.com,"},
// 	}

// 	letColReq := &structs.LetColumnsRequest{
// 		MultiValueColRequest: &structs.MultiValueColLetRequest{
// 			Command:         "makemv",
// 			ColName:         "senders",
// 			DelimiterString: ",",
// 			IsRegex:         false,
// 			AllowEmpty:      true,
// 			Setsv:           false,
// 		},
// 	}

// 	expectedResults := map[string]map[string][]string{
// 		"1": {"senders": {"john@example.com", "jane@example.com", "", "doe@example.com"}},
// 		"2": {"senders": {"", "foo@example.com", "", "bar@example.com", ""}},
// 	}

// 	err := performMultiValueColRequestWithoutGroupby(letColReq, recs)
// 	assert.Nil(t, err)

// 	// Verify the results
// 	for recNum, rec := range recs {
// 		for colName, values := range rec {
// 			assert.Equal(t, expectedResults[recNum][colName], values)
// 		}
// 	}
// }

// func Test_performMultiValueColRequestWithoutGroupby_Setsv(t *testing.T) {
// 	recs := map[string]map[string]interface{}{
// 		"1": {"senders": "john@example.com jane@example.com  doe@example.com"},
// 		"2": {"senders": "foo@example.com  bar@example.com"},
// 	}

// 	letColReq := &structs.LetColumnsRequest{
// 		MultiValueColRequest: &structs.MultiValueColLetRequest{
// 			Command:         "makemv",
// 			ColName:         "senders",
// 			DelimiterString: " ",
// 			IsRegex:         false,
// 			AllowEmpty:      false,
// 			Setsv:           true,
// 		},
// 	}

// 	expectedResults := map[string]map[string]string{
// 		"1": {"senders": "john@example.com jane@example.com doe@example.com"},
// 		"2": {"senders": "foo@example.com bar@example.com"},
// 	}

// 	err := performMultiValueColRequestWithoutGroupby(letColReq, recs)
// 	assert.Nil(t, err)

// 	// Verify the results
// 	for recNum, rec := range recs {
// 		for colName, value := range rec {
// 			assert.Equal(t, expectedResults[recNum][colName], value)
// 		}
// 	}
// }

// func Test_performMultiValueColRequestOnHistogram_OnlyDelimiter(t *testing.T) {

// 	nodeRes := &structs.NodeResult{
// 		Histogram: map[string]*structs.AggregationResult{
// 			"1": {
// 				Results: []*structs.BucketResult{
// 					{
// 						ElemCount: 1,
// 						StatRes:   map[string]sutils.CValueEnclosure{},
// 						BucketKey: []string{
// 							"john@example.com,jane@example.com,doe@example.com", "host@id.com",
// 						},
// 						GroupByKeys: []string{"senders", "host"},
// 					},
// 					{
// 						ElemCount: 2,
// 						StatRes:   map[string]sutils.CValueEnclosure{},
// 						BucketKey: []string{
// 							"foo@example.com,,bar@example.com", "host@id.com",
// 						},
// 						GroupByKeys: []string{"senders", "host"},
// 					},
// 				},
// 			},
// 		},
// 	}

// 	letColReq := &structs.LetColumnsRequest{
// 		MultiValueColRequest: &structs.MultiValueColLetRequest{
// 			Command:         "makemv",
// 			ColName:         "senders",
// 			DelimiterString: ",",
// 			IsRegex:         false,
// 			AllowEmpty:      false,
// 			Setsv:           false,
// 		},
// 	}

// 	expectedResults := make(map[uint64][]interface{})
// 	expectedResults[1] = []interface{}{[]string{"john@example.com", "jane@example.com", "doe@example.com"}, "host@id.com"}

// 	expectedResults[2] = []interface{}{[]string{"foo@example.com", "bar@example.com"}, "host@id.com"}

// 	err := performMultiValueColRequestOnHistogram(nodeRes, letColReq)
// 	assert.Nil(t, err)

// 	for _, bucketResult := range nodeRes.Histogram["1"].Results {
// 		assert.Equal(t, expectedResults[bucketResult.ElemCount], bucketResult.BucketKey)
// 	}
// }

// func Test_performMultiValueColRequestOnHistogram_OnlyRegexDelimiter(t *testing.T) {
// 	nodeRes := &structs.NodeResult{
// 		Histogram: map[string]*structs.AggregationResult{
// 			"1": {
// 				Results: []*structs.BucketResult{
// 					{
// 						ElemCount: 1,
// 						StatRes:   map[string]sutils.CValueEnclosure{},
// 						BucketKey: []string{
// 							"john@example.com|jane@example.com|doe@example.com", "host@id.com",
// 						},
// 						GroupByKeys: []string{"senders", "host"},
// 					},
// 					{
// 						ElemCount: 2,
// 						StatRes:   map[string]sutils.CValueEnclosure{},
// 						BucketKey: []string{
// 							"foo@example.com||bar@example.com", "host@id.com",
// 						},
// 						GroupByKeys: []string{"senders", "host"},
// 					},
// 				},
// 			},
// 		},
// 	}

// 	letColReq := &structs.LetColumnsRequest{
// 		MultiValueColRequest: &structs.MultiValueColLetRequest{
// 			Command:         "makemv",
// 			ColName:         "senders",
// 			DelimiterString: `([^|]+)\|?`,
// 			IsRegex:         true,
// 			AllowEmpty:      false,
// 			Setsv:           false,
// 		},
// 	}

// 	expectedResults := make(map[uint64][]interface{})
// 	expectedResults[1] = []interface{}{[]string{"john@example.com", "jane@example.com", "doe@example.com"}, "host@id.com"}
// 	expectedResults[2] = []interface{}{[]string{"foo@example.com", "bar@example.com"}, "host@id.com"}

// 	err := performMultiValueColRequestOnHistogram(nodeRes, letColReq)
// 	assert.Nil(t, err)

// 	for _, bucketResult := range nodeRes.Histogram["1"].Results {
// 		assert.Equal(t, expectedResults[bucketResult.ElemCount], bucketResult.BucketKey)
// 	}
// }

// func Test_performMultiValueColRequestOnHistogram_DelimiterWithAllowEmptyValues(t *testing.T) {
// 	nodeRes := &structs.NodeResult{
// 		Histogram: map[string]*structs.AggregationResult{
// 			"1": {
// 				Results: []*structs.BucketResult{
// 					{
// 						ElemCount: 1,
// 						StatRes:   map[string]sutils.CValueEnclosure{},
// 						BucketKey: []string{
// 							"john@example.com,jane@example.com,,doe@example.com", "host@id.com",
// 						},
// 						GroupByKeys: []string{"senders", "host"},
// 					},
// 					{
// 						ElemCount: 2,
// 						StatRes:   map[string]sutils.CValueEnclosure{},
// 						BucketKey: []string{
// 							",foo@example.com,,bar@example.com,", "host@id.com",
// 						},
// 						GroupByKeys: []string{"senders", "host"},
// 					},
// 				},
// 			},
// 		},
// 	}

// 	letColReq := &structs.LetColumnsRequest{
// 		MultiValueColRequest: &structs.MultiValueColLetRequest{
// 			Command:         "makemv",
// 			ColName:         "senders",
// 			DelimiterString: ",",
// 			IsRegex:         false,
// 			AllowEmpty:      true,
// 			Setsv:           false,
// 		},
// 	}

// 	expectedResults := make(map[uint64][]interface{})
// 	expectedResults[1] = []interface{}{[]string{"john@example.com", "jane@example.com", "", "doe@example.com"}, "host@id.com"}
// 	expectedResults[2] = []interface{}{[]string{"", "foo@example.com", "", "bar@example.com", ""}, "host@id.com"}

// 	err := performMultiValueColRequestOnHistogram(nodeRes, letColReq)
// 	assert.Nil(t, err)

// 	for _, bucketResult := range nodeRes.Histogram["1"].Results {
// 		assert.Equal(t, expectedResults[bucketResult.ElemCount], bucketResult.BucketKey)
// 	}
// }

// func Test_performMultiValueColRequestOnHistogram_Setsv(t *testing.T) {
// 	nodeRes := &structs.NodeResult{
// 		Histogram: map[string]*structs.AggregationResult{
// 			"1": {
// 				Results: []*structs.BucketResult{
// 					{
// 						ElemCount: 1,
// 						StatRes:   map[string]sutils.CValueEnclosure{},
// 						BucketKey: []string{
// 							"john@example.com jane@example.com  doe@example.com", "host@id.com",
// 						},
// 						GroupByKeys: []string{"senders", "host"},
// 					},
// 					{
// 						ElemCount: 2,
// 						StatRes:   map[string]sutils.CValueEnclosure{},
// 						BucketKey: []string{
// 							"foo@example.com  bar@example.com", "host@id.com",
// 						},
// 						GroupByKeys: []string{"senders", "host"},
// 					},
// 				},
// 			},
// 		},
// 	}

// 	letColReq := &structs.LetColumnsRequest{
// 		MultiValueColRequest: &structs.MultiValueColLetRequest{
// 			Command:         "makemv",
// 			ColName:         "senders",
// 			DelimiterString: " ",
// 			IsRegex:         false,
// 			AllowEmpty:      false,
// 			Setsv:           true,
// 		},
// 	}

// 	expectedResults := make(map[uint64][]interface{})
// 	expectedResults[1] = []interface{}{"john@example.com jane@example.com doe@example.com", "host@id.com"}
// 	expectedResults[2] = []interface{}{"foo@example.com bar@example.com", "host@id.com"}

// 	err := performMultiValueColRequestOnHistogram(nodeRes, letColReq)
// 	assert.Nil(t, err)

// 	for _, bucketResult := range nodeRes.Histogram["1"].Results {
// 		assert.Equal(t, expectedResults[bucketResult.ElemCount], bucketResult.BucketKey)
// 	}
// }

// func Test_findBucketMonth(t *testing.T) {
// 	testTime := time.Date(2024, time.August, 1, 12, 16, 18, 20, time.UTC)
// 	bucket := findBucketMonth(testTime, 3)
// 	expectedTime := uint64(time.Date(2024, time.July, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
// 	assert.Equal(t, expectedTime, bucket)

// 	bucket = findBucketMonth(testTime, 4)
// 	expectedTime = uint64(time.Date(2024, time.May, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
// 	assert.Equal(t, expectedTime, bucket)

// 	bucket = findBucketMonth(testTime, 6)
// 	expectedTime = uint64(time.Date(2024, time.July, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
// 	assert.Equal(t, expectedTime, bucket)

// 	bucket = findBucketMonth(testTime, 12)
// 	expectedTime = uint64(time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
// 	assert.Equal(t, expectedTime, bucket)
// }

// func Test_performBinWithSpan(t *testing.T) {
// 	spanOpt := &structs.BinSpanOptions{
// 		BinSpanLength: &structs.BinSpanLength{
// 			Num:       10,
// 			TimeScale: sutils.TMInvalid,
// 		},
// 	}

// 	val, err := performBinWithSpan(300, spanOpt)
// 	assert.Nil(t, err)
// 	assert.Equal(t, "300-310", fmt.Sprintf("%v", val))

// 	val, err = performBinWithSpan(295, spanOpt)
// 	assert.Nil(t, err)
// 	assert.Equal(t, "290-300", fmt.Sprintf("%v", val))

// 	spanOpt.BinSpanLength.TimeScale = sutils.TMSecond

// 	val, err = performBinWithSpan(300, spanOpt)
// 	assert.Nil(t, err)
// 	assert.Equal(t, "300", fmt.Sprintf("%v", val))

// 	val, err = performBinWithSpan(295, spanOpt)
// 	assert.Nil(t, err)
// 	assert.Equal(t, "290", fmt.Sprintf("%v", val))

// 	spanOpt.BinSpanLength = nil
// 	spanOpt.LogSpan = &structs.LogSpan{
// 		Base:        3,
// 		Coefficient: 2,
// 	}

// 	val, err = performBinWithSpan(301, spanOpt)
// 	assert.Nil(t, err)
// 	assert.Equal(t, "162-486", fmt.Sprintf("%v", val))

// 	val, err = performBinWithSpan(-301, spanOpt)
// 	assert.Nil(t, err)
// 	assert.Equal(t, "-301", fmt.Sprintf("%v", val))

// 	val, err = performBinWithSpan(1, spanOpt)
// 	assert.Nil(t, err)
// 	assert.Equal(t, "0.6666666666666666-2", fmt.Sprintf("%v", val))

// 	val, err = performBinWithSpan(-1, spanOpt)
// 	assert.Nil(t, err)
// 	assert.Equal(t, "-1", fmt.Sprintf("%v", val))

// 	val, err = performBinWithSpan(-0.01, spanOpt)
// 	assert.Nil(t, err)
// 	assert.Equal(t, "-0.01", fmt.Sprintf("%v", val))

// 	val, err = performBinWithSpan(0, spanOpt)
// 	assert.Nil(t, err)
// 	assert.Equal(t, "0", fmt.Sprintf("%v", val))

// 	val, err = performBinWithSpan(0.1, spanOpt)
// 	assert.Nil(t, err)
// 	assert.Equal(t, "0.07407407407407407-0.2222222222222222", fmt.Sprintf("%v", val))

// 	spanOpt.LogSpan = &structs.LogSpan{
// 		Base:        1.2,
// 		Coefficient: 1,
// 	}

// 	val, err = performBinWithSpan(500, spanOpt)
// 	assert.Nil(t, err)
// 	assert.Equal(t, "492.22352429520225-590.6682291542427", fmt.Sprintf("%v", val))
// }

// func Test_getTimeBucketWithAlign(t *testing.T) {
// 	testTime := time.Date(2024, time.July, 7, 17, 0, 35, 398*1000000, time.UTC)
// 	assert.Equal(t, 1720371635398, int(testTime.UnixMilli()))

// 	spanOpt := &structs.BinSpanOptions{
// 		BinSpanLength: &structs.BinSpanLength{
// 			Num: 2,
// 		},
// 	}

// 	val := getTimeBucketWithAlign(testTime, time.Duration(time.Second), spanOpt, nil)
// 	expectedTime := int(time.Date(2024, time.July, 7, 17, 0, 34, 0, time.UTC).UnixMilli())
// 	assert.Equal(t, expectedTime, val)

// 	alignTime := uint64(time.Date(2024, time.July, 7, 0, 0, 0, 0, time.UTC).UnixMilli())
// 	spanOpt.BinSpanLength.Num = 7

// 	val = getTimeBucketWithAlign(testTime, time.Duration(time.Minute), spanOpt, &alignTime)
// 	expectedTime = int(time.Date(2024, time.July, 7, 16, 55, 0, 0, time.UTC).UnixMilli())
// 	assert.Equal(t, expectedTime, val)

// 	alignTime = uint64(time.Date(2024, time.July, 7, 17, 0, 36, 0, time.UTC).UnixMilli())
// 	spanOpt.BinSpanLength.Num = 10

// 	val = getTimeBucketWithAlign(testTime, time.Duration(time.Second), spanOpt, &alignTime)
// 	expectedTime = int(time.Date(2024, time.July, 7, 17, 0, 26, 0, time.UTC).UnixMilli())
// 	assert.Equal(t, expectedTime, val)
// }

// func Test_findSpan(t *testing.T) {
// 	spanOpt, err := findSpan(301, 500, 100, nil, "abc")
// 	assert.Nil(t, err)
// 	assert.Equal(t, float64(10), spanOpt.BinSpanLength.Num)
// 	assert.Equal(t, sutils.TMInvalid, spanOpt.BinSpanLength.TimeScale)

// 	spanOpt, err = findSpan(301, 500, 2, nil, "abc")
// 	assert.Nil(t, err)
// 	assert.Equal(t, float64(1000), spanOpt.BinSpanLength.Num)
// 	assert.Equal(t, sutils.TMInvalid, spanOpt.BinSpanLength.TimeScale)

// 	minSpan := &structs.BinSpanLength{
// 		Num:       1001,
// 		TimeScale: sutils.TMInvalid,
// 	}

// 	spanOpt, err = findSpan(301, 500, 100, minSpan, "abc")
// 	assert.Nil(t, err)
// 	assert.Equal(t, float64(10000), spanOpt.BinSpanLength.Num)
// 	assert.Equal(t, sutils.TMInvalid, spanOpt.BinSpanLength.TimeScale)

// 	minTime := time.Date(2024, time.July, 7, 17, 0, 0, 0, time.UTC).UnixMilli()
// 	maxTime := time.Date(2024, time.July, 7, 17, 0, 35, 0, time.UTC).UnixMilli()

// 	spanOpt, err = findSpan(float64(minTime), float64(maxTime), 100, nil, "timestamp")
// 	assert.Nil(t, err)
// 	assert.Equal(t, float64(1), spanOpt.BinSpanLength.Num)
// 	assert.Equal(t, sutils.TMSecond, spanOpt.BinSpanLength.TimeScale)

// 	spanOpt, err = findSpan(float64(minTime), float64(maxTime), 10, nil, "timestamp")
// 	assert.Nil(t, err)
// 	assert.Equal(t, float64(10), spanOpt.BinSpanLength.Num)
// 	assert.Equal(t, sutils.TMSecond, spanOpt.BinSpanLength.TimeScale)

// 	minSpan.Num = 2
// 	minSpan.TimeScale = sutils.TMMinute

// 	spanOpt, err = findSpan(float64(minTime), float64(maxTime), 10, minSpan, "timestamp")
// 	assert.Nil(t, err)
// 	assert.Equal(t, float64(5), spanOpt.BinSpanLength.Num)
// 	assert.Equal(t, sutils.TMMinute, spanOpt.BinSpanLength.TimeScale)

// 	maxTime = time.Date(2024, time.July, 7, 17, 2, 35, 0, time.UTC).UnixMilli()

// 	spanOpt, err = findSpan(float64(minTime), float64(maxTime), 2, nil, "timestamp")
// 	assert.Nil(t, err)
// 	assert.Equal(t, float64(5), spanOpt.BinSpanLength.Num)
// 	assert.Equal(t, sutils.TMMinute, spanOpt.BinSpanLength.TimeScale)

// }

// func compareValues(cValue1 sutils.CValueEnclosure, cValue2 sutils.CValueEnclosure) bool {
// 	if cValue1.Dtype != cValue2.Dtype {
// 		return false
// 	}
// 	if cValue1.Dtype == sutils.SS_DT_STRING {
// 		return cValue1.CVal.(string) == cValue2.CVal.(string)
// 	}
// 	if cValue1.Dtype == sutils.SS_DT_FLOAT {
// 		return cValue1.CVal.(float64) == cValue2.CVal.(float64)
// 	}
// 	if cValue1.Dtype == sutils.SS_INVALID {
// 		return cValue1.CVal == cValue2.CVal
// 	}

// 	return false
// }

// func WindowStreamStatsHelperTest(t *testing.T, values []sutils.CValueEnclosure, ssOption *structs.StreamStatsOptions, windowSize int, timestamps []uint64, measureAggs []*structs.MeasureAggregator, expectedValues [][]sutils.CValueEnclosure, expectedValues2 [][]sutils.CValueEnclosure, expectedLen [][]int, expectedSecondaryLen [][]int) {

// 	for i, measureAgg := range measureAggs {
// 		ssResults := InitRunningStreamStatsResults(measureAgg.MeasureFunc)
// 		for j, value := range values {
// 			res, exist, err := PerformWindowStreamStatsOnSingleFunc(j, ssOption, ssResults, windowSize, measureAgg, value, timestamps[j], true, true)
// 			assert.Nil(t, err)
// 			if !ssOption.Current {
// 				if j == 0 {
// 					assert.False(t, exist)
// 					assert.True(t, compareValues(sutils.CValueEnclosure{}, res))
// 				} else {
// 					assert.True(t, exist)
// 					assert.True(t, compareValues(expectedValues[i][j-1], res))
// 				}
// 			} else {
// 				assert.True(t, exist)
// 				if measureAgg.MeasureFunc == sutils.Avg {
// 					assert.True(t, compareValues(expectedValues[i][j], res))
// 				}
// 			}
// 			assert.Equal(t, expectedLen[i][j], ssResults.Window.Len())
// 			if measureAgg.MeasureFunc == sutils.Range || measureAgg.MeasureFunc == sutils.Min || measureAgg.MeasureFunc == sutils.Max {
// 				assert.Equal(t, expectedSecondaryLen[i][j], ssResults.SecondaryWindow.Len())
// 			}
// 			if measureAgg.MeasureFunc == sutils.Avg {
// 				assert.True(t, compareValues(expectedValues2[i][j], ssResults.CurrResult))
// 			} else {
// 				assert.True(t, compareValues(expectedValues[i][j], ssResults.CurrResult))
// 			}
// 		}
// 	}
// }

// func WindowStreamStatsHelperTest2(t *testing.T, values []sutils.CValueEnclosure, ssOption *structs.StreamStatsOptions, windowSize int, timestamps []uint64, measureAggs []*structs.MeasureAggregator, expectedValues [][]sutils.CValueEnclosure, expectedLen [][]int, expectedSecondaryLen [][]int) {

// 	for i, measureAgg := range measureAggs {
// 		ssResults := InitRunningStreamStatsResults(measureAgg.MeasureFunc)
// 		for j, value := range values {
// 			res, exist, err := PerformWindowStreamStatsOnSingleFunc(j, ssOption, ssResults, windowSize, measureAgg, value, timestamps[j], true, true)
// 			assert.Nil(t, err)
// 			if !ssOption.Current {
// 				if j == 0 {
// 					assert.False(t, exist)
// 					assert.True(t, compareValues(sutils.CValueEnclosure{}, res))
// 				} else {
// 					assert.True(t, compareValues(expectedValues[i][j-1], res))
// 				}
// 			} else {
// 				assert.True(t, exist)
// 				assert.True(t, compareValues(expectedValues[i][j], res))
// 			}
// 			assert.Equal(t, expectedLen[i][j], ssResults.Window.Len())
// 			assert.Equal(t, expectedSecondaryLen[i][j], ssResults.SecondaryWindow.Len())
// 		}
// 	}
// }

// func StreamStatsValuesHelper(t *testing.T, colValues []sutils.CValueEnclosure, expectedValues [][]string, ssOption *structs.StreamStatsOptions, timestamps []uint64, windowSize int) {
// 	var result sutils.CValueEnclosure
// 	var exist bool
// 	var err error

// 	measureAgg := &structs.MeasureAggregator{
// 		MeasureFunc: sutils.Values,
// 	}

// 	ssResult := InitRunningStreamStatsResults(sutils.Values)
// 	for i, colValue := range colValues {
// 		if windowSize == 0 && ssOption.TimeWindow == nil {
// 			result, exist, err = PerformNoWindowStreamStatsOnSingleFunc(ssOption, ssResult, measureAgg, colValue, true)
// 			ssOption.NumProcessedRecords++
// 		} else {
// 			result, exist, err = PerformWindowStreamStatsOnSingleFunc(i, ssOption, ssResult, windowSize, measureAgg, colValue, timestamps[i], true, true)
// 		}

// 		assert.Nil(t, err)
// 		if !ssOption.Current {
// 			if i == 0 {
// 				assert.False(t, exist)
// 			} else {
// 				assert.True(t, exist)
// 				assert.Equal(t, sutils.SS_DT_STRING_SLICE, result.Dtype)
// 				assert.True(t, utils.CompareStringSlices(expectedValues[i-1], result.CVal.([]string)))
// 			}
// 		} else {
// 			assert.True(t, exist)
// 			assert.True(t, utils.CompareStringSlices(expectedValues[i], result.CVal.([]string)))
// 		}
// 	}
// }

// func getDtypes(len int, stringTypeIndex []int, inValidIndex []int) []sutils.SS_DTYPE {
// 	var dtypes []sutils.SS_DTYPE
// 	for i := 0; i < len; i++ {
// 		dtypes = append(dtypes, sutils.SS_DT_FLOAT)
// 	}

// 	for _, index := range stringTypeIndex {
// 		dtypes[index] = sutils.SS_DT_STRING
// 	}

// 	for _, index := range inValidIndex {
// 		dtypes[index] = sutils.SS_INVALID
// 	}

// 	return dtypes
// }

// func NoWindowStreamStatsHelperTest(t *testing.T, values []sutils.CValueEnclosure, ssOption *structs.StreamStatsOptions, measureAggs []*structs.MeasureAggregator, expectedValues [][]sutils.CValueEnclosure, expectedValues2 [][]sutils.CValueEnclosure, expectedValues3 [][]sutils.CValueEnclosure) {

// 	for i, measureAgg := range measureAggs {
// 		ssResults := InitRunningStreamStatsResults(measureAgg.MeasureFunc)
// 		ssOption.NumProcessedRecords = 0
// 		for j, value := range values {
// 			result, exist, err := PerformNoWindowStreamStatsOnSingleFunc(ssOption, ssResults, measureAgg, value, true)
// 			ssOption.NumProcessedRecords++
// 			assert.Nil(t, err)
// 			if !ssOption.Current {
// 				if j == 0 {
// 					assert.False(t, exist)
// 					assert.True(t, compareValues(sutils.CValueEnclosure{}, result))
// 				} else {
// 					assert.True(t, exist)
// 					assert.True(t, compareValues(expectedValues[i][j-1], result))
// 				}
// 			} else {
// 				assert.Equal(t, 0, ssResults.Window.Len())
// 				assert.Equal(t, 0, ssResults.SecondaryWindow.Len())
// 				if measureAgg.MeasureFunc == sutils.Avg {
// 					assert.True(t, compareValues(expectedValues[i][j], result))
// 					assert.True(t, compareValues(expectedValues2[i][j], ssResults.CurrResult))
// 				} else if measureAgg.MeasureFunc == sutils.Range {
// 					assert.True(t, compareValues(expectedValues[i][j], ssResults.CurrResult))
// 					assert.True(t, compareValues(expectedValues2[i][j], sutils.CValueEnclosure{Dtype: sutils.SS_DT_FLOAT, CVal: ssResults.RangeStat.Max}))
// 					assert.True(t, compareValues(expectedValues3[i][j], sutils.CValueEnclosure{Dtype: sutils.SS_DT_FLOAT, CVal: ssResults.RangeStat.Min}))
// 				} else {
// 					assert.True(t, compareValues(expectedValues[i][j], ssResults.CurrResult))
// 				}
// 			}
// 		}
// 	}
// }

// func NoWindowStreamStatsHelperTest2(t *testing.T, values []sutils.CValueEnclosure, ssOption *structs.StreamStatsOptions, measureAggs []*structs.MeasureAggregator, expectedValues [][]sutils.CValueEnclosure) {

// 	for i, measureAgg := range measureAggs {
// 		ssResults := InitRunningStreamStatsResults(measureAgg.MeasureFunc)
// 		ssOption.NumProcessedRecords = 0
// 		for j, value := range values {
// 			result, exist, err := PerformNoWindowStreamStatsOnSingleFunc(ssOption, ssResults, measureAgg, value, true)
// 			ssOption.NumProcessedRecords++
// 			assert.Nil(t, err)
// 			if !ssOption.Current {
// 				if j == 0 {
// 					assert.False(t, exist)
// 					assert.True(t, compareValues(sutils.CValueEnclosure{}, result))
// 				} else {
// 					assert.True(t, compareValues(expectedValues[i][j-1], result))
// 				}
// 			} else {
// 				assert.Equal(t, 0, ssResults.Window.Len())
// 				assert.Equal(t, 0, ssResults.SecondaryWindow.Len())
// 				assert.True(t, compareValues(expectedValues[i][j], result))
// 			}
// 		}
// 	}
// }

// func getMeasureAggs(measureFuncs []sutils.AggregateFunctions) []*structs.MeasureAggregator {
// 	var measureAggs []*structs.MeasureAggregator
// 	for _, measureFunc := range measureFuncs {
// 		measureAggs = append(measureAggs, &structs.MeasureAggregator{
// 			MeasureFunc: measureFunc,
// 		})
// 	}
// 	return measureAggs

// }

// func createCValues(values []interface{}, dtypes []sutils.SS_DTYPE) []sutils.CValueEnclosure {
// 	var expectedValues []sutils.CValueEnclosure
// 	for i, value := range values {
// 		if dtypes[i] == sutils.SS_DT_FLOAT {
// 			floatVal, _ := strconv.ParseFloat(fmt.Sprintf("%v", value), 64)
// 			expectedValues = append(expectedValues, sutils.CValueEnclosure{Dtype: sutils.SS_DT_FLOAT, CVal: floatVal})
// 		} else if dtypes[i] == sutils.SS_INVALID {
// 			expectedValues = append(expectedValues, sutils.CValueEnclosure{Dtype: sutils.SS_INVALID, CVal: nil})
// 		} else {
// 			expectedValues = append(expectedValues, sutils.CValueEnclosure{Dtype: dtypes[i], CVal: value})
// 		}
// 	}
// 	return expectedValues
// }

// func Test_PerformWindowStreamStatsOnSingleFunc(t *testing.T) {
// 	ssOption := &structs.StreamStatsOptions{
// 		Current: true,
// 		Global:  true,
// 	}

// 	windowSize := 3
// 	expectedLen := []int{1, 2, 3, 3, 3, 3, 3, 3, 3, 3}
// 	expectedMaxLen := []int{1, 2, 2, 2, 1, 2, 1, 2, 1, 2}
// 	expectedMinLen := []int{1, 1, 2, 1, 2, 2, 2, 2, 2, 1}
// 	expectedNilLen := []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

// 	expectedValuesCount := []interface{}{1, 2, 3, 3, 3, 3, 3, 3, 3, 3}
// 	values := []interface{}{7, 2, 5, 1, 6, 3, 8, 4, 9, 2}
// 	expectedValuesSum := []interface{}{7, 9, 14, 8, 12, 10, 17, 15, 21, 15}
// 	expectedValuesAvg := []interface{}{7, 4.5, 4.666666666666667, 2.6666666666666665, 4, 3.3333333333333335, 5.666666666666667, 5, 7, 5}
// 	expectedValuesMax := []interface{}{7, 7, 7, 5, 6, 6, 8, 8, 9, 9}
// 	expectedValuesMin := []interface{}{7, 2, 2, 1, 1, 1, 3, 3, 4, 2}
// 	expectedValuesRange := []interface{}{0, 5, 5, 4, 5, 5, 5, 5, 5, 7}
// 	expectedValuesCardinality := []interface{}{1, 2, 3, 3, 3, 3, 3, 3, 3, 3}

// 	dtypes := getDtypes(len(values), []int{}, []int{})

// 	measureFunctions := []sutils.AggregateFunctions{
// 		sutils.Count,
// 		sutils.Sum,
// 		sutils.Avg,
// 		sutils.Max,
// 		sutils.Min,
// 		sutils.Range,
// 		sutils.Cardinality,
// 	}
// 	measureAggs := getMeasureAggs(measureFunctions)

// 	expectedFuncValues := [][]sutils.CValueEnclosure{
// 		createCValues(expectedValuesCount, dtypes),
// 		createCValues(expectedValuesSum, dtypes),
// 		createCValues(expectedValuesAvg, dtypes),
// 		createCValues(expectedValuesMax, dtypes),
// 		createCValues(expectedValuesMin, dtypes),
// 		createCValues(expectedValuesRange, dtypes),
// 		createCValues(expectedValuesCardinality, dtypes),
// 	}
// 	expectedFuncValues2 := [][]sutils.CValueEnclosure{
// 		createCValues(expectedValuesCount, dtypes),
// 		createCValues(expectedValuesSum, dtypes),
// 		createCValues(expectedValuesSum, dtypes),
// 		createCValues(expectedValuesMax, dtypes),
// 		createCValues(expectedValuesMin, dtypes),
// 		createCValues(expectedValuesRange, dtypes),
// 		createCValues(expectedValuesCardinality, dtypes),
// 	}
// 	expectedPrimaryLen := [][]int{expectedLen, expectedLen, expectedLen, expectedMaxLen, expectedMinLen, expectedMaxLen, expectedLen}
// 	expectedSecondaryLen := [][]int{expectedNilLen, expectedNilLen, expectedNilLen, expectedNilLen, expectedNilLen, expectedMinLen, expectedNilLen}
// 	timestamps := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

// 	optionValue := []bool{true, false}

// 	for i := 0; i < len(optionValue); i++ {
// 		for j := 0; j < len(optionValue); j++ {
// 			ssOption.Current = optionValue[i]
// 			ssOption.Global = optionValue[j]

// 			WindowStreamStatsHelperTest(t, createCValues(values, dtypes), ssOption, windowSize, timestamps, measureAggs, expectedFuncValues, expectedFuncValues2, expectedPrimaryLen, expectedSecondaryLen)
// 		}
// 	}
// }

// func Test_Time_Window(t *testing.T) {
// 	ssOption := &structs.StreamStatsOptions{
// 		Current: true,
// 		Global:  true,
// 		TimeWindow: &structs.BinSpanLength{
// 			Num:       2,
// 			TimeScale: sutils.TMSecond,
// 		},
// 	}

// 	windowSize := 3
// 	expectedLen := []int{1, 2, 3, 1, 2, 3, 3, 1, 2, 1}
// 	expectedMaxLen := []int{1, 2, 2, 1, 1, 2, 1, 1, 1, 1}
// 	expectedMinLen := []int{1, 1, 2, 1, 2, 2, 2, 1, 2, 1}
// 	expectedNilLen := []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

// 	expectedValuesCount := []interface{}{1, 2, 3, 1, 2, 3, 3, 1, 2, 1}
// 	values := []interface{}{7, 2, 5, 1, 6, 3, 8, 4, 9, 2}
// 	expectedValuesSum := []interface{}{7, 9, 14, 1, 7, 10, 17, 4, 13, 2}
// 	expectedValuesAvg := []interface{}{7, 4.5, 4.666666666666667, 1, 3.5, 3.3333333333333335, 5.666666666666667, 4, 6.5, 2}
// 	expectedValuesMax := []interface{}{7, 7, 7, 1, 6, 6, 8, 4, 9, 2}
// 	expectedValuesMin := []interface{}{7, 2, 2, 1, 1, 1, 3, 4, 4, 2}
// 	expectedValuesRange := []interface{}{0, 5, 5, 0, 5, 5, 5, 0, 5, 0}
// 	expectedValuesCardinality := []interface{}{1, 2, 3, 1, 2, 3, 3, 1, 2, 1}

// 	timestamps := []uint64{1000, 2000, 3000, 6000, 6500, 7100, 7300, 10000, 11500, 20000}

// 	dtypes := getDtypes(len(values), []int{}, []int{})

// 	measureFunctions := []sutils.AggregateFunctions{
// 		sutils.Count,
// 		sutils.Sum,
// 		sutils.Avg,
// 		sutils.Max,
// 		sutils.Min,
// 		sutils.Range,
// 		sutils.Cardinality,
// 	}

// 	measureAggs := getMeasureAggs(measureFunctions)

// 	expectedFuncValues := [][]sutils.CValueEnclosure{
// 		createCValues(expectedValuesCount, dtypes),
// 		createCValues(expectedValuesSum, dtypes),
// 		createCValues(expectedValuesAvg, dtypes),
// 		createCValues(expectedValuesMax, dtypes),
// 		createCValues(expectedValuesMin, dtypes),
// 		createCValues(expectedValuesRange, dtypes),
// 		createCValues(expectedValuesCardinality, dtypes),
// 	}
// 	expectedFuncValues2 := [][]sutils.CValueEnclosure{
// 		createCValues(expectedValuesCount, dtypes),
// 		createCValues(expectedValuesSum, dtypes),
// 		createCValues(expectedValuesSum, dtypes),
// 		createCValues(expectedValuesMax, dtypes),
// 		createCValues(expectedValuesMin, dtypes),
// 		createCValues(expectedValuesRange, dtypes),
// 		createCValues(expectedValuesCardinality, dtypes),
// 	}

// 	expectedPrimaryLen := [][]int{expectedLen, expectedLen, expectedLen, expectedMaxLen, expectedMinLen, expectedMaxLen, expectedLen}
// 	expectedSecondaryLen := [][]int{expectedNilLen, expectedNilLen, expectedNilLen, expectedNilLen, expectedNilLen, expectedMinLen, expectedNilLen}

// 	WindowStreamStatsHelperTest(t, createCValues(values, dtypes), ssOption, windowSize, timestamps, measureAggs, expectedFuncValues, expectedFuncValues2, expectedPrimaryLen, expectedSecondaryLen)
// }

// func Test_NoWindow_StreamStats(t *testing.T) {
// 	ssOption := &structs.StreamStatsOptions{
// 		Current: true,
// 		Global:  true,
// 	}

// 	expectedValuesCount := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
// 	values := []interface{}{7, 2, 5, 1, 6, 3, 8, 4, 9, 2}
// 	expectedValuesSum := []interface{}{7, 9, 14, 15, 21, 24, 32, 36, 45, 47}
// 	expectedValuesAvg := []interface{}{7, 4.5, 4.666666666666667, 3.75, 4.2, 4, 4.571428571428571, 4.5, 5, 4.7}
// 	expectedValuesMax := []interface{}{7, 7, 7, 7, 7, 7, 8, 8, 9, 9}
// 	expectedValuesMin := []interface{}{7, 2, 2, 1, 1, 1, 1, 1, 1, 1}
// 	expectedValuesRange := []interface{}{0, 5, 5, 6, 6, 6, 7, 7, 8, 8}
// 	expectedValuesCardinality := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 9}

// 	measureFunctions := []sutils.AggregateFunctions{sutils.Count, sutils.Sum, sutils.Avg, sutils.Max, sutils.Min, sutils.Range, sutils.Cardinality}
// 	measureAggs := getMeasureAggs(measureFunctions)

// 	dtypes := getDtypes(len(values), []int{}, []int{})

// 	expectedFuncValues := [][]sutils.CValueEnclosure{
// 		createCValues(expectedValuesCount, dtypes),
// 		createCValues(expectedValuesSum, dtypes),
// 		createCValues(expectedValuesAvg, dtypes),
// 		createCValues(expectedValuesMax, dtypes),
// 		createCValues(expectedValuesMin, dtypes),
// 		createCValues(expectedValuesRange, dtypes),
// 		createCValues(expectedValuesCardinality, dtypes),
// 	}

// 	expectedFuncValues2 := [][]sutils.CValueEnclosure{
// 		createCValues(expectedValuesCount, dtypes),
// 		createCValues(expectedValuesSum, dtypes),
// 		createCValues(expectedValuesSum, dtypes),
// 		createCValues(expectedValuesMax, dtypes),
// 		createCValues(expectedValuesMin, dtypes),
// 		createCValues(expectedValuesMax, dtypes),
// 		createCValues(expectedValuesCardinality, dtypes),
// 	}

// 	expectedFuncValues3 := [][]sutils.CValueEnclosure{
// 		createCValues(expectedValuesCount, dtypes),
// 		createCValues(expectedValuesSum, dtypes),
// 		createCValues(expectedValuesSum, dtypes),
// 		createCValues(expectedValuesMax, dtypes),
// 		createCValues(expectedValuesMin, dtypes),
// 		createCValues(expectedValuesMin, dtypes),
// 		createCValues(expectedValuesCardinality, dtypes),
// 	}

// 	optionValue := []bool{true, false}

// 	for i := 0; i < len(optionValue); i++ {
// 		for j := 0; j < len(optionValue); j++ {
// 			ssOption.Current = optionValue[i]
// 			ssOption.Global = optionValue[j]

// 			NoWindowStreamStatsHelperTest(t, createCValues(values, dtypes), ssOption, measureAggs, expectedFuncValues, expectedFuncValues2, expectedFuncValues3)
// 		}
// 	}
// }

// func Test_Cardinality(t *testing.T) {
// 	values := []interface{}{7, 2, 2, 2, 1, 2, 3, 2, 4, 5}
// 	timestamps := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
// 	ssOption := &structs.StreamStatsOptions{
// 		Current: true,
// 		Global:  true,
// 	}
// 	dtypes := getDtypes(len(values), []int{}, []int{})

// 	windowSize := 3
// 	expectedLen := []int{1, 2, 3, 3, 3, 3, 3, 3, 3, 3}
// 	expectedNilLen := []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
// 	expectedPrimaryLen := [][]int{expectedLen}
// 	expectedSecondaryLen := [][]int{expectedNilLen}
// 	expectedValuesCardinality := []interface{}{1, 2, 2, 1, 2, 2, 3, 2, 3, 3}
// 	expectedValuesNoWindowCardinality := []interface{}{1, 2, 2, 2, 3, 3, 4, 4, 5, 6}
// 	measureAggs := []*structs.MeasureAggregator{
// 		{
// 			MeasureFunc: sutils.Cardinality,
// 		},
// 	}
// 	expectedFuncValues := [][]sutils.CValueEnclosure{createCValues(expectedValuesCardinality, dtypes)}
// 	expectedNoWindowFuncValues := [][]sutils.CValueEnclosure{createCValues(expectedValuesNoWindowCardinality, dtypes)}
// 	CValues := createCValues(values, dtypes)

// 	optionSettings := []bool{true, false}

// 	for i := 0; i < len(optionSettings); i++ {
// 		for j := 0; j < len(optionSettings); j++ {
// 			ssOption.Current = optionSettings[i]
// 			ssOption.Global = optionSettings[j]

// 			WindowStreamStatsHelperTest(t, CValues, ssOption, windowSize, timestamps, measureAggs, expectedFuncValues, expectedFuncValues, expectedPrimaryLen, expectedSecondaryLen)
// 			NoWindowStreamStatsHelperTest(t, CValues, ssOption, measureAggs, expectedNoWindowFuncValues, expectedNoWindowFuncValues, expectedNoWindowFuncValues)
// 		}
// 	}
// }

// func createExpectedStrings(collection []interface{}) []string {
// 	uniqueStr := make(map[string]struct{}, 0)
// 	result := []string{}
// 	for _, value := range collection {
// 		valueStr := fmt.Sprintf("%v", value)
// 		uniqueStr[valueStr] = struct{}{}
// 	}
// 	for key := range uniqueStr {
// 		result = append(result, key)
// 	}
// 	sort.Strings(result)
// 	return result
// }

// func Test_Values(t *testing.T) {
// 	values := []interface{}{1, "abc", "abc", "Abc", 10, "DEF", 9, "def", 21, "Ab2"}
// 	dtypes := getDtypes(len(values), []int{1, 2, 3, 5, 7, 9}, []int{})
// 	expectedValuesWindow := [][]string{}
// 	expectedValuesNoWindow := [][]string{}
// 	timestamps := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
// 	CValues := createCValues(values, dtypes)
// 	windowSize := 3
// 	for i := range values {
// 		if i < windowSize {
// 			expectedValuesWindow = append(expectedValuesWindow, createExpectedStrings(values[:i+1]))
// 		} else {
// 			expectedValuesWindow = append(expectedValuesWindow, createExpectedStrings(values[i-windowSize+1:i+1]))
// 		}
// 	}
// 	for i := range values {
// 		expectedValuesNoWindow = append(expectedValuesNoWindow, createExpectedStrings(values[:i+1]))
// 	}

// 	ssOption := &structs.StreamStatsOptions{
// 		Current: true,
// 		Global:  true,
// 	}

// 	optionValue := []bool{true, false}

// 	for i := 0; i < len(optionValue); i++ {
// 		for j := 0; j < len(optionValue); j++ {
// 			ssOption.Current = optionValue[i]
// 			ssOption.Global = optionValue[j]

// 			StreamStatsValuesHelper(t, CValues, expectedValuesWindow, ssOption, timestamps, windowSize)
// 			StreamStatsValuesHelper(t, CValues, expectedValuesNoWindow, ssOption, timestamps, 0)
// 		}
// 	}
// }

// func Test_PerformWindowStreamStatsOnSingleFunc_2(t *testing.T) {
// 	ssOption := &structs.StreamStatsOptions{
// 		Current: true,
// 		Global:  true,
// 	}

// 	windowSize := 3
// 	expectedLen := []int{1, 2, 3, 3, 3, 3, 3, 3, 3, 3}
// 	expectedSumLen := []int{1, 2, 2, 2, 1, 2, 1, 1, 1}
// 	expectedMaxLen := []int{1, 1, 1, 1, 1, 2, 1, 1, 1}
// 	expectedMaxSecondaryLen := []int{0, 0, 1, 1, 1, 1, 2, 1, 1}
// 	expectedMinLen := []int{1, 2, 2, 2, 1, 1, 1, 1, 1}
// 	expectedMinSecondaryLen := []int{0, 0, 1, 1, 2, 1, 1, 2, 2}
// 	expectedNilLen := []int{0, 0, 0, 0, 0, 0, 0, 0, 0}

// 	values := []interface{}{-3, 11, "ABC", 100, "Def", -1, "DEF", "abc", 20}
// 	dtypesValues := getDtypes(len(values), []int{2, 4, 6, 7}, []int{})
// 	expectedValuesCount := []interface{}{1, 2, 3, 3, 3, 3, 3, 3, 3}
// 	dtypesFloats := getDtypes(len(expectedValuesCount), []int{}, []int{})
// 	expectedValuesSum := []interface{}{-3, 8, 8, 111, 100, 99, -1, -1, 20}
// 	expectedValuesAvg := []interface{}{-3, 4, 4, 55.5, 100, 49.5, -1, -1, 20}
// 	expectedValuesMax := []interface{}{-3, 11, 11, 100, 100, 100, -1, -1, 20}
// 	expectedValuesMin := []interface{}{-3, -3, -3, 11, 100, -1, -1, -1, 20}
// 	expectedValuesRange := []interface{}{0, 14, 14, 89, 0, 101, 0, 0, 0}
// 	expectedValuesCardinality := []interface{}{1, 2, 3, 3, 3, 3, 3, 3, 3}

// 	measureFunctions := []sutils.AggregateFunctions{
// 		sutils.Count,
// 		sutils.Sum,
// 		sutils.Avg,
// 		sutils.Max,
// 		sutils.Min,
// 		sutils.Range,
// 		sutils.Cardinality,
// 	}
// 	measureAggs := getMeasureAggs(measureFunctions)

// 	expectedFuncValues := [][]sutils.CValueEnclosure{
// 		createCValues(expectedValuesCount, dtypesFloats),
// 		createCValues(expectedValuesSum, dtypesFloats),
// 		createCValues(expectedValuesAvg, dtypesFloats),
// 		createCValues(expectedValuesMax, dtypesFloats),
// 		createCValues(expectedValuesMin, dtypesFloats),
// 		createCValues(expectedValuesRange, dtypesFloats),
// 		createCValues(expectedValuesCardinality, dtypesFloats),
// 	}
// 	expectedFuncValues2 := [][]sutils.CValueEnclosure{
// 		createCValues(expectedValuesCount, dtypesFloats),
// 		createCValues(expectedValuesSum, dtypesFloats),
// 		createCValues(expectedValuesSum, dtypesFloats),
// 		createCValues(expectedValuesMax, dtypesFloats),
// 		createCValues(expectedValuesMin, dtypesFloats),
// 		createCValues(expectedValuesRange, dtypesFloats),
// 		createCValues(expectedValuesCardinality, dtypesFloats),
// 	}
// 	expectedPrimaryLen := [][]int{expectedLen, expectedSumLen, expectedSumLen, expectedMaxLen, expectedMinLen, expectedMaxLen, expectedLen}
// 	expectedSecondaryLen := [][]int{expectedNilLen, expectedNilLen, expectedNilLen, expectedMaxSecondaryLen, expectedMinSecondaryLen, expectedMinLen, expectedNilLen}
// 	timestamps := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

// 	optionValue := []bool{true, false}

// 	for i := 0; i < len(optionValue); i++ {
// 		for j := 0; j < len(optionValue); j++ {
// 			ssOption.Current = optionValue[i]
// 			ssOption.Global = optionValue[j]

// 			WindowStreamStatsHelperTest(t, createCValues(values, dtypesValues), ssOption, windowSize, timestamps, measureAggs, expectedFuncValues, expectedFuncValues2, expectedPrimaryLen, expectedSecondaryLen)
// 		}
// 	}
// }

// func Test_PerformWindowStreamStatsOnSingleFunc_3(t *testing.T) {
// 	ssOption := &structs.StreamStatsOptions{
// 		Current: true,
// 		Global:  true,
// 	}

// 	windowSize := 3
// 	expectedLen := []int{1, 2, 3, 3, 3, 3, 3, 3, 3, 3}
// 	expectedSumLen := []int{1, 2, 2, 1, 0, 1, 1, 2, 2}

// 	expectedMaxLen := []int{1, 2, 2, 1, 0, 1, 1, 2, 1}
// 	expectedMaxSecondaryLen := []int{0, 0, 1, 1, 2, 2, 1, 1, 1}

// 	expectedMinLen := []int{1, 1, 1, 1, 0, 1, 1, 1, 2}
// 	expectedMinSecondaryLen := []int{0, 0, 1, 2, 2, 1, 2, 1, 1}

// 	expectedNilLen := []int{0, 0, 0, 0, 0, 0, 0, 0, 0}

// 	values := []interface{}{11, -3, "ABC", "Def", "DEF", 100, "abc", -1, 20}
// 	dtypesValues := getDtypes(len(values), []int{2, 3, 4, 6}, []int{})
// 	expectedValuesCount := []interface{}{1, 2, 3, 3, 3, 3, 3, 3, 3}
// 	dtypesFloats := getDtypes(len(expectedValuesCount), []int{}, []int{})
// 	dtypesSum := getDtypes(len(expectedValuesCount), []int{}, []int{4})
// 	expectedValuesSum := []interface{}{11, 8, 8, -3, nil, 100, 100, 99, 19}
// 	expectedValuesAvg := []interface{}{11, 4, 4, -3, nil, 100, 100, 49.5, 9.5}
// 	expectedValuesMax := []interface{}{11, 11, 11, -3, "Def", 100, 100, 100, 20}
// 	dtypesMax := getDtypes(len(expectedValuesSum), []int{4}, []int{})
// 	expectedValuesMin := []interface{}{11, -3, -3, -3, "ABC", 100, 100, -1, -1}
// 	expectedValuesRange := []interface{}{0, 14, 14, 0, nil, 0, 0, 101, 21}
// 	expectedValuesCardinality := []interface{}{1, 2, 3, 3, 3, 3, 3, 3, 3}

// 	measureFunctions := []sutils.AggregateFunctions{
// 		sutils.Count,
// 		sutils.Sum,
// 		sutils.Avg,
// 		sutils.Max,
// 		sutils.Min,
// 		sutils.Range,
// 		sutils.Cardinality,
// 	}
// 	measureAggs := getMeasureAggs(measureFunctions)

// 	expectedFuncValues := [][]sutils.CValueEnclosure{
// 		createCValues(expectedValuesCount, dtypesFloats),
// 		createCValues(expectedValuesSum, dtypesSum),
// 		createCValues(expectedValuesAvg, dtypesSum),
// 		createCValues(expectedValuesMax, dtypesMax),
// 		createCValues(expectedValuesMin, dtypesMax),
// 		createCValues(expectedValuesRange, dtypesSum),
// 		createCValues(expectedValuesCardinality, dtypesFloats),
// 	}

// 	expectedPrimaryLen := [][]int{expectedLen, expectedSumLen, expectedSumLen, expectedMaxLen, expectedMinLen, expectedMaxLen, expectedLen}
// 	expectedSecondaryLen := [][]int{expectedNilLen, expectedNilLen, expectedNilLen, expectedMaxSecondaryLen, expectedMinSecondaryLen, expectedMinLen, expectedNilLen}
// 	timestamps := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

// 	optionValue := []bool{true, false}

// 	for i := 0; i < len(optionValue); i++ {
// 		for j := 0; j < len(optionValue); j++ {
// 			ssOption.Current = optionValue[i]
// 			ssOption.Global = optionValue[j]

// 			WindowStreamStatsHelperTest2(t, createCValues(values, dtypesValues), ssOption, windowSize, timestamps, measureAggs, expectedFuncValues, expectedPrimaryLen, expectedSecondaryLen)
// 		}
// 	}
// }

// func Test_Time_Window_2(t *testing.T) {
// 	ssOption := &structs.StreamStatsOptions{
// 		Current: true,
// 		Global:  true,
// 		TimeWindow: &structs.BinSpanLength{
// 			Num:       2,
// 			TimeScale: sutils.TMSecond,
// 		},
// 	}

// 	windowSize := 3
// 	expectedLen := []int{1, 2, 3, 1, 2, 3, 1, 2, 3}
// 	expectedSumLen := []int{1, 2, 2, 0, 0, 1, 0, 1, 2}

// 	expectedMaxLen := []int{1, 2, 2, 0, 0, 1, 0, 1, 1}
// 	expectedMaxSecondaryLen := []int{0, 0, 1, 1, 2, 2, 1, 1, 1}

// 	expectedMinLen := []int{1, 1, 1, 0, 0, 1, 0, 1, 2}
// 	expectedMinSecondaryLen := []int{0, 0, 1, 1, 1, 1, 1, 1, 1}

// 	expectedNilLen := []int{0, 0, 0, 0, 0, 0, 0, 0, 0}

// 	values := []interface{}{11, -3, "ABC", "Def", "DEF", 100, "abc", -1, 20}
// 	dtypesValues := getDtypes(len(values), []int{2, 3, 4, 6}, []int{})
// 	expectedValuesCount := []interface{}{1, 2, 3, 1, 2, 3, 1, 2, 3}
// 	dtypesFloats := getDtypes(len(expectedValuesCount), []int{}, []int{})
// 	dtypesSum := getDtypes(len(expectedValuesCount), []int{}, []int{3, 4, 6})
// 	expectedValuesSum := []interface{}{11, 8, 8, nil, nil, 100, nil, -1, 19}
// 	expectedValuesAvg := []interface{}{11, 4, 4, nil, nil, 100, nil, -1, 9.5}
// 	expectedValuesMax := []interface{}{11, 11, 11, "Def", "Def", 100, "abc", -1, 20}
// 	dtypesMax := getDtypes(len(expectedValuesSum), []int{3, 4, 6}, []int{})
// 	expectedValuesMin := []interface{}{11, -3, -3, "Def", "DEF", 100, "abc", -1, -1}
// 	expectedValuesRange := []interface{}{0, 14, 14, nil, nil, 0, nil, 0, 21}
// 	expectedValuesCardinality := []interface{}{1, 2, 3, 1, 2, 3, 1, 2, 3}

// 	timestamps := []uint64{1000, 2000, 3000, 6000, 6500, 7100, 10000, 11500, 12000}

// 	measureFunctions := []sutils.AggregateFunctions{
// 		sutils.Count,
// 		sutils.Sum,
// 		sutils.Avg,
// 		sutils.Max,
// 		sutils.Min,
// 		sutils.Range,
// 		sutils.Cardinality,
// 	}
// 	measureAggs := getMeasureAggs(measureFunctions)

// 	expectedFuncValues := [][]sutils.CValueEnclosure{
// 		createCValues(expectedValuesCount, dtypesFloats),
// 		createCValues(expectedValuesSum, dtypesSum),
// 		createCValues(expectedValuesAvg, dtypesSum),
// 		createCValues(expectedValuesMax, dtypesMax),
// 		createCValues(expectedValuesMin, dtypesMax),
// 		createCValues(expectedValuesRange, dtypesSum),
// 		createCValues(expectedValuesCardinality, dtypesFloats),
// 	}

// 	expectedPrimaryLen := [][]int{expectedLen, expectedSumLen, expectedSumLen, expectedMaxLen, expectedMinLen, expectedMaxLen, expectedLen}
// 	expectedSecondaryLen := [][]int{expectedNilLen, expectedNilLen, expectedNilLen, expectedMaxSecondaryLen, expectedMinSecondaryLen, expectedMinLen, expectedNilLen}

// 	WindowStreamStatsHelperTest2(t, createCValues(values, dtypesValues), ssOption, windowSize, timestamps, measureAggs, expectedFuncValues, expectedPrimaryLen, expectedSecondaryLen)
// }

// func Test_NoWindow_StreamStats_2(t *testing.T) {
// 	ssOption := &structs.StreamStatsOptions{
// 		Current: true,
// 		Global:  true,
// 	}

// 	values := []interface{}{-3, 11, "ABC", 100, "Def", -1, "DEF", "abc", 20}
// 	dtypesValues := getDtypes(len(values), []int{2, 4, 6, 7}, []int{})
// 	expectedValuesCount := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9}
// 	dtypesFloats := getDtypes(len(expectedValuesCount), []int{}, []int{})
// 	expectedValuesSum := []interface{}{-3, 8, 8, 108, 108, 107, 107, 107, 127}
// 	expectedValuesAvg := []interface{}{-3, 4, 4, 36, 36, 26.75, 26.75, 26.75, 25.4}
// 	expectedValuesMax := []interface{}{-3, 11, 11, 100, 100, 100, 100, 100, 100}
// 	expectedValuesMin := []interface{}{-3, -3, -3, -3, -3, -3, -3, -3, -3}
// 	expectedValuesRange := []interface{}{0, 14, 14, 103, 103, 103, 103, 103, 103}
// 	expectedValuesCardinality := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9}

// 	measureFunctions := []sutils.AggregateFunctions{
// 		sutils.Count,
// 		sutils.Sum,
// 		sutils.Avg,
// 		sutils.Max,
// 		sutils.Min,
// 		sutils.Range,
// 		sutils.Cardinality,
// 	}
// 	measureAggs := getMeasureAggs(measureFunctions)

// 	expectedFuncValues := [][]sutils.CValueEnclosure{
// 		createCValues(expectedValuesCount, dtypesFloats),
// 		createCValues(expectedValuesSum, dtypesFloats),
// 		createCValues(expectedValuesAvg, dtypesFloats),
// 		createCValues(expectedValuesMax, dtypesFloats),
// 		createCValues(expectedValuesMin, dtypesFloats),
// 		createCValues(expectedValuesRange, dtypesFloats),
// 		createCValues(expectedValuesCardinality, dtypesFloats),
// 	}
// 	expectedFuncValues2 := [][]sutils.CValueEnclosure{
// 		createCValues(expectedValuesCount, dtypesFloats),
// 		createCValues(expectedValuesSum, dtypesFloats),
// 		createCValues(expectedValuesSum, dtypesFloats),
// 		createCValues(expectedValuesMax, dtypesFloats),
// 		createCValues(expectedValuesMin, dtypesFloats),
// 		createCValues(expectedValuesMax, dtypesFloats),
// 		createCValues(expectedValuesCardinality, dtypesFloats),
// 	}
// 	expectedFuncValues3 := [][]sutils.CValueEnclosure{
// 		createCValues(expectedValuesCount, dtypesFloats),
// 		createCValues(expectedValuesSum, dtypesFloats),
// 		createCValues(expectedValuesSum, dtypesFloats),
// 		createCValues(expectedValuesMax, dtypesFloats),
// 		createCValues(expectedValuesMin, dtypesFloats),
// 		createCValues(expectedValuesMin, dtypesFloats),
// 		createCValues(expectedValuesCardinality, dtypesFloats),
// 	}

// 	optionValue := []bool{true, false}
// 	cValues := createCValues(values, dtypesValues)

// 	for i := 0; i < len(optionValue); i++ {
// 		for j := 0; j < len(optionValue); j++ {
// 			ssOption.Current = optionValue[i]
// 			ssOption.Global = optionValue[j]
// 			NoWindowStreamStatsHelperTest(t, cValues, ssOption, measureAggs, expectedFuncValues, expectedFuncValues2, expectedFuncValues3)
// 		}
// 	}
// }

// func Test_NoWindow_StreamStats_3(t *testing.T) {
// 	ssOption := &structs.StreamStatsOptions{
// 		Current: true,
// 		Global:  true,
// 	}

// 	values := []interface{}{"ABC", "Def", -1, 100, 11, -3, "DEF", "abc", 20}
// 	dtypesValues := getDtypes(len(values), []int{0, 1, 6, 7}, []int{})
// 	expectedValuesCount := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9}
// 	dtypesFloats := getDtypes(len(expectedValuesCount), []int{}, []int{})
// 	expectedValuesSum := []interface{}{nil, nil, -1, 99, 110, 107, 107, 107, 127}
// 	dtypesSum := getDtypes(len(expectedValuesCount), []int{}, []int{0, 1})
// 	expectedValuesAvg := []interface{}{nil, nil, -1, 49.5, 36.666666666666664, 26.75, 26.75, 26.75, 25.4}
// 	expectedValuesMax := []interface{}{"ABC", "Def", -1, 100, 100, 100, 100, 100, 100}
// 	dtypesMax := getDtypes(len(expectedValuesCount), []int{0, 1}, []int{})
// 	expectedValuesMin := []interface{}{"ABC", "ABC", -1, -1, -1, -3, -3, -3, -3}
// 	expectedValuesRange := []interface{}{nil, nil, 0, 101, 101, 103, 103, 103, 103}
// 	expectedValuesCardinality := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9}

// 	measureFunctions := []sutils.AggregateFunctions{
// 		sutils.Count,
// 		sutils.Sum,
// 		sutils.Avg,
// 		sutils.Max,
// 		sutils.Min,
// 		sutils.Range,
// 		sutils.Cardinality,
// 	}
// 	measureAggs := getMeasureAggs(measureFunctions)

// 	expectedFuncValues := [][]sutils.CValueEnclosure{
// 		createCValues(expectedValuesCount, dtypesFloats),
// 		createCValues(expectedValuesSum, dtypesSum),
// 		createCValues(expectedValuesAvg, dtypesSum),
// 		createCValues(expectedValuesMax, dtypesMax),
// 		createCValues(expectedValuesMin, dtypesMax),
// 		createCValues(expectedValuesRange, dtypesSum),
// 		createCValues(expectedValuesCardinality, dtypesFloats),
// 	}

// 	optionValue := []bool{true, false}
// 	cValues := createCValues(values, dtypesValues)

// 	for i := 0; i < len(optionValue); i++ {
// 		for j := 0; j < len(optionValue); j++ {
// 			ssOption.Current = optionValue[i]
// 			ssOption.Global = optionValue[j]
// 			NoWindowStreamStatsHelperTest2(t, cValues, ssOption, measureAggs, expectedFuncValues)
// 		}
// 	}
// }

// func Test_PerformMVExpand(t *testing.T) {
// 	recs := map[string]map[string]interface{}{
// 		"1": {"senders": []string{"john@example.com", "jane@example.com", "doe@example.com"}},
// 		"2": {"senders": []string{"foo@example.com", "", "bar@example.com"}},
// 		"3": {"numbers": []int{1, 2, 3, 4}},
// 		"4": {"prices": []float64{10.5, 20.75, 30.0}},
// 		"5": {"flags": []bool{true, false, true}},
// 		"6": {"invalid": "not a slice"}, // Non-slice input.
// 	}

// 	expectedResults := map[string]map[string][]interface{}{
// 		"1": {"senders": {"john@example.com", "jane@example.com", "doe@example.com"}},
// 		"2": {"senders": {"foo@example.com", "", "bar@example.com"}},
// 		"3": {"numbers": {1, 2, 3, 4}},
// 		"4": {"prices": {10.5, 20.75, 30.0}},
// 		"5": {"flags": {true, false, true}},
// 		"6": {"invalid": nil}, // Expect nil as this input should fail.
// 	}

// 	for recNum, rec := range recs {
// 		for colName, values := range rec {
// 			expandedValues := performMVExpand(values)
// 			assert.Equal(t, expectedResults[recNum][colName], expandedValues, fmt.Sprintf("Test failed for record %s, column %s", recNum, colName))
// 		}
// 	}
// }

func Test_dummy(t *testing.T) {
	// This is a dummy test
}

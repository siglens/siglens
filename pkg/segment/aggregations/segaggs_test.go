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
	"math/rand"
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

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

func generateTestRecords(numRecords int) []map[string]interface{} {
	records := make([]map[string]interface{}, numRecords)

	for i := 0; i < numRecords; i++ {
		record := make(map[string]interface{})

		record["timestamp"] = uint64(1659874108987)
		record["city"] = cities[rand.Intn(len(cities))]
		record["gender"] = []string{"male", "female"}[rand.Intn(2)]
		record["country"] = countries[rand.Intn(len(countries))]
		record["http_method"] = []string{"GET", "POST", "PUT", "DELETE"}[rand.Intn(4)]
		record["http_status"] = []int{200, 301, 302, 404}[rand.Intn(4)]

		records[i] = record
	}

	return records
}

func Test_processTransactionsOnRecords(t *testing.T) {

	records := generateTestRecords(500)
	allCols := []string{"city", "gender"}

	recordsLengthPositive := make(map[int]bool)

	// CASE 1: Only Fields
	txnArgs1 := &structs.TransactionArguments{
		Fields:     []string{"gender", "city"},
		StartsWith: nil,
		EndsWith:   nil,
	}
	recordsLengthPositive[1] = true

	// CASE 2: Only EndsWith
	txnArgs2 := &structs.TransactionArguments{
		EndsWith:   &structs.FilterStringExpr{StringValue: "DELETE"},
		StartsWith: &structs.FilterStringExpr{StringValue: "GET"},
		Fields:     []string{},
	}
	recordsLengthPositive[2] = true

	// CASE 3: Only StartsWith
	txnArgs3 := &structs.TransactionArguments{
		StartsWith: &structs.FilterStringExpr{StringValue: "GET"},
		EndsWith:   nil,
		Fields:     []string{},
	}
	recordsLengthPositive[3] = true

	// CASE 4: StartsWith and EndsWith
	txnArgs4 := &structs.TransactionArguments{
		StartsWith: &structs.FilterStringExpr{StringValue: "GET"},
		EndsWith:   &structs.FilterStringExpr{StringValue: "DELETE"},
		Fields:     []string{},
	}
	recordsLengthPositive[4] = true

	// CASE 5: StartsWith and EndsWith and one Field
	txnArgs5 := &structs.TransactionArguments{
		StartsWith: &structs.FilterStringExpr{StringValue: "GET"},
		EndsWith:   &structs.FilterStringExpr{StringValue: "DELETE"},
		Fields:     []string{"gender"},
	}
	recordsLengthPositive[5] = true

	// CASE 6: StartsWith and EndsWith and two Fields
	txnArgs6 := &structs.TransactionArguments{
		StartsWith: &structs.FilterStringExpr{StringValue: "GET"},
		EndsWith:   &structs.FilterStringExpr{StringValue: "DELETE"},
		Fields:     []string{"gender", "country"},
	}
	recordsLengthPositive[6] = true

	// CASE 7: StartsWith and EndsWith with String Clauses only OR
	txnArgs7 := &structs.TransactionArguments{
		StartsWith: &structs.FilterStringExpr{StringClauses: [][]string{{"GET", "POST1"}}},
		EndsWith:   &structs.FilterStringExpr{StringClauses: [][]string{{"DELETE", "POST2"}}},
		Fields:     []string{"gender", "country"},
	}
	recordsLengthPositive[7] = true

	// CASE 8: StartsWith and EndsWith with String Clauses only AND (Negative Case)
	txnArgs8 := &structs.TransactionArguments{
		StartsWith: &structs.FilterStringExpr{StringClauses: [][]string{{"GET"}, {"POST2"}}},
		EndsWith:   &structs.FilterStringExpr{StringClauses: [][]string{{"POST"}}},
		Fields:     []string{"gender", "country"},
	}
	recordsLengthPositive[8] = false

	// CASE 9: StartsWith and EndsWith with String Clauses only AND (Positive Case)
	txnArgs9 := &structs.TransactionArguments{
		StartsWith: &structs.FilterStringExpr{StringClauses: [][]string{{"GET"}, {"male"}}},
		EndsWith:   &structs.FilterStringExpr{StringClauses: [][]string{{"DELETE"}}},
		Fields:     []string{"gender", "country"},
	}
	recordsLengthPositive[9] = true

	// CASE 10: StartsWith is a Valid Search Term and EndsWith is String Clause
	txnArgs10 := &structs.TransactionArguments{
		StartsWith: &structs.FilterStringExpr{
			SearchTerm: &structs.SimpleSearchExpr{
				Op:           ">=",
				Field:        "http_status",
				Values:       json.Number("300"),
				ValueIsRegex: false,
				ExprType:     utils.SS_DT_SIGNED_NUM,
				DtypeEnclosure: &utils.DtypeEnclosure{
					Dtype:    utils.SS_DT_SIGNED_NUM,
					FloatVal: float64(300),
				},
			},
		},
		EndsWith: &structs.FilterStringExpr{StringClauses: [][]string{{"DELETE"}}},
	}
	recordsLengthPositive[10] = true

	// CASE 11: StartsWith is not a Valid Search Term (comparing between two string fields) and EndsWith is String value
	txnArgs11 := &structs.TransactionArguments{
		StartsWith: &structs.FilterStringExpr{
			SearchTerm: &structs.SimpleSearchExpr{
				Op:           ">",
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
		EndsWith: &structs.FilterStringExpr{StringValue: "DELETE"},
	}
	recordsLengthPositive[11] = false

	// CASE 12: StartsWith is not a Valid Search Term (comparing between string and number fields) and EndsWith is String Clause
	txnArgs12 := &structs.TransactionArguments{
		StartsWith: &structs.FilterStringExpr{
			SearchTerm: &structs.SimpleSearchExpr{
				Op:           ">",
				Field:        "city",
				Values:       json.Number("300"),
				ValueIsRegex: false,
				ExprType:     utils.SS_DT_SIGNED_NUM,
				DtypeEnclosure: &utils.DtypeEnclosure{
					Dtype:    utils.SS_DT_SIGNED_NUM,
					FloatVal: float64(300),
				},
			},
		},
		EndsWith: &structs.FilterStringExpr{StringClauses: [][]string{{"DELETE"}}},
	}
	recordsLengthPositive[12] = false

	// CASE 13: StartsWith is a Valid Search Term (String1 = String2) and EndsWith is String Value
	txnArgs13 := &structs.TransactionArguments{
		StartsWith: &structs.FilterStringExpr{
			SearchTerm: &structs.SimpleSearchExpr{
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
		EndsWith: &structs.FilterStringExpr{StringValue: "DELETE"},
	}
	recordsLengthPositive[13] = true

	allCasesTxnArgs := []*structs.TransactionArguments{txnArgs1, txnArgs2, txnArgs3, txnArgs4, txnArgs5, txnArgs6, txnArgs7, txnArgs8, txnArgs9, txnArgs10, txnArgs11, txnArgs12, txnArgs13}

	for index, txnArgs := range allCasesTxnArgs {
		// Process Transactions
		processedRecords, cols, err := processTransactionsOnRecords(records, allCols, txnArgs)
		assert.NoError(t, err)
		assert.Equal(t, cols, []string{"timestamp", "duration", "count", "event"})

		// Check if the number of records is positive or negative
		assert.Equal(t, recordsLengthPositive[index+1], len(processedRecords) > 0)

		for _, record := range processedRecords {
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
						if txnArgs.StartsWith.StringValue != "" {
							assert.Equal(t, eventMap["http_method"], txnArgs.StartsWith.StringValue)
						} else if txnArgs.StartsWith.StringClauses != nil {
							assert.Contains(t, txnArgs.StartsWith.StringClauses[0][0], eventMap["http_method"])
						}
					}
				}

				if txnArgs.EndsWith != nil {
					if ind == len(events)-1 {
						if txnArgs.EndsWith.StringValue != "" {
							assert.Equal(t, eventMap["http_method"], txnArgs.EndsWith.StringValue)
						} else if txnArgs.EndsWith.StringClauses != nil {
							assert.Contains(t, txnArgs.EndsWith.StringClauses[0][0], eventMap["http_method"])
						}
					}
				}

			}
		}
	}

}

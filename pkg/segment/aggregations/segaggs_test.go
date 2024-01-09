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
	"math/rand"
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
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
		record["http_status"] = []string{"200", "301", "302", "404"}[rand.Intn(4)]

		records[i] = record
	}

	return records
}

func Test_processTransactionsOnRecords(t *testing.T) {

	records := generateTestRecords(500)
	allCols := []string{"city", "gender"}

	// CASE 1: Only Fields
	txnArgs1 := &structs.TransactionArguments{
		Fields:     []string{"gender", "city"},
		StartsWith: "",
		EndsWith:   "",
	}

	// CASE 2: Only EndsWith
	txnArgs2 := &structs.TransactionArguments{
		EndsWith:   "DELETE",
		StartsWith: "",
		Fields:     []string{},
	}

	// CASE 3: Only StartsWith
	txnArgs3 := &structs.TransactionArguments{
		StartsWith: "GET",
		EndsWith:   "",
		Fields:     []string{},
	}

	// CASE 4: StartsWith and EndsWith
	txnArgs4 := &structs.TransactionArguments{
		StartsWith: "GET",
		EndsWith:   "DELETE",
		Fields:     []string{},
	}

	// CASE 5: StartsWith and EndsWith and one Field
	txnArgs5 := &structs.TransactionArguments{
		StartsWith: "GET",
		EndsWith:   "DELETE",
		Fields:     []string{"gender"},
	}

	// CASE 6: StartsWith and EndsWith and two Fields
	txnArgs6 := &structs.TransactionArguments{
		StartsWith: "GET",
		EndsWith:   "DELETE",
		Fields:     []string{"gender", "country"},
	}

	allCasesTxnArgs := []*structs.TransactionArguments{txnArgs1, txnArgs2, txnArgs3, txnArgs4, txnArgs5, txnArgs6}

	for _, txnArgs := range allCasesTxnArgs {
		// Process Transactions
		processedRecords, cols, err := processTransactionsOnRecords(records, allCols, txnArgs)
		assert.NoError(t, err)
		assert.Equal(t, cols, []string{"timestamp", "duration", "count", "event"})

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

				if txnArgs.StartsWith != "" {
					if ind == 0 {
						assert.Equal(t, eventMap["http_method"], txnArgs.StartsWith)
					}
				}

				if txnArgs.EndsWith != "" {
					if ind == len(events)-1 {
						assert.Equal(t, eventMap["http_method"], txnArgs.EndsWith)
					}
				}

			}
		}
	}

}

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

package segment

import (
	"os"
	"testing"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/query/metadata"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	serverutils "github.com/siglens/siglens/pkg/server/utils"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_SortResultsArossMultipleFiles(t *testing.T) {
	config.InitializeTestingConfig()
	_ = query.InitQueryNode(getMyIds, serverutils.ExtractKibanaRequests)
	numBuffers := 5
	numEntriesForBuffer := 10
	fileCount := 10
	dir := "data/"
	metadata.InitMockColumnarMetadataStore(dir, fileCount, numBuffers, numEntriesForBuffer)

	value1, _ := utils.CreateDtypeEnclosure("*", 0)
	valueFilter := structs.FilterCriteria{
		ExpressionFilter: &structs.ExpressionFilter{
			LeftInput:      &structs.FilterInput{Expression: &structs.Expression{LeftInput: &structs.ExpressionInput{ColumnName: "*"}}},
			FilterOperator: utils.Equals,
			RightInput:     &structs.FilterInput{Expression: &structs.Expression{LeftInput: &structs.ExpressionInput{ColumnValue: value1}}},
		},
	}
	timeRange := &dtu.TimeRange{
		StartEpochMs: 0,
		EndEpochMs:   uint64(numEntriesForBuffer),
	}
	simpleNode := &structs.ASTNode{
		AndFilterCondition: &structs.Condition{FilterCriteria: []*structs.FilterCriteria{&valueFilter}},
		TimeRange:          timeRange,
	}

	descendingSort := &structs.QueryAggregators{
		Sort: &structs.SortRequest{
			ColName:   "key9",
			Ascending: false,
		},
	}
	sizeLimit := uint64(100)
	qc := structs.InitQueryContext("evts", sizeLimit, 0, 0, false)
	result := ExecuteQuery(simpleNode, descendingSort, 1010, qc)
	log.Infof("%d result %+v %+v", 1010, result, result.TotalResults)
	descendingResults := make([]float64, 0)
	for _, v := range result.AllRecords {
		descendingResults = append(descendingResults, v.SortColumnValue)
	}

	assert.Len(t, descendingResults, int(sizeLimit), "count results")
	lastVal := descendingResults[0]
	for i := 1; i < len(descendingResults); i++ {
		assert.True(t, descendingResults[i] <= lastVal, "descending order, so index should be less than last result")
		lastVal = descendingResults[i]
	}

	asendingSort := &structs.QueryAggregators{
		Sort: &structs.SortRequest{
			ColName:   "key9",
			Ascending: true,
		},
	}
	result = ExecuteQuery(simpleNode, asendingSort, 1, qc)
	ascendingResults := make([]float64, 0)
	for _, v := range result.AllRecords {
		ascendingResults = append(ascendingResults, v.SortColumnValue)
	}

	assert.Len(t, ascendingResults, int(sizeLimit), "count results")
	lastVal = ascendingResults[0]
	for i := 1; i < len(ascendingResults); i++ {
		assert.True(t, ascendingResults[i] >= lastVal, "ascending order, so index should be greater than last result")
		lastVal = ascendingResults[i]
	}

	os.RemoveAll(dir)
}

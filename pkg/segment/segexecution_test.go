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

package segment

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	esquery "github.com/siglens/siglens/pkg/es/query"
	"github.com/siglens/siglens/pkg/scroll"
	toputils "github.com/siglens/siglens/pkg/utils"

	"github.com/google/uuid"
	localstorage "github.com/siglens/siglens/pkg/blob/local"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/instrumentation"
	"github.com/siglens/siglens/pkg/segment/memory/limit"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/query/metadata"
	"github.com/siglens/siglens/pkg/segment/reader/microreader"
	"github.com/siglens/siglens/pkg/segment/reader/record"
	"github.com/siglens/siglens/pkg/segment/reader/segread"
	"github.com/siglens/siglens/pkg/segment/results/blockresults"
	"github.com/siglens/siglens/pkg/segment/results/segresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	. "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	serverutils "github.com/siglens/siglens/pkg/server/utils"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func simpleQueryTest(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int) {
	value1, _ := CreateDtypeEnclosure("value1", 0)
	valueFilter := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key1"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&valueFilter}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 1,
			EndEpochMs:   uint64(numEntriesForBuffer) + 1,
		},
	}
	ti := structs.InitTableInfo("evts", 0, false)
	sizeLimit := uint64(10000)
	qc := structs.InitQueryContextWithTableInfo(ti, sizeLimit, 0, 0, false)
	result := ExecuteQuery(simpleNode, &QueryAggregators{}, 0, qc)
	log.Info(result)
	assert.NotNil(t, result, "Query ran successfully")
	assert.Len(t, result.AllRecords, numBuffers*numEntriesForBuffer*fileCount, "all logs in all files should have matched")
	assert.Len(t, result.ErrList, 0, "no errors should have occurred")

	nine, _ := CreateDtypeEnclosure(9, 0)
	rangeFilter := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key2"}}},
			FilterOperator: GreaterThanOrEqualTo,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: nine}}},
		},
	}

	simpleNode.AndFilterCondition.FilterCriteria = append(simpleNode.AndFilterCondition.FilterCriteria, &rangeFilter)
	result = ExecuteQuery(simpleNode, &QueryAggregators{}, 0, qc)
	log.Info(result)
	assert.NotNil(t, result, "Query ran successfully")
	assert.Len(t, result.AllRecords, numBuffers*fileCount, "each buffer in each file will only have one match")
	assert.Len(t, result.ErrList, 0, "no errors should have occurred")

	filterCondition := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key1"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode.ExclusionFilterCondition = &Condition{FilterCriteria: []*FilterCriteria{&filterCondition}}
	log.Infof("%v", simpleNode)
	result = ExecuteQuery(simpleNode, &QueryAggregators{}, 0, qc)
	log.Info(result)
	assert.NotNil(t, result, "Query ran successfully")
	assert.Len(t, result.AllRecords, 0, "exclusion filter criteria should make query return nothing")
	assert.Len(t, result.ErrList, 0, "no errors should have occurred")

	zero, _ := CreateDtypeEnclosure(0, 0)
	orCondition := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key2"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: zero}}},
		},
	}
	simpleNode.OrFilterCondition = &Condition{FilterCriteria: []*FilterCriteria{&orCondition}}
	result = ExecuteQuery(simpleNode, &QueryAggregators{}, 0, qc)
	assert.NotNil(t, result, "Query ran successfully")
	assert.Len(t, result.AllRecords, 0, "or filter shouldhave no effect")
	assert.Len(t, result.ErrList, 0, "no errors should have occurred")

	// TODO: uncomment after isNotNull/isNull logic
	// columnExistsCondition := FilterCriteria{
	//	ExpressionFilter: &ExpressionFilter{
	//		LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key2"}}},
	//		FilterOperator: IsNotNull,
	//	},
	// }
	// columnNode := &ASTNode{
	//	AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&columnExistsCondition}},
	//	TimeRange: &dtu.TimeRange{
	//		StartEpochMs: 0,
	//		EndEpochMs:   uint64(numEntriesForBuffer),
	//	},
	// }
	// result = ExecuteQuery(columnNode, &Aggregators{}, indexName, sizeLimit)
	// assert.NotNil(t, result, "Query ran successfully")
	// assert.Len(t, result.AllRecords, numBuffers*numEntriesForBuffer*fileCount, "all records should have key2")
	// assert.Len(t, result.ErrList, 0, "no errors should have occurred")

	invalidColumnCondition := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "abc"}}},
			FilterOperator: IsNotNull,
		},
	}
	columnNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&invalidColumnCondition}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 1,
			EndEpochMs:   uint64(numEntriesForBuffer) + 1,
		},
	}
	columnNode.AndFilterCondition = &Condition{FilterCriteria: []*FilterCriteria{&invalidColumnCondition}}
	result = ExecuteQuery(columnNode, &QueryAggregators{}, 0, qc)
	assert.NotNil(t, result, "Query ran successfully")
	assert.Len(t, result.AllRecords, 0, "no column abc exists")
	assert.NotEqual(t, 0, result.ErrList, "column not found errors MUST happened")
}

func wildcardQueryTest(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int) {
	value1, _ := CreateDtypeEnclosure("value1", 0)
	// wildcard all columns
	ti := structs.InitTableInfo("evts", 0, false)
	allColumns := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "*"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&allColumns}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 0,
			EndEpochMs:   uint64(numEntriesForBuffer),
		},
	}
	qc := structs.InitQueryContextWithTableInfo(ti, 10000, 0, 0, false)
	result := ExecuteQuery(simpleNode, &QueryAggregators{}, 0, qc)
	assert.NotNil(t, result, "Query ran successfully")
	assert.Len(t, result.AllRecords, numEntriesForBuffer*numBuffers*fileCount, "all log lines match")
	assert.Len(t, result.ErrList, 0, "no errors should have occurred")

	batch0, _ := CreateDtypeEnclosure("batch-0-*", 0)
	allKeyColumns := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key*"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: batch0}}},
		},
	}
	simpleNode = &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&allKeyColumns}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 0,
			EndEpochMs:   uint64(numEntriesForBuffer),
		},
	}
	result = ExecuteQuery(simpleNode, &QueryAggregators{}, 0, qc)
	for _, rrc := range result.AllRecords {

		blkRecIndexes := make(map[uint16]map[uint16]uint64)
		recIdxs := make(map[uint16]uint64)
		recIdxs[rrc.RecordNum] = 1
		blkRecIndexes[rrc.BlockNum] = recIdxs
		segkey := result.SegEncToKey[rrc.SegKeyInfo.SegKeyEnc]
		records, _, err := record.GetRecordsFromSegment(segkey, rrc.VirtualTableName, blkRecIndexes, "timestamp", false, 0, &QueryAggregators{})
		assert.Nil(t, err)

		log.Info(records)
	}
	assert.NotNil(t, result, "Query ran successfully")
	assert.Len(t, result.AllRecords, 0, "partial column wildcard is not supported")
	assert.NotEqual(t, 0, result.ErrList, "column not found errors MUST happen")
}

func timeHistogramQueryTest(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int) {
	value1, _ := CreateDtypeEnclosure("value1", 0)
	ti := structs.InitTableInfo("evts", 0, false)
	allColumns := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key1"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&allColumns}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 1,
			EndEpochMs:   uint64(numEntriesForBuffer) + 1,
		},
	}

	simpleTimeHistogram := &QueryAggregators{
		TimeHistogram: &TimeBucket{
			StartTime:      1,
			EndTime:        uint64(numEntriesForBuffer) + 1,
			IntervalMillis: 1,
			AggName:        "testTime",
		},
	}
	qc := structs.InitQueryContextWithTableInfo(ti, 10000, 0, 0, false)
	result := ExecuteQuery(simpleNode, simpleTimeHistogram, 101, qc)
	seenKeys := make(map[uint64]bool)
	lenHist := len(result.Histogram["testTime"].Results)
	for i := 0; i < lenHist; i++ {
		assert.Equal(t, result.Histogram["testTime"].Results[i].ElemCount, uint64(10))
		currKey := result.Histogram["testTime"].Results[i].BucketKey.(uint64)
		seenKeys[currKey] = true
	}
	assert.Len(t, seenKeys, 10)
	assert.Condition(t, func() (success bool) {
		for i := uint64(1); i < uint64(numEntriesForBuffer+1); i++ {
			if _, ok := seenKeys[i]; !ok {
				return false
			}
		}
		return true
	})
}

func groupByQueryTest(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int) {
	value1, _ := CreateDtypeEnclosure("value1", 0)
	ti := structs.InitTableInfo("evts", 0, false)
	allColumns := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key1"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&allColumns}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 1,
			EndEpochMs:   uint64(numEntriesForBuffer) + 1,
		},
	}

	simpleGroupBy := &QueryAggregators{
		GroupByRequest: &GroupByRequest{
			GroupByColumns: []string{"key11"},
			MeasureOperations: []*MeasureAggregator{
				{MeasureCol: "key2", MeasureFunc: Max},
				{MeasureCol: "key2", MeasureFunc: Min},
				{MeasureCol: "key8", MeasureFunc: Sum},
			},
			AggName:     "test",
			BucketCount: 100,
		},
	}

	mnames := make([]string, len(simpleGroupBy.GroupByRequest.MeasureOperations))
	for i, mOp := range simpleGroupBy.GroupByRequest.MeasureOperations {
		mnames[i] = mOp.String()
	}
	qc := structs.InitQueryContextWithTableInfo(ti, 10000, 0, 0, false)
	result := ExecuteQuery(simpleNode, simpleGroupBy, 102, qc)
	lenHist := len(result.Histogram["test"].Results)
	assert.False(t, result.Histogram["test"].IsDateHistogram)
	assert.Equal(t, lenHist, 2, "only record-batch-1 and record-batch-0 exist")
	totalentries := numEntriesForBuffer * fileCount * numBuffers
	for i := 0; i < lenHist; i++ {
		assert.Equal(t, result.Histogram["test"].Results[i].ElemCount, uint64(totalentries/2))
		bKey := result.Histogram["test"].Results[i].BucketKey

		assert.Len(t, result.Histogram["test"].Results[i].StatRes, len(simpleGroupBy.GroupByRequest.MeasureOperations))
		log.Infof("bkey is %+v", bKey)
		for mFunc, m := range mnames {
			res, ok := result.Histogram["test"].Results[i].StatRes[m]

			assert.True(t, ok)
			if mFunc == 0 {
				if bKey == "record-batch-0" {
					assert.Equal(t, res.CVal, int64(numEntriesForBuffer-2))
				} else if bKey == "record-batch-1" {
					assert.Equal(t, res.CVal, int64(numEntriesForBuffer-1))
				} else {
					assert.Fail(t, "unexpected bkey %+v", bKey)
				}
			} else if mFunc == 1 {
				if bKey == "record-batch-0" {
					assert.Equal(t, res.CVal, int64(0))
				} else if bKey == "record-batch-1" {
					assert.Equal(t, res.CVal, int64(1))
				} else {
					assert.Fail(t, "unexpected bkey %+v", bKey)
				}
			} else if mFunc == 2 {
				assert.Greater(t, res.CVal, int64(numBuffers*fileCount))
			} else {
				assert.Fail(t, "unexpected case %+v", mFunc)
			}
		}
	}
}

func timechartGroupByQueryTest(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int) {
	value1, _ := CreateDtypeEnclosure("value1", 0)
	ti := structs.InitTableInfo("evts", 0, false)
	allColumns := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key1"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&allColumns}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 1,
			EndEpochMs:   uint64(numEntriesForBuffer) + 1,
		},
	}

	timechart := &TimechartExpr{
		ByField: "key11",
	}

	// time range is [1, 11], so it should have 3 time range buckets: [1, 5), [5, 9), [9, 11]
	simpleGroupBy := &QueryAggregators{
		TimeHistogram: &TimeBucket{
			IntervalMillis: 4,
			StartTime:      1,
			EndTime:        uint64(numEntriesForBuffer) + 1,
			Timechart:      timechart,
		},
		GroupByRequest: &GroupByRequest{
			GroupByColumns: []string{"timestamp"},
			MeasureOperations: []*MeasureAggregator{
				{MeasureCol: "key2", MeasureFunc: Avg},
				{MeasureCol: "key8", MeasureFunc: Sum},
			},
			AggName:     "test",
			BucketCount: 100,
		},
	}

	mnames := make([]string, len(simpleGroupBy.GroupByRequest.MeasureOperations))
	for i, mOp := range simpleGroupBy.GroupByRequest.MeasureOperations {
		mnames[i] = mOp.String()
	}
	qc := structs.InitQueryContextWithTableInfo(ti, 10000, 0, 0, false)
	result := ExecuteQuery(simpleNode, simpleGroupBy, 102, qc)
	lenHist := len(result.Histogram["test"].Results)
	assert.False(t, result.Histogram["test"].IsDateHistogram)
	assert.Equal(t, 3, lenHist, "it should have 3 time range buckets: [1, 5), [5, 9), [9, 11]")
	timeBucketsMap := map[string]struct{}{
		"1": {},
		"5": {},
		"9": {},
	}

	totalentries := uint64(numEntriesForBuffer * fileCount * numBuffers)
	sumRecord0 := uint64(0)
	sumRecord1 := uint64(0)
	for i := 0; i < lenHist; i++ {
		bKey := result.Histogram["test"].Results[i].BucketKey
		timestamp, ok := bKey.(string)
		assert.True(t, ok)
		_, exists := timeBucketsMap[timestamp]
		assert.True(t, exists)
		delete(timeBucketsMap, timestamp)

		assert.Len(t, result.Histogram["test"].Results[i].StatRes, len(simpleGroupBy.GroupByRequest.MeasureOperations)*2)
		log.Infof("bkey is %+v", bKey)

		m := mnames[1]

		res0, ok := result.Histogram["test"].Results[i].StatRes[m+": record-batch-0"]
		assert.True(t, ok)
		num1, err := res0.GetUIntValue()
		assert.Nil(t, err)

		res1, ok := result.Histogram["test"].Results[i].StatRes[m+": record-batch-1"]
		assert.True(t, ok)
		num2, err := res1.GetUIntValue()
		assert.Nil(t, err)

		sumRecord0 += num1
		sumRecord1 += num2
	}

	assert.Equal(t, totalentries, sumRecord0)
	assert.Equal(t, totalentries, sumRecord1)
}

func nestedQueryTest(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int) {
	ti := structs.InitTableInfo("evts", 0, false)
	// key6==key2 when i == 0
	one, _ := CreateDtypeEnclosure(1, 0)
	zero, _ := CreateDtypeEnclosure(0, 0)
	columnRelation := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key6"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: zero}}},
		},
	}

	keyRelation := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key2"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: one}}},
		},
	}

	key2Node := &ASTNode{
		AndFilterCondition: &Condition{
			FilterCriteria: []*FilterCriteria{&keyRelation},
		},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 1,
			EndEpochMs:   uint64(numEntriesForBuffer) + 1,
		},
	}

	nestedNode := &ASTNode{
		AndFilterCondition: &Condition{
			FilterCriteria: []*FilterCriteria{&columnRelation},
			NestedNodes:    []*ASTNode{key2Node},
		},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 1,
			EndEpochMs:   uint64(numEntriesForBuffer) + 1,
		},
	}
	qc := structs.InitQueryContextWithTableInfo(ti, 10000, 0, 0, false)
	result := ExecuteQuery(nestedNode, nil, 0, qc)
	assert.Len(t, result.ErrList, 0)
	assert.Len(t, result.AllRecords, 0, "conditions are exclusive, no responses should match")

	nestedNode = &ASTNode{
		OrFilterCondition: &Condition{
			FilterCriteria: []*FilterCriteria{&columnRelation},
			NestedNodes:    []*ASTNode{key2Node},
		},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 1,
			EndEpochMs:   uint64(numEntriesForBuffer) + 1,
		},
	}
	result = ExecuteQuery(nestedNode, nil, 0, qc)
	assert.Len(t, result.ErrList, 0)
	assert.Len(t, result.AllRecords, numBuffers*fileCount*2, "should match when key2=0 and 1")

	multiNestedNode := &ASTNode{
		OrFilterCondition: &Condition{
			NestedNodes: []*ASTNode{nestedNode},
		},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 1,
			EndEpochMs:   uint64(numEntriesForBuffer) + 1,
		},
	}
	result = ExecuteQuery(multiNestedNode, nil, 0, qc)
	assert.Len(t, result.ErrList, 0)
	assert.Len(t, result.AllRecords, numBuffers*fileCount*2, "nesting node another level should have no affect")
}

func nestedAggregationQueryTest(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int) {
	ti := structs.InitTableInfo("evts", 0, false)
	value1, _ := CreateDtypeEnclosure("value1", 0)

	filter := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key1"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&filter}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 1,
			EndEpochMs:   uint64(numEntriesForBuffer) + 1,
		},
	}

	simpleMeasure := &QueryAggregators{
		PipeCommandType: MeasureAggsType,
		MeasureOperations: []*MeasureAggregator{
			&MeasureAggregator{
				MeasureCol:  "key2",
				MeasureFunc: Max,
			},
		},
	}

	// Test renaming the measured column.
	renameAggs := make(map[string]string)
	renameAggs["max(key2)"] = "Max"
	simpleRename := &QueryAggregators{
		PipeCommandType: OutputTransformType,
		OutputTransforms: &OutputTransforms{
			OutputColumns: &ColumnsRequest{
				RenameAggregationColumns: renameAggs,
			},
		},
	}

	simpleMeasure.Next = simpleRename

	qc := structs.InitQueryContextWithTableInfo(ti, 10000, 0, 0, false)
	result := ExecuteQuery(simpleNode, simpleMeasure, 0, qc)

	assert.Len(t, result.AllRecords, 0)
	assert.Zero(t, result.TotalResults.TotalCount)
	assert.False(t, result.TotalResults.EarlyExit)

	assert.Len(t, result.MeasureFunctions, 1)
	assert.Equal(t, result.MeasureFunctions[0], "Max")

	// Test creating a new column using the renamed column.
	simpleLetColumns := &QueryAggregators{
		PipeCommandType: OutputTransformType,
		OutputTransforms: &OutputTransforms{
			LetColumns: &LetColumnsRequest{
				NewColName: "MaxSeconds",
				ValueColRequest: &ValueExpr{
					ValueExprMode: VEMStringExpr,
					StringExpr: &StringExpr{
						StringExprMode: SEMConcatExpr,
						ConcatExpr: &ConcatExpr{

							Atoms: []*ConcatAtom{
								{IsField: true, Value: "Max"},
								{IsField: false, Value: " seconds"},
							},
						},
					},
				},
			},
		},
	}

	simpleRename.Next = simpleLetColumns

	result = ExecuteQuery(simpleNode, simpleMeasure, 0, qc)

	assert.Len(t, result.AllRecords, 0)
	assert.Zero(t, result.TotalResults.TotalCount)
	assert.False(t, result.TotalResults.EarlyExit)

	assert.Len(t, result.MeasureFunctions, 2)
	assert.Equal(t, result.MeasureFunctions[0], "Max")
	assert.Equal(t, result.MeasureFunctions[1], "MaxSeconds")

	assert.Len(t, result.MeasureResults, 1)
	maxStr := result.MeasureResults[0].MeasureVal["Max"].(string)
	assert.Equal(t, result.MeasureResults[0].MeasureVal["Max"], maxStr)
}

func nestedAggregationQueryWithGroupByTest(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int) {
	ti := structs.InitTableInfo("evts", 0, false)
	value1, _ := CreateDtypeEnclosure("value1", 0)

	filter := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key1"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&filter}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 1,
			EndEpochMs:   uint64(numEntriesForBuffer) + 1,
		},
	}

	simpleGroupBy := &QueryAggregators{
		PipeCommandType: GroupByType,
		GroupByRequest: &GroupByRequest{
			GroupByColumns: []string{"key11"},
			MeasureOperations: []*MeasureAggregator{
				{MeasureCol: "key2", MeasureFunc: Max},
				{MeasureCol: "key2", MeasureFunc: Min},
				{MeasureCol: "key8", MeasureFunc: Sum},
			},
			AggName:     "test",
			BucketCount: 100,
		},
	}

	// Create a new column using the aggregation columns.
	firstLetColumns := &QueryAggregators{
		PipeCommandType: OutputTransformType,
		OutputTransforms: &OutputTransforms{
			LetColumns: &LetColumnsRequest{
				NewColName: "Key2Range",
				ValueColRequest: &ValueExpr{
					ValueExprMode: VEMStringExpr,
					StringExpr: &StringExpr{
						StringExprMode: SEMConcatExpr,
						ConcatExpr: &ConcatExpr{
							Atoms: []*ConcatAtom{
								{IsField: true, Value: "min(key2)"},
								{IsField: false, Value: " to "},
								{IsField: true, Value: "max(key2)"},
							},
						},
					},
				},
			},
		},
	}

	mnames := make([]string, 1+len(simpleGroupBy.GroupByRequest.MeasureOperations))
	for i, mOp := range simpleGroupBy.GroupByRequest.MeasureOperations {
		mnames[i] = mOp.String()
	}
	mnames[len(simpleGroupBy.GroupByRequest.MeasureOperations)] = "Key2Range"

	// Write over a groupby column.
	secondLetColumns := &QueryAggregators{
		PipeCommandType: OutputTransformType,
		OutputTransforms: &OutputTransforms{
			LetColumns: &LetColumnsRequest{
				NewColName: "key11",
				ValueColRequest: &ValueExpr{
					ValueExprMode: VEMStringExpr,
					StringExpr: &StringExpr{
						StringExprMode: SEMConcatExpr,
						ConcatExpr: &ConcatExpr{
							Atoms: []*ConcatAtom{
								{IsField: true, Value: "key11"},
								{IsField: false, Value: "A"},
							},
						},
					},
				},
			},
		},
	}

	simpleGroupBy.Next = firstLetColumns
	firstLetColumns.Next = secondLetColumns

	sizeLimit := uint64(0)
	qc := structs.InitQueryContextWithTableInfo(ti, sizeLimit, 0, 0, false)
	result := ExecuteQuery(simpleNode, simpleGroupBy, 0, qc)

	assert.False(t, result.TotalResults.EarlyExit)

	assert.Len(t, result.MeasureFunctions, 4)
	assert.True(t, toputils.SliceContainsString(result.MeasureFunctions, "max(key2)"))
	assert.True(t, toputils.SliceContainsString(result.MeasureFunctions, "min(key2)"))
	assert.True(t, toputils.SliceContainsString(result.MeasureFunctions, "sum(key8)"))
	assert.True(t, toputils.SliceContainsString(result.MeasureFunctions, "Key2Range"))

	// Verify MeasureResults
	assert.Len(t, result.MeasureResults, 2) // We group by key11, which has two values (see WriteMockColSegFile())
	for i := 0; i < len(result.MeasureResults); i++ {
		minStr := fmt.Sprintf("%v", result.MeasureResults[i].MeasureVal["min(key2)"].(int64))
		maxStr := fmt.Sprintf("%v", result.MeasureResults[i].MeasureVal["max(key2)"].(int64))

		assert.Equal(t, result.MeasureResults[i].MeasureVal["Key2Range"], minStr+" to "+maxStr)
	}

	// Verify Histogram

	lenHist := len(result.Histogram["test"].Results)

	assert.False(t, result.Histogram["test"].IsDateHistogram)
	assert.Equal(t, lenHist, 2, "only record-batch-1A and record-batch-0A exist")
	totalentries := numEntriesForBuffer * fileCount * numBuffers
	for i := 0; i < lenHist; i++ {
		assert.Equal(t, result.Histogram["test"].Results[i].ElemCount, uint64(totalentries/2))

		bKey := result.Histogram["test"].Results[i].BucketKey

		assert.Len(t, result.Histogram["test"].Results[i].StatRes, 1+len(simpleGroupBy.GroupByRequest.MeasureOperations))
		for _, measureCol := range mnames {
			res, ok := result.Histogram["test"].Results[i].StatRes[measureCol]
			assert.True(t, ok)

			if measureCol == "max(key2)" {

				if bKey == "record-batch-0A" {
					assert.Equal(t, res.CVal, int64(numEntriesForBuffer-2))
				} else if bKey == "record-batch-1A" {
					assert.Equal(t, res.CVal, int64(numEntriesForBuffer-1))
				} else {
					assert.Fail(t, "unexpected bkey %+v", bKey)
				}
			} else if measureCol == "min(key2)" {
				if bKey == "record-batch-0A" {
					assert.Equal(t, res.CVal, int64(0))
				} else if bKey == "record-batch-1A" {
					assert.Equal(t, res.CVal, int64(1))
				} else {
					assert.Fail(t, "unexpected bkey %+v", bKey)
				}
			} else if measureCol == "sum(key8)" {
				assert.Greater(t, res.CVal, int64(numBuffers*fileCount))
			} else if measureCol == "Key2Range" {
				if bKey == "record-batch-0A" {
					assert.Equal(t, res.CVal, "0 to "+fmt.Sprintf("%v", int64(numEntriesForBuffer-2)))
				} else if bKey == "record-batch-1A" {
					assert.Equal(t, res.CVal, "1 to "+fmt.Sprintf("%v", int64(numEntriesForBuffer-1)))
				} else {
					assert.Fail(t, "unexpected bkey %+v", bKey)
				}
			} else {
				assert.Fail(t, "unexpected case %+v", measureCol)
			}
		}
	}
}

func nestedAggsNumericColRequestTest(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int) {
	ti := structs.InitTableInfo("evts", 0, false)
	value1, _ := CreateDtypeEnclosure("value1", 0)

	filter := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key1"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&filter}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 1,
			EndEpochMs:   uint64(numEntriesForBuffer) + 1,
		},
	}

	simpleMeasure := &QueryAggregators{
		PipeCommandType: MeasureAggsType,
		MeasureOperations: []*MeasureAggregator{
			&MeasureAggregator{
				MeasureCol:  "key2",
				MeasureFunc: Max,
			},
		},
	}

	renameAggs := make(map[string]string)
	renameAggs["max(key2)"] = "Max"
	simpleRename := &QueryAggregators{
		PipeCommandType: OutputTransformType,
		OutputTransforms: &OutputTransforms{
			OutputColumns: &ColumnsRequest{
				RenameAggregationColumns: renameAggs,
			},
		},
	}

	simpleMeasure.Next = simpleRename

	// Test creating a new column using the renamed column and numeric calculations.
	// We'll do 7 - 3 - (Max + 5) * 10 / 2
	simpleLetColumns := &QueryAggregators{
		PipeCommandType: OutputTransformType,
		OutputTransforms: &OutputTransforms{
			LetColumns: &LetColumnsRequest{
				NewColName: "Custom",
				ValueColRequest: &ValueExpr{
					ValueExprMode: VEMNumericExpr,

					NumericExpr: &NumericExpr{
						IsTerminal: false,
						Op:         "-",
						Left: &NumericExpr{
							IsTerminal: false,
							Op:         "-",
							Left: &NumericExpr{
								IsTerminal:   true,
								Value:        "7",
								ValueIsField: false,
							},
							Right: &NumericExpr{
								IsTerminal:   true,
								Value:        "3",
								ValueIsField: false,
							},
						},
						Right: &NumericExpr{
							IsTerminal: false,
							Op:         "/",
							Left: &NumericExpr{
								IsTerminal: false,
								Op:         "*",
								Left: &NumericExpr{
									IsTerminal: false,
									Op:         "+",
									Left: &NumericExpr{
										IsTerminal:      true,
										Value:           "Max",
										ValueIsField:    true,
										NumericExprMode: NEMNumberField,
									},
									Right: &NumericExpr{
										IsTerminal:   true,
										Value:        "5",
										ValueIsField: false,
									},
								},
								Right: &NumericExpr{
									IsTerminal:   true,
									Value:        "10",
									ValueIsField: false,
								},
							},
							Right: &NumericExpr{
								IsTerminal:   true,
								Value:        "2",
								ValueIsField: false,
							},
						},
					},
				},
			},
		},
	}

	simpleRename.Next = simpleLetColumns

	qc := structs.InitQueryContextWithTableInfo(ti, 10000, 0, 0, false)
	result := ExecuteQuery(simpleNode, simpleMeasure, 0, qc)

	assert.Len(t, result.AllRecords, 0)
	assert.Zero(t, result.TotalResults.TotalCount)
	assert.False(t, result.TotalResults.EarlyExit)

	assert.Len(t, result.MeasureFunctions, 2)
	assert.Equal(t, result.MeasureFunctions[0], "Max")
	assert.Equal(t, result.MeasureFunctions[1], "Custom")

	assert.Len(t, result.MeasureResults, 1)
	maxStr := result.MeasureResults[0].MeasureVal["Max"].(string)
	maxFloat, err := strconv.ParseFloat(maxStr, 64)
	assert.Nil(t, err)
	expected := 7 - 3 - (maxFloat+5)*10/2
	actualStr := result.MeasureResults[0].MeasureVal["Custom"].(string)
	actualFloat, err := strconv.ParseFloat(actualStr, 64)
	assert.Nil(t, err)
	assert.Equal(t, actualFloat, expected)
}

func nestedAggsNumericColRequestWithGroupByTest(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int) {
	ti := structs.InitTableInfo("evts", 0, false)
	value1, _ := CreateDtypeEnclosure("value1", 0)

	filter := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key1"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&filter}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 1,
			EndEpochMs:   uint64(numEntriesForBuffer) + 1,
		},
	}

	simpleGroupBy := &QueryAggregators{
		PipeCommandType: GroupByType,
		GroupByRequest: &GroupByRequest{
			GroupByColumns: []string{"key11"},
			MeasureOperations: []*MeasureAggregator{
				{MeasureCol: "key2", MeasureFunc: Max},
				{MeasureCol: "key2", MeasureFunc: Min},
				{MeasureCol: "key8", MeasureFunc: Sum},
			},
			AggName:     "test",
			BucketCount: 100,
		},
	}

	// Make a new column via numeric calculations. We'll do (Max2 - Min2)
	numericLetCol := &QueryAggregators{
		PipeCommandType: OutputTransformType,
		OutputTransforms: &OutputTransforms{
			LetColumns: &LetColumnsRequest{
				NewColName: "Range2",
				ValueColRequest: &ValueExpr{
					ValueExprMode: VEMNumericExpr,
					NumericExpr: &NumericExpr{
						IsTerminal: false,
						Op:         "-",
						Left: &NumericExpr{
							IsTerminal:      true,
							Value:           "max(key2)",
							ValueIsField:    true,
							NumericExprMode: NEMNumberField,
						},
						Right: &NumericExpr{
							IsTerminal:      true,
							Value:           "min(key2)",
							ValueIsField:    true,
							NumericExprMode: NEMNumberField,
						},
					},
				},
			},
		},
	}

	simpleGroupBy.Next = numericLetCol

	mnames := make([]string, 1+len(simpleGroupBy.GroupByRequest.MeasureOperations))
	for i, mOp := range simpleGroupBy.GroupByRequest.MeasureOperations {
		mnames[i] = mOp.String()
	}
	mnames[len(simpleGroupBy.GroupByRequest.MeasureOperations)] = "Range2"

	sizeLimit := uint64(0)
	qc := structs.InitQueryContextWithTableInfo(ti, sizeLimit, 0, 0, false)
	result := ExecuteQuery(simpleNode, simpleGroupBy, 0, qc)

	assert.False(t, result.TotalResults.EarlyExit)

	assert.Len(t, result.MeasureFunctions, 4)
	assert.True(t, toputils.SliceContainsString(result.MeasureFunctions, "max(key2)"))
	assert.True(t, toputils.SliceContainsString(result.MeasureFunctions, "min(key2)"))
	assert.True(t, toputils.SliceContainsString(result.MeasureFunctions, "sum(key8)"))
	assert.True(t, toputils.SliceContainsString(result.MeasureFunctions, "Range2"))

	// Verify MeasureResults
	assert.Len(t, result.MeasureResults, 2) // We group by key11, which has two values (see WriteMockColSegFile())
	for i := 0; i < len(result.MeasureResults); i++ {
		min := result.MeasureResults[i].MeasureVal["min(key2)"].(int64)
		max := result.MeasureResults[i].MeasureVal["max(key2)"].(int64)
		rangeStr := result.MeasureResults[i].MeasureVal["Range2"].(string)
		rangeFloat, err := strconv.ParseFloat(rangeStr, 64)
		assert.Nil(t, err)
		assert.Equal(t, rangeFloat, float64(max-min))
	}

	// Verify Histogram
	lenHist := len(result.Histogram["test"].Results)
	assert.False(t, result.Histogram["test"].IsDateHistogram)
	assert.Equal(t, lenHist, 2, "only record-batch-1 and record-batch-0 exist")
	totalentries := numEntriesForBuffer * fileCount * numBuffers
	for i := 0; i < lenHist; i++ {
		assert.Equal(t, result.Histogram["test"].Results[i].ElemCount, uint64(totalentries/2))
		bKey := result.Histogram["test"].Results[i].BucketKey
		assert.Len(t, result.Histogram["test"].Results[i].StatRes, 1+len(simpleGroupBy.GroupByRequest.MeasureOperations))
		for _, measureCol := range mnames {
			res, ok := result.Histogram["test"].Results[i].StatRes[measureCol]
			assert.True(t, ok)
			if measureCol == "max(key2)" {
				if bKey == "record-batch-0" {
					assert.Equal(t, res.CVal, int64(numEntriesForBuffer-2))
				} else if bKey == "record-batch-1" {
					assert.Equal(t, res.CVal, int64(numEntriesForBuffer-1))
				} else {
					assert.Fail(t, "unexpected bkey %+v", bKey)
				}
			} else if measureCol == "min(key2)" {
				if bKey == "record-batch-0" {
					assert.Equal(t, res.CVal, int64(0))
				} else if bKey == "record-batch-1" {
					assert.Equal(t, res.CVal, int64(1))
				} else {
					assert.Fail(t, "unexpected bkey %+v", bKey)
				}
			} else if measureCol == "sum(key8)" {
				assert.Greater(t, res.CVal, int64(numBuffers*fileCount))
			} else if measureCol == "Range2" {
				assert.Equal(t, res.CVal, float64(numEntriesForBuffer-2))
			} else {
				assert.Fail(t, "unexpected case %+v", measureCol)
			}
		}
	}
}

func nestedAggsFilterRowsWithGroupByTest(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int) {
	ti := structs.InitTableInfo("evts", 0, false)
	value1, _ := CreateDtypeEnclosure("value1", 0)

	filter := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key1"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&filter}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 1,
			EndEpochMs:   uint64(numEntriesForBuffer) + 1,
		},
	}

	simpleGroupBy := &QueryAggregators{
		PipeCommandType: GroupByType,
		GroupByRequest: &GroupByRequest{
			GroupByColumns: []string{"key11"},
			MeasureOperations: []*MeasureAggregator{
				{MeasureCol: "key2", MeasureFunc: Max},
				{MeasureCol: "key2", MeasureFunc: Min},
				{MeasureCol: "key8", MeasureFunc: Sum},
			},
			AggName:     "test",
			BucketCount: 100,
		},
	}

	// Only one row should satisfy this.
	whereBlock := &QueryAggregators{
		PipeCommandType: OutputTransformType,
		OutputTransforms: &OutputTransforms{
			FilterRows: &BoolExpr{
				IsTerminal: true,
				ValueOp:    "=",
				LeftValue: &ValueExpr{
					ValueExprMode: VEMNumericExpr,
					NumericExpr: &NumericExpr{
						IsTerminal:      true,
						ValueIsField:    true,
						Value:           "key11",
						NumericExprMode: NEMNumberField,
					},
				},
				RightValue: &ValueExpr{
					ValueExprMode: VEMStringExpr,
					StringExpr: &StringExpr{
						RawString: "record-batch-1",
					},
				},
			},
		},
	}

	simpleGroupBy.Next = whereBlock

	sizeLimit := uint64(0)
	qc := structs.InitQueryContextWithTableInfo(ti, sizeLimit, 0, 0, false)
	result := ExecuteQuery(simpleNode, simpleGroupBy, 0, qc)
	assert.Len(t, result.MeasureResults, 1)
	assert.True(t, len(result.Histogram) > 0)
	for _, aggResult := range result.Histogram {
		assert.Len(t, aggResult.Results, 1)
	}

	whereBlock.OutputTransforms.FilterRows = &BoolExpr{
		IsTerminal: true,
		ValueOp:    "!=",
		LeftValue: &ValueExpr{
			ValueExprMode: VEMNumericExpr,
			NumericExpr: &NumericExpr{
				IsTerminal: false,
				Op:         "-",
				Left: &NumericExpr{
					IsTerminal:      true,
					Value:           "max(key2)",
					ValueIsField:    true,
					NumericExprMode: NEMNumberField,
				},
				Right: &NumericExpr{
					IsTerminal:      true,
					Value:           "min(key2)",
					ValueIsField:    true,
					NumericExprMode: NEMNumberField,
				},
			},
		},
		RightValue: &ValueExpr{
			ValueExprMode: VEMNumericExpr,
			NumericExpr: &NumericExpr{
				IsTerminal:      true,
				ValueIsField:    false,
				Value:           fmt.Sprintf("%v", float64(numEntriesForBuffer-2)),
				NumericExprMode: NEMNumber,
			},
		},
	}

	result = ExecuteQuery(simpleNode, simpleGroupBy, 0, qc)
	assert.Len(t, result.MeasureResults, 0)
	assert.True(t, len(result.Histogram) > 0)
	for _, aggResult := range result.Histogram {
		assert.Len(t, aggResult.Results, 0)
	}

	// Change it so both rows pass.
	whereBlock.OutputTransforms.FilterRows.ValueOp = ">="

	result = ExecuteQuery(simpleNode, simpleGroupBy, 0, qc)
	assert.Len(t, result.MeasureResults, 2)
	assert.True(t, len(result.Histogram) > 0)
	for _, aggResult := range result.Histogram {
		assert.Len(t, aggResult.Results, 2)
	}

	// Now group by key4. See WriteMockColSegFile() for how it's mocked.
	// key4 will have numEntriesForBuffer values: "0", "2", "4", ..., string(2 * (numEntriesForBuffer - 1))
	simpleGroupBy = &QueryAggregators{
		PipeCommandType: GroupByType,
		GroupByRequest: &GroupByRequest{
			GroupByColumns: []string{"key4"},
			MeasureOperations: []*MeasureAggregator{
				{MeasureCol: "key2", MeasureFunc: Max},
				{MeasureCol: "key2", MeasureFunc: Min},
				{MeasureCol: "key8", MeasureFunc: Sum},
			},
			AggName:     "test",
			BucketCount: 100,
		},
		Next: whereBlock,
	}

	// Test that we can do numeric expressions with key4 even though its values are strings.
	whereBlock.OutputTransforms.FilterRows = &BoolExpr{
		IsTerminal: true,
		ValueOp:    "<",
		LeftValue: &ValueExpr{
			ValueExprMode: VEMNumericExpr,
			NumericExpr: &NumericExpr{
				IsTerminal: false,
				Op:         "/",
				Left: &NumericExpr{
					IsTerminal:      true,
					Value:           "key4",
					ValueIsField:    true,
					NumericExprMode: NEMNumberField,
				},
				Right: &NumericExpr{
					IsTerminal:   true,
					Value:        "2",
					ValueIsField: false,
				},
			},
		},
		RightValue: &ValueExpr{
			ValueExprMode: VEMNumericExpr,
			NumericExpr: &NumericExpr{
				IsTerminal:      true,
				ValueIsField:    false,
				Value:           "3",
				NumericExprMode: NEMNumber,
			},
		},
	}

	result = ExecuteQuery(simpleNode, simpleGroupBy, 0, qc)
	expectedLen := 3
	if numEntriesForBuffer < 3 {
		expectedLen = numEntriesForBuffer

		// If expectedLen is 0, we might pass the below assert.Len() even when
		// the query had an error and returned no MeasureResults.
		assert.True(t, expectedLen > 0)
	}
	assert.Len(t, result.MeasureResults, expectedLen)
	assert.True(t, len(result.Histogram) > 0)
	for _, aggResult := range result.Histogram {
		assert.Len(t, aggResult.Results, expectedLen)
	}

	// Test a non-terminal boolean expression: key4 = "2" OR min(key2) < 1
	// This should let two rows pass: key4 = "2" and key4 = "0" (because here min(key2) = 0)
	whereBlock.OutputTransforms.FilterRows = &BoolExpr{
		IsTerminal: false,
		BoolOp:     BoolOpOr,
		LeftBool: &BoolExpr{
			IsTerminal: true,
			ValueOp:    "=",
			LeftValue: &ValueExpr{
				ValueExprMode: VEMNumericExpr,
				NumericExpr: &NumericExpr{
					IsTerminal:      true,
					ValueIsField:    true,
					Value:           "key4",
					NumericExprMode: NEMNumberField,
				},
			},
			RightValue: &ValueExpr{
				ValueExprMode: VEMNumericExpr,
				NumericExpr: &NumericExpr{
					IsTerminal:      true,
					ValueIsField:    false,
					Value:           "2",
					NumericExprMode: NEMNumber,
				},
			},
		},
		RightBool: &BoolExpr{
			IsTerminal: true,
			ValueOp:    "<",
			LeftValue: &ValueExpr{
				ValueExprMode: VEMNumericExpr,
				NumericExpr: &NumericExpr{
					IsTerminal:      true,
					ValueIsField:    true,
					Value:           "min(key2)",
					NumericExprMode: NEMNumberField,
				},
			},
			RightValue: &ValueExpr{
				ValueExprMode: VEMNumericExpr,
				NumericExpr: &NumericExpr{
					IsTerminal:      true,
					ValueIsField:    false,
					Value:           "1",
					NumericExprMode: NEMNumber,
				},
			},
		},
	}

	result = ExecuteQuery(simpleNode, simpleGroupBy, 0, qc)
	assert.Len(t, result.MeasureResults, 2)
	assert.True(t, len(result.Histogram) > 0)
	for _, aggResult := range result.Histogram {
		assert.Len(t, aggResult.Results, 2)
	}
}

func asyncQueryTest(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int) {
	value1, _ := CreateDtypeEnclosure("*", 0)
	allColumns := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "*"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&allColumns}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 1,
			EndEpochMs:   uint64(numEntriesForBuffer) + 1,
		},
	}

	simpleTimeHistogram := &QueryAggregators{
		TimeHistogram: &TimeBucket{
			StartTime:      1,
			EndTime:        uint64(numEntriesForBuffer) + 1,
			IntervalMillis: 1,
			AggName:        "testTime",
		},
	}
	ti := structs.InitTableInfo("evts", 0, false)
	qid := uint64(10101)
	scroll := 0
	sizeLimit := uint64(50)
	totalPossible := uint64(numBuffers * numEntriesForBuffer * fileCount)
	queryContext := structs.InitQueryContextWithTableInfo(ti, sizeLimit, scroll, 0, false)
	result, err := ExecuteAsyncQuery(simpleNode, simpleTimeHistogram, qid, queryContext)
	assert.Nil(t, err)
	assert.NotNil(t, result)

	sawRunning := false
	sawExit := false
	sawQueryUpdate := false
	sawRRCComplete := false
	var rrcs []*RecordResultContainer
	var qc uint64
	var buckets map[string]*AggregationResult

	for result != nil {
		updateType := <-result
		switch updateType.StateName {
		case query.RUNNING:
			sawRunning = true
		case query.QUERY_UPDATE:
			rrcs, qc, _, err = query.GetRawRecordInfoForQid(scroll, qid)
			assert.Nil(t, err)
			buckets, _ = query.GetBucketsForQid(qid)
			sawQueryUpdate = true
		case query.COMPLETE:
			sawRRCComplete = true
			rrcs, qc, _, err = query.GetRawRecordInfoForQid(scroll, qid)
			buckets, _ = query.GetBucketsForQid(qid)
			assert.Nil(t, err)
			sawExit = true
			result = nil
		}
	}

	assert.True(t, sawRunning, "shouldve seen running update")
	assert.True(t, sawExit, "shouldve seen exit update")
	assert.True(t, sawQueryUpdate, "shouldve seen query update")
	assert.True(t, sawRRCComplete, "shouldve seen rrc complete update")
	assert.NotNil(t, rrcs, "rrcs should have been populated")
	assert.NotNil(t, qc, "query counts should have been populated")
	assert.NotNil(t, buckets, "buckets should have been populated")
	assert.Len(t, rrcs, int(sizeLimit), "only sizeLimit should be returned")
	assert.Equal(t, qc, totalPossible, "should still match all possible")

	finalBuckets, finalErr := query.GetBucketsForQid(qid)
	assert.Nil(t, finalErr, "err should not be nil as qid as not been deleted")
	assert.NotNil(t, finalBuckets, "finalBuckets should not be nil as qid as not been deleted")

	query.DeleteQuery(qid)
	finalBuckets, finalErr = query.GetBucketsForQid(qid)
	assert.Error(t, finalErr, "err should exist as qid should be deleted")
	assert.Nil(t, finalBuckets, "finalBuckets should be nil as qid should be deleted")

}

func testESScroll(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int) {
	var qid uint64 = 1
	value1, _ := CreateDtypeEnclosure("*", qid)
	queryRange := &dtu.TimeRange{
		StartEpochMs: 1,
		EndEpochMs:   uint64(numEntriesForBuffer) + 1,
	}
	valueFilter := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "*"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&valueFilter}},
		TimeRange:          queryRange,
	}
	ti := structs.InitTableInfo("evts", 0, false)
	sizeLimit := uint64(10000)
	qc := structs.InitQueryContextWithTableInfo(ti, sizeLimit, 0, 0, false)
	result := ExecuteQuery(simpleNode, &QueryAggregators{}, 0, qc)
	t.Logf("Execute Query Results :%v", result)
	assert.NotNil(t, result, "Query ran successfully")
	assert.Equal(t, len(result.AllRecords), numBuffers*numEntriesForBuffer*fileCount, "all logs in all files should have matched")
	assert.Len(t, result.ErrList, 0, "no errors should have occurred")
	timeout := time.Now().UTC().Add(time.Minute * 5).Unix()
	var scrollSize uint64 = 10
	var offset = scrollSize
	resulSet := []string{}
	scrollRecord := scroll.Scroll{
		Scroll_id: "faba624a-6428-4d78-8c70-571443f0d509",
		Results:   nil,
		Size:      scrollSize,
		TimeOut:   uint64(timeout),
		Expiry:    "5m",
		Offset:    0,
		Valid:     true,
	}
	rawResults := esquery.GetQueryResponseJson(result, "evts", time.Now(), sizeLimit, qid, &QueryAggregators{})
	scrollRecord.Results = &rawResults
	scroll.SetScrollRecord("faba624a-6428-4d78-8c70-571443f0d509", &scrollRecord)
	httpresponse := esquery.GetQueryResponseJsonScroll("evts", time.Now().UTC(), sizeLimit, &scrollRecord, qid)
	t.Logf("Scroll Query results %v", httpresponse)
	assert.LessOrEqual(t, len(httpresponse.Hits.Hits), int(scrollSize), "scroll returned more records then the scroll size")
	assert.Equal(t, int(httpresponse.Hits.GetHits()), numBuffers*numEntriesForBuffer*fileCount, "all logs in all files should have matched")
	assert.Equal(t, int(scrollRecord.Offset), int(offset), "offset should have been increased by the scroll size")
	assert.Equal(t, checkScrollRecords(httpresponse.Hits.Hits, &resulSet), false, "all records in the scroll should be unique")
	iterations := int(httpresponse.Hits.GetHits() / scrollSize)
	for i := 1; i < iterations; i++ {
		t.Logf("Iteration No : %d for scroll", i)
		offset = offset + scrollSize
		httpresponse = esquery.GetQueryResponseJsonScroll("evts", time.Now().UTC(), sizeLimit, &scrollRecord, qid)
		assert.Equal(t, int(scrollRecord.Offset), int(offset), "offset should have been increased by the scroll size")
		assert.Equal(t, checkScrollRecords(httpresponse.Hits.Hits, &resulSet), false, "all records in the scroll should be unique")
	}
}

func testPipesearchScroll(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int) {
	var qid uint64 = 1
	value1, _ := CreateDtypeEnclosure("*", qid)
	queryRange := &dtu.TimeRange{
		StartEpochMs: 1,
		EndEpochMs:   uint64(numEntriesForBuffer) + 1,
	}
	valueFilter := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "*"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&valueFilter}},
		TimeRange:          queryRange,
	}
	qc := structs.InitQueryContext("evts", uint64(10), 9, 0, false)
	result := ExecuteQuery(simpleNode, &QueryAggregators{}, 0, qc)
	assert.Len(t, result.AllRecords, 1)

	qc.Scroll = 10
	result = ExecuteQuery(simpleNode, &QueryAggregators{}, 0, qc)
	assert.Len(t, result.AllRecords, 0)

	maxPossible := uint64(numBuffers * numEntriesForBuffer * fileCount)
	qc.SizeLimit = maxPossible
	qc.Scroll = int(maxPossible)
	result = ExecuteQuery(simpleNode, &QueryAggregators{}, 0, qc)
	assert.Len(t, result.AllRecords, 0)

	qc.Scroll = int(maxPossible - 5)
	result = ExecuteQuery(simpleNode, &QueryAggregators{}, 0, qc)
	assert.Len(t, result.AllRecords, 5)

}

func checkScrollRecords(response []toputils.Hits, resultSet *[]string) bool {
	log.Printf("Length of records fetched %d", len(response))
	for _, hit := range response {
		if contains(*resultSet, fmt.Sprintf("%v", hit.Source["key5"])) {
			return false
		}
		*resultSet = append(*resultSet, fmt.Sprintf("%v", hit.Source["key5"]))
	}
	return false
}

func contains(slice []string, element string) bool {
	for _, value := range slice {
		if value == element {
			return true
		}
	}
	return false
}

func getMyIds() []uint64 {
	myids := make([]uint64, 1)
	myids[0] = 0
	return myids
}

func Test_Query(t *testing.T) {
	config.InitializeDefaultConfig()
	_ = localstorage.InitLocalStorage()
	limit.InitMemoryLimiter()
	instrumentation.InitMetrics()

	err := query.InitQueryNode(getMyIds, serverutils.ExtractKibanaRequests)
	if err != nil {
		log.Fatalf("Failed to initialize query node: %v", err)
	}
	numBuffers := 5
	numEntriesForBuffer := 10
	fileCount := 2
	metadata.InitMockColumnarMetadataStore("data/", fileCount, numBuffers, numEntriesForBuffer)

	simpleQueryTest(t, numBuffers, numEntriesForBuffer, fileCount)
	wildcardQueryTest(t, numBuffers, numEntriesForBuffer, fileCount)
	timeHistogramQueryTest(t, numBuffers, numEntriesForBuffer, fileCount)
	groupByQueryTest(t, numBuffers, numEntriesForBuffer, fileCount)
	timechartGroupByQueryTest(t, numBuffers, numEntriesForBuffer, fileCount)
	nestedQueryTest(t, numBuffers, numEntriesForBuffer, fileCount)
	nestedAggregationQueryTest(t, numBuffers, numEntriesForBuffer, fileCount)
	nestedAggregationQueryWithGroupByTest(t, numBuffers, numEntriesForBuffer, fileCount)
	nestedAggsNumericColRequestTest(t, numBuffers, numEntriesForBuffer, fileCount)
	nestedAggsNumericColRequestWithGroupByTest(t, numBuffers, numEntriesForBuffer, fileCount)
	nestedAggsFilterRowsWithGroupByTest(t, numBuffers, numEntriesForBuffer, fileCount)
	asyncQueryTest(t, numBuffers, numEntriesForBuffer, fileCount)

	groupByQueryTestsForAsteriskQueries(t, numBuffers, numEntriesForBuffer, fileCount)

	os.RemoveAll("data/")
}

func Test_Scroll(t *testing.T) {
	config.InitializeDefaultConfig()
	limit.InitMemoryLimiter()
	_ = localstorage.InitLocalStorage()

	err := query.InitQueryNode(getMyIds, serverutils.ExtractKibanaRequests)
	if err != nil {
		log.Fatalf("Failed to initialize query node: %v", err)
	}
	numBuffers := 5
	numEntriesForBuffer := 10
	fileCount := 2
	metadata.InitMockColumnarMetadataStore("data/", fileCount, numBuffers, numEntriesForBuffer)
	testESScroll(t, numBuffers, numEntriesForBuffer, fileCount)
	testPipesearchScroll(t, numBuffers, numEntriesForBuffer, fileCount)
	os.RemoveAll("data/")
}

func Test_unrotatedQuery(t *testing.T) {
	config.InitializeTestingConfig()
	config.SetDataPath("unrotatedtest/")
	limit.InitMemoryLimiter()
	err := query.InitQueryNode(getMyIds, serverutils.ExtractKibanaRequests)
	assert.Nil(t, err)
	writer.InitWriterNode()
	_ = localstorage.InitLocalStorage()
	numBatch := 10
	numRec := 100

	// disable dict encoding globally
	writer.SetCardinalityLimit(0)

	for batch := 0; batch < numBatch; batch++ {
		for rec := 0; rec < numRec; rec++ {
			record := make(map[string]interface{})
			record["col1"] = "abc"
			record["col2"] = strconv.Itoa(rec)
			record["col3"] = "batch-" + strconv.Itoa(batch)
			record["col4"] = uuid.New().String()
			if rec >= numRec/2 {
				// add new column after it has reached halfway into filling a block
				// so that past records can we backfilled
				record["col5"] = "def"
			}
			record["timestamp"] = uint64(rec)
			rawJson, err := json.Marshal(record)
			assert.Nil(t, err)
			err = writer.AddEntryToInMemBuf("test1", rawJson, uint64(rec)+1, "test", 10, false,
				SIGNAL_EVENTS, 0)
			assert.Nil(t, err)
		}

		sleep := time.Duration(1)
		time.Sleep(sleep)
		writer.FlushWipBufferToFile(&sleep)
	}
	sleep := time.Duration(1)
	time.Sleep(sleep)
	writer.FlushWipBufferToFile(&sleep)
	aggs := &QueryAggregators{
		EarlyExit: false,
	}

	// col3=batch-1
	value1, _ := CreateDtypeEnclosure("batch-1", 0)
	valueFilter := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "col3"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&valueFilter}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 0,
			EndEpochMs:   uint64(numRec) + 1,
		},
	}
	sizeLimit := uint64(10000)
	scroll := 0
	qc := structs.InitQueryContext("test", sizeLimit, scroll, 0, false)
	result := ExecuteQuery(simpleNode, aggs, uint64(numBatch*numRec*2), qc)
	assert.Equal(t, uint64(numRec), result.TotalResults.TotalCount)
	assert.Equal(t, Equals, result.TotalResults.Op)

	// *=batch-1
	valueFilter = FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "*"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode = &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&valueFilter}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 0,
			EndEpochMs:   uint64(numRec) + 1,
		},
	}
	result = ExecuteQuery(simpleNode, aggs, uint64(numBatch*numRec*2), qc)
	query.DeleteQuery(uint64(numBatch * numRec * 2))
	assert.Equal(t, uint64(numRec), result.TotalResults.TotalCount)
	assert.Equal(t, Equals, result.TotalResults.Op)

	// *=def
	def, _ := CreateDtypeEnclosure("def", 0)
	valueFilter = FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "*"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: def}}},
		},
	}
	simpleNode = &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&valueFilter}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 0,
			EndEpochMs:   uint64(numRec) + 1,
		},
	}
	result = ExecuteQuery(simpleNode, aggs, uint64(numBatch*numRec*2), qc)
	backfillExpectecd := uint64(numRec*numBatch) / 2 // since we added new column halfway through the block
	assert.Equal(t, backfillExpectecd, result.TotalResults.TotalCount,
		"backfillExpectecd: %v, actual: %v", backfillExpectecd, result.TotalResults.TotalCount)
	assert.Equal(t, Equals, result.TotalResults.Op)

	// col5=def
	valueFilter = FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "col5"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: def}}},
		},
	}
	simpleNode = &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&valueFilter}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 0,
			EndEpochMs:   uint64(numRec) + 1,
		},
	}
	result = ExecuteQuery(simpleNode, aggs, uint64(numBatch*numRec*2), qc)
	assert.Equal(t, backfillExpectecd, result.TotalResults.TotalCount)
	assert.Equal(t, Equals, result.TotalResults.Op)
	os.RemoveAll(config.GetDataPath())
}

func Test_EncodeDecodeBlockSummary(t *testing.T) {

	batchSize := 10
	entryCount := 10
	dir := "data/"
	err := os.MkdirAll(dir, os.FileMode(0755))
	_ = localstorage.InitLocalStorage()

	if err != nil {
		log.Fatal(err)
	}
	currFile := dir + "query_test.seg"
	_, blockSummaries, _, _, allBmhInMem, _ := writer.WriteMockColSegFile(currFile, batchSize, entryCount)
	blockSumFile := dir + "query_test.bsu"

	writer.WriteMockBlockSummary(blockSumFile, blockSummaries, allBmhInMem)
	blockSums, readAllBmh, _, err := microreader.ReadBlockSummaries(blockSumFile, []byte{})
	if err != nil {
		os.RemoveAll(dir)
		log.Fatal(err)
	}

	for i := 0; i < len(blockSums); i++ {
		assert.Equal(t, blockSums[i].HighTs, blockSummaries[i].HighTs)
		assert.Equal(t, blockSums[i].LowTs, blockSummaries[i].LowTs)
		assert.Equal(t, blockSums[i].RecCount, blockSummaries[i].RecCount)

		// cnames are create in WriteMockColSegFile, we will only verify one of cnames
		// cnames start from key0..key11
		// key1 stores "value1", and the blockLen was calculated by running thw writemock.. func with print statement
		assert.Equal(t, uint32(30), readAllBmh[uint16(i)].ColumnBlockLen["key1"])
		assert.Equal(t, int64(i*30), readAllBmh[uint16(i)].ColumnBlockOffset["key1"])
	}
	os.RemoveAll(dir)
}

func Benchmark_agileTreeQueryReader(t *testing.B) {
	// go test -run=Bench -bench=Benchmark_agileTreeQueryReader -benchmem -memprofile memprofile.out -o rawsearch_mem
	// go test -run=Bench -bench=Benchmark_agileTreeQueryReader -cpuprofile cpuprofile.out -o rawsearch_cpu

	segKeyPref := "/Users/kunalnawale/work/perf/siglens/data/Kunals-MacBook-Pro.local/final/ind-0/0-3544697602014606120/"

	grpByCols := []string{"passenger_count", "pickup_date", "trip_distance"}
	measureOps := []*structs.MeasureAggregator{
		{MeasureCol: "total_amount", MeasureFunc: utils.Count},
	}
	grpByRequest := &GroupByRequest{MeasureOperations: measureOps, GroupByColumns: grpByCols}

	aggs := &QueryAggregators{
		GroupByRequest: grpByRequest,
	}

	agileTreeBuf := make([]byte, 300_000_000)
	qid := uint64(67)
	qType := structs.QueryType(structs.RRCCmd)

	allSearchResults, err1 := segresults.InitSearchResults(0, aggs, qType, qid)
	assert.NoError(t, err1)

	numSegs := 114
	for skNum := 0; skNum < numSegs; skNum++ {
		sTime := time.Now()

		segKey := fmt.Sprintf("%v/%v/%v", segKeyPref, skNum, skNum)
		blkResults, err := blockresults.InitBlockResults(0, aggs, qid)
		assert.NoError(t, err)

		str, err := segread.InitNewAgileTreeReader(segKey, qid)
		assert.NoError(t, err)

		err1 := str.ApplyGroupByJit(grpByRequest.GroupByColumns, measureOps, blkResults, qid, agileTreeBuf)
		assert.NoError(t, err1)

		//log.Infof("Aggs seg: %v query, time: %+v", skNum, time.Since(sTime))

		res := blkResults.GetGroupByBuckets()
		assert.NotNil(t, res)
		assert.NotEqual(t, 0, res.Results)

		allSearchResults.AddBlockResults(blkResults)

		srRes := allSearchResults.BlockResults.GetGroupByBuckets()
		assert.NotNil(t, srRes)
		assert.NotEqual(t, 0, srRes.Results)

		log.Infof("Aggs query, segNum: %v, Num of bkt key: %v, time: %v", skNum,
			len(srRes.Results), time.Since(sTime))

		_ = allSearchResults.GetBucketResults()
	}

	res := allSearchResults.BlockResults.GetGroupByBuckets()
	log.Infof("Aggs query, Num of bkt key: %v", len(res.Results))

}

func measureColsTest(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int, measureCol string, measureFunc AggregateFunctions) {
	value1, _ := CreateDtypeEnclosure("value1", 0)
	// wildcard all columns
	ti := structs.InitTableInfo("evts", 0, false)
	allColumns := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "*"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&allColumns}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 0,
			EndEpochMs:   uint64(numEntriesForBuffer),
		},
	}
	qc := structs.InitQueryContextWithTableInfo(ti, 10000, 0, 0, false)
	result := ExecuteQuery(simpleNode, &QueryAggregators{
		MeasureOperations: []*MeasureAggregator{
			{MeasureCol: measureCol, MeasureFunc: measureFunc},
		},
	}, 0, qc)

	if measureCol == "*" && measureFunc != Count {
		assert.Len(t, result.AllRecords, 0)
		assert.Zero(t, result.TotalResults.TotalCount)
		assert.False(t, result.TotalResults.EarlyExit)
	}
}

func groupByAggQueryTest(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int, measureCol string, measureFunc AggregateFunctions) *NodeResult {
	value1, _ := CreateDtypeEnclosure("value1", 0)
	ti := structs.InitTableInfo("evts", 0, false)
	allColumns := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key1"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&allColumns}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 1,
			EndEpochMs:   uint64(numEntriesForBuffer) + 1,
		},
	}

	simpleGroupBy := &QueryAggregators{
		GroupByRequest: &GroupByRequest{
			GroupByColumns: []string{"key11"},
			MeasureOperations: []*MeasureAggregator{
				{MeasureCol: measureCol, MeasureFunc: measureFunc},
			},
			AggName:     "test",
			BucketCount: 100,
		},
	}

	qc := structs.InitQueryContextWithTableInfo(ti, 10000, 0, 0, false)
	result := ExecuteQuery(simpleNode, simpleGroupBy, 102, qc)
	lenHist := len(result.Histogram["test"].Results)
	assert.False(t, result.Histogram["test"].IsDateHistogram)
	if measureFunc == Count {
		assert.Equal(t, lenHist, 2, "only record-batch-1 and record-batch-0 exist")
		totalentries := numEntriesForBuffer * fileCount * numBuffers
		for i := 0; i < lenHist; i++ {
			assert.Equal(t, result.Histogram["test"].Results[i].ElemCount, uint64(totalentries/2))
			bKey := result.Histogram["test"].Results[i].BucketKey
			assert.Len(t, result.Histogram["test"].Results[i].StatRes, len(simpleGroupBy.GroupByRequest.MeasureOperations))
			log.Infof("bkey is %+v", bKey)
			res, ok := result.Histogram["test"].Results[i].StatRes[fmt.Sprintf("count(%v)", measureCol)]
			assert.True(t, ok)
			assert.Equal(t, res.CVal, uint64(50))
		}
	} else if measureCol == "*" && measureFunc != Count {
		assert.NotZero(t, len(result.ErrList))
		assert.Len(t, result.AllRecords, 0)
		assert.Zero(t, result.TotalResults.TotalCount)
		assert.False(t, result.TotalResults.EarlyExit)
	}

	return result
}

func groupByQueryTestsForAsteriskQueries(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int) {
	asteriskResult := groupByAggQueryTest(t, numBuffers, numEntriesForBuffer, fileCount, "*", Count)
	columnarResult := groupByAggQueryTest(t, numBuffers, numEntriesForBuffer, fileCount, "key11", Count)

	assert.Equal(t, asteriskResult.TotalRRCCount, columnarResult.TotalRRCCount)
	assert.Equal(t, asteriskResult.TotalResults.TotalCount, columnarResult.TotalResults.TotalCount)

	for recIdx, rec := range asteriskResult.AllRecords {
		assert.Equal(t, rec.RecordNum, columnarResult.AllRecords[recIdx].RecordNum)
		assert.Equal(t, rec.SortColumnValue, columnarResult.AllRecords[recIdx].SortColumnValue)
		assert.Equal(t, rec.VirtualTableName, columnarResult.AllRecords[recIdx].VirtualTableName)
	}

	groupByAggQueryTest(t, numBuffers, numEntriesForBuffer, fileCount, "*", Avg)
	measureColsTest(t, numBuffers, numEntriesForBuffer, fileCount, "*", Avg)
}

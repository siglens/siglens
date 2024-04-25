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

package segread

/*
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	bbp "github.com/valyala/bytebufferpool"
*/

/*
   // todo decoding this test since we are not sure yet if we will support filters via agileTree

func Test_StartTreeColumnFilter(t *testing.T) {
	allCols := make(map[string]bool)
	segstats := make(map[string]*structs.SegStats)

	wipBlock := WipBlock{
		columnBlooms:       make(map[string]*writer.BloomIndex),
		columnRangeIndexes: make(map[string]*RangeIndex),
		colWips:            make(map[string]*ColWip),
		pqMatches:          make(map[string]*pqmr.PQMatchResults),
		columnsInBlock:     make(map[string]bool),
		tomRollup:          make(map[uint64]*RolledRecs),
		tohRollup:          make(map[uint64]*RolledRecs),
		todRollup:          make(map[uint64]*RolledRecs),
		bb:                 bbp.Get(),
		blockTs:            make([]uint64, 0),
	}
	segStore := &SegStore{
		wipBlock:       wipBlock,
		SegmentKey:     "test-segkey",
		AllSeenColumns: allCols,
		pqTracker:      initPQTracker(),
		AllSst:         segstats,
		numBlocks:      0,
	}

	entryCount := uint16(16_000)
	tsKey := config.GetTimeStampKey()
	for i := uint16(0); i < entryCount; i++ {
		entry := make(map[string]interface{})
		entry["key1"] = "match words 123 abc"
		entry["key2"] = "value1"
		entry["key3"] = i
		if i%2 == 0 {
			entry["key4"] = "even"
		} else {
			entry["key4"] = "odd"
		}
		entry["key5"] = fmt.Sprintf("batch-%v", rand.Intn(10))
		entry["key6"] = rand.Int()

		timestp := uint64(i) + 1 // dont start with 0 as timestamp
		raw, _ := json.Marshal(entry)
		_, _, err := segStore.EncodeColumns(raw, timestp, &tsKey)
		assert.NoError(t, err)
		segStore.wipBlock.blockSummary.RecCount += 1
	}

	groupByCols := []string{"key2", "key4", "key5"}
	aggFunctions := make([]*structs.MeasureAggregator, 0)

	for _, col := range []string{"key3", "key6"} {
		for _, fun := range []utils.AggregateFunctions{utils.Sum, utils.Min, utils.Max} {
			aggFunctions = append(aggFunctions, &structs.MeasureAggregator{MeasureCol: col, MeasureFunc: fun})
		}
	}

	even, _ := utils.CreateDtypeEnclosure("even", 0)
	evenQuery := &structs.SearchQuery{
		ExpressionFilter: &structs.SearchExpression{
			LeftSearchInput:  &structs.SearchExpressionInput{ColumnName: "key4"},
			FilterOp:         utils.Equals,
			RightSearchInput: &structs.SearchExpressionInput{ColumnValue: even},
		},
		SearchType: structs.SimpleExpression,
	}
	evenQuery.GetQueryInfo()
	var builder StarTreeBuilder

	expected := make([]uint16, entryCount/2)
	idx := 0
	for i := uint16(0); i < entryCount; i += 2 {
		expected[idx] = i
		idx++
	}

	for i := 0; i < 100; i++ {
		rand.Shuffle(len(groupByCols), func(i, j int) { groupByCols[i], groupByCols[j] = groupByCols[j], groupByCols[i] })
		log.Infof("iteration %+v using groupby cols %+v", i, groupByCols)
		builder.Reset(&segStore.wipBlock, groupByCols)
		result := builder.ComputeStarTree(&segStore.wipBlock, groupByCols, aggFunctions)
		data, err := builder.EncodeStarTree(&segStore.wipBlock, &result, groupByCols, aggFunctions)
		assert.Nil(t, err)
		decoded, err := DecodeStarTree(data)
		assert.Nil(t, err)
		check(t, *decoded, groupByCols, aggFunctions, &result)
		retVal, err := decoded.ApplyColumnFilter(evenQuery)
		log.Infof("iteration %+v has %+v results", i, len(retVal))
		assert.Equal(t, decoded.metadata.GroupByKeys, groupByCols)
		assert.Nil(t, err)
		assert.Equal(t, uint16(len(retVal)), entryCount/2)
		sort.Slice(retVal, func(i, j int) bool { return retVal[i] < retVal[j] })
		assert.Equal(t, retVal, expected)
	}
	}


func Test_StartTreeGroupBy(t *testing.T) {
	allCols := make(map[string]bool)
	segstats := make(map[string]*structs.SegStats)

	wipBlock := writer.WipBlock{
		columnBlooms:       make(map[string]*writer.BloomIndex),
		columnRangeIndexes: make(map[string]*writer.RangeIndex),
		colWips:            make(map[string]*writer.ColWip),
		pqMatches:          make(map[string]*pqmr.PQMatchResults),
		columnsInBlock:     make(map[string]bool),
		tomRollup:          make(map[uint64]*RolledRecs),
		tohRollup:          make(map[uint64]*RolledRecs),
		todRollup:          make(map[uint64]*RolledRecs),
		bb:                 bbp.Get(),
		blockTs:            make([]uint64, 0),
	}
	segStore := &SegStore{
		wipBlock:       wipBlock,
		SegmentKey:     "test-segkey",
		AllSeenColumns: allCols,
		pqTracker:      initPQTracker(),
		AllSst:         segstats,
		numBlocks:      0,
	}

	entryCount := 16_000
	tsKey := config.GetTimeStampKey()
	for i := 0; i < entryCount; i++ {
		entry := make(map[string]interface{})
		entry["key1"] = "match words 123 abc"
		entry["key2"] = "value1"
		entry["key3"] = i
		if i%2 == 0 {
			entry["key4"] = "even"
		} else {
			entry["key4"] = "odd"
		}
		entry["key5"] = fmt.Sprintf("batch-%v", rand.Intn(10))
		entry["key6"] = rand.Int()

		timestp := uint64(i) + 1 // dont start with 0 as timestamp
		raw, _ := json.Marshal(entry)
		_, _, err := segStore.EncodeColumns(raw, timestp, &tsKey)
		assert.NoError(t, err)
		segStore.wipBlock.blockSummary.RecCount += 1
	}

	groupByCols := []string{"key2", "key4", "key5"}
	aggFunctions := make([]*structs.MeasureAggregator, 0)

	for _, col := range []string{"key3", "key6"} {
		for _, fun := range []utils.AggregateFunctions{utils.Sum, utils.Min, utils.Max} {
			aggFunctions = append(aggFunctions, &structs.MeasureAggregator{MeasureCol: col, MeasureFunc: fun})
		}
	}

	grpByCols := []string{"key4"}
	measureOps := []*structs.MeasureAggregator{
		{MeasureCol: "key3", MeasureFunc: utils.Min},
		{MeasureCol: "key3", MeasureFunc: utils.Max},
		{MeasureCol: "key3", MeasureFunc: utils.Sum},
	}
	grpByRequest := &structs.GroupByRequest{MeasureOperations: measureOps, GroupByColumns: grpByCols}

	var builder StarTreeBuilder

	oddSum := int64(0)
	evenSum := int64(0)
	for i := int64(0); i < int64(entryCount); i++ {
		if i%2 == 0 {
			evenSum += i
		} else {
			oddSum += i
		}
	}

	for i := 0; i < 100; i++ {
		rand.Shuffle(len(aggFunctions), func(i, j int) { aggFunctions[i], aggFunctions[j] = aggFunctions[j], aggFunctions[i] })
		log.Infof("iteration %+v using agg fns cols %+v", i, aggFunctions)
		builder.Reset(&segStore.wipBlock, groupByCols)
		result := builder.ComputeStarTree(&segStore.wipBlock, groupByCols, aggFunctions)
		_, err := builder.EncodeStarTree(&segStore.wipBlock, &result, groupByCols, aggFunctions)
		assert.Nil(t, err)


		// todo write UTs to have a str.ReadMeta and compare the decoded treeMeta.
		// todo write a just-in-time decoder to see if the aggvalues that are returned are accurate
		// we need to first go through each block and write this block the .str file
		// and create blocksummary as you encode each tree and then pass it to str.InitNewAgileTreeReader
		// use the WriteMockSegFile to create a segfile


		decoded, err := DecodeStarTree(data)
		assert.Nil(t, err)
		check(t, *decoded, groupByCols, aggFunctions, &result)
		retVal, err := decoded.ApplyGroupBy(grpByRequest)
		assert.Equal(t, decoded.metadata.GroupByKeys, groupByCols)
		assert.Nil(t, err)
		assert.Len(t, retVal, 2, "key4 has 2 unique values")
		assert.Contains(t, retVal, "even")
		assert.Contains(t, retVal, "odd")

		evenAggs := retVal["even"]
		assert.Len(t, evenAggs, len(measureOps))
		assert.Equal(t, evenAggs[0].CVal.(int64), int64(0), "min is 0")
		assert.Equal(t, evenAggs[1].CVal.(int64), int64(entryCount-2))
		assert.Equal(t, evenAggs[2].CVal.(int64), evenSum, "sum must be greater than max")

		oddAggs := retVal["odd"]
		assert.Len(t, oddAggs, len(measureOps))
		assert.Equal(t, oddAggs[0].CVal.(int64), int64(1), "min is 1")
		assert.Equal(t, oddAggs[1].CVal.(int64), int64(entryCount-1))
		assert.Equal(t, oddAggs[2].CVal.(int64), oddSum, "sum must be greater than max")

	}
}
*/

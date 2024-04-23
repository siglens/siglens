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

package writer

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/querytracker"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_writePQSFiles(t *testing.T) {
	config.InitializeTestingConfig()
	config.SetPQSEnabled(true)
	InitWriterNode()
	numBatch := 10
	numRec := 100
	numStreams := 10

	value1, _ := utils.CreateDtypeEnclosure("batch-0", 0)
	query := &structs.SearchQuery{
		ExpressionFilter: &structs.SearchExpression{
			LeftSearchInput:  &structs.SearchExpressionInput{ColumnName: "col3"},
			FilterOp:         utils.Equals,
			RightSearchInput: &structs.SearchExpressionInput{ColumnValue: value1},
		},
		SearchType: structs.SimpleExpression,
	}

	node := &structs.SearchNode{
		AndSearchConditions: &structs.SearchCondition{
			SearchQueries: []*structs.SearchQuery{query},
		},
		NodeType: structs.ColumnValueQuery,
	}
	node.AddQueryInfoForNode()

	querytracker.UpdateQTUsage([]string{"test"}, node, nil)

	for batch := 0; batch < numBatch; batch++ {
		for rec := 0; rec < numRec; rec++ {
			record := make(map[string]interface{})
			record["col1"] = "abc"
			record["col2"] = strconv.Itoa(rec)
			record["col3"] = "batch-" + strconv.Itoa(batch%2)
			record["col4"] = uuid.New().String()
			record["timestamp"] = uint64(rec)

			for stremNum := 0; stremNum < numStreams; stremNum++ {
				record["col5"] = strconv.Itoa(stremNum)
				streamid := fmt.Sprintf("stream-%d", stremNum)
				raw, _ := json.Marshal(record)
				err := AddEntryToInMemBuf(streamid, raw, uint64(rec), "test", 10, false, utils.SIGNAL_EVENTS, 0)
				assert.Nil(t, err)
			}
		}

		sleep := time.Duration(1 * time.Millisecond)
		time.Sleep(sleep)
		FlushWipBufferToFile(&sleep)
	}

	assert.Greaterf(t, TotalUnrotatedMetadataSizeBytes, uint64(0), "data in unrotated metadata == 0")
	assert.Len(t, AllUnrotatedSegmentInfo, numStreams)
	numLoadedUnrotated, totalUnrotated := GetUnrotatedMetadataInfo()
	assert.Equal(t, numLoadedUnrotated, totalUnrotated)

	pqid := querytracker.GetHashForQuery(node)
	for segKey, usi := range AllUnrotatedSegmentInfo {
		log.Infof("segkey is %+v", segKey)
		spqmr, ok := usi.unrotatedPQSResults[pqid]
		assert.True(t, ok, "unrotatedPQSResults[pqid] should exist")
		assert.Equal(t, spqmr.GetNumBlocks(), uint16(numBatch))
		for i := uint16(0); i < uint16(numBatch); i++ {
			blkRes, ok := spqmr.GetBlockResults(i)
			assert.True(t, ok, "blkRes should exist")
			if i%2 == 0 {
				assert.Equal(t, blkRes.GetNumberOfSetBits(), uint(numRec))
			} else {
				assert.Equal(t, blkRes.GetNumberOfSetBits(), uint(0))
			}
		}

		assert.Len(t, usi.blockSummaries, numBatch)
		assert.Len(t, usi.allColumns, 6)
		assert.Len(t, usi.unrotatedBlockCmis, numBatch)
		assert.Greater(t, usi.searchMetadataSize, uint64(0))
		assert.Greater(t, usi.cmiSize, uint64(0))
		assert.Equal(t, usi.TableName, "test")
	}

	os.RemoveAll(config.GetDataPath())
}

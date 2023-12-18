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

package blockresults

import (
	"github.com/axiomhq/hyperloglog"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
)

type RunningBucketResults struct {
	runningStats []runningStats               // maps a stat name to running stats
	currStats    []*structs.MeasureAggregator // measure aggregators in result
	count        uint64                       // total number of elements belonging to the bucket
}

type runningStats struct {
	rawVal utils.CValueEnclosure // raw value
	hll    *hyperloglog.Sketch
}

func initRunningGroupByBucket(internalMeasureFns []*structs.MeasureAggregator) *RunningBucketResults {

	retVal := make([]runningStats, len(internalMeasureFns))
	for i := 0; i < len(internalMeasureFns); i++ {
		if internalMeasureFns[i].MeasureFunc == utils.Cardinality {
			retVal[i] = runningStats{hll: hyperloglog.New()}
		}
	}

	return &RunningBucketResults{
		count:        0,
		runningStats: retVal,
		currStats:    internalMeasureFns,
	}
}

func initRunningTimeBucket() *RunningBucketResults {

	return &RunningBucketResults{
		count: 0,
	}
}

func (rr *RunningBucketResults) AddTimeToBucketStats(count uint16) {
	rr.count += uint64(count)
}

func (rr *RunningBucketResults) AddMeasureResults(measureResults []utils.CValueEnclosure, qid uint64,
	cnt uint64) {
	for i := 0; i < len(rr.runningStats); i++ {
		if rr.currStats[i].MeasureFunc == utils.Cardinality {
			rawVal, err := measureResults[i].GetUIntValue()
			if err != nil {
				log.Errorf("AddMeasureResults: failed to add measurement to running stats: %v", err)
				continue
			}
			rr.runningStats[i].hll.InsertHash(rawVal)
		} else if rr.currStats[i].MeasureFunc == utils.Values {
			strVal, err := measureResults[i].GetString()
			if err != nil {
				log.Errorf("AddMeasureResults: failed to add measurement to running stats: %v", err)
				continue
			}

			if rr.runningStats[i].rawVal.CVal == nil {
				rr.runningStats[i].rawVal = utils.CValueEnclosure{
					Dtype: utils.SS_DT_STRING_LIST,
					CVal:  make([]string, 0),
				}
			}
			strs := rr.runningStats[i].rawVal.CVal.([]string)
			strs = append(strs, strVal)
			rr.runningStats[i].rawVal.CVal = strs
		} else {
			retVal, err := utils.Reduce(rr.runningStats[i].rawVal, measureResults[i], rr.currStats[i].MeasureFunc)
			if err != nil {
				log.Errorf("AddMeasureResults: failed to add measurement to running stats: %v", err)
			} else {
				rr.runningStats[i].rawVal = retVal
			}
		}
	}
	rr.count += cnt
}

// This assumes the order of bucketResults.RunningStats are in the same order, referencing the same measure request
func (rr *RunningBucketResults) MergeRunningBuckets(toJoin *RunningBucketResults) {
	if toJoin == nil {
		return
	}
	for i := 0; i < len(toJoin.runningStats); i++ {
		if rr.currStats[i].MeasureFunc == utils.Cardinality {
			err := rr.runningStats[i].hll.Merge(toJoin.runningStats[i].hll)
			if err != nil {
				log.Errorf("MergeRunningBuckets: failed merge HLL!: %v", err)
			}
		} else {
			retVal, err := utils.Reduce(rr.runningStats[i].rawVal, toJoin.runningStats[i].rawVal, rr.currStats[i].MeasureFunc)
			if err != nil {
				log.Errorf("MergeRunningBuckets: failed to add measurement to running stats: %v", err)
			} else {
				rr.runningStats[i].rawVal = retVal
			}
		}
	}
	rr.count += toJoin.count
}

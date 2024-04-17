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
	"errors"
	"math"
	"time"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	rutils "github.com/siglens/siglens/pkg/readerUtils"
	agg "github.com/siglens/siglens/pkg/segment/aggregations"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/results/mresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"

	log "github.com/sirupsen/logrus"
)

func ExecuteMetricsQuery(mQuery *structs.MetricsQuery, timeRange *dtu.MetricsTimeRange, qid uint64) *mresults.MetricsResult {
	querySummary := summary.InitQuerySummary(summary.METRICS, qid)
	defer querySummary.LogMetricsQuerySummary(mQuery.OrgId)
	_, err := query.StartQuery(qid, false)
	if err != nil {
		log.Errorf("ExecuteAsyncQuery: Error initializing query status! %+v", err)
		return &mresults.MetricsResult{
			ErrList: []error{err},
		}
	}
	res := query.ApplyMetricsQuery(mQuery, timeRange, qid, querySummary)
	query.DeleteQuery(qid)
	querySummary.IncrementNumResultSeries(res.GetNumSeries())
	return res
}

func ExecuteMultipleMetricsQuery(hashList []uint64, mQueries []*structs.MetricsQuery, queryOps []structs.QueryArithmetic, timeRange *dtu.MetricsTimeRange, qid uint64) *mresults.MetricsResult {
	resMap := make(map[uint64]*mresults.MetricsResult)
	for index, mQuery := range mQueries {
		querySummary := summary.InitQuerySummary(summary.METRICS, qid)
		defer querySummary.LogMetricsQuerySummary(mQuery.OrgId)
		_, err := query.StartQuery(qid, false)
		if err != nil {
			log.Errorf("ExecuteAsyncQuery: Error initializing query status! %+v", err)
			return &mresults.MetricsResult{
				ErrList: []error{err},
			}
		}
		res := query.ApplyMetricsQuery(mQuery, timeRange, qid, querySummary)
		query.DeleteQuery(qid)
		querySummary.IncrementNumResultSeries(res.GetNumSeries())
		qid = rutils.GetNextQid()
		resMap[hashList[index]] = res
		if len(mQueries) == 1 && len(queryOps) == 0 {
			return res
		}
	}

	return helperQueryArithmetic(queryOps, resMap)
}

func helperQueryArithmetic(queryOps []structs.QueryArithmetic, resMap map[uint64]*mresults.MetricsResult) *mresults.MetricsResult {
	finalResult := make(map[string]map[uint32]float64)
	for _, queryOp := range queryOps {
		resultLHS := resMap[queryOp.LHS]
		resultRHS := resMap[queryOp.RHS]
		swapped := false
		if queryOp.ConstantOp {
			resultLHS, ok := resMap[queryOp.LHS]
			valueRHS := queryOp.Constant
			if !ok { //this means the rhs is a vector result
				swapped = true
				resultLHS = resMap[queryOp.RHS]
			}

			for groupID, tsLHS := range resultLHS.Results {
				finalResult[groupID] = make(map[uint32]float64)
				for timestamp, valueLHS := range tsLHS {
					switch queryOp.Operation {
					case utils.Add:
						finalResult[groupID][timestamp] = valueLHS + valueRHS
					case utils.Divide:
						if valueRHS == 0 {
							continue
						}
						if swapped {
							valueRHS = 1 / valueRHS
						}
						finalResult[groupID][timestamp] = valueLHS / valueRHS
					case utils.Multiply:
						finalResult[groupID][timestamp] = valueLHS * valueRHS
					case utils.Subtract:
						val := valueLHS - valueRHS
						if swapped {
							val = val * -1
						}
						finalResult[groupID][timestamp] = val
					}
				}
			}

		} else {
			for groupID, tsLHS := range resultLHS.Results {
				if _, ok := resultRHS.Results[groupID]; !ok {
					continue
				} //Entries for which no matching entry in the right-hand vector are dropped
				finalResult[groupID] = make(map[uint32]float64)
				for timestamp, valueLHS := range tsLHS {
					valueRHS := resultRHS.Results[groupID][timestamp]
					switch queryOp.Operation {
					case utils.Add:
						finalResult[groupID][timestamp] = valueLHS + valueRHS
					case utils.Divide:
						if valueRHS == 0 {
							continue
						}
						finalResult[groupID][timestamp] = valueLHS / valueRHS
					case utils.Multiply:
						finalResult[groupID][timestamp] = valueLHS * valueRHS
					case utils.Subtract:
						finalResult[groupID][timestamp] = valueLHS - valueRHS
					}
				}
			}
		}
	}

	return &mresults.MetricsResult{Results: finalResult, State: mresults.AGGREGATED}
}

func ExecuteQuery(root *structs.ASTNode, aggs *structs.QueryAggregators, qid uint64, qc *structs.QueryContext) *structs.NodeResult {

	rQuery, err := query.StartQuery(qid, false)
	if err != nil {
		log.Errorf("ExecuteQuery: Error initializing query status! %+v", err)
		return &structs.NodeResult{
			ErrList: []error{err},
		}
	}
	res := executeQueryInternal(root, aggs, qid, qc, rQuery)
	res.ApplyScroll(qc.Scroll)
	res.TotalRRCCount, err = query.GetNumMatchedRRCs(qid)

	if err != nil {
		log.Errorf("qid=%d, ExecuteQuery: failed to get number of RRCs for qid! Error: %v", qid, err)
	}

	query.DeleteQuery(qid)

	return res
}

// The caller of this function is responsible for calling query.DeleteQuery(qid) to remove the qid info from memory.
// Returns a channel that will have events for query status or any error. An error means the query was not successfully started
func ExecuteAsyncQuery(root *structs.ASTNode, aggs *structs.QueryAggregators, qid uint64, qc *structs.QueryContext) (chan *query.QueryStateChanData, error) {
	rQuery, err := query.StartQuery(qid, true)
	if err != nil {
		log.Errorf("ExecuteAsyncQuery: Error initializing query status! %+v", err)
		return nil, err
	}

	go func() {
		_ = executeQueryInternal(root, aggs, qid, qc, rQuery)
	}()
	return rQuery.StateChan, nil
}

func executeQueryInternal(root *structs.ASTNode, aggs *structs.QueryAggregators, qid uint64,
	qc *structs.QueryContext, rQuery *query.RunningQueryState) *structs.NodeResult {

	startTime := time.Now()

	if qc.GetNumTables() == 0 {
		log.Infof("qid=%d, ExecuteQuery: empty array of Index Names provided", qid)
		return &structs.NodeResult{
			ErrList: []error{errors.New("empty array of Index Names provided")},
		}
	}
	if qc.SizeLimit == math.MaxUint64 {
		qc.SizeLimit = math.MaxInt64 // temp Fix: Will debug and remove it.
	}
	// if query aggregations exist, get all results then truncate after
	nodeRes := query.ApplyFilterOperator(root, root.TimeRange, aggs, qid, qc)
	if aggs != nil {
		numTotalSegments, err := query.GetTotalSegmentsToSearch(qid)
		if err != nil {
			log.Errorf("executeQueryInternal: failed to get number of total segments for qid! Error: %v", err)
		}
		nodeRes = agg.PostQueryBucketCleaning(nodeRes, aggs, nil, nil, nil, numTotalSegments, false)
	}
	// truncate final results after running post aggregations
	if uint64(len(nodeRes.AllRecords)) > qc.SizeLimit {
		nodeRes.AllRecords = nodeRes.AllRecords[0:qc.SizeLimit]
	}
	log.Infof("qid=%d, Finished execution in %+v", qid, time.Since(startTime))

	if rQuery.IsAsync() && aggs != nil && aggs.Next != nil {
		err := query.SetFinalStatsForQid(qid, nodeRes)
		if err != nil {
			log.Errorf("executeQueryInternal: failed to set final stats: %v", err)
		}
		rQuery.SendQueryStateComplete()
	}
	return nodeRes
}

func LogSearchNode(prefix string, sNode *structs.SearchNode, qid uint64) {

	sNodeJSON, _ := json.Marshal(sNode)
	log.Infof("qid=%d, SearchNode for %v: %v", qid, prefix, string(sNodeJSON))
}

func LogASTNode(prefix string, astNode *structs.ASTNode, qid uint64) {

	fullASTNodeJSON, _ := json.Marshal(astNode)
	log.Infof("qid=%d, ASTNode for %v: %v", qid, prefix, string(fullASTNodeJSON))
}

func LogNode(prefix string, node any, qid uint64) {

	nodeJSON, _ := json.Marshal(node)
	log.Infof("qid=%d, Raw value for %v: %v", qid, prefix, string(nodeJSON))
}

func LogQueryAggsNode(prefix string, node *structs.QueryAggregators, qid uint64) {

	nodeval, _ := json.Marshal(node)
	log.Infof("qid=%d, QueryAggregators for %v: %v", qid, prefix, string(nodeval))
}

func LogMetricsQuery(prefix string, mQRequest *structs.MetricsQueryRequest, qid uint64) {
	mQRequestJSON, _ := json.Marshal(mQRequest)
	log.Infof("qid=%d, mQRequest for %v: %v", qid, prefix, string(mQRequestJSON))
}

func LogQueryContext(qc *structs.QueryContext, qid uint64) {
	fullQueryContextJSON, _ := json.Marshal(qc)
	log.Infof("qid=%d,Query context: %v", qid, string(fullQueryContextJSON))
}

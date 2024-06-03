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
	"fmt"
	"math"
	"time"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	putils "github.com/siglens/siglens/pkg/integrations/prometheus/utils"
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

	mRes, err := HelperQueryArithmeticAndLogical(queryOps, resMap)
	if err != nil {
		return &mresults.MetricsResult{
			ErrList: []error{err},
		}
	}

	return mRes
}

func HelperQueryArithmeticAndLogical(queryOps []structs.QueryArithmetic, resMap map[uint64]*mresults.MetricsResult) (*mresults.MetricsResult, error) {
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
			if resultLHS == nil { // for the case where both LHS and RHS are constants
				continue
			}

			for groupID, tsLHS := range resultLHS.Results {
				finalResult[groupID] = make(map[uint32]float64)
				for timestamp, valueLHS := range tsLHS {
					putils.SetFinalResult(queryOp, finalResult, groupID, timestamp, valueLHS, valueRHS, swapped)
				}
			}

		} else {
			// Since each grpID is unique and contains label set information, we can map lGrpID to labelSet and labelSet to rGrpID.
			// This way, we can quickly find the corresponding rGrpID for a given lGrpID in the other vector. If there is no corresponding result, it means there are no matching labels between the two vectors.
			idToMatchingLabelSet := make(map[string]string)
			matchingLabelValTorightGroupID := make(map[string]string)
			hasVectorMatchingOp := queryOp.VectorMatching != nil && len(queryOp.VectorMatching.MatchingLabels) > 0
			if hasVectorMatchingOp {
				// Place the vector with higher cardinality on the left side.
				if queryOp.VectorMatching.Cardinality == structs.CardOneToMany {
					swapped = true
					resultLHS = resMap[queryOp.RHS]
					resultRHS = resMap[queryOp.LHS]
				}

				matchingLabelsComb := make(map[string]struct{}, 0)
				for lGroupID := range resultLHS.Results {
					matchingLabelValStr := putils.ExtractMatchingLabelSet(lGroupID, queryOp.VectorMatching.MatchingLabels, queryOp.VectorMatching.On)
					// If the left vector is the 'One' (vector with lower cardinality), it should not have repeated MatchingLabels.
					// E.g.: (metric1{type="compact"} on (color,type) group_right metric2), the value combinations of (color,type) must be unique in the metric1
					if queryOp.VectorMatching.Cardinality == structs.CardOneToOne {
						_, exists := matchingLabelsComb[matchingLabelValStr]
						if exists {
							return nil, fmt.Errorf("HelperQueryArithmeticAndLogical: many-to-many matching not allowed: matching labels must be unique on one side")
						}
						matchingLabelsComb[matchingLabelValStr] = struct{}{}
					}
					idToMatchingLabelSet[lGroupID] = matchingLabelValStr
				}

				matchingLabelsComb = make(map[string]struct{}, 0)
				for rGroupID := range resultRHS.Results {
					matchingLabelValStr := putils.ExtractMatchingLabelSet(rGroupID, queryOp.VectorMatching.MatchingLabels, queryOp.VectorMatching.On)
					// Right vector is always the 'One' (vector with lower cardinality), it should not have repeated MatchingLabels.
					_, exists := matchingLabelsComb[matchingLabelValStr]
					if exists {
						return nil, fmt.Errorf("HelperQueryArithmeticAndLogical: many-to-many matching not allowed: matching labels must be unique on one side")
					}
					matchingLabelsComb[matchingLabelValStr] = struct{}{}

					matchingLabelValTorightGroupID[matchingLabelValStr] = rGroupID
				}
			}

			labelStrSet := make(map[string]struct{})
			for lGroupID, tsLHS := range resultLHS.Results {
				// lGroupId is like: metricName{key:value,...
				// So, if we want to determine whether there are elements with the same labels in another metric, we need to appropriately modify the group ID.
				rGroupID := ""

				if hasVectorMatchingOp {
					matchingLabelVal, exists := idToMatchingLabelSet[lGroupID]
					if !exists {
						continue
					}
					rGroupID, exists = matchingLabelValTorightGroupID[matchingLabelVal]
					if !exists {
						continue
					}
				} else {
					labelStr := ""
					if len(lGroupID) >= len(resultLHS.MetricName) {
						labelStr = lGroupID[len(resultLHS.MetricName):]
						rGroupID = resultRHS.MetricName + labelStr
					}

					if queryOp.Operation == utils.LetOr || queryOp.Operation == utils.LetUnless {
						labelStrSet[labelStr] = struct{}{}
					}
				}

				// If 'and' operation cannot find a matching label set in the right vector, we should skip the current label set in the left vector.
				// However, for the 'or', 'unless' we do not want to skip that.
				if _, ok := resultRHS.Results[rGroupID]; !ok && queryOp.Operation != utils.LetOr && queryOp.Operation != utils.LetUnless {
					continue
				} //Entries for which no matching entry in the right-hand vector are dropped
				finalResult[lGroupID] = make(map[uint32]float64)
				for timestamp, valueLHS := range tsLHS {
					valueRHS := resultRHS.Results[rGroupID][timestamp]
					putils.SetFinalResult(queryOp, finalResult, lGroupID, timestamp, valueLHS, valueRHS, swapped)
				}
			}
			if queryOp.Operation == utils.LetOr || queryOp.Operation == utils.LetUnless {
				for rGroupID, tsRHS := range resultRHS.Results {
					labelStr := ""
					if len(rGroupID) >= len(resultRHS.MetricName) {
						labelStr = rGroupID[len(resultRHS.MetricName):]
					}

					// For 'unless' op, all matching elements in both vectors are dropped
					if queryOp.Operation == utils.LetUnless {
						lGroupID := resultLHS.MetricName + labelStr
						delete(finalResult, lGroupID)
						continue
					} else { // For 'or' op, check if the vector on the right has a label set that does not exist in the vector on the left.
						_, exists := labelStrSet[labelStr]
						// If exists, which means we already add that label set when traversing the resultLHS
						if exists {
							continue
						}

						finalResult[rGroupID] = make(map[uint32]float64)
						for timestamp, valueRHS := range tsRHS {
							finalResult[rGroupID][timestamp] = valueRHS
						}
					}
				}
			}

		}
	}

	if len(finalResult) == 0 {
		// For the case where both LHS and RHS are constants
		// We are not processing those constants. So we return the result as is.
		if len(resMap) == 1 {
			for _, res := range resMap {
				if res != nil {
					finalResult = res.Results
				}
			}
		} else {
			return &mresults.MetricsResult{ErrList: []error{errors.New("no results found")}}, nil
		}
	}

	return &mresults.MetricsResult{Results: finalResult, State: mresults.AGGREGATED}, nil
}

func setFinalRes(finalResult map[string]map[uint32]float64, groupID string, timestamp uint32, returnBool bool, comparisonBool bool, val float64) {
	// If a comparison operator, return 0/1 rather than filtering.
	if returnBool {
		finalResult[groupID][timestamp] = 0
		val = 1
	}
	if comparisonBool {
		finalResult[groupID][timestamp] = val
	}
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

	if aggs != nil && aggs.PipeCommandType == structs.VectorArithmeticExprType {
		return query.ApplyVectorArithmetic(aggs, qid)
	}

	// if query aggregations exist, get all results then truncate after
	nodeRes := query.ApplyFilterOperator(root, root.TimeRange, aggs, qid, qc)
	if aggs != nil {
		numTotalSegments, err := query.GetTotalSegmentsToSearch(qid)
		if err != nil {
			log.Errorf("executeQueryInternal: failed to get number of total segments for qid: %v! Error: %v", qid, err)
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

func LogMetricsQueryOps(prefix string, queryOps []structs.QueryArithmetic, qid uint64) {
	queryOpsJSON, _ := json.Marshal(queryOps)
	log.Infof("qid=%d, QueryOps for %v: %v", qid, prefix, string(queryOpsJSON))
}

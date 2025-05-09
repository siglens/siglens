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

package query

import (
	"fmt"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/querytracker"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/search"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

// Holder struct for all query information
type QueryInformation struct {
	sNode              *structs.SearchNode
	aggs               *structs.QueryAggregators
	queryRange         *dtu.TimeRange
	colsToSearch       map[string]bool
	indexInfo          *structs.TableInfo
	sizeLimit          uint64
	scrollFrom         int // The scroll from value for the query. This also means the number of records were already sent to the client and can be skipped
	pqid               string
	parallelismPerFile int64
	dqs                DistributedQueryServiceInterface
	persistentQuery    bool
	qid                uint64
	sNodeType          structs.SearchNodeType
	qType              structs.QueryType
	orgId              int64
	alreadyDistributed bool
	containsKibana     bool
	batchErr           *utils.BatchError
}

type QuerySegmentRequest struct {
	QueryInformation
	segKey               string
	segKeyTsRange        *dtu.TimeRange
	tableName            string
	sType                structs.SegType
	blkTracker           *structs.BlockTracker
	HasMatchedRrc        bool
	ConsistentCValLenMap map[string]uint32
	TotalRecords         uint32
}

func (qi *QueryInformation) GetSearchNode() *structs.SearchNode {
	return qi.sNode
}

func (qi *QueryInformation) GetAggregators() *structs.QueryAggregators {
	return qi.aggs
}

func (qi *QueryInformation) GetTimeRange() *dtu.TimeRange {
	return qi.queryRange
}

func (qi *QueryInformation) SetQueryTimeRange(queryRange *dtu.TimeRange) {
	qi.queryRange = queryRange
}

func (qi *QueryInformation) GetColsToSearch() map[string]bool {
	return qi.colsToSearch
}

func (qi *QueryInformation) GetQueryRangeStartMs() uint64 {
	return qi.queryRange.StartEpochMs
}

func (qi *QueryInformation) GetQueryRangeEndMs() uint64 {
	return qi.queryRange.EndEpochMs
}

func (qi *QueryInformation) GetIndexInfo() *structs.TableInfo {
	return qi.indexInfo
}

func (qi *QueryInformation) GetSizeLimit() uint64 {
	return qi.sizeLimit
}

func (qi *QueryInformation) SetSizeLimit(sizeLimit uint64) {
	qi.sizeLimit = sizeLimit
}

func (qi *QueryInformation) GetScrollFrom() int {
	return qi.scrollFrom
}

func (qi *QueryInformation) GetPqid() string {
	return qi.pqid
}

func (qi *QueryInformation) GetParallelismPerFile() int64 {
	return qi.parallelismPerFile
}

func (qi *QueryInformation) GetSearchNodeType() structs.SearchNodeType {
	return qi.sNodeType
}

func (qi *QueryInformation) SetSearchNodeType(sNodeType structs.SearchNodeType) {
	qi.sNodeType = sNodeType
}

func (qi *QueryInformation) GetQueryType() structs.QueryType {
	return qi.qType
}

func (qi *QueryInformation) GetQid() uint64 {
	return qi.qid
}

func (qi *QueryInformation) GetOrgId() int64 {
	return qi.orgId
}

func (qi *QueryInformation) IsDistributed() bool {
	if qi.dqs == nil {
		return false
	}
	return qi.dqs.IsDistributed()
}

func (qi *QueryInformation) IsAlreadyDistributed() bool {
	return qi.alreadyDistributed
}

func (qi *QueryInformation) SetAlreadyDistributed() {
	qi.alreadyDistributed = true
}

func (qi *QueryInformation) ContainsKibana() bool {
	return qi.containsKibana
}

func (qi *QueryInformation) GetBatchError() *utils.BatchError {
	return qi.batchErr
}

func (qsr *QuerySegmentRequest) GetSegKey() string {
	return qsr.segKey
}

func (qsr *QuerySegmentRequest) SetSegKey(segKey string) {
	qsr.segKey = segKey
}

func (qsr *QuerySegmentRequest) GetTimeRange() *dtu.TimeRange {
	return qsr.segKeyTsRange
}

func (qsr *QuerySegmentRequest) SetTimeRange(segKeyTsRange *dtu.TimeRange) {
	qsr.segKeyTsRange = segKeyTsRange
}

func (qsr *QuerySegmentRequest) GetTableName() string {
	return qsr.tableName
}

func (qsr *QuerySegmentRequest) GetSegType() structs.SegType {
	return qsr.sType
}

func (qsr *QuerySegmentRequest) GetStartEpochMs() uint64 {
	return qsr.segKeyTsRange.StartEpochMs
}

func (qsr *QuerySegmentRequest) GetEndEpochMs() uint64 {
	return qsr.segKeyTsRange.EndEpochMs
}

func (qsr *QuerySegmentRequest) SetSegType(sType structs.SegType) {
	qsr.sType = sType
}

func (qsr *QuerySegmentRequest) SetBlockTracker(blkTracker *structs.BlockTracker) {
	qsr.blkTracker = blkTracker
}

/*
Returns a holder struct with query information

# This contains DistributedQueryServiceInterface, which will be used to send grpcs to other nodes as needed

The caller is responsible for calling qs.Wait() to wait for all grpcs to finish
*/
func InitQueryInformation(s *structs.SearchNode, aggs *structs.QueryAggregators, queryRange *dtu.TimeRange,
	indexInfo *structs.TableInfo, sizeLimit uint64, parallelismPerFile int64, qid uint64,
	dqs DistributedQueryServiceInterface, orgid int64, scrollFrom int, containsKibana bool) (*QueryInformation, error) {
	colsToSearch, _, _ := search.GetAggColsAndTimestamp(aggs)
	isQueryPersistent, err := querytracker.IsQueryPersistent(indexInfo.GetQueryTables(), s)
	if err != nil {
		log.Errorf("InitQueryInformation: failed to check if query is persistent! Err %v", err)
		return &QueryInformation{}, err
	}
	pqid := querytracker.GetHashForQuery(s)
	sNodeType, qType := GetNodeAndQueryTypes(s, aggs)
	return &QueryInformation{
		sNode:              s,
		aggs:               aggs,
		queryRange:         queryRange,
		colsToSearch:       colsToSearch,
		indexInfo:          indexInfo,
		sizeLimit:          sizeLimit,
		scrollFrom:         scrollFrom,
		pqid:               pqid,
		parallelismPerFile: parallelismPerFile,
		dqs:                dqs,
		persistentQuery:    isQueryPersistent,
		qid:                qid,
		sNodeType:          sNodeType,
		qType:              qType,
		orgId:              orgid,
		containsKibana:     containsKibana,
		batchErr:           utils.GetOrCreateBatchErrorWithQid(qid),
	}, nil
}

// waits and closes the distributed query service
func (qi *QueryInformation) Wait(querySummary *summary.QuerySummary) error {
	return qi.dqs.Wait(qi.qid, querySummary)
}

func (qi *QueryInformation) GetDQS() DistributedQueryServiceInterface {
	return qi.dqs
}

func (qi *QueryInformation) GetSegEncToKeyBaseValue() uint32 {
	if qi.dqs == nil {
		return 0
	}
	return qi.dqs.GetSegEncToKeyBaseValue()
}

// returns map[table] -> map[segKey] -> blkTracker to pass into MicroIndexCheck and ExtractSSRFromSearchNode
// Returns error if qsr.blkTracker is nil
func (qsr *QuerySegmentRequest) GetMicroIndexFilter() (map[string]map[string]*structs.BlockTracker, error) {
	if qsr.blkTracker == nil {
		return nil, fmt.Errorf("GetMicroIndexFilter: qsr.blkTracker is nil! Cannot construct keys & blocks to filter")
	}
	retVal := make(map[string]map[string]*structs.BlockTracker)
	retVal[qsr.tableName] = make(map[string]*structs.BlockTracker)
	retVal[qsr.tableName][qsr.segKey] = qsr.blkTracker
	return retVal, nil
}

// returns map[table] -> map[segKey] -> entire file block tracker to pass into MicroIndexCheck and ExtractSSRFromSearchNode
func (qsr *QuerySegmentRequest) GetEntireFileMicroIndexFilter() map[string]map[string]*structs.BlockTracker {
	retVal := make(map[string]map[string]*structs.BlockTracker)
	retVal[qsr.tableName] = make(map[string]*structs.BlockTracker)
	retVal[qsr.tableName][qsr.segKey] = structs.InitEntireFileBlockTracker()
	return retVal
}

func ConvertASTNodeToSearchNode(node *structs.ASTNode, qid uint64) *structs.SearchNode {
	currNode := &structs.SearchNode{}
	if node.AndFilterCondition != nil {
		currNode.AndSearchConditions = convertASTConditionToSearchCondition(node.AndFilterCondition, qid)
	}

	if node.OrFilterCondition != nil {
		currNode.OrSearchConditions = convertASTConditionToSearchCondition(node.OrFilterCondition, qid)
	}
	// for exclusion, only join the column info for files that exist and not the actual search request info
	// exclusion conditions should not influence raw blocks to search
	if node.ExclusionFilterCondition != nil {
		currNode.ExclusionSearchConditions = convertASTConditionToSearchCondition(node.ExclusionFilterCondition, qid)
	}
	currNode.AddQueryInfoForNode()
	return currNode
}

func convertASTConditionToSearchCondition(condition *structs.Condition, qid uint64) *structs.SearchCondition {
	currSearch := &structs.SearchCondition{}
	if condition.FilterCriteria != nil && len(condition.FilterCriteria) > 0 {
		currSearch.SearchQueries = convertFilterCriteraToSearchQuery(condition.FilterCriteria, qid)
	}

	if condition.NestedNodes != nil && len(condition.NestedNodes) > 0 {
		for _, node := range condition.NestedNodes {
			searchNodes := ConvertASTNodeToSearchNode(node, qid)

			if currSearch.SearchNode == nil {
				currSearch.SearchNode = make([]*structs.SearchNode, 0)
			}
			currSearch.SearchNode = append(currSearch.SearchNode, searchNodes)
		}
	}
	return currSearch
}

func convertFilterCriteraToSearchQuery(conditions []*structs.FilterCriteria, qid uint64) []*structs.SearchQuery {
	finalSearchQueries := make([]*structs.SearchQuery, 0)
	for _, filter := range conditions {
		currQuery := structs.GetSearchQueryFromFilterCriteria(filter, qid)
		finalSearchQueries = append(finalSearchQueries, currQuery)
	}
	return finalSearchQueries
}

// put this in segwriter -> raw search unrotated
func ExtractSSRFromSearchNode(node *structs.SearchNode, filesToSearch map[string]map[string]*structs.BlockTracker, timeRange *dtu.TimeRange,
	indexNames []string, querySummary *summary.QuerySummary, qid uint64, isQueryPersistent bool, pqid string) map[string]*structs.SegmentSearchRequest {
	// todo: better joining of intermediate results of block summaries
	finalList := make(map[string]*structs.SegmentSearchRequest)
	if node.AndSearchConditions != nil {
		andSegmentFiles := extractSSRFromCondition(node.AndSearchConditions, segutils.And,
			filesToSearch, timeRange, indexNames, querySummary, qid, isQueryPersistent, pqid)
		for fileName, searchReq := range andSegmentFiles {
			if _, ok := finalList[fileName]; !ok {
				finalList[fileName] = searchReq
				continue
			}
			finalList[fileName].JoinRequest(searchReq, segutils.And)
		}
	}

	if node.OrSearchConditions != nil {
		orSegmentFiles := extractSSRFromCondition(node.OrSearchConditions, segutils.Or,
			filesToSearch, timeRange, indexNames, querySummary, qid, isQueryPersistent, pqid)
		for fileName, searchReq := range orSegmentFiles {
			if _, ok := finalList[fileName]; !ok {
				finalList[fileName] = searchReq
				continue
			}
			finalList[fileName].JoinRequest(searchReq, segutils.Or)
		}
	}
	// for exclusion, only join the column info for files that exist and not the actual search request info
	// exclusion conditions should not influence raw blocks to search
	if node.ExclusionSearchConditions != nil {
		exclustionSegmentFiles := extractSSRFromCondition(node.ExclusionSearchConditions, segutils.And,
			filesToSearch, timeRange, indexNames, querySummary, qid, isQueryPersistent, pqid)
		for fileName, searchReq := range exclustionSegmentFiles {
			if _, ok := finalList[fileName]; !ok {
				continue
			}
			finalList[fileName].JoinColumnInfo(searchReq)
		}
	}

	return finalList
}

func extractSSRFromCondition(condition *structs.SearchCondition, op segutils.LogicalOperator, filesToSearch map[string]map[string]*structs.BlockTracker,
	timeRange *dtu.TimeRange, indexNames []string, querySummary *summary.QuerySummary, qid uint64, isQueryPersistent bool, pqid string) map[string]*structs.SegmentSearchRequest {
	finalSegFiles := make(map[string]*structs.SegmentSearchRequest)
	if condition.SearchQueries != nil {
		for _, query := range condition.SearchQueries {
			segFiles, err := MicroIndexCheck(query, filesToSearch, timeRange, indexNames, querySummary, qid, isQueryPersistent, pqid)
			if err != nil {
				log.Errorf("qid=%d, error when checking micro indices: %+v", qid, err)
			}
			for fileName, searchReq := range segFiles {
				if _, ok := finalSegFiles[fileName]; !ok {
					finalSegFiles[fileName] = searchReq
				} else {
					finalSegFiles[fileName].JoinRequest(searchReq, op)
				}
			}
		}
	}

	if condition.SearchNode != nil {
		for _, node := range condition.SearchNode {
			segmentFiles := ExtractSSRFromSearchNode(node, filesToSearch, timeRange, indexNames, querySummary, qid, isQueryPersistent, pqid)

			for fileName, searchReq := range segmentFiles {
				if _, ok := finalSegFiles[fileName]; !ok {
					finalSegFiles[fileName] = searchReq
					continue
				}
				finalSegFiles[fileName].JoinRequest(searchReq, op)
			}
		}
	}
	return finalSegFiles
}

// todo: better and more generic node types.
// Right now, we just assume if its not ColumnValue, then it has to be TimeRangeQuery
func GetNodeTypeFromNode(node *structs.SearchNode) structs.SearchNodeType {
	var s structs.SearchNodeType
	if node.AndSearchConditions != nil {
		nodeType := GetNodeTypeFromCondition(node.AndSearchConditions)
		if nodeType == structs.ColumnValueQuery {
			return structs.ColumnValueQuery
		}
	}

	if node.OrSearchConditions != nil {
		nodeType := GetNodeTypeFromCondition(node.OrSearchConditions)
		if nodeType == structs.ColumnValueQuery {
			return structs.ColumnValueQuery
		}
	}

	if node.ExclusionSearchConditions != nil {
		nodeType := GetNodeTypeFromCondition(node.ExclusionSearchConditions)
		if nodeType == structs.ColumnValueQuery {
			return structs.ColumnValueQuery
		}
	}
	return s
}

func GetNodeTypeFromCondition(searchCond *structs.SearchCondition) structs.SearchNodeType {
	if searchCond.SearchNode != nil {
		for _, search := range searchCond.SearchNode {
			nodeType := GetNodeTypeFromNode(search)
			if nodeType == structs.ColumnValueQuery {
				return structs.ColumnValueQuery
			}
		}
	}
	if searchCond.SearchQueries != nil {
		for _, search := range searchCond.SearchQueries {
			nodeType := GetNodeTypeFromQuery(search)
			if nodeType == structs.ColumnValueQuery {
				return structs.ColumnValueQuery
			}
		}
	}
	return structs.MatchAllQuery
}

func GetNodeTypeFromQuery(query *structs.SearchQuery) structs.SearchNodeType {
	if query.ExpressionFilter != nil {
		if !query.ExpressionFilter.IsTimeRangeFilter() {
			return structs.ColumnValueQuery
		}
	} else {
		if query.MatchFilter.MatchColumn == "*" {
			return structs.MatchAllQuery
		}
		if query.MatchFilter.MatchColumn != config.GetTimeStampKey() {
			return structs.ColumnValueQuery
		}
	}
	return structs.MatchAllQuery
}

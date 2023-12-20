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

package query

import (
	"fmt"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/querytracker"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/search"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
)

// Holder struct for all query information
type queryInformation struct {
	sNode              *structs.SearchNode
	aggs               *structs.QueryAggregators
	queryRange         *dtu.TimeRange
	colsToSearch       map[string]bool
	indexInfo          *structs.TableInfo
	sizeLimit          uint64
	pqid               string
	parallelismPerFile int64
	dqs                *DistributedQueryService
	persistentQuery    bool
	qid                uint64
	sNodeType          structs.SearchNodeType
	qType              structs.QueryType
	orgId              uint64
}

type querySegmentRequest struct {
	queryInformation
	segKey        string
	segKeyTsRange *dtu.TimeRange
	tableName     string
	sType         structs.SegType
	blkTracker    *structs.BlockTracker
	HasMatchedRrc bool
}

/*
Returns a holder struct with query information

# This contains DistributedQueryService, which will be used to send grpcs to other nodes as needed

The caller is responsible for calling qs.Wait() to wait for all grpcs to finish
*/
func InitQueryInformation(s *structs.SearchNode, aggs *structs.QueryAggregators, queryRange *dtu.TimeRange,
	indexInfo *structs.TableInfo, sizeLimit uint64, parallelismPerFile int64, qid uint64,
	dqs *DistributedQueryService, orgid uint64) (*queryInformation, error) {
	colsToSearch, _, _ := search.GetAggColsAndTimestamp(aggs)
	isQueryPersistent, err := querytracker.IsQueryPersistent(indexInfo.GetQueryTables(), s)
	if err != nil {
		log.Errorf("InitQueryInformation: failed to check if query is persistent! Err %v", err)
		return &queryInformation{}, err
	}
	pqid := querytracker.GetHashForQuery(s)
	sNodeType, qType := getQueryType(s, aggs)
	return &queryInformation{
		sNode:              s,
		aggs:               aggs,
		queryRange:         queryRange,
		colsToSearch:       colsToSearch,
		indexInfo:          indexInfo,
		sizeLimit:          sizeLimit,
		pqid:               pqid,
		parallelismPerFile: parallelismPerFile,
		dqs:                dqs,
		persistentQuery:    isQueryPersistent,
		qid:                qid,
		sNodeType:          sNodeType,
		qType:              qType,
		orgId:              orgid,
	}, nil
}

// waits and closes the distributed query service
func (qi *queryInformation) Wait(querySummary *summary.QuerySummary) error {
	return qi.dqs.Wait(qi.qid, querySummary)
}

// returns map[table] -> map[segKey] -> blkTracker to pass into MicroIndexCheck and ExtractSSRFromSearchNode
// Returns error if qsr.blkTracker is nil
func (qsr *querySegmentRequest) GetMicroIndexFilter() (map[string]map[string]*structs.BlockTracker, error) {
	if qsr.blkTracker == nil {
		log.Errorf("GetMicroIndexFilter: qsr.blkTracker is nil! Cannot construct keys & blocks to filter")
		return nil, fmt.Errorf("GetMicroIndexFilter: qsr.blkTracker is nil! Cannot construct keys & blocks to filter")
	}
	retVal := make(map[string]map[string]*structs.BlockTracker)
	retVal[qsr.tableName] = make(map[string]*structs.BlockTracker)
	retVal[qsr.tableName][qsr.segKey] = qsr.blkTracker
	return retVal, nil
}

// returns map[table] -> map[segKey] -> entire file block tracker to pass into MicroIndexCheck and ExtractSSRFromSearchNode
func (qsr *querySegmentRequest) GetEntireFileMicroIndexFilter() map[string]map[string]*structs.BlockTracker {
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
		andSegmentFiles := extractSSRFromCondition(node.AndSearchConditions, utils.And,
			filesToSearch, timeRange, indexNames, querySummary, qid, isQueryPersistent, pqid)
		for fileName, searchReq := range andSegmentFiles {
			if _, ok := finalList[fileName]; !ok {
				finalList[fileName] = searchReq
				continue
			}
			finalList[fileName].JoinRequest(searchReq, utils.And)
		}
	}

	if node.OrSearchConditions != nil {
		orSegmentFiles := extractSSRFromCondition(node.OrSearchConditions, utils.Or,
			filesToSearch, timeRange, indexNames, querySummary, qid, isQueryPersistent, pqid)
		for fileName, searchReq := range orSegmentFiles {
			if _, ok := finalList[fileName]; !ok {
				finalList[fileName] = searchReq
				continue
			}
			finalList[fileName].JoinRequest(searchReq, utils.Or)
		}
	}
	// for exclusion, only join the column info for files that exist and not the actual search request info
	// exclusion conditions should not influence raw blocks to search
	if node.ExclusionSearchConditions != nil {
		exclustionSegmentFiles := extractSSRFromCondition(node.ExclusionSearchConditions, utils.And,
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

func extractSSRFromCondition(condition *structs.SearchCondition, op utils.LogicalOperator, filesToSearch map[string]map[string]*structs.BlockTracker,
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

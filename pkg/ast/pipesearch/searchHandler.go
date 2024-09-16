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

package pipesearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/siglens/siglens/pkg/alerts/alertutils"
	"github.com/siglens/siglens/pkg/common/dtypeutils"
	fileutils "github.com/siglens/siglens/pkg/common/fileutils"
	rutils "github.com/siglens/siglens/pkg/readerUtils"
	"github.com/siglens/siglens/pkg/segment"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/query/metadata"
	"github.com/siglens/siglens/pkg/segment/reader/record"
	"github.com/siglens/siglens/pkg/segment/results/segresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

/*
Example incomingBody

{"searchText":"*","startEpoch":1656716713300,"endEpoch":1656717613300,"indexName":"*", "size": 1000, "from": 0}

# Returns searchText,startEpoch,endEpoch,finalSize,indexName,scrollFrom

finalSize = size + from
*/
func ParseSearchBody(jsonSource map[string]interface{}, nowTs uint64) (string, uint64, uint64, uint64, string, int) {
	var searchText, indexName string
	var startEpoch, endEpoch, finalSize uint64
	var scrollFrom int
	sText, ok := jsonSource["searchText"]
	if !ok || sText == "" {
		searchText = "*"
	} else {
		switch val := sText.(type) {
		case string:
			searchText = val
		default:
			log.Errorf("ParseSearchBody: searchText is not a string! val: %+v", val)
		}
	}

	iText, ok := jsonSource["indexName"]
	if !ok || iText == "" {
		indexName = "*"
	} else {
		switch val := iText.(type) {
		case string:
			indexName = val
		case []string:
			indexName = strings.Join(val[:], ",")
		case []interface{}:

			valLen := len(val)
			indexName = ""
			for idx, indVal := range val {
				if idx == valLen-1 {
					indexName += fmt.Sprintf("%v", indVal)
				} else {
					indexName += fmt.Sprintf("%v,", indVal)
				}
			}

		default:
			log.Errorf("ParseSearchBody: indexName is not a string! val: %+v, type: %T", val, iText)
		}
	}

	startE, ok := jsonSource["startEpoch"]
	if !ok || startE == nil {
		startEpoch = nowTs - (15 * 60 * 1000)
	} else {
		switch val := startE.(type) {
		case json.Number:
			temp, _ := val.Int64()
			startEpoch = uint64(temp)
		case float64:
			startEpoch = uint64(val)
		case int64:
			startEpoch = uint64(val)
		case uint64:
			startEpoch = uint64(val)
		case string:
			defValue := nowTs - (15 * 60 * 1000)
			startEpoch = utils.ParseAlphaNumTime(nowTs, string(val), defValue)
		default:
			startEpoch = nowTs - (15 * 60 * 1000)
		}
	}

	endE, ok := jsonSource["endEpoch"]
	if !ok || endE == nil {
		endEpoch = nowTs
	} else {
		switch val := endE.(type) {
		case json.Number:
			temp, _ := val.Int64()
			endEpoch = uint64(temp)
		case float64:
			endEpoch = uint64(val)
		case int64:
			endEpoch = uint64(val)
		case uint64:
			endEpoch = uint64(val)
		case string:
			endEpoch = utils.ParseAlphaNumTime(nowTs, string(val), nowTs)
		default:
			endEpoch = nowTs
		}
	}

	size, ok := jsonSource["size"]
	if !ok || size == 0 {
		finalSize = uint64(100)
	} else {
		switch val := size.(type) {
		case json.Number:
			temp, _ := val.Int64()
			finalSize = uint64(temp)
		case float64:
			finalSize = uint64(val)
		case int64:
			finalSize = uint64(val)
		case uint64:
			finalSize = uint64(val)
		case int32:
			finalSize = uint64(val)
		case uint32:
			finalSize = uint64(val)
		case int16:
			finalSize = uint64(val)
		case uint16:
			finalSize = uint64(val)
		case int8:
			finalSize = uint64(val)
		case uint8:
			finalSize = uint64(val)
		case int:
			finalSize = uint64(val)
		default:
			finalSize = uint64(100)
		}
	}

	scroll, ok := jsonSource["from"]
	if !ok || scroll == nil {
		scrollFrom = 0
	} else {
		switch val := scroll.(type) {
		case json.Number:
			temp, _ := val.Int64()
			scrollFrom = int(temp)
		case float64:
			scrollFrom = int(val)
		case int64:
			scrollFrom = int(val)
		case uint64:
			scrollFrom = int(val)
		case int32:
			scrollFrom = int(val)
		case uint32:
			scrollFrom = int(val)
		case int16:
			scrollFrom = int(val)
		case uint16:
			scrollFrom = int(val)
		case int8:
			scrollFrom = int(val)
		case uint8:
			scrollFrom = int(val)
		case int:
			scrollFrom = val
		default:
			log.Infof("ParseSearchBody: unknown type %T for scroll", val)
			scrollFrom = 0
		}
	}
	finalSize = finalSize + uint64(scrollFrom)

	return searchText, startEpoch, endEpoch, finalSize, indexName, scrollFrom
}

// ProcessAlertsPipeSearchRequest processes the logs search request for alert queries.
func ProcessAlertsPipeSearchRequest(queryParams alertutils.QueryParams) (*PipeSearchResponseOuter, *dtypeutils.TimeRange, error) {
	orgid := uint64(0)
	dbPanelId := "-1"
	queryStart := time.Now()

	qid := rutils.GetNextQid()
	readJSON := make(map[string]interface{})
	var err error
	readJSON["from"] = "0"
	readJSON["indexName"] = "*"
	readJSON["queryLanguage"] = queryParams.QueryLanguage
	readJSON["searchText"] = queryParams.QueryText
	readJSON["startEpoch"] = queryParams.StartTime
	readJSON["endEpoch"] = queryParams.EndTime
	readJSON["state"] = "query"

	httpRespOuter, isScrollMax, timeRange, err := ParseAndExecutePipeRequest(readJSON, qid, orgid, queryStart, dbPanelId)
	if err != nil {
		return nil, nil, err
	}

	if isScrollMax {
		return nil, nil, fmt.Errorf("scrollFrom is greater than 10_000")
	}

	return httpRespOuter, timeRange, nil
}

func ParseAndExecutePipeRequest(readJSON map[string]interface{}, qid uint64, myid uint64, queryStart time.Time, dbPanelId string) (*PipeSearchResponseOuter, bool, *dtypeutils.TimeRange, error) {
	var err error

	nowTs := utils.GetCurrentTimeInMs()
	searchText, startEpoch, endEpoch, sizeLimit, indexNameIn, scrollFrom := ParseSearchBody(readJSON, nowTs)

	if scrollFrom > 10_000 {
		return nil, true, nil, nil
	}

	ti := structs.InitTableInfo(indexNameIn, myid, false)
	log.Infof("qid=%v, ParseAndExecutePipeRequest: index=[%s], searchString=[%v] ",
		qid, ti.String(), searchText)

	queryLanguageType := readJSON["queryLanguage"]
	var simpleNode *structs.ASTNode
	var aggs *structs.QueryAggregators
	var parsedIndexNames []string
	if queryLanguageType == "SQL" {
		simpleNode, aggs, parsedIndexNames, err = ParseRequest(searchText, startEpoch, endEpoch, qid, "SQL", indexNameIn)
	} else if queryLanguageType == "Pipe QL" {
		simpleNode, aggs, parsedIndexNames, err = ParseRequest(searchText, startEpoch, endEpoch, qid, "Pipe QL", indexNameIn)
	} else if queryLanguageType == "Log QL" {
		simpleNode, aggs, parsedIndexNames, err = ParseRequest(searchText, startEpoch, endEpoch, qid, "Log QL", indexNameIn)
	} else if queryLanguageType == "Splunk QL" {
		simpleNode, aggs, parsedIndexNames, err = ParseRequest(searchText, startEpoch, endEpoch, qid, "Splunk QL", indexNameIn)
		if err != nil {
			err = fmt.Errorf("qid=%v, ParseAndExecutePipeRequest: Error parsing query: %+v, err: %+v", qid, searchText, err)
			log.Error(err.Error())
			return nil, false, nil, err
		}
		err = structs.CheckUnsupportedFunctions(aggs)
	} else {
		log.Infof("ParseAndExecutePipeRequest: unknown queryLanguageType: %v; using Splunk QL instead", queryLanguageType)
		simpleNode, aggs, parsedIndexNames, err = ParseRequest(searchText, startEpoch, endEpoch, qid, "Splunk QL", indexNameIn)
	}

	if err != nil {
		err = fmt.Errorf("qid=%v, ParseAndExecutePipeRequest: Error parsing query:%+v, err: %+v", qid, searchText, err)
		log.Error(err.Error())
		return nil, false, nil, err
	}
	// This is for SPL queries where the index name is parsed from the query
	if len(parsedIndexNames) > 0 {
		ti = structs.InitTableInfo(strings.Join(parsedIndexNames, ","), myid, false)
	}

	sizeLimit = GetFinalSizelimit(aggs, sizeLimit)

	// If MaxRows is used to limit the number of returned results, set `sizeLimit`
	// to it. Currently MaxRows is only valid as the root QueryAggregators.
	if aggs != nil && aggs.Limit != 0 {
		sizeLimit = uint64(aggs.Limit)
	}
	if queryLanguageType == "SQL" && aggs != nil && aggs.TableName != "*" {
		indexNameIn = aggs.TableName
		ti = structs.InitTableInfo(indexNameIn, myid, false) // Re-initialize ti with the updated indexNameIn
	}

	qc := structs.InitQueryContextWithTableInfo(ti, sizeLimit, scrollFrom, myid, false)
	qc.RawQuery = searchText
	result := segment.ExecuteQuery(simpleNode, aggs, qid, qc)
	httpRespOuter := getQueryResponseJson(result, indexNameIn, queryStart, sizeLimit, qid, aggs, result.TotalRRCCount, dbPanelId, result.AllColumnsInAggs)

	return &httpRespOuter, false, simpleNode.TimeRange, nil
}

func ProcessPipeSearchRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	qid := rutils.GetNextQid()
	defer fileutils.DeferableAddAccessLogEntry(
		time.Now(),
		func() time.Time { return time.Now() },
		"No-user", // TODO : Add logged in user when user auth is implemented
		qid,
		ctx.Request.URI().String(),
		string(ctx.PostBody()),
		func() int { return ctx.Response.StatusCode() },
		false,
		fileutils.AccessLogFile,
	)

	fileutils.AddLogEntry(dtypeutils.LogFileData{
		TimeStamp:   time.Now().Format("2006-01-02 15:04:05"),
		UserName:    "No-user", // TODO : Add logged in user when user auth is implemented
		QueryID:     qid,
		URI:         ctx.Request.URI().String(),
		RequestBody: string(ctx.PostBody()),
	}, false, fileutils.QueryLogFile)

	dbPanelId := utils.ExtractParamAsString(ctx.UserValue("dbPanel-id"))
	queryStart := time.Now()
	rawJSON := ctx.PostBody()
	if rawJSON == nil {
		log.Errorf("ProcessPipeSearchRequest: received empty search request body")
		utils.SetBadMsg(ctx, "")
		return
	}

	readJSON := make(map[string]interface{})
	var jsonc = jsoniter.ConfigCompatibleWithStandardLibrary
	decoder := jsonc.NewDecoder(bytes.NewReader(rawJSON))
	decoder.UseNumber()
	err := decoder.Decode(&readJSON)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		_, err = ctx.WriteString(err.Error())
		if err != nil {
			log.Errorf("qid=%v, ProcessPipeSearchRequest: could not write error message, err: %v", qid, err)
		}
		log.Errorf("qid=%v, ProcessPipeSearchRequest: failed to decode search request body! err: %+v", qid, err)
	}

	httpRespOuter, isScrollMax, _, err := ParseAndExecutePipeRequest(readJSON, qid, myid, queryStart, dbPanelId)
	if err != nil {
		utils.SendError(ctx, fmt.Sprintf("Error processing search request: %v", err), "", err)
		return
	}

	if isScrollMax {
		processMaxScrollCount(ctx, qid)
		return
	}

	utils.WriteJsonResponse(ctx, httpRespOuter)

	ctx.SetStatusCode(fasthttp.StatusOK)
}

func getQueryResponseJson(nodeResult *structs.NodeResult, indexName string, queryStart time.Time, sizeLimit uint64, qid uint64, aggs *structs.QueryAggregators, numRRCs uint64, dbPanelId string, allColsInAggs map[string]struct{}) PipeSearchResponseOuter {
	var httpRespOuter PipeSearchResponseOuter
	var httpResp PipeSearchResponse

	// aggs exist, so just return aggregations instead of all results
	httpRespOuter.Aggs = convertBucketToAggregationResponse(nodeResult.Histogram)
	if len(nodeResult.ErrList) > 0 {
		for _, err := range nodeResult.ErrList {
			httpRespOuter.Errors = append(httpRespOuter.Errors, err.Error())
		}
	}

	allMeasRes, measFuncs, added := segresults.CreateMeasResultsFromAggResults(aggs.BucketLimit, nodeResult.Histogram)

	if added == 0 {
		allMeasRes = nodeResult.MeasureResults
		measFuncs = nodeResult.MeasureFunctions
	}

	json, allCols, err := convertRRCsToJSONResponse(nodeResult.AllRecords, sizeLimit, qid, nodeResult.SegEncToKey, aggs, allColsInAggs)
	if err != nil {
		httpRespOuter.Errors = append(httpRespOuter.Errors, err.Error())
		return httpRespOuter
	}
	if nodeResult.RemoteLogs != nil {
		json = append(json, nodeResult.RemoteLogs...)
	}

	var canScrollMore bool
	if numRRCs == sizeLimit {
		// if the number of RRCs is exactly equal to the requested size, there may be more than size matches. Hence, we can scroll more
		canScrollMore = true
	}
	httpResp.Hits = json
	httpResp.TotalMatched = convertQueryCountToTotalResponse(nodeResult.TotalResults)
	httpRespOuter.Hits = httpResp
	httpRespOuter.AllPossibleColumns = allCols
	httpRespOuter.ElapedTimeMS = time.Since(queryStart).Milliseconds()
	httpRespOuter.Qtype = nodeResult.Qtype
	httpRespOuter.CanScrollMore = canScrollMore
	httpRespOuter.TotalRRCCount = numRRCs
	httpRespOuter.MeasureFunctions = measFuncs
	httpRespOuter.MeasureResults = allMeasRes
	httpRespOuter.GroupByCols = nodeResult.GroupByCols
	httpRespOuter.BucketCount = nodeResult.BucketCount
	httpRespOuter.DashboardPanelId = dbPanelId

	httpRespOuter.ColumnsOrder = allCols
	// The length of AllCols is 0, which means it is not a async query
	if len(allCols) == 0 {
		httpRespOuter.ColumnsOrder = query.GetFinalColsOrder(nodeResult.ColumnsOrder)
	}

	if nodeResult.RecsAggsType == structs.GroupByType && nodeResult.GroupByRequest != nil {
		httpRespOuter.MeasureAggregationCols = structs.GetMeasureAggregatorStrEncColumns(nodeResult.GroupByRequest.MeasureOperations)
	} else if nodeResult.RecsAggsType == structs.MeasureAggsType && nodeResult.MeasureOperations != nil {
		httpRespOuter.MeasureAggregationCols = structs.GetMeasureAggregatorStrEncColumns(nodeResult.MeasureOperations)
	}
	httpRespOuter.RenameColumns = nodeResult.RenameColumns

	log.Infof("qid=%d, Query Took %+v ms", qid, httpRespOuter.ElapedTimeMS)

	return httpRespOuter
}

// returns converted json, all columns, or any errors
func convertRRCsToJSONResponse(rrcs []*sutils.RecordResultContainer, sizeLimit uint64,
	qid uint64, segencmap map[uint16]string, aggs *structs.QueryAggregators, allColsInAggs map[string]struct{}) ([]map[string]interface{}, []string, error) {

	hits := make([]map[string]interface{}, 0)
	// if sizeLimit is 0, return empty hits
	// And Even if len(rrcs) is 0, we will still proceed to the json records
	// So that any Aggregations that require all the segments to be processed can be done.
	if sizeLimit == 0 {
		return hits, []string{}, nil
	}

	allJsons, allCols, err := record.GetJsonFromAllRrc(rrcs, false, qid, segencmap, aggs, allColsInAggs)
	if err != nil {
		log.Errorf("qid=%d, convertRRCsToJSONResponse: failed to get allrecords from rrc, err: %v", qid, err)
		return allJsons, allCols, err
	}

	if sizeLimit < uint64(len(allJsons)) {
		allJsons = allJsons[:sizeLimit]
	}
	return allJsons, allCols, nil
}

func convertBucketToAggregationResponse(buckets map[string]*structs.AggregationResult) map[string]AggregationResults {
	resp := make(map[string]AggregationResults)
	for aggName, aggRes := range buckets {
		allBuckets := make([]map[string]interface{}, len(aggRes.Results))

		for idx, hist := range aggRes.Results {
			res := make(map[string]interface{})
			var bucketKey interface{}
			bucketKeyList, ok := hist.BucketKey.([]string)
			if ok && len(bucketKeyList) == 1 {
				bucketKey = bucketKeyList[0]
			} else {
				bucketKey = hist.BucketKey
			}
			res["key"] = bucketKey
			res["doc_count"] = hist.ElemCount
			if aggRes.IsDateHistogram {
				res["key_as_string"] = fmt.Sprintf("%v", hist.BucketKey)
			}
			for name, value := range hist.StatRes {
				res[name] = utils.StatResponse{
					Value: value.CVal,
				}
			}

			allBuckets[idx] = res
		}
		resp[aggName] = AggregationResults{Buckets: allBuckets}
	}
	return resp
}

func convertQueryCountToTotalResponse(qc *structs.QueryCount) interface{} {
	if qc == nil {
		return 0
	}

	if !qc.EarlyExit {
		return qc.TotalCount
	}

	return utils.HitsCount{Value: qc.TotalCount, Relation: qc.Op.ToString()}
}

func GetAutoCompleteData(ctx *fasthttp.RequestCtx, myid uint64) {

	var resp utils.AutoCompleteDataInfo
	allVirtualTableNames, err := vtable.GetVirtualTableNames(myid)
	if err != nil {
		log.Errorf("GetAutoCompleteData: failed to get all virtual table names, err: %v", err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
	}

	sortedIndices := make([]string, 0, len(allVirtualTableNames))

	for k := range allVirtualTableNames {
		sortedIndices = append(sortedIndices, k)
	}

	sort.Strings(sortedIndices)

	for _, indexName := range sortedIndices {
		if indexName == "" {
			log.Errorf("GetAutoCompleteData: skipping an empty index name indexName: %v", indexName)
			continue
		}

	}

	resp.ColumnNames = metadata.GetAllColNames(sortedIndices)
	resp.MeasureFunctions = []string{"min", "max", "avg", "count", "sum", "cardinality"}
	utils.WriteJsonResponse(ctx, resp)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func processMaxScrollCount(ctx *fasthttp.RequestCtx, qid uint64) {
	resp := &PipeSearchResponseOuter{
		CanScrollMore: false,
	}
	qType := query.GetQueryType(qid)
	resp.Qtype = qType.String()
	utils.WriteJsonResponse(ctx, resp)
	ctx.SetStatusCode(fasthttp.StatusOK)

}

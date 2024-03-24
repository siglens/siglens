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

package pipesearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/siglens/siglens/pkg/alerts/alertutils"
	rutils "github.com/siglens/siglens/pkg/readerUtils"
	"github.com/siglens/siglens/pkg/segment"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/query/metadata"
	"github.com/siglens/siglens/pkg/segment/reader/record"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

const MIN_IN_MS = 60_000
const HOUR_IN_MS = 3600_000
const DAY_IN_MS = 86400_000

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
			log.Errorf("parseSearchBody searchText is not a string! Val %+v", val)
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
			log.Errorf("parseSearchBody indexName is not a string! Val %+v, type: %T", val, iText)
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
			startEpoch = parseAlphaNumTime(nowTs, string(val), defValue)
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
			endEpoch = parseAlphaNumTime(nowTs, string(val), nowTs)
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
			log.Infof("parseSearchBody: unknown type for scroll=%T", val)
			scrollFrom = 0
		}
	}
	finalSize = finalSize + uint64(scrollFrom)

	return searchText, startEpoch, endEpoch, finalSize, indexName, scrollFrom
}

func ProcessAlertsPipeSearchRequest(queryParams alertutils.QueryParams) int {

	queryData := fmt.Sprintf(`{
		"from": "0",
		"indexName": "*",
		"queryLanguage": "%s",
		"searchText": "%s",
		"startEpoch": "%s",
		"endEpoch" : "%s",
		"state": "query"
	}`, queryParams.QueryLanguage, utils.EscapeQuotes(queryParams.QueryText), queryParams.StartTime, queryParams.EndTime)
	orgid := uint64(0)
	dbPanelId := "-1"
	queryStart := time.Now()

	rawJSON := []byte(queryData)
	if rawJSON == nil {
		log.Errorf("ALERTSERVICE: ProcessAlertsPipeSearchRequest: received empty search request body ")
		return -1
	}

	qid := rutils.GetNextQid()
	readJSON := make(map[string]interface{})
	var jsonc = jsoniter.ConfigCompatibleWithStandardLibrary
	decoder := jsonc.NewDecoder(bytes.NewReader(rawJSON))
	decoder.UseNumber()
	err := decoder.Decode(&readJSON)
	if err != nil {
		log.Errorf("qid=%v, ALERTSERVICE: ProcessAlertsPipeSearchRequest: failed to decode search request body! Err=%+v", qid, err)
	}

	nowTs := utils.GetCurrentTimeInMs()
	searchText, startEpoch, endEpoch, sizeLimit, indexNameIn, scrollFrom := ParseSearchBody(readJSON, nowTs)

	if scrollFrom > 10_000 {
		return -1
	}

	ti := structs.InitTableInfo(indexNameIn, orgid, false)
	log.Infof("qid=%v, ALERTSERVICE: ProcessAlertsPipeSearchRequest: index=[%s], searchString=[%v] ",
		qid, ti.String(), searchText)

	queryLanguageType := readJSON["queryLanguage"]
	var simpleNode *structs.ASTNode
	var aggs *structs.QueryAggregators
	if queryLanguageType == "Pipe QL" {
		simpleNode, aggs, err = ParseRequest(searchText, startEpoch, endEpoch, qid, "Pipe QL", indexNameIn)
		if err != nil {
			log.Errorf("qid=%v, ALERTSERVICE: ProcessAlertsPipeSearchRequest: Error parsing query err=%+v", qid, err)
			return -1
		}

		if aggs != nil && (aggs.GroupByRequest != nil || aggs.MeasureOperations != nil) {
			sizeLimit = 0
		} else if aggs.HasDedupBlockInChain() || aggs.HasSortBlockInChain() || aggs.HasRexBlockInChainWithStats() {
			// 1. Dedup needs to see all the matched records before it can return any
			// of them when there's a sortby option.
			// 2. If there's a Rex block in the chain followed by a Stats block, we need to
			// see all the matched records before we apply or calculate the stats.
			sizeLimit = math.MaxUint64
		}
		qc := structs.InitQueryContextWithTableInfo(ti, sizeLimit, scrollFrom, orgid, false)
		result := segment.ExecuteQuery(simpleNode, aggs, qid, qc)
		httpRespOuter := getQueryResponseJson(result, indexNameIn, queryStart, sizeLimit, qid, aggs, result.TotalRRCCount, dbPanelId)

		if httpRespOuter.MeasureResults != nil && len(httpRespOuter.MeasureResults) > 0 && httpRespOuter.MeasureResults[0].MeasureVal != nil {
			measureVal, ok := httpRespOuter.MeasureResults[0].MeasureVal[queryParams.QueryText].(string)
			if ok {
				measureVal = strings.ReplaceAll(measureVal, ",", "")

				measureNum, err := strconv.Atoi(measureVal)
				if err != nil {
					log.Errorf("ALERTSERVICE: ProcessAlertsPipeSearchRequest Error parsing int from a string: %s", err)
					return -1
				}
				return measureNum
			}
		}
	} else if queryLanguageType == "Splunk QL" {
		simpleNode, aggs, err = ParseRequest(searchText, startEpoch, endEpoch, qid, "Splunk QL", indexNameIn)
		if err != nil {
			log.Errorf("qid=%v, ALERTSERVICE: ProcessAlertsPipeSearchRequest: Error parsing query err=%+v", qid, err)
			return -1
		}

		if aggs != nil && (aggs.GroupByRequest != nil || aggs.MeasureOperations != nil) {
			sizeLimit = 0
		}
		qc := structs.InitQueryContextWithTableInfo(ti, sizeLimit, scrollFrom, orgid, false)
		result := segment.ExecuteQuery(simpleNode, aggs, qid, qc)
		httpRespOuter := getQueryResponseJson(result, indexNameIn, queryStart, sizeLimit, qid, aggs, result.TotalRRCCount, dbPanelId)
		if httpRespOuter.MeasureResults != nil && len(httpRespOuter.MeasureResults) > 0 && httpRespOuter.MeasureResults[0].MeasureVal != nil {
			measureVal, ok := httpRespOuter.MeasureResults[0].MeasureVal[httpRespOuter.MeasureFunctions[0]].(string)
			if ok {
				measureVal = strings.ReplaceAll(measureVal, ",", "")
				measureNum, err := strconv.ParseFloat(measureVal, 64)
				if err != nil {
					log.Errorf("ALERTSERVICE: ProcessAlertsPipeSearchRequest Error parsing int from a string: %s", err)
					return -1
				}
				return int(measureNum)
			}
		}
	} else {
		log.Infof("ProcessAlertsPipeSearchRequest: unknown queryLanguageType: %v;", queryLanguageType)
	}

	return -1
}

func ProcessPipeSearchRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	defer utils.DeferableAddAccessLogEntry(
		time.Now(),
		func() time.Time { return time.Now() },
		"No-user", // TODO : Add logged in user when user auth is implemented
		ctx.Request.URI().String(),
		string(ctx.PostBody()),
		func() int { return ctx.Response.StatusCode() },
		false,
		"access.log",
	)

	dbPanelId := utils.ExtractParamAsString(ctx.UserValue("dbPanel-id"))
	queryStart := time.Now()
	rawJSON := ctx.PostBody()
	if rawJSON == nil {
		log.Errorf(" ProcessPipeSearchRequest: received empty search request body ")
		utils.SetBadMsg(ctx, "")
		return
	}
	qid := rutils.GetNextQid()

	readJSON := make(map[string]interface{})
	var jsonc = jsoniter.ConfigCompatibleWithStandardLibrary
	decoder := jsonc.NewDecoder(bytes.NewReader(rawJSON))
	decoder.UseNumber()
	err := decoder.Decode(&readJSON)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		_, err = ctx.WriteString(err.Error())
		if err != nil {
			log.Errorf("qid=%v, ProcessPipeSearchRequest: could not write error message err=%v", qid, err)
		}
		log.Errorf("qid=%v, ProcessPipeSearchRequest: failed to decode search request body! Err=%+v", qid, err)
	}

	nowTs := utils.GetCurrentTimeInMs()
	searchText, startEpoch, endEpoch, sizeLimit, indexNameIn, scrollFrom := ParseSearchBody(readJSON, nowTs)

	if scrollFrom > 10_000 {
		processMaxScrollCount(ctx, qid)
		return
	}

	ti := structs.InitTableInfo(indexNameIn, myid, false)
	log.Infof("qid=%v, ProcessPipeSearchRequest: index=[%s], searchString=[%v] ",
		qid, ti.String(), searchText)

	queryLanguageType := readJSON["queryLanguage"]
	var simpleNode *structs.ASTNode
	var aggs *structs.QueryAggregators
	if queryLanguageType == "SQL" {
		simpleNode, aggs, err = ParseRequest(searchText, startEpoch, endEpoch, qid, "SQL", indexNameIn)
	} else if queryLanguageType == "Pipe QL" {
		simpleNode, aggs, err = ParseRequest(searchText, startEpoch, endEpoch, qid, "Pipe QL", indexNameIn)
	} else if queryLanguageType == "Log QL" {
		simpleNode, aggs, err = ParseRequest(searchText, startEpoch, endEpoch, qid, "Log QL", indexNameIn)
	} else if queryLanguageType == "Splunk QL" {
		simpleNode, aggs, err = ParseRequest(searchText, startEpoch, endEpoch, qid, "Splunk QL", indexNameIn)
	} else {
		log.Infof("ProcessPipeSearchRequest: unknown queryLanguageType: %v; using Pipe QL instead", queryLanguageType)
		simpleNode, aggs, err = ParseRequest(searchText, startEpoch, endEpoch, qid, "Pipe QL", indexNameIn)
	}

	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		_, err = ctx.WriteString(err.Error())
		if err != nil {
			log.Errorf("qid=%v, ProcessPipeSearchRequest: could not write error message err=%v", qid, err)
		}
		log.Errorf("qid=%v, ProcessPipeSearchRequest: Error parsing query err=%+v", qid, err)
		return
	}

	if aggs != nil && (aggs.GroupByRequest != nil || aggs.MeasureOperations != nil) {
		sizeLimit = 0
	} else if aggs.HasDedupBlockInChain() {
		// Dedup needs to see all the matched records before it can return any
		// of them when there's a sortby option.
		sizeLimit = math.MaxUint64
	} else if aggs.HasRexBlockInChainWithStats() {
		// If there's a Stats block in the chain followed by a Rex block, we need to
		// see all the matched records before we apply or calculate the stats.
		sizeLimit = math.MaxUint64
	}

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
	result := segment.ExecuteQuery(simpleNode, aggs, qid, qc)
	httpRespOuter := getQueryResponseJson(result, indexNameIn, queryStart, sizeLimit, qid, aggs, result.TotalRRCCount, dbPanelId)
	utils.WriteJsonResponse(ctx, httpRespOuter)

	ctx.SetStatusCode(fasthttp.StatusOK)
}

func getQueryResponseJson(nodeResult *structs.NodeResult, indexName string, queryStart time.Time, sizeLimit uint64, qid uint64, aggs *structs.QueryAggregators, numRRCs uint64, dbPanelId string) PipeSearchResponseOuter {
	var httpRespOuter PipeSearchResponseOuter
	var httpResp PipeSearchResponse

	// aggs exist, so just return aggregations instead of all results
	httpRespOuter.Aggs = convertBucketToAggregationResponse(nodeResult.Histogram)
	if len(nodeResult.ErrList) > 0 {
		for _, err := range nodeResult.ErrList {
			httpRespOuter.Errors = append(httpRespOuter.Errors, err.Error())
		}
	}
	json, allCols, err := convertRRCsToJSONResponse(nodeResult.AllRecords, sizeLimit, qid, nodeResult.SegEncToKey, aggs)
	if err != nil {
		httpRespOuter.Errors = append(httpRespOuter.Errors, err.Error())
		return httpRespOuter
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
	httpRespOuter.MeasureFunctions = nodeResult.MeasureFunctions
	httpRespOuter.MeasureResults = nodeResult.MeasureResults
	httpRespOuter.GroupByCols = nodeResult.GroupByCols
	httpRespOuter.BucketCount = nodeResult.BucketCount
	httpRespOuter.DashboardPanelId = dbPanelId

	log.Infof("qid=%d, Query Took %+v ms", qid, httpRespOuter.ElapedTimeMS)

	return httpRespOuter
}

// returns converted json, all columns, or any errors
func convertRRCsToJSONResponse(rrcs []*sutils.RecordResultContainer, sizeLimit uint64,
	qid uint64, segencmap map[uint16]string, aggs *structs.QueryAggregators) ([]map[string]interface{}, []string, error) {

	hits := make([]map[string]interface{}, 0)
	if sizeLimit == 0 || len(rrcs) == 0 {
		return hits, []string{}, nil
	}

	allJsons, allCols, err := record.GetJsonFromAllRrc(rrcs, false, qid, segencmap, aggs)
	if err != nil {
		log.Errorf("qid=%d, convertRRCsToJSONResponse: failed to get allrecords from rrc, err=%v", qid, err)
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
	if !qc.EarlyExit {
		return qc.TotalCount
	}

	return utils.HitsCount{Value: qc.TotalCount, Relation: qc.Op.ToString()}
}

/*
   Supports "now-[Num][Unit]"
   Num ==> any positive integer
   Unit ==> m(minutes), h(hours), d(days)
*/

func parseAlphaNumTime(nowTs uint64, inp string, defValue uint64) uint64 {

	sanTime := strings.ReplaceAll(inp, " ", "")

	if sanTime == "now" {
		return nowTs
	}

	retVal := defValue

	strln := len(sanTime)
	if strln < 6 {
		return retVal
	}

	unit := sanTime[strln-1]
	num, err := strconv.ParseInt(sanTime[4:strln-1], 0, 64)
	if err != nil {
		return defValue
	}

	switch unit {
	case 'm':
		retVal = nowTs - MIN_IN_MS*uint64(num)
	case 'h':
		retVal = nowTs - HOUR_IN_MS*uint64(num)
	case 'd':
		retVal = nowTs - DAY_IN_MS*uint64(num)
	}
	return retVal
}

func GetAutoCompleteData(ctx *fasthttp.RequestCtx, myid uint64) {

	var resp utils.AutoCompleteDataInfo
	allVirtualTableNames, err := vtable.GetVirtualTableNames(myid)
	if err != nil {
		log.Errorf("GetAutoCompleteData: failed to get all virtual table names, err=%v", err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
	}

	sortedIndices := make([]string, 0, len(allVirtualTableNames))

	for k := range allVirtualTableNames {
		sortedIndices = append(sortedIndices, k)
	}

	sort.Strings(sortedIndices)

	for _, indexName := range sortedIndices {
		if indexName == "" {
			log.Errorf("GetAutoCompleteData: skipping an empty index name indexName=%v", indexName)
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

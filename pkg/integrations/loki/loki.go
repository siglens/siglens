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

package loki

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/golang/snappy"
	"github.com/siglens/siglens/pkg/ast"
	"github.com/siglens/siglens/pkg/ast/pipesearch"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/es/writer"
	"github.com/siglens/siglens/pkg/grpc"
	"github.com/siglens/siglens/pkg/hooks"
	lokilog "github.com/siglens/siglens/pkg/integrations/loki/log"
	rutils "github.com/siglens/siglens/pkg/readerUtils"
	"github.com/siglens/siglens/pkg/segment"
	"github.com/siglens/siglens/pkg/segment/metadata"
	"github.com/siglens/siglens/pkg/segment/reader/record"
	"github.com/siglens/siglens/pkg/segment/search"
	"github.com/siglens/siglens/pkg/segment/structs"
	segwriter "github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/utils"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	ContentJson        = "application/json; charset=utf-8"
	LOKIINDEX_STAR     = "*"
	LOKIINDEX          = "loki-index"
	TimeStamp          = "timestamp"
	Index              = "_index"
	DefaultLimit       = 100
	MsToNanoConversion = 1_000_000
	DAY_IN_MS          = 86_400_000
	HOUR_IN_MS         = 3_600_000
)

func parseLabels(labelsString string) map[string]string {
	labelsString = strings.Trim(labelsString, "{}")
	labelPairs := strings.Split(labelsString, ", ")

	labels := make(map[string]string)

	for _, pair := range labelPairs {
		parts := strings.Split(pair, "=")
		if len(parts) == 2 {
			key := strings.Trim(parts[0], "\"")
			value := strings.Trim(parts[1], "\"")
			labels[key] = value
		}
	}

	return labels
}

// If startTime and endTime are not given:
// But time is given, then that is used.
// If time is not given, then the last 24 hours is used.
// If endTime is not given, then the current time is used.
// If startTime is not given, then the last 24 hours is used.
// If since is given, then that is used as the delta between startTime and endTime.
// But if startTime is given, then since is ignored.
func parseTimeRangeInMS(ctx *fasthttp.RequestCtx, defaultDelta uint64) (uint64, uint64, error) {
	startTime := string(ctx.QueryArgs().Peek("start"))
	endTime := string(ctx.QueryArgs().Peek("end"))
	time := string(ctx.QueryArgs().Peek("time"))
	since := string(ctx.QueryArgs().Peek("since"))

	var startTimeMs, endTimeMs uint64
	var err error

	if since != "" && startTime == "" {
		defaultDeltaNano, err := strconv.ParseUint(since, 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("error parsing since value: %v. Error: %v", since, err)
		}
		defaultDelta = defaultDeltaNano / MsToNanoConversion
	}

	if startTime == "" && endTime == "" {
		if time != "" {
			timeInMS, err := utils.ConvertTimestampToMillis(time)
			if err != nil {
				return 0, 0, err
			}
			startTimeMs = timeInMS - 1*1000 // subtract 1 second
			endTimeMs = timeInMS + 1*1000   // add 1  second

			return startTimeMs, endTimeMs, nil
		}

		endTimeMs = utils.GetCurrentTimeInMs()
		startTimeMs = endTimeMs - defaultDelta
		return startTimeMs, endTimeMs, nil
	} else if startTime == "" {
		endTimeMs, err = utils.ConvertTimestampToMillis(endTime)
		if err != nil {
			return 0, 0, err
		}

		startTimeMs = endTimeMs - defaultDelta
		return startTimeMs, endTimeMs, nil
	} else if endTime == "" {
		startTimeMs, err = utils.ConvertTimestampToMillis(startTime)
		if err != nil {
			return 0, 0, err
		}
		endTimeMs = utils.GetCurrentTimeInMs()
		return startTimeMs, endTimeMs, nil
	}

	startTimeMs, err = utils.ConvertTimestampToMillis(startTime)
	if err != nil {
		return 0, 0, err
	}

	endTimeMs, err = utils.ConvertTimestampToMillis(endTime)
	if err != nil {
		return 0, 0, err
	}

	return startTimeMs, endTimeMs, nil
}

func getVectorArithmeticResponse(queryResult *structs.NodeResult) map[string]interface{} {
	responsebody := make(map[string]interface{})
	responsebody["status"] = "success"
	responsebody["data"] = make(map[string]interface{})
	responsebody["data"].(map[string]interface{})["resultType"] = "vector"
	vectorRes := make([]VectorValue, 0)
	vectorVal := VectorValue{
		Metric: make(map[string]interface{}),
		Values: [][]interface{}{{uint32(utils.GetCurrentTimeInMs() / 1000), fmt.Sprintf("%v", queryResult.VectorResultValue)}},
	}
	vectorRes = append(vectorRes, vectorVal)
	responsebody["data"].(map[string]interface{})["result"] = vectorRes
	return responsebody
}

// See https://grafana.com/docs/loki/latest/reference/loki-http-api/#ingest-logs
func ProcessLokiLogsIngestRequest(ctx *fasthttp.RequestCtx, myid int64) {
	contentType := string(ctx.Request.Header.ContentType())
	switch contentType {
	case "application/json":
		processJsonLogs(ctx, myid)
	case "application/x-protobuf":
		processPromtailLogs(ctx, myid)
	default:
		utils.SendError(ctx, fmt.Sprintf("Unknown content type: %v", contentType), "", nil)
	}
}

// Format of loki logs generated by promtail:
//
//	{
//		"streams": [
//		  {
//			"labels": "{filename=\"test.log\",job=\"test\"}",
//			"entries": [
//			  {
//				"timestamp": "2021-03-31T18:00:00.000Z",
//				"line": "test log line"
//			  }
//			]
//		  }
//		]
//	}
func processPromtailLogs(ctx *fasthttp.RequestCtx, myid int64) {
	if hook := hooks.GlobalHooks.OverrideIngestRequestHook; hook != nil {
		alreadyHandled := hook(ctx, myid, grpc.INGEST_FUNC_LOKI, false)
		if alreadyHandled {
			return
		}
	}
	responsebody := make(map[string]interface{})
	buf, err := snappy.Decode(nil, ctx.PostBody())
	if err != nil {
		utils.SendError(ctx, "Failed to decode request", fmt.Sprintf("body: %s", ctx.PostBody()), err)
		return
	}

	logLine := lokilog.PushRequest{}

	err = proto.Unmarshal(buf, &logLine)
	if err != nil {
		utils.SendError(ctx, "Unable to unmarshal request", "", err)
		return
	}

	logJson := protojson.Format(&logLine)

	tsNow := utils.GetCurrentTimeInMs()
	indexNameIn := LOKIINDEX
	if !vtable.IsVirtualTablePresent(&indexNameIn, myid) {
		log.Errorf("processPromtailLogs: Index name %v does not exist. Adding virtual table name and mapping.", indexNameIn)
		body := logJson
		err = vtable.AddVirtualTable(&indexNameIn, myid)
		if err != nil {
			utils.SendInternalError(ctx, "Failed to add virtual table for index", "", err)
			return
		}
		err = vtable.AddMappingFromADoc(&indexNameIn, &body, myid)
		if err != nil {
			utils.SendInternalError(ctx, "Failed to add mapping from a doc for index", "", err)
			return
		}
	}

	localIndexMap := make(map[string]string)

	var jsonData map[string][]map[string]interface{}
	err = json.Unmarshal([]byte(logJson), &jsonData)
	if err != nil {
		utils.SendError(ctx, "Unable to unmarshal request", "", err)
		return
	}

	streams := jsonData["streams"]
	allIngestData := make(map[string]interface{})

	idxToStreamIdCache := make(map[string]string)
	cnameCacheByteHashToStr := make(map[uint64]string)
	var jsParsingStackbuf [utils.UnescapeStackBufSize]byte

	tsKey := config.GetTimeStampKey()

	pleArray := make([]*segwriter.ParsedLogEvent, 0)
	defer segwriter.ReleasePLEs(pleArray)

	for _, stream := range streams {
		labels := stream["labels"].(string)
		ingestCommonFields := parseLabels(labels)

		// Note: We might not need separate filename and job fields in the future
		allIngestData["filename"] = ingestCommonFields["filename"]
		allIngestData["job"] = ingestCommonFields["job"]
		allIngestData["labels"] = labels

		entries, ok := stream["entries"].([]interface{})
		if !ok {
			utils.SendError(ctx, "Unable to convert entries", "failed to convert to []interface{}", nil)
			return
		}

		if len(entries) > 0 {
			for _, entry := range entries {
				entryMap, ok := entry.(map[string]interface{})
				if !ok {
					utils.SendError(ctx, "Unable to convert entries", "failed to convert to map[string]interface{}", err)
					return
				}
				timestamp, ok := entryMap["timestamp"].(string)
				if !ok {
					utils.SendError(ctx, "Unable to convert timestamp", "", err)
					return
				}
				line, ok := entryMap["line"].(string)
				if !ok {
					utils.SendError(ctx, "Unable to convert line to string", "", err)
					return
				}

				allIngestData["timestamp"] = timestamp
				allIngestData["line"] = line

				test, err := json.Marshal(allIngestData)
				if err != nil {
					utils.SendError(ctx, "Unable to marshal json", fmt.Sprintf("allIngestData: %v", allIngestData), err)
					return
				}

				ple, err := segwriter.GetNewPLE(test, tsNow, indexNameIn, &tsKey, jsParsingStackbuf[:])
				if err != nil {
					log.Errorf("processPromtailLogs: failed to get new PLE, test: %v, err: %v", test, err)
					return
				}
				pleArray = append(pleArray, ple)
			}
		}
	}

	err = writer.ProcessIndexRequestPle(tsNow, indexNameIn, false, localIndexMap, myid, 0, idxToStreamIdCache, cnameCacheByteHashToStr, jsParsingStackbuf[:], pleArray)
	if err != nil {
		utils.SendError(ctx, "Failed to ingest record", "", err)
		return
	}

	responsebody["status"] = "Success"
	utils.WriteJsonResponse(ctx, responsebody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

// LokiLogStream represents log entry in format acceptable for Loki HTTP API: https://grafana.com/docs/loki/latest/reference/loki-http-api/#ingest-logs
// Acceptable json example:
//
//	{
//	 "streams": [
//	   {
//		 "stream": {
//		   "label": "value"
//		 },
//		 "values": [
//			 [ "<unix epoch in nanoseconds>", "<log line>"],
//			 [ "<unix epoch in nanoseconds>", "<log line>", {"trace_id": "0242ac120002", "user_id": "superUser123"}]
//		 ]
//	   }
//	 ]
//	}
func processJsonLogs(ctx *fasthttp.RequestCtx, myid int64) {
	if hook := hooks.GlobalHooks.OverrideIngestRequestHook; hook != nil {
		alreadyHandled := hook(ctx, myid, grpc.INGEST_FUNC_LOKI, false)
		if alreadyHandled {
			return
		}
	}
	responsebody := make(map[string]interface{})
	buf := ctx.PostBody()

	var logData LokiLogData
	err := json.Unmarshal(buf, &logData)
	if err != nil {
		utils.SendError(ctx, "Unable to unmarshal request", "", err)
		return
	}
	tsKey := config.GetTimeStampKey()
	tsNow := utils.GetCurrentTimeInMs()
	indexNameIn := LOKIINDEX
	if !vtable.IsVirtualTablePresent(&indexNameIn, myid) {
		log.Errorf("processJsonLogs: Index name %v does not exist. Adding virtual table name and mapping.", indexNameIn)
		err = vtable.AddVirtualTable(&indexNameIn, myid)
		if err != nil {
			utils.SendInternalError(ctx, "Failed to add virtual table for index", "", err)
			return
		}
	}

	localIndexMap := make(map[string]string)

	idxToStreamIdCache := make(map[string]string)
	cnameCacheByteHashToStr := make(map[uint64]string)
	var jsParsingStackbuf [utils.UnescapeStackBufSize]byte

	pleArray := make([]*segwriter.ParsedLogEvent, 0)
	defer segwriter.ReleasePLEs(pleArray)

	for _, stream := range logData.Streams {
		allIngestData := make(map[string]interface{})

		// Extracting the labels and adding to the ingest data
		for label, value := range stream.Stream {
			allIngestData[label] = value
		}

		for _, value := range stream.Values {
			if len(value) < 2 {
				utils.SendError(ctx, "Invalid log entry format", "", nil)
				return
			}

			timestamp, ok := value[0].(string)
			if !ok {
				utils.SendError(ctx, "Invalid timestamp format", "", nil)
				return
			}

			line, ok := value[1].(string)
			if !ok {
				utils.SendError(ctx, "Invalid line format", "", nil)
				return
			}

			allIngestData["timestamp"] = timestamp
			allIngestData["line"] = line

			// Handle optional fields
			if len(value) > 2 {
				if additionalFields, ok := value[2].(map[string]interface{}); ok {
					for k, v := range additionalFields {
						allIngestData[k] = v
					}
				}
			}

			// Marshal the map to JSON
			allIngestDataBytes, err := json.Marshal(allIngestData)
			if err != nil {
				utils.SendError(ctx, "Unable to marshal json", fmt.Sprintf("allIngestData: %v", allIngestData), err)
				return
			}

			ple, err := segwriter.GetNewPLE(allIngestDataBytes, tsNow, indexNameIn, &tsKey, jsParsingStackbuf[:])
			if err != nil {
				utils.SendError(ctx, "failed to get new PLE", fmt.Sprintf("allIngestData: %v", allIngestData), err)
				return
			}
			pleArray = append(pleArray, ple)
		}
	}

	err = writer.ProcessIndexRequestPle(tsNow, indexNameIn, false, localIndexMap, myid, 0, idxToStreamIdCache, cnameCacheByteHashToStr, jsParsingStackbuf[:], pleArray)
	if err != nil {
		utils.SendError(ctx, "Failed to ingest record", "", err)
		return
	}

	responsebody["status"] = "Success"
	utils.WriteJsonResponse(ctx, responsebody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func fetchColumnNamesFromAllIndexes(myid int64) []string {
	allColsNamesMap := map[string]struct{}{}
	indexNamesRetrieved := vtable.ExpandAndReturnIndexNames(LOKIINDEX_STAR, myid, false)
	for _, indexName := range indexNamesRetrieved {
		indexColNames := metadata.GetAllColNames([]string{indexName})
		for _, colName := range indexColNames {
			_, ok := allColsNamesMap[colName]
			if !ok {
				allColsNamesMap[colName] = struct{}{}
			}
		}
	}

	colNames := make([]string, 0, len(allColsNamesMap))
	for colName := range allColsNamesMap {
		if colName == "line" {
			continue
		}
		colNames = append(colNames, colName)
	}

	return colNames
}

func addAscSortColRequestToQueryAggs(queryAggs *structs.QueryAggregators, sizeLimit uint64) *structs.QueryAggregators {

	sortAggs := &structs.QueryAggregators{
		PipeCommandType: structs.OutputTransformType,
		OutputTransforms: &structs.OutputTransforms{
			LetColumns: &structs.LetColumnsRequest{},
		},
	}

	sortAggs.OutputTransforms.LetColumns.SortColRequest = &structs.SortExpr{}

	sortElement := &structs.SortElement{
		SortByAsc: true,
		Field:     "timestamp",
	}

	sortEles := make([]*structs.SortElement, 0)
	sortEles = append(sortEles, sortElement)

	sortAggs.OutputTransforms.LetColumns.SortColRequest.SortEles = sortEles
	sortAggs.OutputTransforms.LetColumns.SortColRequest.Limit = sizeLimit
	sortAggs.OutputTransforms.LetColumns.SortColRequest.SortAscending = []int{1}
	sortAggs.OutputTransforms.LetColumns.SortColRequest.SortRecords = make(map[string]map[string]interface{})

	if queryAggs == nil {
		return sortAggs
	}

	tempQueryAggs := queryAggs

	for tempQueryAggs.Next != nil {
		tempQueryAggs = tempQueryAggs.Next
	}

	tempQueryAggs.Next = sortAggs

	return queryAggs
}

func ProcessLokiLabelRequest(ctx *fasthttp.RequestCtx, myid int64) {
	indexName := []string{LOKIINDEX}
	responsebody := make(map[string]interface{})
	colNames := remove(metadata.GetAllColNames(indexName), "line")
	// Check with default Loki Index, if not present, then fetch from all indexes
	if len(colNames) == 0 {
		colNames = fetchColumnNamesFromAllIndexes(myid)
	}
	responsebody["data"] = colNames
	responsebody["status"] = "Success"
	utils.WriteJsonResponse(ctx, responsebody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessLokiLabelValuesRequest(ctx *fasthttp.RequestCtx, myid int64) {
	responsebody := make(map[string]interface{})

	labelName := utils.ExtractParamAsString(ctx.UserValue("labelName"))
	indexName := LOKIINDEX_STAR
	qid := rutils.GetNextQid()

	var astNode *structs.ASTNode
	var aggNode *structs.QueryAggregators

	startTimeMs, endTimeMs, err := parseTimeRangeInMS(ctx, uint64(6*HOUR_IN_MS))
	if err != nil {
		utils.SendError(ctx, "Error parsing time range", fmt.Sprintf("Request Args: %v", ctx.QueryArgs()), err)
		return
	}
	timeRange := &dtu.TimeRange{StartEpochMs: startTimeMs, EndEpochMs: endTimeMs}

	query := string(ctx.QueryArgs().Peek("query"))
	if query != "" {
		astNode, aggNode, _, err = pipesearch.ParseRequest(query, timeRange.StartEpochMs, timeRange.EndEpochMs, qid, "Log QL", indexName)
		if err != nil {
			utils.SendError(ctx, "Error parsing query", fmt.Sprintf("qid=%v, QUERY: %v", qid, query), err)
			return
		}
	}

	colVals, err := ast.GetColValues(labelName, indexName, astNode, aggNode, timeRange, qid, myid)

	if err != nil {
		utils.SendError(ctx, "Failed to process request", "", err)
		return
	}

	responsebody["data"] = colVals
	responsebody["status"] = "Success"
	utils.WriteJsonResponse(ctx, responsebody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessQueryRequest(ctx *fasthttp.RequestCtx, myid int64) {
	query := removeUnimplementedMethods(string(ctx.QueryArgs().Peek("query")))
	if query == "" {
		utils.SendError(ctx, "Search request is empty", "", nil)
		return
	}

	var sizeLimit uint64 = 100
	var err error

	limitStr := string(ctx.QueryArgs().Peek("limit"))
	if limitStr != "" {
		sizeLimit, err = strconv.ParseUint(string(ctx.QueryArgs().Peek("limit")), 10, 64)
		if err != nil {
			utils.SendError(ctx, "Failed to parse limit", "", err)
			return
		}
	}

	var sortOrder string

	direction := string(ctx.QueryArgs().Peek("direction"))
	if direction == "forward" {
		sortOrder = "ASC"
	} else {
		sortOrder = "DESC"
	}

	qid := rutils.GetNextQid()

	ti := structs.InitTableInfo(LOKIINDEX_STAR, myid, false)
	startTimeMs, endTimeMs, err := parseTimeRangeInMS(ctx, uint64(HOUR_IN_MS))
	if err != nil {
		utils.SendError(ctx, "Failed to parse time range", "", err)
		return
	}
	simpleNode, aggs, _, err := pipesearch.ParseRequest(query, startTimeMs, endTimeMs, qid, "Log QL", LOKIINDEX_STAR)
	if err != nil {
		utils.SendError(ctx, "Failed to parse request", "", err)
		return
	}

	if aggs != nil && aggs.GroupByRequest != nil {
		aggs.GroupByRequest.GroupByColumns = remove(aggs.GroupByRequest.GroupByColumns, "line")
	}

	if sortOrder == "ASC" {
		aggs = addAscSortColRequestToQueryAggs(aggs, sizeLimit)
	}

	sizeLimit = pipesearch.GetFinalSizelimit(aggs, sizeLimit)

	segment.LogASTNode("logql query parser", simpleNode, qid)
	segment.LogQueryAggsNode("logql aggs parser", aggs, qid)
	startTime := utils.GetCurrentTimeInMs()
	qc := structs.InitQueryContextWithTableInfo(ti, sizeLimit, 0, myid, false)
	queryResult := segment.ExecuteQuery(simpleNode, aggs, qid, qc)

	if queryResult != nil && queryResult.ErrList != nil && len(queryResult.ErrList) > 0 {
		responsebody := make(map[string]interface{})
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		errors := make([]string, len(queryResult.ErrList))
		for _, err := range queryResult.ErrList {
			errors = append(errors, err.Error())
		}
		responsebody["error"] = strings.Join(errors, ", ")
		utils.WriteJsonResponse(ctx, responsebody)
		return
	}

	if queryResult != nil && queryResult.Qtype == "VectorArithmeticExprType" {
		ctx.SetStatusCode(fasthttp.StatusOK)
		utils.WriteJsonResponse(ctx, getVectorArithmeticResponse(queryResult))
		return
	}

	allJsons, allCols, err := record.GetJsonFromAllRrcOldPipeline(queryResult.AllRecords, false, qid, queryResult.SegEncToKey, aggs, queryResult.AllColumnsInAggs)

	if len(queryResult.MeasureResults) > 0 {
		lokiMetricsResponse := getMetricsResponse(queryResult)
		utils.WriteJsonResponse(ctx, lokiMetricsResponse)
		ctx.SetStatusCode(fasthttp.StatusOK)
		return
	}

	lokiQueryResponse := LokiQueryResponse{}

	if len(allJsons) > 0 {
		lokiQueryResponse.Data = Data{ResultType: "streams"}
		lokiQueryResponse.Data.Result = make([]StreamValue, 0)
		for _, row := range allJsons {
			queryResultLine := StreamValue{Values: make([][]string, 0)}
			// If column line is present, use it as line, else use the entire row as line
			line, ok := row["line"].(string)
			if !ok {
				jsonRow, err := json.Marshal(row)
				if err != nil {
					utils.SendError(ctx, "Unable to marshal row", fmt.Sprintf("row: %v", row), err)
				}
				line = string(jsonRow)
			}
			valuesRow := make([]string, 0)
			timeStamp, ok := row["timestamp"].(uint64)
			if !ok {
				utils.SendError(ctx, "Unable to get the timestamp", fmt.Sprintf("row: %v", row), nil)
			}
			valuesRow = append(valuesRow, fmt.Sprintf("%v", timeStamp*MsToNanoConversion), line)
			queryResultLine.Values = append(queryResultLine.Values, valuesRow)

			newRow := make(map[string]interface{}, 0)
			labelsKeys := remove(allCols, "line")
			for _, label := range labelsKeys {
				if label != TimeStamp && label != Index {
					newRow[label] = fmt.Sprintf("%v", row[label])
				}
			}

			queryResultLine.Stream = newRow
			lokiQueryResponse.Status = "success"
			lokiQueryResponse.Data.Result = append(lokiQueryResponse.Data.Result, queryResultLine)
		}
	} else {
		lokiQueryResponse.Data.Result = make([]StreamValue, 0)
		lokiQueryResponse.Data.ResultType = "streams"
		lokiQueryResponse.Status = "success"
	}

	if err != nil {
		log.Errorf(" ProcessMetricsSearchRequest: received empty search request body ")
		responsebody := make(map[string]interface{})
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responsebody["error"] = "received empty search request body"
		utils.WriteJsonResponse(ctx, responsebody)
		_, err = ctx.WriteString(err.Error())
		if err != nil {
			log.Errorf("qid=%v, ProcessMetricsSearchRequest: could not write error message err=%v", qid, err)
		}
		log.Errorf("qid=%v, ProcessMetricsSearchRequest: failed to decode search request body! Err=%+v", qid, err)
		return
	}

	lokiQueryResponse.Data.Stats = getQueryStats(queryResult, startTime, myid)

	utils.WriteJsonResponse(ctx, lokiQueryResponse)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessIndexStatsRequest(ctx *fasthttp.RequestCtx, myid int64) {
	query := removeUnimplementedMethods(string(ctx.QueryArgs().Peek("query")))

	qid := rutils.GetNextQid()

	ti := structs.InitTableInfo(LOKIINDEX_STAR, myid, false)
	simpleNode, aggs, _, err := pipesearch.ParseQuery(query, qid, "Log QL")
	if err != nil {
		writeEmptyIndexStatsResponse(ctx)
		return
	}

	segment.LogASTNode("logql query parser", simpleNode, qid)
	segment.LogQueryAggsNode("logql aggs parser", aggs, qid)

	simpleNode.TimeRange = rutils.GetESDefaultQueryTimeRange()
	qc := structs.InitQueryContextWithTableInfo(ti, rutils.DefaultBucketCount, 0, myid, false)

	queryResult := segment.ExecuteQuery(simpleNode, aggs, qid, qc)
	allJsons, allCols, err := record.GetJsonFromAllRrcOldPipeline(queryResult.AllRecords, false, qid, queryResult.SegEncToKey, aggs, queryResult.AllColumnsInAggs)
	if err != nil {
		writeEmptyIndexStatsResponse(ctx)
		return
	}

	responsebody := make(map[string]interface{})
	responsebody["streams"] = len(allCols)
	responsebody["chunks"] = getChunkCount(queryResult)
	responsebody["entries"] = len(allJsons)
	byteCount := 0
	for _, row := range allJsons {
		lineString, ok := row["line"].(string)
		if !ok {
			writeEmptyIndexStatsResponse(ctx)
			return
		}
		byteCount += len([]byte(lineString))
	}
	responsebody["bytes"] = byteCount
	utils.WriteJsonResponse(ctx, responsebody)
	ctx.SetStatusCode(fasthttp.StatusOK)

}

func ProcessLokiSeriesRequest(ctx *fasthttp.RequestCtx, myid int64) {
	responsebody := make(map[string]interface{})
	query := string(ctx.QueryArgs().Peek("match[]"))
	startEpoch, err := strconv.ParseUint(string(ctx.QueryArgs().Peek("start")), 10, 64)
	if err != nil {
		utils.SendError(ctx, "Failed to get start time", "", err)
		return
	}
	endEpoch, err := strconv.ParseUint(string(ctx.QueryArgs().Peek("end")), 10, 64)
	if err != nil {
		utils.SendError(ctx, "Failed to get end time", "", err)
		return
	}
	responsebody["status"] = "success"

	qid := rutils.GetNextQid()

	ti := structs.InitTableInfo(LOKIINDEX_STAR, myid, false)
	simpleNode, aggs, _, err := pipesearch.ParseQuery(query, qid, "Log QL")
	if err != nil {
		utils.SendError(ctx, "Failed to parse query", fmt.Sprintf("query: %v", query), err)
		return
	}

	segment.LogASTNode("logql query parser", simpleNode, qid)
	segment.LogQueryAggsNode("logql aggs parser", aggs, qid)

	simpleNode.TimeRange = &dtu.TimeRange{
		StartEpochMs: startEpoch / MsToNanoConversion,
		EndEpochMs:   endEpoch / MsToNanoConversion,
	}
	qc := structs.InitQueryContextWithTableInfo(ti, DefaultLimit, 0, myid, false)
	queryResult := segment.ExecuteQuery(simpleNode, aggs, qid, qc)
	allJsons, _, err := record.GetJsonFromAllRrcOldPipeline(queryResult.AllRecords, false, qid, queryResult.SegEncToKey, aggs, queryResult.AllColumnsInAggs)
	if err != nil {
		utils.SendError(ctx, "Failed to get matching records", "", err)
		return
	}
	responsebody["data"] = allJsons
	utils.WriteJsonResponse(ctx, responsebody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func writeEmptyIndexStatsResponse(ctx *fasthttp.RequestCtx) {
	responsebody := make(map[string]interface{})
	responsebody["streams"] = 0
	responsebody["chunks"] = 0
	responsebody["entries"] = 0
	responsebody["bytes"] = 0
	utils.WriteJsonResponse(ctx, responsebody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func remove(slice []string, stringToRemove string) []string {
	for i, v := range slice {
		if v == stringToRemove {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

// needs to be modified as changes are made to logql parsing. Logfmt, json, label/line expression
// and stream selectors are supported
func removeUnimplementedMethods(queryString string) string {

	// Define the regular expression pattern
	pattern := `sum\s+by\s+\(level\)\s+\(count_over_time\(`
	regex := regexp.MustCompile(pattern)
	matchIndex := regex.FindStringIndex(queryString)
	if matchIndex != nil {
		extractedString := queryString[matchIndex[1]:]
		return extractedString
	} else {
		return queryString
	}
}

func getMetricsResponse(queryResult *structs.NodeResult) LokiMetricsResponse {
	lokiMetricsResponse := LokiMetricsResponse{}
	lokiMetricsResponse.Status = "success"
	lokiMetricsResponse.Data = MetricsData{ResultType: "vector"}
	lokiMetricsResponse.Data.MetricResult = make([]MetricValue, 0)
	if queryResult.MeasureResults != nil {
		metricResult := make([]MetricValue, 0)
		for _, bucket := range queryResult.MeasureResults {
			groupByCols := queryResult.GroupByCols
			newMetricVal := MetricValue{}
			newMetricVal.Stream = make(map[string]interface{})
			for index, colName := range groupByCols {
				newMetricVal.Stream[colName] = bucket.GroupByValues[index]
			}
			valResult := make([]interface{}, 0)
			valResult = append(valResult, 1689919818.158, queryResult.MeasureFunctions[0])
			newMetricVal.Values = valResult
			metricResult = append(metricResult, newMetricVal)
		}
		lokiMetricsResponse.Data.MetricResult = metricResult
	}

	lokiMetricsResponse.Data.Stats = MetricStats{}
	return lokiMetricsResponse
}
func getQueryStats(queryResult *structs.NodeResult, startTime uint64, myid int64) Stats {
	lokiQueryStats := Stats{}
	if queryResult == nil {
		return lokiQueryStats
	}

	allSegmetas := segwriter.ReadGlobalSegmetas()

	allCnts := segwriter.GetVTableCountsForAll(myid, allSegmetas)
	segwriter.GetUnrotatedVTableCountsForAll(myid, allCnts)

	var bytesReceivedCount uint64
	var recordCount uint64
	var onDiskBytesCount uint64

	for _, cnts := range allCnts {
		bytesReceivedCount += cnts.BytesCount
		recordCount += cnts.RecordCount
		onDiskBytesCount += cnts.OnDiskBytesCount
	}

	chunkCount := getChunkCount(queryResult)

	ingesterStats := Ingester{}
	ingesterStats.CompressedBytes = onDiskBytesCount
	ingesterStats.DecompressedBytes = bytesReceivedCount
	ingesterStats.DecompressedLines = recordCount
	ingesterStats.TotalReached = 1 //single node
	ingesterStats.TotalLinesSent = len(queryResult.AllRecords)
	ingesterStats.TotalChunksMatched = chunkCount
	ingesterStats.TotalBatches = chunkCount * search.BLOCK_BATCH_SIZE
	ingesterStats.HeadChunkBytes = onDiskBytesCount
	ingesterStats.HeadChunkLines = recordCount

	lokiQueryStats.Ingester = ingesterStats

	storeStats := Store{}
	storeStats.DecompressedBytes = onDiskBytesCount
	summaryStats := Summary{}

	summaryStats.TotalBytesProcessed = bytesReceivedCount
	summaryStats.ExecTime = float64(utils.GetCurrentTimeInMs() - startTime)
	summaryStats.TotalLinesProcessed = len(queryResult.AllRecords)

	return lokiQueryStats
}

func getChunkCount(queryResult *structs.NodeResult) int {
	uniqueChunks := make(map[uint16]bool)
	for _, record := range queryResult.AllRecords {
		uniqueChunks[record.BlockNum] = true
	}
	return len(uniqueChunks)
}

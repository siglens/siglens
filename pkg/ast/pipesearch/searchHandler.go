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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	sutils "github.com/siglens/siglens/pkg/segment/utils"

	"github.com/fasthttp/websocket"
	"github.com/siglens/siglens/pkg/alerts/alertutils"
	"github.com/siglens/siglens/pkg/ast/pipesearch/multiplexer"
	"github.com/siglens/siglens/pkg/common/dtypeutils"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	fileutils "github.com/siglens/siglens/pkg/common/fileutils"
	rutils "github.com/siglens/siglens/pkg/readerUtils"
	"github.com/siglens/siglens/pkg/segment"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

const KEY_INDEX_NAME string = "indexName"

// When this flag is set, run a timechart query as well; only applicable when
// the query returns logs.
const runTimechartFlag = "runTimechart"

/*
Example incomingBody

{"searchText":"*","startEpoch":1656716713300,"endEpoch":1656717613300,"indexName":"*", "size": 1000, "from": 0}

# Returns searchText,startEpoch,endEpoch,finalSize,indexName,scrollFrom

finalSize = size + from
*/
func ParseSearchBody(jsonSource map[string]interface{}, nowTs uint64) (string, uint64, uint64, uint64, string, int, bool, bool) {
	var searchText, indexName string
	var startEpoch, endEpoch, finalSize uint64
	var scrollFrom int
	var includeNulls bool
	var runTimechart bool
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

	iText, ok := jsonSource[KEY_INDEX_NAME]
	if !ok || iText == "" {
		indexName = "*"
	} else if iText == KEY_TRACE_RELATED_LOGS_INDEX {
		// TODO: set indexNameIn to otel-collector indexes
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
			var err error
			startEpoch, err = strconv.ParseUint(val, 10, 64)
			if err != nil {
				defValue := nowTs - (15 * 60 * 1000)
				startEpoch = utils.ParseAlphaNumTime(nowTs, string(val), defValue)
			}
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
			var err error
			endEpoch, err = strconv.ParseUint(val, 10, 64)
			if err != nil {
				endEpoch = utils.ParseAlphaNumTime(nowTs, string(val), nowTs)
			}
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

	includeNullsVal, ok := jsonSource["includeNulls"]
	if !ok {
		includeNulls = false
	} else {
		switch val := includeNullsVal.(type) {
		case bool:
			includeNulls = val
		case string:
			includeNulls = val == "true"
		default:
			log.Infof("ParseSearchBody: unexpected type for includeNulls: %T, value: %+v. Defaulting to false", val, val)
			includeNulls = false
		}
	}

	timechartFlagVal, ok := jsonSource[runTimechartFlag]
	if !ok {
		runTimechart = false
	} else {
		switch val := timechartFlagVal.(type) {
		case bool:
			runTimechart = val
		case string:
			runTimechart = val == "true"
		default:
			log.Infof("ParseSearchBody: unexpected type for runTimechartQuery: %T, value: %+v. Defaulting to false", val, val)
			runTimechart = false
		}
	}

	finalSize = finalSize + uint64(scrollFrom)

	return searchText, startEpoch, endEpoch, finalSize, indexName, scrollFrom, includeNulls, runTimechart
}

// ProcessAlertsPipeSearchRequest processes the logs search request for alert queries.
func ProcessAlertsPipeSearchRequest(queryParams alertutils.QueryParams,
	orgid int64, ctx *fasthttp.RequestCtx,
) (*structs.PipeSearchResponseOuter, *dtypeutils.TimeRange, error) {
	dbPanelId := "-1"
	queryStart := time.Now()

	qid := rutils.GetNextQid()
	readJSON := make(map[string]interface{})
	var err error
	readJSON["from"] = "0"
	readJSON[KEY_INDEX_NAME] = queryParams.Index
	readJSON["queryLanguage"] = queryParams.QueryLanguage
	readJSON["searchText"] = queryParams.QueryText
	readJSON["startEpoch"] = queryParams.StartTime
	readJSON["endEpoch"] = queryParams.EndTime
	readJSON["state"] = "query"

	httpRespOuter, isScrollMax, timeRange, err := ParseAndExecutePipeRequest(readJSON, qid, orgid, queryStart, dbPanelId, ctx)
	if err != nil {
		return nil, nil, err
	}

	if isScrollMax {
		return nil, nil, fmt.Errorf("scrollFrom is greater than 10_000")
	}

	return httpRespOuter, timeRange, nil
}

func ParseAndExecutePipeRequest(readJSON map[string]interface{}, qid uint64, myid int64, queryStart time.Time, dbPanelId string, ctx *fasthttp.RequestCtx) (*structs.PipeSearchResponseOuter, bool, *dtypeutils.TimeRange, error) {
	var err error

	nowTs := utils.GetCurrentTimeInMs()
	searchText, startEpoch, endEpoch, sizeLimit, indexNameIn, scrollFrom, includeNulls, _ := ParseSearchBody(readJSON, nowTs)
	limit := sizeLimit
	if scrollFrom > 10_000 {
		return nil, true, nil, nil
	}

	ti := structs.InitTableInfo(indexNameIn, myid, false, ctx)
	log.Infof("qid=%v, ParseAndExecutePipeRequest: index=[%s], searchString=[%v] , startEpoch: %v, endEpoch: %v",
		qid, ti.String(), searchText, startEpoch, endEpoch)

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
		ti = structs.InitTableInfo(strings.Join(parsedIndexNames, ","), myid, false, ctx)
	}

	sizeLimit = GetFinalSizelimit(aggs, sizeLimit)

	// If MaxRows is used to limit the number of returned results, set `sizeLimit`
	// to it. Currently MaxRows is only valid as the root QueryAggregators.
	if aggs != nil && aggs.Limit != 0 {
		sizeLimit = uint64(aggs.Limit)
	}
	if queryLanguageType == "SQL" && aggs != nil && aggs.TableName != "*" {
		indexNameIn = aggs.TableName
		ti = structs.InitTableInfo(indexNameIn, myid, false, ctx) // Re-initialize ti with the updated indexNameIn
	}

	qc := structs.InitQueryContextWithTableInfo(ti, sizeLimit, scrollFrom, myid, false)
	qc.IncludeNulls = includeNulls
	qc.RawQuery = searchText
	return RunQueryForNewPipeline(nil, qid, simpleNode, aggs, nil, nil, qc, limit)
}

func ProcessPipeSearchRequest(ctx *fasthttp.RequestCtx, myid int64) {
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

	readJSON, err := utils.DecodeJsonToMap(rawJSON)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		_, err = ctx.WriteString(err.Error())
		if err != nil {
			log.Errorf("qid=%v, ProcessPipeSearchRequest: could not write error message, err: %v", qid, err)
		}
		log.Errorf("qid=%v, ProcessPipeSearchRequest: failed to decode search request body! err: %+v", qid, err)
	}

	httpRespOuter, isScrollMax, _, err := ParseAndExecutePipeRequest(readJSON, qid, myid, queryStart, dbPanelId, ctx)
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

func processMaxScrollCount(ctx *fasthttp.RequestCtx, qid uint64) {
	resp := &structs.PipeSearchResponseOuter{
		CanScrollMore: false,
	}
	qType := query.GetQueryType(qid)
	resp.Qtype = qType.String()
	utils.WriteJsonResponse(ctx, resp)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func RunQueryForNewPipeline(conn *websocket.Conn, qid uint64, root *structs.ASTNode, aggs *structs.QueryAggregators,
	timechartRoot *structs.ASTNode, timechartAggs *structs.QueryAggregators,
	qc *structs.QueryContext, sizeLimit uint64,
) (*structs.PipeSearchResponseOuter, bool, *dtu.TimeRange, error) {
	isAsync := conn != nil

	runTimechartQuery := (timechartRoot != nil && timechartAggs != nil)
	var timechartQid uint64
	var timechartQuery *query.RunningQueryState
	var timechartStateChan chan *query.QueryStateChanData
	var err error
	if runTimechartQuery {
		timechartQid = rutils.GetNextQid()
		timechartQuery, err = query.StartQueryAsCoordinator(timechartQid, isAsync, nil, timechartRoot, timechartAggs, qc, nil, false)
		if err != nil {
			log.Errorf("qid=%v, RunQueryForNewPipeline: failed to start timechart query, err: %v", qid, err)
			return nil, false, nil, err
		}

		timechartStateChan = timechartQuery.StateChan
	}

	rQuery, err := query.StartQueryAsCoordinator(qid, isAsync, nil, root, aggs, qc, nil, false)
	if err != nil {
		log.Errorf("qid=%v, RunQueryForNewPipeline: failed to start query, err: %v", qid, err)
		return nil, false, nil, err
	}

	var httpRespOuter *structs.PipeSearchResponseOuter

	stateChan := multiplexer.NewQueryStateMultiplexer(rQuery.StateChan, timechartStateChan).Multiplex()

	for {
		queryStateData, ok := <-stateChan
		if !ok {
			log.Errorf("qid=%v, RunQueryForNewPipeline: Got non ok, state: %+v", qid, queryStateData)
			query.DeleteQuery(qid)
			return httpRespOuter, false, root.TimeRange, fmt.Errorf("qid=%v, RunQueryForNewPipeline: Got non ok, state: %+v", qid, queryStateData)
		}

		if queryStateData.Qid != qid && queryStateData.Qid != timechartQid {
			log.Errorf("RunQueryForNewPipeline: qid mismatch, expected %v or %v, got: %v",
				qid, timechartQid, queryStateData.Qid)
			continue
		}

		rQuery.SetLatestQueryState(queryStateData.StateName)

		switch queryStateData.StateName {
		case query.WAITING:
			// do nothing
		case query.READY:
			switch queryStateData.ChannelIndex {
			case multiplexer.MainIndex:
				go segment.ExecuteQueryInternalNewPipeline(qid, isAsync, root, aggs, qc, rQuery, sizeLimit)
			case multiplexer.TimechartIndex:
				go segment.ExecuteQueryInternalNewPipeline(timechartQid, isAsync, timechartRoot, timechartAggs, qc, timechartQuery, sizeLimit)
			}
		case query.RUNNING:
			if isAsync {
				switch queryStateData.ChannelIndex {
				case multiplexer.MainIndex:
					processQueryStateUpdate(conn, qid, queryStateData.StateName)
				case multiplexer.TimechartIndex:
					processQueryStateUpdate(conn, timechartQid, queryStateData.StateName)
				}
			}
		case query.QUERY_UPDATE:
			if isAsync {
				wErr := conn.WriteJSON(queryStateData.UpdateWSResp)
				if wErr != nil {
					log.Errorf("qid=%v, RunQueryForNewPipeline: failed to write json to websocket, err: %v", qid, wErr)
				}
			}
		case query.COMPLETE:
			defer query.DeleteQuery(qid)
			if runTimechartQuery {
				defer query.DeleteQuery(timechartQid)
			}

			if isAsync {
				if runTimechartQuery {
					err = populateMissingBuckets(queryStateData.CompleteWSResp.TimechartComplete, root.TimeRange)
					if err != nil {
						log.Errorf("RunQueryForNewPipeline: failed to get final buckets err: %v", err)
						return nil, false, root.TimeRange, err
					}
				}

				wErr := conn.WriteJSON(queryStateData.CompleteWSResp)
				if wErr != nil {
					log.Errorf("qid=%v, RunQueryForNewPipeline: failed to write json to websocket, err: %v", qid, wErr)
				}
				return nil, false, root.TimeRange, nil
			} else {
				httpRespOuter = queryStateData.HttpResponse
				return httpRespOuter, false, root.TimeRange, nil
			}
		case query.ERROR:
			if rQuery.IsCoordinator() && utils.IsRPCUnavailableError(queryStateData.Error) {
				newRQuery, newQid, err := listenToRestartQuery(qid, rQuery, isAsync, conn)
				if err != nil {
					log.Errorf("qid=%v, RunQueryForNewPipeline: failed to restart query for rpc failure, err: %v", qid, err)
				} else {
					newTimechartQuery, newTimechartQid, err := listenToRestartQuery(timechartQid, timechartQuery, isAsync, conn)
					if err != nil {
						log.Errorf("qid=%v, RunQueryForNewPipeline: failed to restart timechart query for rpc failure, err: %v", qid, err)
						defer query.DeleteQuery(newQid)
					} else {
						rQuery = newRQuery
						qid = newQid

						timechartQuery = newTimechartQuery
						timechartQid = newTimechartQid

						continue
					}
				}
			}

			defer query.DeleteQuery(qid)
			if runTimechartQuery {
				defer query.DeleteQuery(timechartQid)
			}
			if isAsync {
				wErr := conn.WriteJSON(createErrorResponse(queryStateData.Error.Error()))
				if wErr != nil {
					log.Errorf("qid=%v, RunQueryForNewPipeline: failed to write json to websocket, err: %v", qid, wErr)
				}
				return nil, false, root.TimeRange, nil
			} else {
				return nil, false, root.TimeRange, queryStateData.Error
			}
		case query.QUERY_RESTART:
			switch queryStateData.ChannelIndex {
			case multiplexer.MainIndex:
				newRQuery, newQid, err := handleRestartQuery(qid, rQuery, isAsync, conn)
				if err != nil {
					log.Errorf("qid=%v, RunQueryForNewPipeline: failed to restart query, err: %v", qid, err)
					continue
				}

				rQuery = newRQuery
				qid = newQid
			case multiplexer.TimechartIndex:
				newRQuery, newQid, err := handleRestartQuery(timechartQid, timechartQuery, isAsync, conn)
				if err != nil {
					log.Errorf("qid=%v, RunQueryForNewPipeline: failed to restart timechart query, err: %v", qid, err)
					continue
				}

				timechartQuery = newRQuery
				timechartQid = newQid
			}
		case query.TIMEOUT:
			defer query.DeleteQuery(qid)
			defer query.DeleteQuery(timechartQid)
			if isAsync {
				processTimeoutUpdate(conn, qid)
				if runTimechartQuery {
					processTimeoutUpdate(conn, timechartQid)
				}
			} else {
				return nil, false, root.TimeRange, fmt.Errorf("qid=%v, RunQueryForNewPipeline: query timed out", qid)
			}
		case query.CANCELLED:
			log.Infof("qid=%v, RunQueryForNewPipeline: query cancelled", qid)
			defer query.DeleteQuery(qid)
			if runTimechartQuery {
				defer query.DeleteQuery(timechartQid)
			}

			if isAsync {
				processCancelQuery(conn, qid)
				if runTimechartQuery {
					processCancelQuery(conn, timechartQid)
				}
			}
			return nil, false, root.TimeRange, nil
		}
	}
}

// populateMissingBuckets fills missing buckets in the response based on the time range and span options.
func populateMissingBuckets(completeResp *structs.PipeSearchCompleteResponse, timeRange *dtu.TimeRange) error {
	spanOptions, err := structs.GetDefaultTimechartSpanOptions(timeRange.StartEpochMs, timeRange.EndEpochMs, 0)
	if err != nil {
		return fmt.Errorf("failed to get span options: %w", err)
	}
	var duration time.Duration = 0
	if spanOptions.SpanLength.TimeScalr != sutils.TMMonth {
		duration, err = getDuration(spanOptions.SpanLength.Num, spanOptions.SpanLength.TimeScalr)
		if err != nil {
			return fmt.Errorf("failed to get duration: %w", err)
		}
	}

	bucketHolderMap, err := initEmptyBuckets(completeResp, timeRange, duration, spanOptions.SpanLength.TimeScalr, spanOptions.SpanLength.Num)
	if err != nil {
		return fmt.Errorf("failed to initialize empty buckets: %w", err)
	}

	if err = populateBucketsWithExistingResults(completeResp, bucketHolderMap, spanOptions.SpanLength.TimeScalr); err != nil {
		return fmt.Errorf("failed to populate buckets with existing results: %w", err)
	}

	completeResp.MeasureResults = completeResp.MeasureResults[:0]
	for _, v := range bucketHolderMap {
		completeResp.MeasureResults = append(completeResp.MeasureResults, v)
	}
	completeResp.BucketCount = len(completeResp.MeasureResults)
	return nil
}

func getDuration(interval int, scaler sutils.TimeUnit) (time.Duration, error) {
	switch scaler {
	case sutils.TMSecond:
		return time.Duration(interval) * time.Second, nil
	case sutils.TMMinute:
		return time.Duration(interval) * time.Minute, nil
	case sutils.TMHour:
		return time.Duration(interval) * time.Hour, nil
	case sutils.TMDay:
		return time.Duration(interval) * 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("unsupported time scaler: %v", scaler)
	}
}

func initEmptyBuckets(
	completeResp *structs.PipeSearchCompleteResponse,
	timeRange *dtu.TimeRange,
	duration time.Duration,
	timeScaler sutils.TimeUnit,
	interval int,
) (map[string]*structs.BucketHolder, error) {
	runningTs := time.UnixMilli(int64(timeRange.StartEpochMs))
	endEpoch := time.UnixMilli(int64(timeRange.EndEpochMs))
	bucketHolderMap := make(map[string]*structs.BucketHolder)

	// For TMMonth, we cannot use fixed durations like time.Duration because months have variable lengths (28–31 days).
	// Instead, we generate buckets by incrementing the month by interval,
	if timeScaler == sutils.TMMonth {
		runningTs = time.Date(runningTs.Year(), runningTs.Month(), 1, 0, 0, 0, 0, runningTs.Location())
		for !runningTs.After(endEpoch) {
			bucketInterval := strconv.FormatInt(runningTs.UnixMilli(), 10)
			bucketHolderMap[bucketInterval] = createEmptyBucket(bucketInterval, completeResp)
			runningTs = time.Date(runningTs.Year(), time.Month(int(runningTs.Month())+interval), 1, 0, 0, 0, 0, runningTs.Location())
		}
	} else {
		// For all other time scalers (seconds, minutes, hours, days),
		// we can use a fixed duration to increment time safely.
		for !runningTs.After(endEpoch) {
			alignedTs := alignToScalerStart(runningTs, timeScaler)
			if alignedTs.IsZero() {
				return nil, fmt.Errorf("unsupported time scaler: %v", timeScaler)
			}
			bucketInterval := strconv.FormatInt(alignedTs.UnixMilli(), 10)
			bucketHolderMap[bucketInterval] = createEmptyBucket(bucketInterval, completeResp)
			runningTs = runningTs.Add(duration)
		}
	}

	return bucketHolderMap, nil
}

func alignToScalerStart(ts time.Time, scaler sutils.TimeUnit) time.Time {
	switch scaler {
	case sutils.TMSecond:
		return time.Date(ts.Year(), ts.Month(), ts.Day(), ts.Hour(), ts.Minute(), ts.Second(), 0, ts.Location())
	case sutils.TMMinute:
		return time.Date(ts.Year(), ts.Month(), ts.Day(), ts.Hour(), ts.Minute(), 0, 0, ts.Location())
	case sutils.TMHour:
		return time.Date(ts.Year(), ts.Month(), ts.Day(), ts.Hour(), 0, 0, 0, ts.Location())
	case sutils.TMDay:
		return time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, ts.Location())
	case sutils.TMMonth:
		return time.Date(ts.Year(), ts.Month(), 1, 0, 0, 0, 0, ts.Location())
	default:
		return time.Time{}
	}
}

func createEmptyBucket(intervalStr string, completeResp *structs.PipeSearchCompleteResponse) *structs.BucketHolder {
	key := ""
	if len(completeResp.MeasureFunctions) > 0 {
		key = completeResp.MeasureFunctions[0]
	}
	return &structs.BucketHolder{
		GroupByValues: []string{intervalStr},
		MeasureVal:    map[string]interface{}{key: 0},
	}
}

func populateBucketsWithExistingResults(
	completeResp *structs.PipeSearchCompleteResponse,
	bucketHolderMap map[string]*structs.BucketHolder,
	timeScaler sutils.TimeUnit,
) error {
	for _, bucket := range completeResp.MeasureResults {
		if bucket.GroupByValues == nil || len(bucket.GroupByValues) == 0 {
			return fmt.Errorf("bucket.GroupByValues is empty, cannot extract timestamp")
		}
		millisStr := bucket.GroupByValues[0]
		millisInt, err := strconv.ParseInt(millisStr, 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse bucket groupBy timestamp '%s': %w", millisStr, err)
		}

		aligned := alignToScalerStart(time.UnixMilli(millisInt), timeScaler)
		bucketInterval := strconv.FormatInt(aligned.UnixMilli(), 10)

		bucketHolderMap[bucketInterval] = bucket
	}
	return nil
}

func listenToRestartQuery(qid uint64, rQuery *query.RunningQueryState, isAsync bool, conn *websocket.Conn) (*query.RunningQueryState, uint64, error) {
	if rQuery == nil {
		return nil, 0, fmt.Errorf("listenToRestartQuery: rQuery is nil")
	}

	timeout := time.After(10 * time.Second) // wait for 10 seconds for query restart before timing out

	for {
		select {
		case queryStateData, ok := <-rQuery.StateChan:
			if !ok {
				query.DeleteQuery(qid)
				return nil, 0, fmt.Errorf("qid=%v, listenToRestartQuery: Got non ok, state: %v", qid, queryStateData.StateName)
			}

			if queryStateData.Qid != qid {
				continue
			}

			if queryStateData.StateName == query.QUERY_RESTART {
				return handleRestartQuery(qid, rQuery, isAsync, conn)
			}
		case <-timeout:
			return nil, 0, fmt.Errorf("qid=%v, listenToRestartQuery: timed out waiting for query restart", qid)
		}
	}
}

func handleRestartQuery(qid uint64, rQuery *query.RunningQueryState, isAsync bool, conn *websocket.Conn) (*query.RunningQueryState, uint64, error) {
	newRQuery, newQid, err := rQuery.RestartQuery(true)
	if err != nil {
		errorState := &query.QueryStateChanData{
			Qid:       qid,
			StateName: query.ERROR,
			Error:     err,
		}
		rQuery.StateChan <- errorState

		return nil, 0, err
	}

	if isAsync {
		processQueryStateUpdate(conn, qid, query.QUERY_RESTART)
	}

	return newRQuery, newQid, nil
}

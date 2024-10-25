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
	"fmt"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/fasthttp/websocket"
	"github.com/siglens/siglens/pkg/common/dtypeutils"
	fileutils "github.com/siglens/siglens/pkg/common/fileutils"
	"github.com/siglens/siglens/pkg/config"
	rutils "github.com/siglens/siglens/pkg/readerUtils"
	"github.com/siglens/siglens/pkg/segment"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

func ProcessPipeSearchWebsocket(conn *websocket.Conn, orgid uint64, ctx *fasthttp.RequestCtx) {

	qid := rutils.GetNextQid()
	event, err := readInitialEvent(qid, conn)
	defer fileutils.DeferableAddAccessLogEntry(
		time.Now(),
		func() time.Time { return time.Now() },
		"No-user", // TODO : Add logged in user when user auth is implemented
		qid,
		ctx.Request.URI().String(),
		fmt.Sprintf("%+v", event),
		func() int { return ctx.Response.StatusCode() },
		true, // Log this even though it's a websocket connection
		fileutils.AccessLogFile,
	)

	fileutils.AddLogEntry(dtypeutils.LogFileData{
		TimeStamp:   time.Now().Format("2006-01-02 15:04:05"),
		UserName:    "No-user", // TODO : Add logged in user when user auth is implemented
		QueryID:     qid,
		URI:         ctx.Request.URI().String(),
		RequestBody: fmt.Sprintf("%+v", event),
	}, true, fileutils.QueryLogFile)

	if err != nil {
		log.Errorf("qid=%d, ProcessPipeSearchWebsocket: Failed to read initial event! err: %+v", qid, err)
		wErr := conn.WriteJSON(createErrorResponse(err.Error()))
		if wErr != nil {
			log.Errorf("qid=%d, ProcessPipeSearchWebsocket: failed to write error response to websocket! err: %+v", qid, wErr)
		}
		return
	}
	eventState, ok := event["state"]
	if !ok {
		log.Errorf("qid=%d, ProcessPipeSearchWebsocket: first request does not have 'state' as a key!", qid)
		wErr := conn.WriteJSON(createErrorResponse("request missing required key 'state'"))
		if wErr != nil {
			log.Errorf("qid=%d, ProcessPipeSearchWebsocket: failed to write error response to websocket! err: %+v", qid, wErr)
		}
		return
	}
	if eventState != "query" {
		log.Errorf("qid=%d, ProcessPipeSearchWebsocket: first request is not a query 'state'!", qid)
		wErr := conn.WriteJSON(createErrorResponse("first request should have 'state':'query'"))
		if wErr != nil {
			log.Errorf("qid=%d, ProcessPipeSearchWebsocket: failed to write error response to websocket! err: %+v", qid, wErr)
		}
		return
	}

	nowTs := utils.GetCurrentTimeInMs()
	searchText, startEpoch, endEpoch, sizeLimit, indexNameIn, scrollFrom := ParseSearchBody(event, nowTs)

	if scrollFrom > 10_000 {
		processMaxScrollComplete(conn, qid)
		return
	}

	ti := structs.InitTableInfo(indexNameIn, orgid, false)
	log.Infof("qid=%v, ProcessPipeSearchWebsocket: index=[%v] searchString=[%v] scrollFrom=[%v]",
		qid, ti.String(), searchText, scrollFrom)

	queryLanguageType := event["queryLanguage"]
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
			wErr := conn.WriteJSON(createErrorResponse(err.Error()))
			if wErr != nil {
				log.Errorf("qid=%d, ProcessPipeSearchWebsocket: failed to write error response to websocket! err: %+v", qid, wErr)
			}
		} else {
			err = structs.CheckUnsupportedFunctions(aggs)
		}
	} else {
		log.Infof("ProcessPipeSearchWebsocket: unknown queryLanguageType: %v; using Splunk QL instead", queryLanguageType)
		simpleNode, aggs, parsedIndexNames, err = ParseRequest(searchText, startEpoch, endEpoch, qid, "Splunk QL", indexNameIn)
	}

	if err != nil {
		log.Errorf("qid=%d, ProcessPipeSearchWebsocket: failed to parse query, err: %v", qid, err)
		wErr := conn.WriteJSON(createErrorResponse(err.Error()))
		if wErr != nil {
			log.Errorf("qid=%d, ProcessPipeSearchWebsocket: failed to write error response to websocket! err: %+v", qid, wErr)
		}
		return
	}

	// This is for SPL queries where the index name is parsed from the query
	if len(parsedIndexNames) > 0 {
		ti = structs.InitTableInfo(strings.Join(parsedIndexNames, ","), orgid, false)
	}

	if queryLanguageType == "SQL" && aggs != nil && aggs.TableName != "*" {
		indexNameIn = aggs.TableName
		ti = structs.InitTableInfo(indexNameIn, orgid, false) // Re-initialize ti with the updated indexNameIn
	}

	sizeLimit = GetFinalSizelimit(aggs, sizeLimit)

	qc := structs.InitQueryContextWithTableInfo(ti, sizeLimit, scrollFrom, orgid, false)
	qc.RawQuery = searchText
	eventC, err := segment.ExecuteAsyncQuery(simpleNode, aggs, qid, qc)
	if err != nil {
		log.Errorf("qid=%d, ProcessPipeSearchWebsocket: failed to execute query, err: %v", qid, err)
		wErr := conn.WriteJSON(createErrorResponse(err.Error()))
		if wErr != nil {
			log.Errorf("qid=%d, ProcessPipeSearchWebsocket: failed to write error response to websocket! err: %+v", qid, wErr)
		}
		return
	}
	websocketR := make(chan map[string]interface{})
	go listenToConnection(qid, websocketR, conn)
	for {
		select {
		case qscd, ok := <-eventC:
			switch qscd.StateName {
			case query.RUNNING:
				processRunningUpdate(conn, qid)
			case query.QUERY_UPDATE:
				processQueryUpdate(conn, qid, sizeLimit, scrollFrom, qscd, aggs)
			case query.TIMEOUT:
				processTimeoutUpdate(conn, qid)
				return
			case query.COMPLETE:
				processCompleteUpdate(conn, sizeLimit, qid, aggs)
				query.DeleteQuery(qid)
				return
			default:
				log.Errorf("qid=%v, Got unknown state: %v", qid, qscd.StateName)
			}
			if !ok {
				log.Errorf("qid=%v, ProcessPipeSearchWebsocket: Got non ok, state: %v", qid, qscd.StateName)
				query.LogGlobalSearchErrors(qid)
				return
			}
		case readMsg := <-websocketR:
			log.Infof("qid=%d, Got message from websocket: %+v", qid, readMsg)
			if readMsg["state"] == "cancel" {
				query.CancelQuery(qid)
				processCancelQuery(conn, qid)
				query.DeleteQuery(qid)
			}
		}
	}
}

func listenToConnection(qid uint64, e chan map[string]interface{}, conn *websocket.Conn) {
	for {
		readEvent := make(map[string]interface{})
		err := conn.ReadJSON(&readEvent)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Errorf("qid=%d, listenToConnection unexpected error: %+v", qid, err.Error())
			}
			cancelEvent := map[string]interface{}{"state": "cancel", "message": "websocket connection is closed"}
			e <- cancelEvent
			return
		}
		e <- readEvent
	}
}

func readInitialEvent(qid uint64, conn *websocket.Conn) (map[string]interface{}, error) {
	readEvent := make(map[string]interface{})
	err := conn.ReadJSON(&readEvent)
	if err != nil {
		log.Errorf("qid=%d, readInitialEvent: Failed to read initial event from websocket! err: %v", qid, err)
		return readEvent, err
	}

	log.Infof("qid=%d, Read initial event from websocket: %+v", qid, readEvent)
	return readEvent, nil
}

func createErrorResponse(errMsg string) map[string]interface{} {
	e := map[string]interface{}{
		"state":   "error",
		"message": errMsg,
	}
	return e
}

func processTimeoutUpdate(conn *websocket.Conn, qid uint64) {
	e := map[string]interface{}{
		"state":          query.TIMEOUT.String(),
		"qid":            qid,
		"timeoutSeconds": fmt.Sprintf("%v", query.CANCEL_QUERY_AFTER_SECONDS),
	}
	err := conn.WriteJSON(e)
	if err != nil {
		log.Errorf("qid=%d, processTimeoutUpdate: failed to write to websocket! err: %+v", qid, err)
	}
}

func processCancelQuery(conn *websocket.Conn, qid uint64) {
	e := map[string]interface{}{
		"state": query.CANCELLED.String(),
		"qid":   qid,
	}
	err := conn.WriteJSON(e)
	if err != nil {
		log.Errorf("qid=%d, processCancelQuery: failed to write to websocket! err: %+v", qid, err)
	}
}

func processRunningUpdate(conn *websocket.Conn, qid uint64) {

	e := map[string]interface{}{
		"state": query.RUNNING.String(),
		"qid":   qid,
	}
	wErr := conn.WriteJSON(e)
	if wErr != nil {
		log.Errorf("qid=%d, processRunningUpdate: failed to write error response to websocket! err: %+v", qid, wErr)
	}
}

func processQueryUpdate(conn *websocket.Conn, qid uint64, sizeLimit uint64, scrollFrom int, qscd *query.QueryStateChanData,
	aggs *structs.QueryAggregators) {
	searchPercent := qscd.PercentComplete

	totalEventsSearched, totalPossibleEvents, err := query.GetTotalSearchedAndPossibleEventsForQid(qid)
	if err != nil {
		log.Errorf("qid=%d, processQueryUpdate: failed to get total searched and possible records: %+v", qid, err)
		wErr := conn.WriteJSON(createErrorResponse(err.Error()))
		if wErr != nil {
			log.Errorf("qid=%d, processQueryUpdate: failed to write error response to websocket! err: %+v", qid, wErr)
		}
		return
	}

	var wsResponse *structs.PipeSearchWSUpdateResponse
	if qscd.QueryUpdate == nil {
		log.Errorf("qid=%d, processQueryUpdate: got nil query update!", qid)
		wErr := conn.WriteJSON(createErrorResponse("Got nil query update"))
		if wErr != nil {
			log.Errorf("qid=%d, processQueryUpdate: failed to write RRC response to websocket! err: %+v", qid, wErr)
		}
		return
	}

	wsResponse, err = createRecsWsResp(qid, sizeLimit, searchPercent, scrollFrom, totalEventsSearched, qscd.QueryUpdate, aggs, totalPossibleEvents)
	if err != nil {
		wErr := conn.WriteJSON(createErrorResponse(err.Error()))
		if wErr != nil {
			log.Errorf("qid=%d, processQueryUpdate: failed to write RRC response to websocket! err: %+v", qid, wErr)
		}
		return
	}

	wErr := conn.WriteJSON(wsResponse)
	if wErr != nil {
		log.Errorf("qid=%d, processQueryUpdate: failed to write update response to websocket! err: %+v", qid, wErr)
	}
}

func processCompleteUpdate(conn *websocket.Conn, sizeLimit, qid uint64, aggs *structs.QueryAggregators) {
	queryC := query.GetQueryCountInfoForQid(qid)
	totalEventsSearched, err := query.GetTotalsRecsSearchedForQid(qid)
	if !config.IsNewQueryPipelineEnabled() && aggs.HasGeneratedEventsWithoutSearch() {
		queryC.TotalCount = uint64(len(aggs.GenerateEvent.GeneratedRecords))
	}
	if err != nil {
		log.Errorf("qid=%d, processCompleteUpdate: failed to get total records searched, err: %+v", qid, err)
	}
	numRRCs, err := query.GetNumMatchedRRCs(qid)
	if err != nil {
		log.Errorf("qid=%d, processCompleteUpdate: failed to get number of RRCs for qid! Error: %v", qid, err)
	}

	aggMeasureRes, aggMeasureFunctions, aggGroupByCols, columnsOrder, bucketCount := query.GetMeasureResultsForQid(qid, true, 0, aggs.BucketLimit) //aggs.BucketLimit

	var canScrollMore bool
	if numRRCs == sizeLimit {
		// if the number of RRCs is exactly equal to the requested size, there may be more than size matches. Hence, we can scroll more
		canScrollMore = true
	}
	queryType := query.GetQueryType(qid)
	resp := &structs.PipeSearchCompleteResponse{
		TotalMatched:        convertQueryCountToTotalResponse(queryC),
		State:               query.COMPLETE.String(),
		TotalEventsSearched: humanize.Comma(int64(totalEventsSearched)),
		CanScrollMore:       canScrollMore,
		TotalRRCCount:       numRRCs,
		MeasureResults:      aggMeasureRes,
		MeasureFunctions:    aggMeasureFunctions,
		GroupByCols:         aggGroupByCols,
		Qtype:               queryType.String(),
		BucketCount:         bucketCount,
		IsTimechart:         aggs.UsedByTimechart(),
		ColumnsOrder:        columnsOrder,
	}

	if config.IsNewQueryPipelineEnabled() {
		response := query.GetPipeResp(qid)
		if response == nil {
			log.Errorf("qid=%d, processCompleteUpdate: failed to get new query pipeline response, err: %+v", qid, err)
			return
		}
		resp.TotalRRCCount = len(response.Hits.Hits)
	}

	searchErrors, err := query.GetUniqueSearchErrors(qid)
	if err != nil {
		log.Errorf("qid=%d, processCompleteUpdate: failed to get search Errors for qid! Error: %v", qid, err)
	} else if searchErrors == "" {
		wErr := conn.WriteJSON(resp)
		if wErr != nil {
			log.Errorf("qid=%d, processCompleteUpdate: failed to write complete response to websocket! err: %+v", qid, wErr)
		}
	} else {
		wErr := conn.WriteJSON(createErrorResponse(searchErrors))
		if wErr != nil {
			log.Errorf("qid=%d, processCompleteUpdate: failed to write error response to websocket! err: %+v", qid, wErr)
		}
	}
}

func processMaxScrollComplete(conn *websocket.Conn, qid uint64) {
	resp := &structs.PipeSearchCompleteResponse{
		CanScrollMore: false,
	}
	qType := query.GetQueryType(qid)
	resp.Qtype = qType.String()
	wErr := conn.WriteJSON(resp)
	if wErr != nil {
		log.Errorf("qid=%d, processMaxScrollComplete: failed to write complete response to websocket! err: %+v", qid, wErr)
	}
}

func UpdateWSResp(wsResponse *structs.PipeSearchWSUpdateResponse, qType structs.QueryType, qid uint64) error {
	getResp := query.GetPipeResp(qid)
	if getResp == nil {
		return fmt.Errorf("qid=%d, UpdateWSResp: failed to get new query pipeline response", qid)
	}
	switch qType {
	case structs.SegmentStatsCmd, structs.GroupByCmd:
		wsResponse.MeasureResults = getResp.MeasureResults
		wsResponse.MeasureFunctions = getResp.MeasureFunctions
		wsResponse.GroupByCols = getResp.GroupByCols
		wsResponse.BucketCount = getResp.BucketCount
		wsResponse.ColumnsOrder = getResp.ColumnsOrder
		wsResponse.Qtype = qType.String()
	case structs.RRCCmd:
		wsResponse.Hits = getResp.Hits
		wsResponse.AllPossibleColumns = getResp.AllPossibleColumns
		wsResponse.Qtype = qType.String()
		wsResponse.ColumnsOrder = getResp.ColumnsOrder
	default:
		return fmt.Errorf("qid=%d, UpdateWSResp: unknown query type: %v", qid, qType)
	}
	return nil
}

func createRecsWsResp(qid uint64, sizeLimit uint64, searchPercent float64, scrollFrom int,
	totalEventsSearched uint64, qUpdate *query.QueryUpdate, aggs *structs.QueryAggregators, totalPossibleEvents uint64) (*structs.PipeSearchWSUpdateResponse, error) {

	qType := query.GetQueryType(qid)
	wsResponse := &structs.PipeSearchWSUpdateResponse{
		Completion:               searchPercent,
		State:                    query.QUERY_UPDATE.String(),
		TotalEventsSearched:      humanize.Comma(int64(totalEventsSearched)),
		TotalPossibleEvents:      humanize.Comma(int64(totalPossibleEvents)),
		Qtype:                    qType.String(),
		SortByTimestampAtDefault: !aggs.HasSortBlockInChain(),
	}

	if config.IsNewQueryPipelineEnabled() {
		err := UpdateWSResp(wsResponse, qType, qid)
		if err != nil {
			return nil, fmt.Errorf("qid=%d, createRecsWsResp: failed to update ws response, err: %+v", qid, err)
		}
		return wsResponse, nil
	}

	switch qType {
	case structs.SegmentStatsCmd, structs.GroupByCmd:
		if aggs.Next == nil { // We'll do chained aggs after all segments are searched.
			var doPull bool
			if qUpdate.RemoteID != "" {
				doPull = true
			}
			aggMeasureRes, aggMeasureFunctions, aggGroupByCols, columnsOrder, bucketCount := query.GetMeasureResultsForQid(qid, doPull, qUpdate.SegKeyEnc, aggs.BucketLimit)
			wsResponse.MeasureResults = aggMeasureRes
			wsResponse.MeasureFunctions = aggMeasureFunctions
			wsResponse.GroupByCols = aggGroupByCols
			wsResponse.Qtype = qType.String()
			wsResponse.BucketCount = bucketCount
			wsResponse.ColumnsOrder = columnsOrder
		}
	case structs.RRCCmd:
		useAnySegKey := false
		if aggs.OutputTransforms != nil && (aggs.OutputTransforms.HeadRequest != nil && (aggs.OutputTransforms.HeadRequest.BoolExpr == nil && aggs.OutputTransforms.HeadRequest.MaxRows != 0)) {
			// For only getting MaxRows rows, don't show any rows until the
			// search has completed (so that we don't show a row and later in
			// the search find out another row has higher priority and the row
			// we displayed is no longer in the top MaxRows rows.)
			if searchPercent < 100 {
				break
			}

			sizeLimit = uint64(aggs.OutputTransforms.HeadRequest.MaxRows)

			useAnySegKey = true
		}

		inrrcs, qc, segencmap, allColsInAggs, err := query.GetRawRecordInfoForQid(scrollFrom, qid)
		if err != nil {
			log.Errorf("qid=%d, createRecsWsResp: failed to get rrcs, err: %v", qid, err)
			return nil, err
		}

		// filter out the rrcs that don't match the segkey
		var allJson []map[string]interface{}
		var allCols []string
		if qUpdate.QUpdate == query.QUERY_UPDATE_REMOTE {
			// handle remote
			allJson, allCols, err = query.GetRemoteRawLogInfo(qUpdate.RemoteID, inrrcs, qid)
			if err != nil {
				log.Errorf("qid=%d, createRecsWsResp: failed to get remote raw logs and columns, err: %+v", qid, err)
				return nil, err
			}
		} else {
			// handle local
			allJson, allCols, err = getRawLogsAndColumns(inrrcs, qUpdate.SegKeyEnc, useAnySegKey, sizeLimit, segencmap, aggs, qid, allColsInAggs)
			if err != nil {
				log.Errorf("qid=%d, createRecsWsResp: failed to get raw logs and columns, err: %+v", qid, err)
				return nil, err
			}
		}
		if err != nil {
			log.Errorf("qid=%d, createRecsWsResp: failed to convert rrcs to json, err: %+v", qid, err)
			return nil, err
		}

		wsResponse.Hits = structs.PipeSearchResponse{
			Hits:         allJson,
			TotalMatched: qc,
		}
		wsResponse.AllPossibleColumns = allCols
		wsResponse.Qtype = qType.String()

		wsResponse.ColumnsOrder = allCols
	}
	return wsResponse, nil
}

func getRawLogsAndColumns(inrrcs []*segutils.RecordResultContainer, skEnc uint16, anySegKey bool, sizeLimit uint64,
	segencmap map[uint16]string, aggs *structs.QueryAggregators, qid uint64, allColsInAggs map[string]struct{}) ([]map[string]interface{}, []string, error) {
	found := uint64(0)
	rrcs := make([]*segutils.RecordResultContainer, len(inrrcs))
	for i := 0; i < len(inrrcs); i++ {
		if !inrrcs[i].SegKeyInfo.IsRemote && (anySegKey || inrrcs[i].SegKeyInfo.SegKeyEnc == skEnc) {
			rrcs[found] = inrrcs[i]
			found++
		}
	}
	rrcs = rrcs[:found]
	allJson, allCols, err := convertRRCsToJSONResponse(rrcs, sizeLimit, qid, segencmap, aggs, allColsInAggs)
	if err != nil {
		log.Errorf("qid=%d, getRawLogsAndColumns: failed to convert rrcs to json, err: %+v", qid, err)
		return nil, nil, err
	}
	return allJson, allCols, nil
}

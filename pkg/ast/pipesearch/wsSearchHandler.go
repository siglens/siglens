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
	"fmt"

	"github.com/dustin/go-humanize"
	"github.com/fasthttp/websocket"
	rutils "github.com/siglens/siglens/pkg/readerUtils"
	"github.com/siglens/siglens/pkg/segment"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

func ProcessPipeSearchWebsocket(conn *websocket.Conn, orgid uint64) {
	qid := rutils.GetNextQid()
	event, err := readInitialEvent(qid, conn)
	if err != nil {
		log.Errorf("qid=%d, ProcessPipeSearchWebsocket: Failed to read initial event %+v!", qid, err)
		wErr := conn.WriteJSON(createErrorResponse(err.Error()))
		if wErr != nil {
			log.Errorf("qid=%d, ProcessPipeSearchWebsocket: failed to write error response to websocket! %+v", qid, wErr)
		}
		return
	}
	eventState, ok := event["state"]
	if !ok {
		log.Errorf("qid=%d, ProcessPipeSearchWebsocket: first request does not have 'state' as a key!", qid)
		wErr := conn.WriteJSON(createErrorResponse("request missing required key 'state'"))
		if wErr != nil {
			log.Errorf("qid=%d, ProcessPipeSearchWebsocket: failed to write error response to websocket! %+v", qid, wErr)
		}
		return
	}
	if eventState != "query" {
		log.Errorf("qid=%d, ProcessPipeSearchWebsocket: first request is not a query 'state'!", qid)
		wErr := conn.WriteJSON(createErrorResponse("first request should have 'state':'query'"))
		if wErr != nil {
			log.Errorf("qid=%d, ProcessPipeSearchWebsocket: failed to write error response to websocket! %+v", qid, wErr)
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

	if queryLanguageType == "SQL" {
		simpleNode, aggs, err = ParseRequest(searchText, startEpoch, endEpoch, qid, "SQL", indexNameIn)
	} else if queryLanguageType == "Pipe QL" {
		simpleNode, aggs, err = ParseRequest(searchText, startEpoch, endEpoch, qid, "Pipe QL", indexNameIn)
	} else if queryLanguageType == "Log QL" {
		simpleNode, aggs, err = ParseRequest(searchText, startEpoch, endEpoch, qid, "Log QL", indexNameIn)
	} else if queryLanguageType == "Splunk QL" {
		simpleNode, aggs, err = ParseRequest(searchText, startEpoch, endEpoch, qid, "Splunk QL", indexNameIn)
	} else {
		log.Infof("ProcessPipeSearchWebsocket: unknown queryLanguageType: %v; using Pipe QL instead", queryLanguageType)
		simpleNode, aggs, err = ParseRequest(searchText, startEpoch, endEpoch, qid, "Pipe QL", indexNameIn)
	}

	if err != nil {
		log.Errorf("qid=%d, ProcessPipeSearchWebsocket: failed to parse query err=%v", qid, err)
		wErr := conn.WriteJSON(createErrorResponse(err.Error()))
		if wErr != nil {
			log.Errorf("qid=%d, ProcessPipeSearchWebsocket: failed to write error response to websocket! %+v", qid, wErr)
		}
		return
	}

	if aggs != nil && (aggs.GroupByRequest != nil || aggs.MeasureOperations != nil) {
		sizeLimit = 0
	}

	// If MaxRows is used to limit the number of returned results, set `sizeLimit`
	// to it. Currently MaxRows is only valid as the root QueryAggregators.
	if aggs != nil && aggs.OutputTransforms != nil && aggs.OutputTransforms.MaxRows != 0 {
		sizeLimit = aggs.OutputTransforms.MaxRows
	}

	qc := structs.InitQueryContextWithTableInfo(ti, sizeLimit, scrollFrom, orgid, false)
	eventC, err := segment.ExecuteAsyncQuery(simpleNode, aggs, qid, qc)
	if err != nil {
		log.Errorf("qid=%d, ProcessPipeSearchWebsocket: failed to execute query err=%v", qid, err)
		wErr := conn.WriteJSON(createErrorResponse(err.Error()))
		if wErr != nil {
			log.Errorf("qid=%d, ProcessPipeSearchWebsocket: failed to write error response to websocket! %+v", qid, wErr)
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
				log.Errorf("qid=%v, Got unknown state %v", qid, qscd.StateName)
			}
			if !ok {
				log.Errorf("qid=%v, ProcessPipeSearchWebsocket: Got non ok, state: %v", qid, qscd.StateName)
				return
			}
		case readMsg := <-websocketR:
			log.Infof("qid=%d, Got message from websocket %+v", qid, readMsg)
			if readMsg["state"] == "cancel" {
				query.CancelQuery(qid)
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
		log.Errorf("qid=%d, readInitialEvent: Failed to read initial event from websocket!: %v", qid, err)
		return readEvent, err
	}

	log.Infof("qid=%d, Read initial event from websocket %+v", qid, readEvent)
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
		log.Errorf("qid=%d, processTimeoutUpdate: failed to write to websocket! %+v", qid, err)
	}
}

func processRunningUpdate(conn *websocket.Conn, qid uint64) {

	e := map[string]interface{}{
		"state": query.RUNNING.String(),
		"qid":   qid,
	}
	wErr := conn.WriteJSON(e)
	if wErr != nil {
		log.Errorf("qid=%d, processRunningUpdate: failed to write error response to websocket! %+v", qid, wErr)
	}
}

func processQueryUpdate(conn *websocket.Conn, qid uint64, sizeLimit uint64, scrollFrom int, qscd *query.QueryStateChanData,
	aggs *structs.QueryAggregators) {
	searchPercent := qscd.PercentComplete
	totalEventsSearched, err := query.GetTotalsRecsSearchedForQid(qid)
	if err != nil {
		log.Errorf("qid=%d, processQueryUpdate: failed to get total records searched: %+v", qid, err)
		wErr := conn.WriteJSON(createErrorResponse(err.Error()))
		if wErr != nil {
			log.Errorf("qid=%d, processQueryUpdate: failed to write error response to websocket! %+v", qid, wErr)
		}
		return
	}

	var wsResponse *PipeSearchWSUpdateResponse
	if qscd.QueryUpdate == nil {
		log.Errorf("qid=%d, processQueryUpdate: got nil query update!", qid)
		wErr := conn.WriteJSON(createErrorResponse(err.Error()))
		if wErr != nil {
			log.Errorf("qid=%d, processQueryUpdate: failed to write RRC response to websocket! %+v", qid, wErr)
		}
		return
	}

	wsResponse, err = createRecsWsResp(qid, sizeLimit, searchPercent, scrollFrom, totalEventsSearched, qscd.QueryUpdate, aggs)
	if err != nil {
		wErr := conn.WriteJSON(createErrorResponse(err.Error()))
		if wErr != nil {
			log.Errorf("qid=%d, processQueryUpdate: failed to write RRC response to websocket! %+v", qid, wErr)
		}
		return
	}

	wErr := conn.WriteJSON(wsResponse)
	if wErr != nil {
		log.Errorf("qid=%d, processQueryUpdate: failed to write update response to websocket! %+v", qid, wErr)
	}
}

func processCompleteUpdate(conn *websocket.Conn, sizeLimit, qid uint64, aggs *structs.QueryAggregators) {
	queryC := query.GetQueryCountInfoForQid(qid)
	totalEventsSearched, err := query.GetTotalsRecsSearchedForQid(qid)
	if err != nil {
		log.Errorf("qid=%d, processCompleteUpdate: failed to get total records searched: %+v", qid, err)
	}
	numRRCs, err := query.GetNumMatchedRRCs(qid)
	if err != nil {
		log.Errorf("qid=%d, processCompleteUpdate: failed to get number of RRCs for qid! Error: %v", qid, err)
	}

	aggMeasureRes, aggMeasureFunctions, aggGroupByCols, bucketCount := query.GetMeasureResultsForQid(qid, true, 0, aggs.BucketLimit) //aggs.BucketLimit

	var canScrollMore bool
	if numRRCs == sizeLimit {
		// if the number of RRCs is exactly equal to the requested size, there may be more than size matches. Hence, we can scroll more
		canScrollMore = true
	}
	queryType := query.GetQueryType(qid)
	resp := &PipeSearchCompleteResponse{
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
	}
	searchErrors, err := query.GetUniqueSearchErrors(qid)
	if err != nil {
		log.Errorf("qid=%d, processCompleteUpdate: failed to get search Errors for qid! Error: %v", qid, err)
	} else if searchErrors == "" {
		wErr := conn.WriteJSON(resp)
		if wErr != nil {
			log.Errorf("qid=%d, processCompleteUpdate: failed to write complete response to websocket! %+v", qid, wErr)
		}
	} else {
		wErr := conn.WriteJSON(createErrorResponse(searchErrors))
		if wErr != nil {
			log.Errorf("qid=%d, processCompleteUpdate: failed to write error response to websocket! %+v", qid, wErr)
		}
	}
}

func processMaxScrollComplete(conn *websocket.Conn, qid uint64) {
	resp := &PipeSearchCompleteResponse{
		CanScrollMore: false,
	}
	qType := query.GetQueryType(qid)
	resp.Qtype = qType.String()
	wErr := conn.WriteJSON(resp)
	if wErr != nil {
		log.Errorf("qid=%d, processMaxScrollComplete: failed to write complete response to websocket! %+v", qid, wErr)
	}
}

func createRecsWsResp(qid uint64, sizeLimit uint64, searchPercent float64, scrollFrom int,
	totalEventsSearched uint64, qUpdate *query.QueryUpdate, aggs *structs.QueryAggregators) (*PipeSearchWSUpdateResponse, error) {

	qType := query.GetQueryType(qid)
	wsResponse := &PipeSearchWSUpdateResponse{
		Completion:          searchPercent,
		State:               query.QUERY_UPDATE.String(),
		TotalEventsSearched: humanize.Comma(int64(totalEventsSearched)),
		Qtype:               qType.String(),
	}

	switch qType {
	case structs.SegmentStatsCmd, structs.GroupByCmd:
		if aggs.Next == nil { // We'll do chained aggs after all segments are searched.
			var doPull bool
			if qUpdate.RemoteID != "" {
				doPull = true
			}
			aggMeasureRes, aggMeasureFunctions, aggGroupByCols, bucketCount := query.GetMeasureResultsForQid(qid, doPull, qUpdate.SegKeyEnc, aggs.BucketLimit)
			wsResponse.MeasureResults = aggMeasureRes
			wsResponse.MeasureFunctions = aggMeasureFunctions
			wsResponse.GroupByCols = aggGroupByCols
			wsResponse.Qtype = qType.String()
			wsResponse.BucketCount = bucketCount
		}
	case structs.RRCCmd:
		useAnySegKey := false
		if aggs.OutputTransforms != nil && aggs.OutputTransforms.MaxRows != 0 {
			// For only getting MaxRows rows, don't show any rows until the
			// search has completed (so that we don't show a row and later in
			// the search find out another row has higher priority and the row
			// we displayed is no longer in the top MaxRows rows.)
			if searchPercent < 100 {
				break
			}

			useAnySegKey = true
		}

		inrrcs, qc, segencmap, err := query.GetRawRecordInfoForQid(scrollFrom, qid)
		if err != nil {
			log.Errorf("qid=%d, createRecsWsResp: failed to get rrcs %v", qid, err)
			return nil, err
		}

		// filter out the rrcs that don't match the segkey
		var allJson []map[string]interface{}
		var allCols []string
		if qUpdate.QUpdate == query.QUERY_UPDATE_REMOTE {
			// handle remote
			allJson, allCols, err = query.GetRemoteRawLogInfo(qUpdate.RemoteID, inrrcs, qid)
			if err != nil {
				log.Errorf("qid=%d, createRecsWsResp: failed to get remote raw logs and columns: %+v", qid, err)
				return nil, err
			}
		} else {
			// handle local
			allJson, allCols, err = getRawLogsAndColumns(inrrcs, qUpdate.SegKeyEnc, useAnySegKey, sizeLimit, segencmap, aggs, qid)
			if err != nil {
				log.Errorf("qid=%d, createRecsWsResp: failed to get raw logs and columns: %+v", qid, err)
				return nil, err
			}
		}
		if err != nil {
			log.Errorf("qid=%d, createRecsWsResp: failed to convert rrcs to json: %+v", qid, err)
			return nil, err
		}

		wsResponse.Hits = PipeSearchResponse{
			Hits:         allJson,
			TotalMatched: qc,
		}
		wsResponse.AllPossibleColumns = allCols
		wsResponse.Qtype = qType.String()
	}
	return wsResponse, nil
}

func getRawLogsAndColumns(inrrcs []*segutils.RecordResultContainer, skEnc uint16, anySegKey bool, sizeLimit uint64,
	segencmap map[uint16]string, aggs *structs.QueryAggregators, qid uint64) ([]map[string]interface{}, []string, error) {
	found := uint64(0)
	rrcs := make([]*segutils.RecordResultContainer, len(inrrcs))
	for i := 0; i < len(inrrcs); i++ {
		if !inrrcs[i].SegKeyInfo.IsRemote && (anySegKey || inrrcs[i].SegKeyInfo.SegKeyEnc == skEnc) {
			rrcs[found] = inrrcs[i]
			found++
		}
	}
	rrcs = rrcs[:found]
	allJson, allCols, err := convertRRCsToJSONResponse(rrcs, sizeLimit, qid, segencmap, aggs)
	if err != nil {
		log.Errorf("qid=%d, getRawLogsAndColumns: failed to convert rrcs to json: %+v", qid, err)
		return nil, nil, err
	}
	return allJson, allCols, nil
}

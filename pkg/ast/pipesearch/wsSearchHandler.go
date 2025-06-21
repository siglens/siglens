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

	"github.com/fasthttp/websocket"
	"github.com/siglens/siglens/pkg/common/dtypeutils"
	fileutils "github.com/siglens/siglens/pkg/common/fileutils"
	"github.com/siglens/siglens/pkg/config"
	rutils "github.com/siglens/siglens/pkg/readerUtils"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

const KEY_TRACE_RELATED_LOGS_INDEX = "trace-related-logs"

func ProcessPipeSearchWebsocket(conn *websocket.Conn, orgid int64, ctx *fasthttp.RequestCtx) {
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
	searchText, startEpoch, endEpoch, sizeLimit, indexNameIn, scrollFrom, includeNulls, runTimechart := ParseSearchBody(event, nowTs)
	limit := sizeLimit
	if scrollFrom > 10_000 {
		processMaxScrollComplete(conn, qid)
		return
	}

	ti := structs.InitTableInfo(indexNameIn, orgid, false, ctx)
	log.Infof("qid=%v, ProcessPipeSearchWebsocket: index=[%v] searchString=[%v] scrollFrom=[%v]",
		qid, ti.String(), searchText, scrollFrom)

	var timechartSimpleNode *structs.ASTNode
	var timechartAggs *structs.QueryAggregators

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
		}

		if runTimechart && shouldRunTimechartQuery(aggs) {
			searchText += " | timechart count"
			timechartSimpleNode, timechartAggs, _, err = ParseRequest(searchText, startEpoch, endEpoch, qid, "Splunk QL", indexNameIn)
			if err != nil {
				wErr := conn.WriteJSON(createErrorResponse(err.Error()))
				if wErr != nil {
					log.Errorf("qid=%d, ProcessPipeSearchWebsocket: failed to write error response to websocket! err: %+v", qid, wErr)
				}
			}
		}

		if err == nil {
			err = structs.CheckUnsupportedFunctions(aggs)
		}
		if err == nil {
			err = structs.CheckUnsupportedFunctions(timechartAggs)
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
		ti = structs.InitTableInfo(strings.Join(parsedIndexNames, ","), orgid, false, ctx)
	}

	if queryLanguageType == "SQL" && aggs != nil && aggs.TableName != "*" {
		indexNameIn = aggs.TableName
		ti = structs.InitTableInfo(indexNameIn, orgid, false, ctx) // Re-initialize ti with the updated indexNameIn
	}

	sizeLimit = GetFinalSizelimit(aggs, sizeLimit)

	qc := structs.InitQueryContextWithTableInfo(ti, sizeLimit, scrollFrom, orgid, false)
	qc.RawQuery = searchText
	qc.IncludeNulls = includeNulls

	RunAsyncQueryForNewPipeline(conn, qid, simpleNode, aggs, timechartSimpleNode, timechartAggs, qc, limit, scrollFrom)
}

func RunAsyncQueryForNewPipeline(conn *websocket.Conn, qid uint64, simpleNode *structs.ASTNode, aggs *structs.QueryAggregators,
	timechartSimpleNode *structs.ASTNode, timechartAggs *structs.QueryAggregators,
	qc *structs.QueryContext, sizeLimit uint64, scrollFrom int,
) {
	websocketR := make(chan map[string]interface{})

	go listenToConnection(qid, websocketR, conn)

	go func() {
		for {
			readMsg := <-websocketR

			if readMsg["state"] == "cancel" {
				log.Infof("qid=%d, RunAsyncQueryForNewPipeline: Got message from websocket: %+v", qid, readMsg)
				query.CancelQuery(qid)
			} else if readMsg["state"] == "exit" {
				return
			}
		}
	}()

	defer func() {
		websocketR <- map[string]interface{}{"state": "exit"}
	}()

	_, _, _, err := RunQueryForNewPipeline(conn, qid, simpleNode, aggs, timechartSimpleNode, timechartAggs, qc, sizeLimit)
	if err != nil {
		log.Errorf("qid=%d, RunAsyncQueryForNewPipeline: failed to execute query, err: %v", qid, err)
		wErr := conn.WriteJSON(createErrorResponse(err.Error()))
		if wErr != nil {
			log.Errorf("qid=%d, RunAsyncQueryForNewPipeline: failed to write error response to websocket! err: %+v", qid, wErr)
		}
		return
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
		"timeoutSeconds": fmt.Sprintf("%v", config.GetQueryTimeoutSecs()),
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

func processQueryStateUpdate(conn *websocket.Conn, qid uint64, queryState query.QueryState) {
	e := map[string]interface{}{
		"state": queryState.String(),
		"qid":   qid,
	}
	wErr := conn.WriteJSON(e)
	if wErr != nil {
		log.Errorf("qid=%d, processQueryStateUpdate: failed to write error response to websocket! err: %+v", qid, wErr)
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

func shouldRunTimechartQuery(aggs *structs.QueryAggregators) bool {
	if aggs.HasTimechartInChain() || aggs.HasStatsBlockInChain() || aggs.HasStreamStatsInChain() {
		return false
	}

	return true
}

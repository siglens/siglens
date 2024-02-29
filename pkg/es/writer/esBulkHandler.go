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

package writer

import (
	"bufio"
	"bytes"
	"errors"
	"strings"
	"time"

	jp "github.com/buger/jsonparser"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/siglens/siglens/pkg/config"
	segment "github.com/siglens/siglens/pkg/segment/utils"

	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/usageStats"
	"github.com/siglens/siglens/pkg/utils"

	// segstructs "github.com/siglens/siglens/pkg/segment/structs"

	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

const (
	INDEX = iota
	CREATE
	UPDATE
	DELETE
)

const INDEX_TOP_STR string = "index"
const CREATE_TOP_STR string = "create"
const UPDATE_TOP_STR string = "update"
const INDEX_UNDER_STR string = "_index"

type kibanaIngHandlerFnDef func(
	ctx *fasthttp.RequestCtx, request map[string]interface{},
	indexNameConverted string, updateArg bool, idVal string, tsNow uint64, myid uint64) error

func ProcessBulkRequest(ctx *fasthttp.RequestCtx, myid uint64, kibanaIngHandlerFn kibanaIngHandlerFnDef) {

	processedCount, response, err := HandleBulkBody(ctx.PostBody(), ctx, myid, kibanaIngHandlerFn)
	if err != nil {
		PostBulkErrorResponse(ctx)
		return
	}

	//request body empty
	if processedCount == 0 {
		PostBulkErrorResponse(ctx)
	} else {
		utils.WriteJsonResponse(ctx, response)
	}
}

func HandleBulkBody(postBody []byte, ctx *fasthttp.RequestCtx, myid uint64, kibanaIngHandlerFn kibanaIngHandlerFnDef) (int, map[string]interface{}, error) {

	r := bytes.NewReader(postBody)

	response := make(map[string]interface{})
	//to have a check if there are any errors in the request
	var overallError bool
	//to check for status : 200 or 400
	var success bool
	//to check if json is greater than MAX_RECORD_SIZE
	var maxRecordSizeExceeded bool
	startTime := time.Now().UnixNano()
	var inCount int = 0
	var processedCount int = 0
	tsNow := utils.GetCurrentTimeInMs()
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanLines)

	var bytesReceived int
	// store all request index
	var items = make([]interface{}, 0)
	atleastOneSuccess := false
	localIndexMap := make(map[string]string)
	for scanner.Scan() {
		inCount++
		esAction, indexName, idVal := extractIndexAndValidateAction(scanner.Bytes())
		switch esAction {

		case INDEX, CREATE:
			scanner.Scan()
			rawJson := scanner.Bytes()
			numBytes := len(rawJson)
			bytesReceived += numBytes
			//update only if body is less than MAX_RECORD_SIZE
			if numBytes < segment.MAX_RECORD_SIZE {
				processedCount++
				success = true
				if strings.Contains(indexName, ".kibana") {
					indexNameConverted := AddAndGetRealIndexName(indexName, localIndexMap, myid)
					if idVal == "" {
						idVal = uuid.New().String()
					}
					request := make(map[string]interface{})
					var json = jsoniter.ConfigCompatibleWithStandardLibrary
					decoder := json.NewDecoder(bytes.NewReader(rawJson))
					decoder.UseNumber()
					err := decoder.Decode(&request)
					if err != nil {
						success = false
					}
					err = kibanaIngHandlerFn(ctx, request, indexNameConverted, false, idVal, tsNow, myid)
					if err != nil {
						success = false
					}
				} else {
					err := ProcessIndexRequest(rawJson, tsNow, indexName, uint64(numBytes), false, localIndexMap, myid)
					if err != nil {
						success = false
					}
				}
			} else {
				success = false
				maxRecordSizeExceeded = true
			}

		case UPDATE:
			success = false
			scanner.Scan()
		default:
			success = false
		}

		responsebody := make(map[string]interface{})
		if !success {
			if maxRecordSizeExceeded {
				error_response := utils.BulkErrorResponse{
					ErrorResponse: *utils.NewBulkErrorResponseInfo("request entity too large", "request_entity_exception"),
				}
				responsebody["index"] = error_response
				responsebody["status"] = 413
				items = append(items, responsebody)
			} else {
				overallError = true
				error_response := utils.BulkErrorResponse{
					ErrorResponse: *utils.NewBulkErrorResponseInfo("indexing request failed", "mapper_parse_exception"),
				}
				responsebody["index"] = error_response
				responsebody["status"] = 400
				items = append(items, responsebody)
			}
		} else {
			atleastOneSuccess = true
			statusbody := make(map[string]interface{})
			statusbody["status"] = 201
			responsebody["index"] = statusbody
			items = append(items, responsebody)
		}
	}
	usageStats.UpdateStats(uint64(bytesReceived), uint64(inCount), myid)
	timeTook := time.Now().UnixNano() - (startTime)
	response["took"] = timeTook / 1000
	response["error"] = overallError
	response["items"] = items

	if atleastOneSuccess {
		return processedCount, response, nil
	} else {
		return processedCount, response, errors.New("all bulk requests failed")
	}
}

func extractIndexAndValidateAction(rawJson []byte) (int, string, string) {
	val, dType, _, err := jp.Get(rawJson, INDEX_TOP_STR)
	if err == nil && dType == jp.Object {
		idVal, err := jp.GetString(val, "_id")
		if err != nil {
			idVal = ""
		}

		idxVal, err := jp.GetString(val, INDEX_UNDER_STR)
		if err != nil {
			idxVal = ""
		}
		return INDEX, idxVal, idVal
	}

	val, dType, _, err = jp.Get(rawJson, CREATE_TOP_STR)
	if err == nil && dType == jp.Object {
		idVal, err := jp.GetString(val, "_id")
		if err != nil {
			idVal = ""
		}

		idxVal, err := jp.GetString(val, INDEX_UNDER_STR)
		if err != nil {
			idxVal = ""
		}
		return CREATE, idxVal, idVal
	}
	val, dType, _, err = jp.Get(rawJson, UPDATE_TOP_STR)
	if err == nil && dType == jp.Object {
		idVal, err := jp.GetString(val, "_id")
		if err != nil {
			idVal = ""
		}

		idxVal, err := jp.GetString(val, INDEX_UNDER_STR)
		if err != nil {
			idxVal = ""
		}
		return UPDATE, idxVal, idVal
	}
	return DELETE, "eventType", ""
}

func AddAndGetRealIndexName(indexNameIn string, localIndexMap map[string]string, myid uint64) string {

	// first check localCopy of map, if it exists then avoid the lock inside vtables.
	// note that this map gets reset on every bulk request
	lVal, ok := localIndexMap[indexNameIn]
	if ok {
		return lVal
	}

	var indexNameConverted string
	if pres, idxName := vtable.IsAlias(indexNameIn, myid); pres {
		indexNameConverted = idxName
	} else {
		indexNameConverted = indexNameIn
	}

	localIndexMap[indexNameIn] = indexNameConverted

	err := vtable.AddVirtualTable(&indexNameConverted, myid)
	if err != nil {
		log.Errorf("AddAndGetRealIndexName: failed to add virtual table, err=%v", err)
	}
	return indexNameConverted
}

func ProcessIndexRequest(rawJson []byte, tsNow uint64, indexNameIn string,
	bytesReceived uint64, flush bool, localIndexMap map[string]string, myid uint64) error {

	indexNameConverted := AddAndGetRealIndexName(indexNameIn, localIndexMap, myid)
	cfgkey := config.GetTimeStampKey()

	var docType segment.SIGNAL_TYPE
	if strings.HasPrefix(indexNameConverted, "jaeger-") {
		docType = segment.SIGNAL_JAEGER_TRACES
		cfgkey = "startTimeMillis"
	} else {
		docType = segment.SIGNAL_EVENTS
	}

	ts_millis := utils.ExtractTimeStamp(rawJson, &cfgkey)
	if ts_millis == 0 {
		ts_millis = tsNow
	}
	streamid := utils.CreateStreamId(indexNameConverted, myid)

	// TODO: we used to add _index in the json_source doc, since it is needed during
	// json-rsponse formation during query-resp. We should either add it in this AddEntryToInMemBuf
	// OR in json-resp creation we add it in the resp using the vtable name

	err := writer.AddEntryToInMemBuf(streamid, rawJson, ts_millis, indexNameConverted, bytesReceived, flush,
		docType, myid)
	if err != nil {
		log.Errorf("ProcessIndexRequest: failed to add entry to in mem buffer, err=%v", err)
		return err
	}
	return nil
}

func ProcessPutIndex(ctx *fasthttp.RequestCtx, myid uint64) {

	r := string(ctx.PostBody())
	indexName := ctx.UserValue("indexName").(string)

	log.Infof("ProcessPutIndex: adding index and mapping: indexName=%v", indexName)

	err := vtable.AddVirtualTableAndMapping(&indexName, &r, myid)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		_, err = ctx.Write([]byte("Failed to put index/mapping"))
		if err != nil {
			log.Errorf("ProcessPutIndex: failed to write response, err=%v", err)
		}
		ctx.SetContentType(utils.ContentJson)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
}

func PostBulkErrorResponse(ctx *fasthttp.RequestCtx) {

	ctx.SetStatusCode(fasthttp.StatusBadRequest)
	responsebody := make(map[string]interface{})
	error_response := utils.BulkErrorResponse{
		ErrorResponse: *utils.NewBulkErrorResponseInfo("request body is required", "parse_exception"),
	}
	responsebody["index"] = error_response
	responsebody["status"] = 400
	utils.WriteJsonResponse(ctx, responsebody)
}

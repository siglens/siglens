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

package writer

import (
	"bufio"
	"bytes"
	"errors"
	"strings"
	"sync"
	"time"
	"unsafe"

	jp "github.com/buger/jsonparser"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/grpc"
	"github.com/siglens/siglens/pkg/hooks"
	segment "github.com/siglens/siglens/pkg/segment/utils"

	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/usageStats"
	"github.com/siglens/siglens/pkg/utils"

	"github.com/siglens/siglens/pkg/segment/query/metadata"
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

const MAX_INDEX_NAME_LEN = 256
const RESP_ITEMS_INITIAL_LEN = 4000

var resp_status_201 map[string]interface{}

var respItemsPool = sync.Pool{
	New: func() interface{} {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
		slice := make([]interface{}, RESP_ITEMS_INITIAL_LEN)
		return &slice
	},
}

func init() {
	resp_status_201 = make(map[string]interface{})
	statusbody := make(map[string]interface{})
	statusbody["status"] = 201
	resp_status_201["index"] = statusbody
}

func ProcessBulkRequest(ctx *fasthttp.RequestCtx, myid uint64, useIngestHook bool) {
	if hook := hooks.GlobalHooks.OverrideIngestRequestHook; hook != nil {
		alreadyHandled := hook(ctx, myid, grpc.INGEST_FUNC_ES_BULK, useIngestHook)
		if alreadyHandled {
			return
		}
	}

	var rid uint64
	if hook := hooks.GlobalHooks.BeforeHandlingBulkRequest; hook != nil {
		var alreadyHandled bool
		alreadyHandled, rid = hook(ctx, myid)
		if alreadyHandled {
			return
		}
	}

	processedCount, response, err := HandleBulkBody(ctx.PostBody(), ctx, rid, myid, useIngestHook)
	if err != nil {
		PostBulkErrorResponse(ctx)
		return
	}

	if hook := hooks.GlobalHooks.AfterHandlingBulkRequest; hook != nil {
		alreadyHandled := hook(ctx, rid)
		if alreadyHandled {
			return
		}
	}

	//request body empty
	if processedCount == 0 {
		PostBulkErrorResponse(ctx)
	} else {
		utils.WriteJsonResponse(ctx, response)
	}
}

func HandleBulkBody(postBody []byte, ctx *fasthttp.RequestCtx, rid uint64, myid uint64, useIngestHook bool) (int, map[string]interface{}, error) {

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

	items := *respItemsPool.Get().(*[]interface{})
	// if we end up extending items, then save the orig pointer, so that we can put it back
	origItems := items
	defer respItemsPool.Put(&origItems)

	// kunal todo check , if items gets extended and we put back origitems then does the os
	// delete the old items array ?
	itemsLen := len(items)

	atleastOneSuccess := false
	localIndexMap := make(map[string]string)

	idxToStreamIdCache := make(map[string]string)
	cnameCacheByteHashToStr := make(map[uint64]string)
	// stack-allocated array for allocation-free unescaping of small strings
	var jsParsingStackbuf [utils.UnescapeStackBufSize]byte

	// we will accept indexnames only upto 256 bytes
	idxNameParsingBuf := make([]byte, MAX_INDEX_NAME_LEN)

	for scanner.Scan() {
		inCount++
		if inCount >= itemsLen {
			newArr := make([]interface{}, 100)
			items = append(items, newArr...)
			itemsLen += 1000
		}

		esAction, indexName, idVal := extractIndexAndValidateAction(scanner.Bytes(),
			idxNameParsingBuf)

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
					if useIngestHook {
						if hook := hooks.GlobalHooks.EsBulkIngestInternalHook; hook != nil {
							err = hook(ctx, request, indexNameConverted, false, idVal, tsNow, myid)
							if err != nil {
								log.Errorf("HandleBulkBody: failed to call EsBulkIngestInternalHook, err=%v", err)
								success = false
							}
						}
					}
				} else {
					err := ProcessIndexRequest(rawJson, tsNow, indexName, uint64(numBytes),
						false, localIndexMap, myid, rid, idxToStreamIdCache,
						cnameCacheByteHashToStr, jsParsingStackbuf[:])
					if err != nil {
						log.Errorf("HandleBulkBody: failed to process index request, indexName=%v, err=%v", indexName, err)
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

		if !success {
			responsebody := make(map[string]interface{})
			if maxRecordSizeExceeded {
				error_response := utils.BulkErrorResponse{
					ErrorResponse: *utils.NewBulkErrorResponseInfo("request entity too large", "request_entity_exception"),
				}
				responsebody["index"] = error_response
				responsebody["status"] = 413
				items[inCount-1] = responsebody
			} else {
				overallError = true
				error_response := utils.BulkErrorResponse{
					ErrorResponse: *utils.NewBulkErrorResponseInfo("indexing request failed", "mapper_parse_exception"),
				}
				responsebody["index"] = error_response
				responsebody["status"] = 400
				items[inCount-1] = responsebody
			}
		} else {
			atleastOneSuccess = true
			items[inCount-1] = resp_status_201
		}
	}
	usageStats.UpdateStats(uint64(bytesReceived), uint64(inCount), myid)
	timeTook := time.Now().UnixNano() - (startTime)
	response["took"] = timeTook / 1000
	response["errors"] = overallError
	response["items"] = items[0:inCount]

	if atleastOneSuccess {
		return processedCount, response, nil
	} else {
		return processedCount, response, errors.New("all bulk requests failed")
	}
}

func extractIndexAndValidateAction(rawJson []byte,
	idxNameParsingBuf []byte) (int, string, string) {

	val, dType, _, err := jp.Get(rawJson, INDEX_TOP_STR)
	if err == nil && dType == jp.Object {
		idVal, err := jp.GetString(val, "_id")
		if err != nil {
			idVal = ""
		}

		idxVal, idxDType, _, err := jp.Get(val, INDEX_UNDER_STR)
		if err != nil || idxDType != jp.String {
			idxVal = []byte("")
		}
		copy(idxNameParsingBuf[:], idxVal[:])
		idxNameParsingBuf = idxNameParsingBuf[0:len(idxVal)]

		return INDEX, *(*string)(unsafe.Pointer(&idxNameParsingBuf)), idVal
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
		log.Errorf("AddAndGetRealIndexName: failed to add virtual table=%v, err=%v", indexNameConverted, err)
	}
	return indexNameConverted
}

func ProcessIndexRequest(rawJson []byte, tsNow uint64, indexNameIn string,
	bytesReceived uint64, flush bool, localIndexMap map[string]string, myid uint64,
	rid uint64, idxToStreamIdCache map[string]string,
	cnameCacheByteHashToStr map[uint64]string, jsParsingStackbuf []byte) error {

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

	var streamid string
	var ok bool
	streamid, ok = idxToStreamIdCache[indexNameConverted]
	if !ok {
		streamid = utils.CreateStreamId(indexNameConverted, myid)
		idxToStreamIdCache[indexNameConverted] = streamid
	}

	// TODO: we used to add _index in the json_source doc, since it is needed during
	// json-rsponse formation during query-resp. We should either add it in this AddEntryToInMemBuf
	// OR in json-resp creation we add it in the resp using the vtable name

	err := writer.AddEntryToInMemBuf(streamid, rawJson, ts_millis, indexNameConverted, bytesReceived, flush,
		docType, myid, rid, cnameCacheByteHashToStr, jsParsingStackbuf)
	if err != nil {
		log.Errorf("ProcessIndexRequest: failed to add entry to in mem buffer, StreamId=%v, rawJson=%v, err=%v", streamid, rawJson, err)
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
			log.Errorf("ProcessPutIndex: failed to write byte response, err=%v", err)
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

// Accepts wildcard index names e.g. "ind-*"
func ProcessDeleteIndex(ctx *fasthttp.RequestCtx, myid uint64) {
	inIndexName := utils.ExtractParamAsString(ctx.UserValue("indexName"))
	responseBody := make(map[string]interface{})
	if inIndexName == "traces" {
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		responseBody["error"] = *utils.NewDeleteIndexErrorResponseInfo(inIndexName)
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}
	convertedIndexNames, indicesNotFound := deleteIndex(inIndexName, myid)
	if indicesNotFound == len(convertedIndexNames) {
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		responseBody["error"] = *utils.NewDeleteIndexErrorResponseInfo(inIndexName)
		utils.WriteJsonResponse(ctx, responseBody)
		return
	} else {
		ctx.SetStatusCode(fasthttp.StatusOK)
		responseBody["message"] = "Index deleted successfully"
		utils.WriteJsonResponse(ctx, responseBody)
	}
}

func deleteIndex(inIndexName string, myid uint64) ([]string, int) {
	convertedIndexNames := vtable.ExpandAndReturnIndexNames(inIndexName, myid, true)
	indicesNotFound := 0
	for _, indexName := range convertedIndexNames {

		indexPresent := vtable.IsVirtualTablePresent(&indexName, myid)
		if !indexPresent {
			indicesNotFound++
			continue
		}

		ok, _ := vtable.IsAlias(indexName, myid)
		if ok {
			aliases, _ := vtable.GetAliasesAsArray(indexName, myid)
			err := vtable.RemoveAliases(indexName, aliases, myid)
			if err != nil {
				log.Errorf("deleteIndex : No Aliases removed for indexName = %v, alias: %v, Error=%v", indexName, aliases, err)
			}
		}
		err := vtable.DeleteVirtualTable(&indexName, myid)
		if err != nil {
			log.Errorf("deleteIndex : Failed to delete virtual table for indexName = %v err: %v", indexName, err)
		}

		currSegmeta := writer.GetLocalSegmetaFName()
		writer.DeleteSegmentsForIndex(currSegmeta, indexName)
		writer.DeleteVirtualTableSegStore(indexName)
		metadata.DeleteVirtualTable(indexName, myid)
	}
	return convertedIndexNames, indicesNotFound
}

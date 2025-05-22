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

package sampledataset

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/siglens/siglens/pkg/config"
	writer "github.com/siglens/siglens/pkg/es/writer"
	"github.com/siglens/siglens/pkg/grpc"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/otlp"
	segwriter "github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/usageStats"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/bytebufferpool"
	"github.com/valyala/fasthttp"
)

func generateESBody(recs int, actionLine string, rdr Generator,
	bb *bytebufferpool.ByteBuffer) ([]byte, error) {

	for i := 0; i < recs; i++ {
		_, _ = bb.WriteString(actionLine)
		logline, err := rdr.GetLogLine()
		if err != nil {
			return nil, err
		}
		_, _ = bb.Write(logline)
		_, _ = bb.WriteString("\n")
	}
	payLoad := bb.Bytes()
	return payLoad, nil
}

func populateActionLines(idxPrefix string, indexName string, numIndices int) []string {
	if numIndices == 0 {
		log.Fatalf("number of indices cannot be zero!")
	}
	actionLines := make([]string, numIndices)
	for i := 0; i < numIndices; i++ {
		var idx string
		if indexName != "" {
			idx = indexName
		} else {
			idx = fmt.Sprintf("%s-%d", idxPrefix, i)
		}
		actionLine := "{\"index\": {\"_index\": \"" + idx + "\", \"_type\": \"_doc\"}}\n"
		actionLines[i] = actionLine
	}
	return actionLines
}

func processSingleTrace(orgId int64, wg *sync.WaitGroup, generateSymmetricTree bool) {
	defer wg.Done()

	now := utils.GetCurrentTimeInMs()
	indexName := "traces"
	shouldFlush := false
	localIndexMap := make(map[string]string)
	tsKey := config.GetTimeStampKey()
	var jsParsingStackbuf [utils.UnescapeStackBufSize]byte

	idxToStreamIdCache := make(map[string]string)
	cnameCacheByteHashToStr := make(map[uint64]string)

	f := InitTraceFaker(0)
	depth := 7
	width := 2
	startTime := time.Now().UnixNano()
	totalDurationOfTrace := int64(time.Millisecond * 100)
	endTime := startTime + totalDurationOfTrace
	pleArray := make([]*segwriter.ParsedLogEvent, 0)
	traceId := strings.ReplaceAll(f.Faker.HexUint32(), "0x", "")
	log.Errorf("ProcessSyntheticTraceRequest: %v", traceId)
	serviceName := f.Faker.Word()
	spanArray := make([]*map[string]any, 0)
	levelParent := make(map[int][]string)

	if depth > 0 {
		parentSpanId := strings.ReplaceAll(f.Faker.HexUint32(), "0x", "")
		g, _ := generateSpan(traceId, parentSpanId, "", serviceName, f, "", startTime, endTime)
		spanArray = append(spanArray, g)
		levelParent[0] = []string{parentSpanId}
	}
	for i := 1; i < depth; i++ {
		for k := range levelParent[i-1] {
			for j := 0; j < width; j++ {
				if f.Faker.Rand.Float32() >= 0.9 && !generateSymmetricTree {
					continue
				} else {
					currSpanId := strings.ReplaceAll(f.Faker.HexUint32(), "0x", "")
					g, _ := generateSpan(traceId, currSpanId, levelParent[i-1][k], serviceName, f, (*spanArray[i-1])["name"].(string), startTime+int64(time.Millisecond*5), endTime-int64(time.Millisecond*10))
					spanArray = append(spanArray, g)
					if _, ok := levelParent[i]; !ok {
						levelParent[i] = []string{currSpanId}
					} else {
						levelParent[i] = append(levelParent[i], currSpanId)
					}
				}
			}
		}
	}

	for _, spanPtr := range spanArray {
		if spanPtr != nil {
			jsonData, err := json.Marshal(spanPtr)
			if err != nil {
				log.Errorf("ProcessSyntheticTraceRequest: failed to marshal span %s: %v. Service name: %s", spanPtr, err, jsonData)
			}

			ple, err := segwriter.GetNewPLE(jsonData, now, indexName, &tsKey, jsParsingStackbuf[:])
			if err != nil {
				log.Errorf("ProcessSyntheticTraceRequest: failed to get new PLE, jsonData: %v, err: %v", jsonData, err)
			}
			pleArray = append(pleArray, ple)
		}
	}

	err := writer.ProcessIndexRequestPle(now, indexName, shouldFlush, localIndexMap, orgId, 0, idxToStreamIdCache, cnameCacheByteHashToStr, jsParsingStackbuf[:], pleArray)
	if err != nil {
		log.Errorf("ProcessSyntheticTraceRequest: Failed to ingest traces, err: %v", err)
	}
	log.Errorf("ProcessSyntheticTraceRequest: Transaction Complete")
}

func ProcessSyntheticTraceRequest(ctx *fasthttp.RequestCtx, orgId int64) {
	if hook := hooks.GlobalHooks.OverrideIngestRequestHook; hook != nil {
		alreadyHandled := hook(ctx, orgId, grpc.INGEST_FUNC_OTLP_TRACES, false)
		if alreadyHandled {
			return
		}
	}

	var wg sync.WaitGroup
	numTracesToGenerate := 5000

	for n := 0; n < numTracesToGenerate; n++ {
		wg.Add(1)
		go processSingleTrace(orgId, &wg, false)
	}

	wg.Wait()
	log.Printf("ProcessSyntheticTraceRequest: All %d trace generations completed.", numTracesToGenerate)
	otlp.HandleTraceIngestionResponse(ctx, 5000, 0)
}

func generateSpan(traceId string, spanId string, parentId string, service string, f *TraceFaker, parentName string, parentStartTime int64, parentEndTime int64) (*map[string]any, error) {
	span := make(map[string]any)
	span["trace_id"] = traceId
	span["span_id"] = spanId
	if parentId != "" {
		span["parent_span_id"] = parentId
	} else {
		span["parent_span_id"] = hex.EncodeToString([]byte(""))
	}
	span["service"] = service
	span["trace_state"] = generateTraceState(2, f)
	span["name"] = fmt.Sprintf("%s/%s", parentName, f.Faker.Word())
	span["kind"] = "SPAN_KIND_SERVER"
	span["start_time"] = parentStartTime
	span["end_time"] = parentEndTime
	span["duration"] = span["end_time"].(int64) - span["start_time"].(int64)
	span["dropped_attributes_count"] = 0
	span["dropped_events_count"] = 0
	span["dropped_links_count"] = 0
	span["status"] = "STATUS_CODE_OK"
	// TODO ADD COLUMN FOR EACH ATTRIBUTE
	return &span, nil
}

func generateTraceState(len int8, f *TraceFaker) string {
	if len <= 0 {
		return ""
	}
	var parts []string
	for i := int8(0); i < len; i++ {
		parts = append(parts, fmt.Sprintf("%s=%s", f.Faker.LoremIpsumWord(), f.Faker.LoremIpsumWord()))
	}
	return strings.Join(parts, ",")
}

func ProcessSyntheicDataRequest(ctx *fasthttp.RequestCtx, orgId int64) {
	if hook := hooks.GlobalHooks.OverrideIngestRequestHook; hook != nil {
		alreadyHandled := hook(ctx, orgId, grpc.INGEST_FUNC_FAKE_DATA, false)
		if alreadyHandled {
			return
		}
	}

	actLines := populateActionLines("", "test-data", 1)

	tsNow := utils.GetCurrentTimeInMs()
	// TODO: init based on dataset selected later
	rdr := InitDynamicUserGenerator(true, 2)
	err := rdr.Init()
	if err != nil {
		log.Errorf("ProcessSyntheicDataRequest: Error in rdr Init: %v", err)
		return
	}
	bb := bytebufferpool.Get()
	payload, err := generateESBody(20000, actLines[0], rdr, bb)
	if err != nil {
		log.Errorf("Error generating bulk body!: %v", err)
		bytebufferpool.Put(bb)
		return
	}

	r := bytes.NewReader(payload)
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanLines)
	localIndexMap := make(map[string]string)
	tsKey := config.GetTimeStampKey()

	idxToStreamIdCache := make(map[string]string)

	cnameCacheByteHashToStr := make(map[uint64]string)
	var jsParsingStackbuf [utils.UnescapeStackBufSize]byte

	pleArray := make([]*segwriter.ParsedLogEvent, 0)
	defer segwriter.ReleasePLEs(pleArray)

	responsebody := make(map[string]interface{})
	totalBytes := 0
	for scanner.Scan() {
		scanner.Scan()
		rawJson := scanner.Bytes()
		jsonCopy := make([]byte, len(rawJson))
		copy(jsonCopy, rawJson)
		totalBytes += len(rawJson)

		ple, err := segwriter.GetNewPLE(jsonCopy, tsNow, "test-data", &tsKey, jsParsingStackbuf[:])
		if err != nil {
			utils.SendError(ctx, "Failed to ingest data", "", err)
			return
		}
		pleArray = append(pleArray, ple)
	}
	if err := scanner.Err(); err != nil {
		log.Errorf("ProcessSyntheicDataRequest: Error scanning payload %v, err: %v", payload, err)
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		responsebody["message"] = "Failed to ingest all of the data"
		utils.WriteJsonResponse(ctx, responsebody)
		return
	}

	err = writer.ProcessIndexRequestPle(tsNow, "test-data", false, localIndexMap,
		orgId, 0, idxToStreamIdCache, cnameCacheByteHashToStr,
		jsParsingStackbuf[:], pleArray)
	if err != nil {
		log.Errorf("ProcessSyntheicDataRequest: failed to process request, err: %v", err)
		utils.SendError(ctx, "Failed to process request", "", err)
		return
	}

	usageStats.UpdateStats(uint64(totalBytes), uint64(len(pleArray)), orgId)

	ctx.SetStatusCode(fasthttp.StatusOK)
	responsebody["message"] = "Successfully ingested 20k lines of logs!"
	utils.WriteJsonResponse(ctx, responsebody)
}

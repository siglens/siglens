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
	"fmt"

	"github.com/siglens/siglens/pkg/config"
	writer "github.com/siglens/siglens/pkg/es/writer"
	"github.com/siglens/siglens/pkg/grpc"
	"github.com/siglens/siglens/pkg/hooks"
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

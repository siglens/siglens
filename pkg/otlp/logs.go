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

package otlp

import (
	"github.com/siglens/siglens/pkg/grpc"
	"github.com/siglens/siglens/pkg/hooks"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

func ProcessLogIngest(ctx *fasthttp.RequestCtx, myid int64) {
	if hook := hooks.GlobalHooks.OverrideIngestRequestHook; hook != nil {
		alreadyHandled := hook(ctx, myid, grpc.INGEST_FUNC_OTLP_LOGS, false)
		if alreadyHandled {
			return
		}
	}

	_, err := getDataToUnmarshal(ctx)
	if err != nil {
		log.Errorf("ProcessTraceIngest: failed to get data to unmarshal: %v", err)
		setFailureResponse(ctx, fasthttp.StatusBadRequest, err.Error())
		return
	}

	// Unmarshal the data.
	// request, err := unmarshalLogRequest(data)
	// if err != nil {
	// 	log.Errorf("ProcessTraceIngest: failed to unpack Data: %s with err %v", string(data), err)
	// 	setFailureResponse(ctx, fasthttp.StatusBadRequest, "Unable to unmarshal traces")
	// 	return
	// }

	// err = writer.ProcessIndexRequestPle(now, indexName, shouldFlush, localIndexMap, myid, 0, idxToStreamIdCache, cnameCacheByteHashToStr, jsParsingStackbuf[:], pleArray)
	// if err != nil {
	// 	log.Errorf("ProcessTraceIngest: Failed to ingest traces, err: %v", err)
	// 	numFailedSpans += len(pleArray)
	// }

	// log.Debugf("ProcessTraceIngest: %v spans in the request and failed to ingest %v of them", numSpans, numFailedSpans)
	// usageStats.UpdateStats(uint64(len(data)), uint64(numSpans), myid)

	// // Send the appropriate response.
	// handleTraceIngestionResponse(ctx, numSpans, numFailedSpans)
}

// func unmarshalLogRequest(data []byte) (*coltracepb.ExportTraceServiceRequest, error) {
// 	// var trace coltracepb.ExportTraceServiceRequest
// 	// err := proto.Unmarshal(data, &trace)
// 	// if err != nil {
// 	// 	log.Errorf("unmarshalTraceRequest: failed to unmarshal trace request. err: %v data: %v", err, string(data))
// 	// 	return nil, err
// 	// }
// 	// return &trace, nil
// 	panic("unmarshalLogRequest not implemented")
// }

// func handleLogIngestionResponse(ctx *fasthttp.RequestCtx, numSpans int, numFailedSpans int) {
// 	// if numFailedSpans == 0 {
// 	// 	// This request was successful.
// 	// 	response, err := proto.Marshal(&coltracepb.ExportTraceServiceResponse{})
// 	// 	if err != nil {
// 	// 		log.Errorf("handleTraceIngestionResponse: failed to marshal successful response. err: %v. NumSpans: %d", err, numSpans)
// 	// 		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
// 	// 		return
// 	// 	}
// 	// 	_, err = ctx.Write(response)
// 	// 	if err != nil {
// 	// 		log.Errorf("handleTraceIngestionResponse: failed to write successful response. err: %v. NumSpans: %d", err, numSpans)
// 	// 		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
// 	// 		return
// 	// 	}

// 	// 	ctx.SetStatusCode(fasthttp.StatusOK)
// 	// 	return
// 	// } else if numFailedSpans < numSpans {
// 	// 	// This request was partially successful.
// 	// 	traceResponse := coltracepb.ExportTraceServiceResponse{
// 	// 		PartialSuccess: &coltracepb.ExportTracePartialSuccess{
// 	// 			RejectedSpans: int64(numFailedSpans),
// 	// 		},
// 	// 	}

// 	// 	response, err := proto.Marshal(&traceResponse)
// 	// 	if err != nil {
// 	// 		log.Errorf("handleTraceIngestionResponse: failed to marshal partially successful response: %v. NumSpans: %d, NumFailedSpans: %d, Trace Response: %v", err, numSpans, numFailedSpans, &traceResponse)
// 	// 		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
// 	// 		return
// 	// 	}
// 	// 	_, err = ctx.Write(response)
// 	// 	if err != nil {
// 	// 		log.Errorf("handleTraceIngestionResponse: failed to write partially successful response: %v. NumSpans: %d, NumFailedSpans: %d, response: %v", err, numSpans, numFailedSpans, response)
// 	// 		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
// 	// 		return
// 	// 	}

// 	// 	ctx.SetStatusCode(fasthttp.StatusOK)
// 	// 	return
// 	// } else {
// 	// 	// Every span failed to be ingested.
// 	// 	if numFailedSpans > numSpans {
// 	// 		log.Errorf("handleTraceIngestionResponse: error in counting number of total and failed spans. Counted NumSpans: %d, Counted NumFailedSpans: %d", numSpans, numFailedSpans)
// 	// 	}

// 	// 	log.Errorf("handleTraceIngestionResponse: every span failed ingestion. NumSpans: %d, NumFailedSpans: %d", numSpans, numFailedSpans)
// 	// 	setFailureResponse(ctx, fasthttp.StatusInternalServerError, "Every span failed ingestion")
// 	// 	return
// 	// }
// }

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
	"bytes"
	"encoding/csv"
	"encoding/json"
	"io"
	"strings"

	"github.com/siglens/siglens/pkg/grpc"
	"github.com/siglens/siglens/pkg/hooks"
	. "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/usageStats"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

type InfluxPutResp struct {
	Failed  uint64   `json:"failed"`
	Success uint64   `json:"success"`
	Errors  []string `json:"errors,omitempty"`
}

func PutMetrics(ctx *fasthttp.RequestCtx, myid int64) {
	if hook := hooks.GlobalHooks.OverrideIngestRequestHook; hook != nil {
		alreadyHandled := hook(ctx, myid, grpc.INGEST_FUNC_INFLUX_METRICS, false)
		if alreadyHandled {
			return
		}
	}

	var processedCount uint64
	var failedCount uint64
	var err error

	cType := string(ctx.Request.Header.ContentType())
	if strings.Contains(cType, "text/plain") {
		eType := string(ctx.Request.Header.ContentEncoding())
		if eType == "gzip" {
			var body []byte
			body, err = ctx.Request.BodyGunzip()
			if err != nil {
				log.Errorf("PutMetrics: error unzipping body! %v", err)
				writeInfluxResponse(ctx, processedCount, failedCount, "Failed to gunzip request", fasthttp.StatusBadRequest)
				return
			}

			processedCount, failedCount, err = HandlePutMetrics(body, myid)
			if err != nil {
				log.Errorf("PutMetrics: failed to process request. body=%v, err=%v", body, err)
				writeInfluxResponse(ctx, processedCount, failedCount, "Failed to process request", fasthttp.StatusBadRequest)
				return
			}

		} else {
			body := ctx.PostBody()
			processedCount, failedCount, err = HandlePutMetrics(body, myid)
			if err != nil {
				log.Errorf("PutMetrics: failed to process request. body=%v, err=%v", body, err)
				writeInfluxResponse(ctx, processedCount, failedCount, "Failed to process request", fasthttp.StatusBadRequest)
				return
			}
		}
	} else {
		log.Errorf("PutMetrics: unknown content type [%s]! %v", cType, err)
		writeInfluxResponse(ctx, processedCount, failedCount, "unknown content type", fasthttp.StatusBadRequest)
		return
	}

	writeInfluxResponse(ctx, processedCount, failedCount, "", fasthttp.StatusNoContent)
}

func HandlePutMetrics(fullData []byte, myid int64) (uint64, uint64, error) {

	//to have a check if there are any errors in the request
	//to check for status : 200 or 400
	//to check if json is greater than MAX_RECORD_SIZE

	var successCount uint64 = 0
	var failedCount uint64 = 0
	var err error = nil

	bytesReceived := uint64(len(fullData))
	reader := csv.NewReader(bytes.NewBuffer(fullData))
	for {
		record, err := reader.Read()
		if err != nil {
			// If there is an error, check if it's EOF
			if err == io.EOF {
				break // End of file
			}

			log.Errorf("HandlePutMetrics: failed to read %v (length %v) as csv, err=%v", fullData, len(fullData), err)
			return 0, 0, err
		} else {
			csvRow := strings.Join(record, ",")
			mErr := writer.AddTimeSeriesEntryToInMemBuf([]byte(csvRow), SIGNAL_METRICS_INFLUX, myid)
			if mErr != nil {
				log.Errorf("HandlePutMetrics: failed to add time series for csvRow=%v, myid=%v, err=%v", csvRow, myid, mErr)
				failedCount++
			} else {
				successCount++
			}

		}

	}

	if err != nil {
		mErr := writer.AddTimeSeriesEntryToInMemBuf(fullData, SIGNAL_METRICS_INFLUX, myid)
		if mErr != nil {
			log.Errorf("HandlePutMetrics: failed to add full data as a time series entry for myid=%v, err=%v", myid, mErr)
			failedCount++
		} else {
			successCount++
		}
		return failedCount, successCount, err
	}

	usageStats.UpdateMetricsStats(bytesReceived, successCount, myid)

	return successCount, failedCount, nil
}

func writeInfluxResponse(ctx *fasthttp.RequestCtx, processedCount uint64, failedCount uint64, err string, code int) {
	resp := InfluxPutResp{Success: processedCount, Failed: failedCount}
	if err != "" {
		resp.Errors = []string{err}
	}

	ctx.SetStatusCode(code)
	ctx.SetContentType(utils.ContentJson)
	jval, mErr := json.Marshal(resp)
	if mErr != nil {
		log.Errorf("writeInfluxResponse: failed to marshal response %+v to json, err=%v", resp, mErr)
	}

	_, mErr = ctx.Write(jval)
	if mErr != nil {
		log.Errorf("writeInfluxResponse: failed to write json %v to http request, err=%v", jval, mErr)
	}
}

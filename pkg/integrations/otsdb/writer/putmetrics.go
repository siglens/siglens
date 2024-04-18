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
	"encoding/json"

	jp "github.com/buger/jsonparser"
	. "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/usageStats"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

const METRICS_INDEXNAME = "metricsdb"

type OtsdbPutResp struct {
	Failed  uint64   `json:"failed"`
	Success uint64   `json:"success"`
	Errors  []string `json:"errors,omitempty"`
}

func PutMetrics(ctx *fasthttp.RequestCtx, myid uint64) {
	var processedCount uint64
	var failedCount uint64
	var err error

	cType := string(ctx.Request.Header.ContentType())
	switch cType {
	case "gzip":
		var body []byte
		body, err = ctx.Request.BodyGunzip()
		if err != nil {
			log.Errorf("PutMetrics: error unzipping body! %v", err)
			break
		}
		processedCount, failedCount, err = HandlePutMetrics(body, myid)
	case "application/json", "json":
		body := ctx.PostBody()
		processedCount, failedCount, err = HandlePutMetrics(body, myid)
	default:
		log.Errorf("PutMetrics: unknown content type [%s]! %v", cType, err)
		writeOtsdbResponse(ctx, processedCount, failedCount, "unknown content type", fasthttp.StatusBadRequest)
		return
	}
	if err != nil {
		writeOtsdbResponse(ctx, processedCount, failedCount, err.Error(), fasthttp.StatusBadRequest)
	}
	writeOtsdbResponse(ctx, processedCount, failedCount, "", fasthttp.StatusOK)
}

func HandlePutMetrics(fullData []byte, myid uint64) (uint64, uint64, error) {

	//to have a check if there are any errors in the request
	//to check for status : 200 or 400
	//to check if json is greater than MAX_RECORD_SIZE
	var inCount uint64 = 0
	var successCount uint64 = 0
	var failedCount uint64 = 0

	bytesReceived := uint64(len(fullData))
	_, err := jp.ArrayEach(fullData, func(value []byte, valueType jp.ValueType, offset int, err error) {
		inCount++
		switch valueType {
		case jp.Object:
			mErr := writer.AddTimeSeriesEntryToInMemBuf(value, SIGNAL_METRICS_OTSDB, myid)
			if mErr != nil {
				log.Errorf("HandlePutMetrics: failed to add time series entry %+v", mErr)
				failedCount++
			} else {
				successCount++
			}
		default:
			log.Errorf("HandlePutMetrics: Unknown type %+v for a read put metrics reqeust", valueType)
			failedCount++
		}
	})
	if err != nil {
		mErr := writer.AddTimeSeriesEntryToInMemBuf(fullData, SIGNAL_METRICS_OTSDB, myid)
		if mErr != nil {
			log.Errorf("HandlePutMetrics: failed to add time series entry %+v", mErr)
			failedCount++
		} else {
			successCount++
		}
		return 0, 0, err
	}
	usageStats.UpdateMetricsStats(bytesReceived, successCount, myid)
	return successCount, failedCount, nil

}

func writeOtsdbResponse(ctx *fasthttp.RequestCtx, processedCount uint64, failedCount uint64, err string, code int) {

	resp := OtsdbPutResp{Success: processedCount, Failed: failedCount}
	if err != "" {
		resp.Errors = []string{err}
	}

	ctx.SetStatusCode(code)
	ctx.SetContentType(utils.ContentJson)
	jval, mErr := json.Marshal(resp)
	if mErr != nil {
		log.Errorf("writeInfluxResponse: failed to marshal resp %+v", mErr)
	}
	_, mErr = ctx.Write(jval)

	if mErr != nil {
		log.Errorf("writeInfluxResponse: failed to write jval to http request %+v", mErr)
	}
}

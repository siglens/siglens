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
	"bytes"
	"encoding/csv"
	"encoding/json"
	"io"
	"strings"

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

func PutMetrics(ctx *fasthttp.RequestCtx, myid uint64) {

	var processedCount uint64
	var failedCount uint64
	var err error

	cType := string(ctx.Request.Header.ContentType())
	if cType == "text/plain; charset=utf-8" {
		eType := string(ctx.Request.Header.ContentEncoding())
		if eType == "gzip" {
			var body []byte
			body, err = ctx.Request.BodyGunzip()
			if err != nil {
				log.Errorf("PutMetrics: error unzipping body! %v", err)
			}
			processedCount, failedCount, err = HandlePutMetrics(body, myid)
		} else {
			body := ctx.PostBody()
			processedCount, failedCount, err = HandlePutMetrics(body, myid)
		}
	} else {
		log.Errorf("PutMetrics: unknown content type [%s]! %v", cType, err)
		writeInfluxResponse(ctx, processedCount, failedCount, "unknown content type", fasthttp.StatusBadRequest)
		return
	}

	if err != nil {
		writeInfluxResponse(ctx, processedCount, failedCount, err.Error(), fasthttp.StatusBadRequest)
	}
	writeInfluxResponse(ctx, processedCount, failedCount, "", fasthttp.StatusOK)
}

func HandlePutMetrics(fullData []byte, myid uint64) (uint64, uint64, error) {

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
			return 0, 0, err

		} else {

			csvRow := strings.Join(record, ",")
			mErr := writer.AddTimeSeriesEntryToInMemBuf([]byte(csvRow), SIGNAL_METRICS_INFLUX, myid)
			if mErr != nil {
				log.Errorf("HandlePutMetrics: failed to add time series entry %+v", mErr)
				failedCount++
			} else {
				successCount++
			}

		}

	}

	if err != nil {
		mErr := writer.AddTimeSeriesEntryToInMemBuf(fullData, SIGNAL_METRICS_INFLUX, myid)
		if mErr != nil {
			log.Errorf("HandlePutMetrics: failed to add time series entry %+v", mErr)
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
		log.Errorf("writeInfluxResponse: failed to marshal resp %+v", mErr)
	}
	_, mErr = ctx.Write(jval)

	if mErr != nil {
		log.Errorf("writeInfluxResponse: failed to write jval to http request %+v", mErr)
	}

}

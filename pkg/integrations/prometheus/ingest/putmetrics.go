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
	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	. "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/usageStats"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

type PrometheusPutResp struct {
	Failed  uint64   `json:"failed"`
	Success uint64   `json:"success"`
	Errors  []string `json:"errors,omitempty"`
}

func decodeWriteRequest(compressed []byte) (*prompb.WriteRequest, error) {
	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		log.Errorf("decodeWriteRequest: Error decompressing request body, err: %v", err)
		return nil, err
	}
	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		log.Errorf("decodeWriteRequest: Error unmarshalling request body, err: %v", err)
		return nil, err
	}
	return &req, nil
}

func PutMetrics(ctx *fasthttp.RequestCtx) {
	var processedCount uint64
	var failedCount uint64
	var err error
	version := string(ctx.Request.Header.Peek("X-Prometheus-Remote-Write-Version"))
	if version != "0.1.0" {
		log.Errorf("PutMetrics: Unsupported remote write protocol version %v", version)
		writePrometheusResponse(ctx, processedCount, failedCount, "unsupported remote write protocol", fasthttp.StatusBadRequest)
		return
	}
	cType := string(ctx.Request.Header.ContentType())
	if cType != "application/x-protobuf" {
		log.Errorf("PutMetrics: unknown content type [%s]! %v", cType, err)
		writePrometheusResponse(ctx, processedCount, failedCount, "unknown content type", fasthttp.StatusBadRequest)
		return
	}
	encoding := string(ctx.Request.Header.ContentEncoding())
	if encoding != "snappy" {
		log.Errorf("PutMetrics: unknown content encoding [%s]! %v", encoding, err)
		writePrometheusResponse(ctx, processedCount, failedCount, "unknown content encoding", fasthttp.StatusBadRequest)
		return
	}

	compressed := ctx.PostBody()
	processedCount, failedCount, err = HandlePutMetrics(compressed)
	if err != nil {
		writePrometheusResponse(ctx, processedCount, failedCount, err.Error(), fasthttp.StatusBadRequest)
		return
	}
	writePrometheusResponse(ctx, processedCount, failedCount, "", fasthttp.StatusOK)
}

func HandlePutMetrics(compressed []byte) (uint64, uint64, error) {
	var successCount uint64 = 0
	var failedCount uint64 = 0

	req, err := decodeWriteRequest(compressed)
	if err != nil {
		return successCount, failedCount, nil
	}

	for _, ts := range req.Timeseries {
		metric := make(model.Metric, len(ts.Labels))
		for _, l := range ts.Labels {
			metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

		for _, s := range ts.Samples {
			var sample model.Sample = model.Sample{
				Metric:    metric,
				Value:     model.SampleValue(s.Value),
				Timestamp: model.Time(s.Timestamp),
			}

			data, err := sample.MarshalJSON()
			if err != nil {
				failedCount++
				log.Errorf("HandlePutMetrics: failed to marshal data, err: %+v", err)
				continue
			}

			var dataJson map[string]interface{}
			err = json.Unmarshal(data, &dataJson)
			if err != nil {
				failedCount++
				log.Errorf("HandlePutMetrics: failed to Unmarshal data, err: %+v", err)
				continue
			}

			var metricName string
			tags := "{"
			for key, val := range dataJson {
				if key == "metric" {
					valMap, ok := val.(map[string]interface{})
					if ok {
						for k, v := range valMap {
							if k == "__name__" {
								valString, ok := v.(string)
								if ok {
									metricName = valString
								}
							}
							valString, ok := v.(string)
							if ok {
								tags += `"` + k + `":"` + valString + `",`
							}
						}
					}
				}
			}
			tags += `"metric"` + `:"` + metricName + `",`
			if tags[len(tags)-1] == ',' {
				tags = tags[:len(tags)-1]
			}
			tags += "}"

			modifiedData := `{"metric":"` + metricName + `","tags":` + tags + `,"timestamp":` + strconv.FormatInt(s.Timestamp, 10) + `,"value":` + strconv.FormatFloat(s.Value, 'f', -1, 64) + `}`

			err = writer.AddTimeSeriesEntryToInMemBuf([]byte(modifiedData), SIGNAL_METRICS_OTSDB, uint64(0))
			if err != nil {
				log.Errorf("HandlePutMetrics: failed to add time series entry %+v", err)
				failedCount++
			} else {
				successCount++
			}
		}
	}
	bytesReceived := uint64(len(compressed))
	usageStats.UpdateMetricsStats(bytesReceived, successCount, 0)
	return successCount, failedCount, nil
}

func writePrometheusResponse(ctx *fasthttp.RequestCtx, processedCount uint64, failedCount uint64, err string, code int) {

	resp := PrometheusPutResp{Success: processedCount, Failed: failedCount}
	if err != "" {
		resp.Errors = []string{err}
	}

	ctx.SetStatusCode(code)
	ctx.SetContentType(utils.ContentJson)
	jval, mErr := json.Marshal(resp)
	if mErr != nil {
		log.Errorf("writePrometheusResponse: failed to marshal resp %+v", mErr)
		return
	}
	_, mErr = ctx.Write(jval)

	if mErr != nil {
		log.Errorf("writePrometheusResponse: failed to write jval to http request %+v", mErr)
		return
	}
}

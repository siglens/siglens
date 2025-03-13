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
	"encoding/json"
	"regexp"
	"strconv"

	"github.com/siglens/siglens/pkg/grpc"
	"github.com/siglens/siglens/pkg/hooks"
	. "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/usageStats"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	collmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
)

type processedMetric struct {
	Name         string
	Attributes   map[string]string
	TimeUnixNano uint64
	Value        uint64
}

func ProcessMetricsIngest(ctx *fasthttp.RequestCtx, myid int64) {

	if hook := hooks.GlobalHooks.OverrideIngestRequestHook; hook != nil {
		alreadyHandled := hook(ctx, myid, grpc.INGEST_FUNC_OTLP_METRICS, false)
		if alreadyHandled {
			return
		}
	}

	data, err := getDataToUnmarshal(ctx)
	if err != nil {
		log.Errorf("ProcessMetricIngest: failed to get data to unmarshal: %v", err)
		setFailureResponse(ctx, fasthttp.StatusBadRequest, err.Error())
		return
	}

	request, err := unmarshalMetricRequest(data)
	if err != nil {
		log.Errorf("ProcessMetricIngest: failed to unpack Data: %s with err %v", string(data), err)
		setFailureResponse(ctx, fasthttp.StatusBadRequest, "Unable to unmarshal metrics")
		return
	}

	dpCount, numFailedRecords := ingestMetrics(request, myid)
	usageStats.UpdateMetricsStats(uint64(len(data)), uint64(dpCount), myid)

	setMetricsIngestionResponse(ctx, dpCount, numFailedRecords)
}

func ingestMetrics(request *collmetricspb.ExportMetricsServiceRequest, myid int64) (int, int) {
	dpCount := 0
	numFailedRecords := 0

	for _, resourceMetrics := range request.ResourceMetrics {
		for _, scopeMetrics := range resourceMetrics.ScopeMetrics {

			for _, metrics := range scopeMetrics.Metrics {
				extractedMetrics := processMetric(metrics)
				for _, metric := range extractedMetrics {
					dpCount++
					data, err := ConvertToOTLPMetricsFormat(metric, int64(metric.TimeUnixNano), float64(metric.Value))
					if err != nil {
						numFailedRecords++
						log.Errorf("OLTPMetrics: failed to ConvertToOTLPMetricsFormat data=%+v, err=%v", data, err)
						continue
					}
					err = writer.AddTimeSeriesEntryToInMemBuf([]byte(data), SIGNAL_METRICS_OTLP, myid)
					if err != nil {
						numFailedRecords++
						log.Errorf("OLTPMetrics: failed to add time series entry for data=%+v, err=%v", data, err)
					}
				}

			}
		}

	}
	return dpCount, numFailedRecords
}

func unmarshalMetricRequest(data []byte) (*collmetricspb.ExportMetricsServiceRequest, error) {
	var metrics collmetricspb.ExportMetricsServiceRequest
	err := proto.Unmarshal(data, &metrics)
	if err != nil {
		log.Errorf("unmarshalMetricRequest: failed with err: %v data: %v", err, string(data))
		return nil, err
	}

	return &metrics, nil
}

func setMetricsIngestionResponse(ctx *fasthttp.RequestCtx, numTotalRecords int, numFailedRecords int) {
	if numFailedRecords == 0 {
		response, err := proto.Marshal(&collmetricspb.ExportMetricsServiceRequest{})
		if err != nil {
			log.Errorf("setMetricsIngestionResponse: failed to marshal successful response; err=%v", err)
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			return
		}

		_, err = ctx.Write(response)
		if err != nil {
			log.Errorf("setMetricsIngestionResponse: failed to write successful response; err=%v", err)
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			return
		}
	} else if numFailedRecords < numTotalRecords {
		response, err := proto.Marshal(&collmetricspb.ExportMetricsServiceRequest{})
		if err != nil {
			log.Errorf("setMetricsIngestionResponse: failed to marshal partially successful response; err=%v", err)
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			return
		}

		_, err = ctx.Write(response)
		if err != nil {
			log.Errorf("setMetricsIngestionResponse: failed to write partially successful response; err=%v", err)
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			return
		}
	} else {
		setFailureResponse(ctx, fasthttp.StatusInternalServerError, "Every Metrics record failed ingestion")
	}
}

func processMetric(metric *metricspb.Metric) []processedMetric {
	var extracted []processedMetric

	if gauge := metric.GetGauge(); gauge != nil {
		for _, dataPoint := range gauge.DataPoints {
			extracted = append(extracted, processedMetric{
				Name:         metric.Name,
				Attributes:   extractAttributes(dataPoint.Attributes),
				TimeUnixNano: dataPoint.TimeUnixNano,
				Value:        uint64(dataPoint.GetAsDouble()),
			})
		}
		return extracted
	}

	if sum := metric.GetSum(); sum != nil {
		for _, dataPoint := range sum.DataPoints {
			extracted = append(extracted, processedMetric{
				Name:         metric.Name,
				Attributes:   extractAttributes(dataPoint.Attributes),
				TimeUnixNano: dataPoint.TimeUnixNano,
				Value:        uint64(dataPoint.GetAsDouble()),
			})
		}
		return extracted
	}

	// Process Histogram
	if histogram := metric.GetHistogram(); histogram != nil {
		for _, dataPoint := range histogram.DataPoints {
			extracted = append(extracted, processedMetric{
				Name:         metric.Name,
				Attributes:   extractAttributes(dataPoint.Attributes),
				TimeUnixNano: dataPoint.TimeUnixNano,
				Value:        dataPoint.Count,
			})
		}
		return extracted
	}

	// Process Exponential Histogram
	if expHistogram := metric.GetExponentialHistogram(); expHistogram != nil {
		for _, dataPoint := range expHistogram.DataPoints {
			extracted = append(extracted, processedMetric{
				Name:         metric.Name,
				Attributes:   extractAttributes(dataPoint.Attributes),
				TimeUnixNano: dataPoint.TimeUnixNano,
				Value:        uint64(dataPoint.Scale),
			})
		}
		return extracted
	}

	// Process Summary
	if summary := metric.GetSummary(); summary != nil {
		for _, dataPoint := range summary.DataPoints {
			extracted = append(extracted, processedMetric{
				Name:         metric.Name,
				Attributes:   extractAttributes(dataPoint.Attributes),
				TimeUnixNano: dataPoint.TimeUnixNano,
				Value:        dataPoint.Count,
			})
		}
		return extracted
	}

	return extracted
}

func ConvertToOTLPMetricsFormat(data processedMetric, timestamp int64, value float64) ([]byte, error) {
	type Metric struct {
		Name      string            `json:"metric"`
		Tags      map[string]string `json:"tags"`
		Timestamp int64             `json:"timestamp"`
		Value     float64           `json:"value"`
	}

	var metricName string
	tags := make(map[string]string)
	re := regexp.MustCompile(`[^a-zA-Z0-9_]`)
	metricName = re.ReplaceAllString(data.Name, "_")
	for key, val := range data.Attributes {
		tags[key] = val
	}

	modifiedMetric := Metric{
		Name:      metricName,
		Tags:      tags,
		Timestamp: timestamp,
		Value:     value,
	}

	modifiedData, err := json.Marshal(modifiedMetric)
	if err != nil {
		return nil, err
	}

	return modifiedData, nil
}

func extractAttributes(attributes []*commonpb.KeyValue) map[string]string {
	attrMap := make(map[string]string)
	for _, attr := range attributes {
		re := regexp.MustCompile(`[^a-zA-Z0-9_]`)
		key := re.ReplaceAllString(attr.Key, "_")
		value := attr.Value.GetValue()

		switch v := value.(type) {
		case *commonpb.AnyValue_StringValue:
			attrMap[key] = v.StringValue
		case *commonpb.AnyValue_BoolValue:
			attrMap[key] = strconv.FormatBool(v.BoolValue)
		case *commonpb.AnyValue_IntValue:
			attrMap[key] = strconv.FormatInt(v.IntValue, 10)
		case *commonpb.AnyValue_DoubleValue:
			attrMap[key] = strconv.FormatFloat(v.DoubleValue, 'f', -1, 64)
		}
	}
	return attrMap
}

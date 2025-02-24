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
	Description  string
	Unit         string
	Type         string
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

	numTotalRecords, numFailedRecords := ingestMetrics(request, myid)
	usageStats.UpdateStats(uint64(len(data)), uint64(max(0, numTotalRecords-numFailedRecords)), myid)

	setMetricsIngestionResponse(ctx, numTotalRecords, numFailedRecords)
}

func ingestMetrics(request *collmetricspb.ExportMetricsServiceRequest, myid int64) (int, int) {
	numTotalRecords := 0
	numFailedRecords := 0
	for _, resourceMetrics := range request.ResourceMetrics {

		for _, scopeMetrics := range resourceMetrics.ScopeMetrics {

			for _, metrics := range scopeMetrics.Metrics {
				metricType, extractedMetrics := processMetric(metrics)
				for _, metric := range extractedMetrics {
					metric.Type = metricType
					numTotalRecords++

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

	return numTotalRecords, numFailedRecords
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

func processMetric(metric *metricspb.Metric) (string, []processedMetric) {
	var extracted []processedMetric

	if gauge := metric.GetGauge(); gauge != nil {
		for _, dataPoint := range gauge.DataPoints {
			extracted = append(extracted, processedMetric{
				Name:         metric.Name,
				Description:  metric.Description,
				Unit:         metric.Unit,
				Attributes:   extractAttributes(dataPoint.Attributes),
				TimeUnixNano: dataPoint.TimeUnixNano,
				Value:        uint64(dataPoint.GetAsDouble()),
			})
		}
		return "Gauge", extracted
	}

	// Process Sum
	if sum := metric.GetSum(); sum != nil {
		for _, dataPoint := range sum.DataPoints {
			extracted = append(extracted, processedMetric{
				Name:         metric.Name,
				Description:  metric.Description,
				Unit:         metric.Unit,
				Attributes:   extractAttributes(dataPoint.Attributes),
				TimeUnixNano: dataPoint.TimeUnixNano,
				Value:        uint64(dataPoint.GetAsDouble()),
			})
		}
		return "Sum", extracted
	}

	// Process Histogram
	if histogram := metric.GetHistogram(); histogram != nil {
		for _, dataPoint := range histogram.DataPoints {
			extracted = append(extracted, processedMetric{
				Name:         metric.Name,
				Description:  metric.Description,
				Unit:         metric.Unit,
				Attributes:   extractAttributes(dataPoint.Attributes),
				TimeUnixNano: dataPoint.TimeUnixNano,
				Value:        dataPoint.Count,
			})
		}
		return "Histogram", extracted
	}

	// Process Exponential Histogram
	if expHistogram := metric.GetExponentialHistogram(); expHistogram != nil {
		for _, dataPoint := range expHistogram.DataPoints {
			extracted = append(extracted, processedMetric{
				Name:         metric.Name,
				Description:  metric.Description,
				Unit:         metric.Unit,
				Attributes:   extractAttributes(dataPoint.Attributes),
				TimeUnixNano: dataPoint.TimeUnixNano,
				Value:        uint64(dataPoint.Scale),
			})
		}
		return "ExponentialHistogram", extracted
	}

	// Process Summary
	if summary := metric.GetSummary(); summary != nil {
		for _, dataPoint := range summary.DataPoints {
			extracted = append(extracted, processedMetric{
				Name:         metric.Name,
				Description:  metric.Description,
				Unit:         metric.Unit,
				Attributes:   extractAttributes(dataPoint.Attributes),
				TimeUnixNano: dataPoint.TimeUnixNano,
				Value:        dataPoint.Count,
			})
		}
		return "Summary", extracted
	}

	return "Unknown", extracted
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
	metricName = data.Name
	tags["unit"] = data.Unit
	tags["description"] = data.Description
	tags["type"] = data.Type

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

// Helper function to extract attributes from KeyValue pairs
func extractAttributes(attributes []*commonpb.KeyValue) map[string]string {
	attrMap := make(map[string]string)
	for _, attr := range attributes {
		attrMap[attr.Key] = attr.Value.GetStringValue()
	}
	return attrMap
}

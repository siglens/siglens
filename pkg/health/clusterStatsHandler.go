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

package health

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/dustin/go-humanize"
	jsoniter "github.com/json-iterator/go"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/writer"
	segwriter "github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/segment/writer/metrics"
	mmeta "github.com/siglens/siglens/pkg/segment/writer/metrics/meta"
	"github.com/siglens/siglens/pkg/usageStats"
	"github.com/siglens/siglens/pkg/utils"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

var excludedInternalIndices = [...]string{"traces", "red-traces", "service-dependency"}

func ProcessClusterStatsHandler(ctx *fasthttp.RequestCtx, myid uint64) {

	var httpResp utils.ClusterStatsResponseInfo
	var err error
	if hook := hooks.GlobalHooks.MiddlewareExtractOrgIdHook; hook != nil {
		myid, err = hook(ctx)
		if err != nil {
			log.Errorf("ProcessClusterStatsHandler: failed to extract orgId from context. Err=%+v", err)
			utils.SetBadMsg(ctx, "")
			return
		}
	}
	indexData, logsEventCount, logsIncomingBytes, logsOnDiskBytes := getIngestionStats(myid)
	queryCount, totalResponseTimeSinceRestart, totalResponseTimeSinceInstall, queriesSinceInstall := usageStats.GetQueryStats(myid)

	metricsIncomingBytes, metricsDatapointsCount, metricsOnDiskBytes := GetMetricsStats(myid)
	traceIndexData, traceSpanCount, totalTraceBytes, totalTraceOnDiskBytes := GetTracesStats(myid)
	metricsInMemBytes := metrics.GetTotalEncodedSize()

	if hook := hooks.GlobalHooks.AddMultinodeStatsHook; hook != nil {
		hook(indexData, myid, &logsIncomingBytes, &logsOnDiskBytes, &logsEventCount,
			&metricsIncomingBytes, &metricsOnDiskBytes, &metricsDatapointsCount,
			&queryCount, &totalResponseTimeSinceRestart)
	}

	httpResp.IngestionStats = make(map[string]interface{})
	httpResp.QueryStats = make(map[string]interface{})
	httpResp.MetricsStats = make(map[string]interface{})
	httpResp.TraceStats = make(map[string]interface{})

	httpResp.IngestionStats["Log Incoming Volume"] = convertBytesToGB(logsIncomingBytes)
	httpResp.IngestionStats["Incoming Volume"] = convertBytesToGB(logsIncomingBytes + float64(metricsIncomingBytes))

	httpResp.IngestionStats["Metrics Incoming Volume"] = convertBytesToGB(float64(metricsIncomingBytes))

	httpResp.IngestionStats["Event Count"] = humanize.Comma(int64(logsEventCount))

	httpResp.IngestionStats["Log Storage Used"] = convertBytesToGB(logsOnDiskBytes)
	httpResp.IngestionStats["Metrics Storage Used"] = convertBytesToGB(float64(metricsOnDiskBytes + metricsInMemBytes))
	httpResp.IngestionStats["Logs Storage Saved"] = calculateStorageSavedPercentage(logsIncomingBytes, logsOnDiskBytes)
	httpResp.IngestionStats["Metrics Storage Saved"] = calculateStorageSavedPercentage(float64(metricsIncomingBytes), float64(metricsOnDiskBytes+metricsInMemBytes))

	if hook := hooks.GlobalHooks.SetExtraIngestionStatsHook; hook != nil {
		hook(httpResp.IngestionStats)
	}

	httpResp.MetricsStats["Incoming Volume"] = convertBytesToGB(float64(metricsIncomingBytes))
	httpResp.MetricsStats["Datapoints Count"] = humanize.Comma(int64(metricsDatapointsCount))

	httpResp.QueryStats["Query Count Since Restart"] = queryCount
	httpResp.QueryStats["Query Count Since Install"] = queriesSinceInstall

	if queriesSinceInstall > 1 {
		httpResp.QueryStats["Average Query Latency (since install)"] = fmt.Sprintf("%v", utils.ToFixed(totalResponseTimeSinceInstall/float64(queriesSinceInstall), 3)) + " ms"
	} else {
		httpResp.QueryStats["Average Query Latency (since install)"] = fmt.Sprintf("%v", utils.ToFixed(totalResponseTimeSinceInstall, 3)) + " ms"
	}

	if queryCount > 1 {
		httpResp.QueryStats["Average Query Latency (since restart)"] = fmt.Sprintf("%v", utils.ToFixed(totalResponseTimeSinceRestart/float64(queryCount), 3)) + " ms"
	} else {
		httpResp.QueryStats["Average Query Latency (since restart)"] = fmt.Sprintf("%v", utils.ToFixed(totalResponseTimeSinceRestart, 3)) + " ms"
	}
	httpResp.TraceStats["Trace Span Count"] = humanize.Comma(int64(traceSpanCount))
	httpResp.TraceStats["Total Trace Volume"] = convertBytesToGB(float64(totalTraceBytes))
	httpResp.TraceStats["Trace Storage Used"] = convertBytesToGB(float64(totalTraceOnDiskBytes))
	httpResp.TraceStats["Trace Storage Saved"] = calculateStorageSavedPercentage(float64(totalTraceBytes), float64(totalTraceOnDiskBytes))

	httpResp.IndexStats = convertIndexDataToSlice(indexData)
	httpResp.TraceIndexStats = convertTraceIndexDataToSlice(traceIndexData)
	utils.WriteJsonResponse(ctx, httpResp)

}

func calculateStorageSavedPercentage(incomingBytes, onDiskBytes float64) float64 {
	storageSaved := 0.0
	if incomingBytes > 0 {
		storageSaved = (1 - (onDiskBytes / incomingBytes)) * 100
		if storageSaved < 0 {
			storageSaved = 0
		}
	}
	return storageSaved
}

func convertDataToSlice(indexToAllIndexStats map[string]utils.AllIndexesStats, volumeField, countField,
	segmentCountField string) []map[string]map[string]interface{} {

	retVal := make([]map[string]map[string]interface{}, 0)
	for idx, allIndexStats := range indexToAllIndexStats {
		indexStats, ok := allIndexStats.IndexToStats[idx]
		if !ok {
			log.Errorf("convertDataToSlice: indexStats not found for index=%v", idx)
			continue
		}

		nextVal := make(map[string]map[string]interface{})
		nextVal[idx] = make(map[string]interface{})
		nextVal[idx][volumeField] = convertBytesToGB(float64(indexStats.NumBytesIngested))
		nextVal[idx][countField] = humanize.Comma(int64(indexStats.NumRecords))
		nextVal[idx][segmentCountField] = humanize.Comma(int64(indexStats.NumSegments))

		retVal = append(retVal, nextVal)
	}

	return retVal
}

func convertIndexDataToSlice(indexData map[string]utils.AllIndexesStats) []map[string]map[string]interface{} {
	return convertDataToSlice(indexData, "ingestVolume", "eventCount", "segmentCount")
}

func convertTraceIndexDataToSlice(traceIndexData map[string]utils.AllIndexesStats) []map[string]map[string]interface{} {
	return convertDataToSlice(traceIndexData, "traceVolume", "traceSpanCount", "segmentCount")
}

func ProcessClusterIngestStatsHandler(ctx *fasthttp.RequestCtx, orgId uint64) {
	var err error
	if hook := hooks.GlobalHooks.MiddlewareExtractOrgIdHook; hook != nil {
		orgId, err = hook(ctx)
		if err != nil {
			log.Errorf("ProcessClusterIngestStatsHandler: failed to extract orgId from context. Err=%+v", err)
			utils.SetBadMsg(ctx, "")
			return
		}
	}

	var httpResp utils.ClusterStatsResponseInfo
	rawJSON := ctx.PostBody()
	if rawJSON == nil {
		log.Errorf(" ClusterIngestStatsHandler: received empty search request body ")
		utils.SetBadMsg(ctx, "")
		return
	}

	readJSON := make(map[string]interface{})
	var jsonc = jsoniter.ConfigCompatibleWithStandardLibrary
	decoder := jsonc.NewDecoder(bytes.NewReader(rawJSON))
	decoder.UseNumber()
	err = decoder.Decode(&readJSON)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		_, err = ctx.WriteString(err.Error())
		if err != nil {
			log.Errorf(" ClusterIngestStatsHandler: could not write error message err=%v", err)
		}
		log.Errorf(" ClusterIngestStatsHandler: failed to decode search request body! Err=%+v", err)
		return
	}

	pastXhours, granularity := parseIngestionStatsRequest(readJSON)
	rStats, _ := usageStats.GetUsageStats(pastXhours, granularity, orgId)
	httpResp.ChartStats = make(map[string]map[string]interface{})

	for k, entry := range rStats {
		httpResp.ChartStats[k] = make(map[string]interface{}, 2)
		httpResp.ChartStats[k]["TotalGBCount"] = float64(entry.BytesCount) / 1_000_000_000
		httpResp.ChartStats[k]["LogsEventCount"] = entry.EventCount
		httpResp.ChartStats[k]["MetricsDatapointsCount"] = entry.MetricsDatapointsCount
		httpResp.ChartStats[k]["LogsGBCount"] = float64(entry.LogsBytesCount) / 1_000_000_000
		httpResp.ChartStats[k]["MetricsGBCount"] = float64(entry.MetricsBytesCount) / 1_000_000_000
		httpResp.ChartStats[k]["TraceGBCount"] = float64(entry.TraceBytesCount) / 1_000_000_000
		httpResp.ChartStats[k]["TraceSpanCount"] = entry.TraceSpanCount
	}
	utils.WriteJsonResponse(ctx, httpResp)
}

func parseAlphaNumTime(inp string, defValue uint64) (uint64, usageStats.UsageStatsGranularity) {
	granularity := usageStats.Daily
	sanTime := strings.ReplaceAll(inp, " ", "")
	retVal := defValue

	strln := len(sanTime)
	if strln < 6 {
		return retVal, usageStats.Daily
	}

	unit := sanTime[strln-1]
	num, err := strconv.ParseInt(sanTime[4:strln-1], 0, 64)
	if err != nil {
		return defValue, usageStats.Daily
	}

	switch unit {
	case 'h':
		retVal = uint64(num)
		granularity = usageStats.Hourly
	case 'd':
		retVal = 24 * uint64(num)
		granularity = usageStats.Daily
	}
	//for past 2 days , set granularity to Hourly
	if num <= 2 {
		granularity = usageStats.Hourly
	}
	return retVal, granularity
}

func parseIngestionStatsRequest(jsonSource map[string]interface{}) (uint64, usageStats.UsageStatsGranularity) {
	var pastXhours uint64
	granularity := usageStats.Daily
	startE, ok := jsonSource["startEpoch"]
	if !ok || startE == nil {
		pastXhours = uint64(7 * 24)
	} else {
		switch val := startE.(type) {
		case json.Number:
			temp, _ := val.Int64()
			pastXhours = uint64(temp)
		case float64:
			pastXhours = uint64(val)
		case int64:
			pastXhours = uint64(val)
		case uint64:
			pastXhours = uint64(val)
		case string:
			defValue := uint64(7 * 24)
			pastXhours, granularity = parseAlphaNumTime(string(val), defValue)
		default:
			pastXhours = uint64(7 * 24)
		}
	}

	// If pastXhours is less than 24, set granularity to ByMinute
	if pastXhours < 24 {
		granularity = usageStats.ByMinute
	}

	return pastXhours, granularity
}
func isTraceRelatedIndex(indexName string) bool {
	for _, value := range excludedInternalIndices {
		if indexName == value {
			return true
		}
	}
	return false
}

func getStats(myid uint64, filterFunc func(string) bool) (map[string]utils.AllIndexesStats, int64, float64, float64) {
	totalBytes := float64(0)
	totalEventCount := int64(0)
	totalOnDiskBytes := float64(0)

	stats := make(map[string]utils.AllIndexesStats)
	allVirtualTableNames, err := vtable.GetVirtualTableNames(myid)
	indices := make([]string, 0)

	for k := range allVirtualTableNames {
		if filterFunc(k) {
			indices = append(indices, k)
		}
	}
	sort.Strings(indices)

	if err != nil {
		log.Errorf("getStats: Error in getting virtual table names, err:%v", err)
	}

	allVTableCounts := segwriter.GetVTableCountsForAll(myid)

	// Read segment metadata file
	smFile := writer.GetLocalSegmetaFName()
	allSegMetas, err := writer.ReadSegmeta(smFile)
	if err != nil {
		log.Errorf("getStats: error when trying to read meta file=%+v. Error=%+v", smFile, err)
		return nil, 0, 0, 0
	}

	// Create a map to store segment counts per index
	segmentCounts := make(map[string]int)
	for _, segMeta := range allSegMetas {
		indexName := segMeta.VirtualTableName
		segmentCounts[indexName]++
	}

	for _, indexName := range indices {
		if indexName == "" {
			log.Errorf("getStats: skipping an empty index name indexName=%v", indexName)
			continue
		}

		counts, ok := allVTableCounts[indexName]
		if !ok {
			// We still want to check for unrotated data, so don't skip this
			// loop iteration.
			counts = &structs.VtableCounts{}
		}

		unrotatedByteCount, unrotatedEventCount, unrotatedOnDiskBytesCount := segwriter.GetUnrotatedVTableCounts(indexName, myid)

		totalEventsForIndex := uint64(counts.RecordCount) + uint64(unrotatedEventCount)
		totalEventCount += int64(totalEventsForIndex)

		totalBytesReceivedForIndex := float64(counts.BytesCount + unrotatedByteCount)
		totalBytes += totalBytesReceivedForIndex

		totalOnDiskBytesCountForIndex := uint64(counts.OnDiskBytesCount + unrotatedOnDiskBytesCount)
		totalOnDiskBytes += float64(totalOnDiskBytesCountForIndex)

		var perIndexStats utils.AllIndexesStats
		perIndexStats.IndexToStats = make(map[string]utils.IndexStats)
		perIndexStats.IndexToStats[indexName] = utils.IndexStats{
			NumBytesIngested: uint64(totalBytesReceivedForIndex),
			NumRecords:       totalEventsForIndex,
			NumSegments:      uint64(segmentCounts[indexName]),
		}

		stats[indexName] = perIndexStats
	}
	return stats, totalEventCount, totalBytes, totalOnDiskBytes
}

func getIngestionStats(myid uint64) (map[string]utils.AllIndexesStats, int64, float64, float64) {
	return getStats(myid, func(indexName string) bool {
		return !isTraceRelatedIndex(indexName)
	})
}

func GetTracesStats(myid uint64) (map[string]utils.AllIndexesStats, int64, float64, float64) {
	return getStats(myid, isTraceRelatedIndex)
}

func convertBytesToGB(bytes float64) string {
	convertedGB := bytes / 1_000_000_000
	finalStr := fmt.Sprintf("%.3f", convertedGB) + " GB"
	return finalStr
}

func GetMetricsStats(myid uint64) (uint64, uint64, uint64) {
	bytesCount := uint64(0)
	onDiskBytesCount := uint64(0)
	recCount := uint64(0)
	allMetricsMetas, err := mmeta.GetAllMetricsMetaEntries(myid)
	if err != nil {
		log.Errorf("populateMetricsMetadata: unable to get all the metrics meta entries. Error: %v", err)
		return bytesCount, recCount, onDiskBytesCount
	}
	for _, mMetaInfo := range allMetricsMetas {
		if mMetaInfo.OrgId == myid {
			onDiskBytesCount += mMetaInfo.OnDiskBytes
			bytesCount += mMetaInfo.BytesReceivedCount
			recCount += mMetaInfo.DatapointCount
		}
	}
	unrotatedIncoming, unrotatedOnDisk, unrotatedRecs := metrics.GetUnrotatedMetricStats(myid)
	bytesCount += unrotatedIncoming
	onDiskBytesCount += unrotatedOnDisk
	recCount += unrotatedRecs
	return bytesCount, recCount, onDiskBytesCount
}

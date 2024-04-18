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
	segwriter "github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/segment/writer/metrics"
	mmeta "github.com/siglens/siglens/pkg/segment/writer/metrics/meta"
	"github.com/siglens/siglens/pkg/usageStats"
	"github.com/siglens/siglens/pkg/utils"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

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
	queryCount, totalResponseTime, querieSinceInstall := usageStats.GetQueryStats(myid)

	metricsIncomingBytes, metricsDatapointsCount, metricsOnDiskBytes := getMetricsStats(myid)
	metricsImMemBytes := metrics.GetTotalEncodedSize()

	if hook := hooks.GlobalHooks.AddMultinodeStatsHook; hook != nil {
		hook(indexData, myid, &logsIncomingBytes, &logsOnDiskBytes, &logsEventCount,
			&metricsIncomingBytes, &metricsOnDiskBytes, &metricsDatapointsCount,
			&queryCount, &totalResponseTime)
	}

	httpResp.IngestionStats = make(map[string]interface{})
	httpResp.QueryStats = make(map[string]interface{})
	httpResp.MetricsStats = make(map[string]interface{})

	httpResp.IngestionStats["Log Incoming Volume"] = convertBytesToGB(logsIncomingBytes)
	httpResp.IngestionStats["Incoming Volume"] = convertBytesToGB(logsIncomingBytes + float64(metricsIncomingBytes))

	httpResp.IngestionStats["Metrics Incoming Volume"] = convertBytesToGB(float64(metricsIncomingBytes))

	httpResp.IngestionStats["Event Count"] = humanize.Comma(int64(logsEventCount))

	httpResp.IngestionStats["Log Storage Used"] = convertBytesToGB(logsOnDiskBytes)
	httpResp.IngestionStats["Metrics Storage Used"] = convertBytesToGB(float64(metricsOnDiskBytes + metricsImMemBytes))
	totalOnDiskBytes := logsOnDiskBytes + float64(metricsOnDiskBytes) + float64(metricsImMemBytes)
	httpResp.IngestionStats["Storage Saved"] = (1 - (totalOnDiskBytes / (logsIncomingBytes + float64(metricsIncomingBytes)))) * 100

	if hook := hooks.GlobalHooks.SetExtraIngestionStatsHook; hook != nil {
		hook(httpResp.IngestionStats)
	}

	httpResp.MetricsStats["Incoming Volume"] = convertBytesToGB(float64(metricsIncomingBytes))
	httpResp.MetricsStats["Datapoints Count"] = humanize.Comma(int64(metricsDatapointsCount))

	httpResp.QueryStats["Query Count"] = queryCount
	httpResp.QueryStats["Queries Since Install"] = querieSinceInstall

	if queryCount > 1 {
		httpResp.QueryStats["Average Latency"] = fmt.Sprintf("%v", utils.ToFixed(totalResponseTime/float64(queryCount), 3)) + " ms"
	} else {
		httpResp.QueryStats["Average Latency"] = fmt.Sprintf("%v", utils.ToFixed(totalResponseTime, 3)) + " ms"
	}

	httpResp.IndexStats = convertIndexDataToSlice(indexData)
	utils.WriteJsonResponse(ctx, httpResp)

}

func convertIndexDataToSlice(indexData map[string]utils.ResultPerIndex) []utils.ResultPerIndex {
	retVal := make([]utils.ResultPerIndex, 0, len(indexData))
	i := 0
	for idx, v := range indexData {
		nextVal := make(utils.ResultPerIndex)
		nextVal[idx] = make(map[string]interface{})
		nextVal[idx]["ingestVolume"] = convertBytesToGB(v[idx]["ingestVolume"].(float64))
		nextVal[idx]["eventCount"] = humanize.Comma(int64(v[idx]["eventCount"].(uint64)))
		retVal = append(retVal, nextVal)
		i++
	}
	return retVal[:i]
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
		httpResp.ChartStats[k]["EventCount"] = entry.EventCount
		httpResp.ChartStats[k]["MetricsCount"] = entry.MetricsDatapointsCount
		httpResp.ChartStats[k]["GBCount"] = float64(entry.BytesCount) / 1_000_000_000
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
	return pastXhours, granularity
}

func getIngestionStats(myid uint64) (map[string]utils.ResultPerIndex, int64, float64, float64) {

	totalIncomingBytes := float64(0)
	totalEventCount := int64(0)
	totalOnDiskBytes := float64(0)

	ingestionStats := make(map[string]utils.ResultPerIndex)
	allVirtualTableNames, err := vtable.GetVirtualTableNames(myid)
	sortedIndices := make([]string, 0, len(allVirtualTableNames))

	for k := range allVirtualTableNames {
		sortedIndices = append(sortedIndices, k)
	}
	sort.Strings(sortedIndices)

	if err != nil {
		log.Errorf("getIngestionStats: Error in getting virtual table names, err:%v", err)
	}

	allvtableCnts := segwriter.GetVTableCountsForAll(myid)

	for _, indexName := range sortedIndices {
		if indexName == "" {
			log.Errorf("getIngestionStats: skipping an empty index name indexName=%v", indexName)
			continue
		}

		cnts, ok := allvtableCnts[indexName]
		if !ok {
			continue
		}

		unrotatedByteCount, unrotatedEventCount, unrotatedOnDiskBytesCount := segwriter.GetUnrotatedVTableCounts(indexName, myid)

		totalEventsForIndex := uint64(cnts.RecordCount) + uint64(unrotatedEventCount)
		totalEventCount += int64(totalEventsForIndex)

		totalBytesReceivedForIndex := float64(cnts.BytesCount + unrotatedByteCount)
		totalIncomingBytes += totalBytesReceivedForIndex

		totalOnDiskBytesCountForIndex := uint64(cnts.OnDiskBytesCount + unrotatedOnDiskBytesCount)
		totalOnDiskBytes += float64(totalOnDiskBytesCountForIndex)

		perIndexStat := make(map[string]map[string]interface{})

		perIndexStat[indexName] = make(map[string]interface{})

		perIndexStat[indexName]["ingestVolume"] = totalBytesReceivedForIndex
		perIndexStat[indexName]["eventCount"] = totalEventsForIndex

		ingestionStats[indexName] = perIndexStat
	}
	return ingestionStats, totalEventCount, totalIncomingBytes, totalOnDiskBytes
}

func convertBytesToGB(bytes float64) string {
	convertedGB := bytes / 1_000_000_000
	finalStr := fmt.Sprintf("%.3f", convertedGB) + " GB"
	return finalStr
}

func getMetricsStats(myid uint64) (uint64, uint64, uint64) {
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

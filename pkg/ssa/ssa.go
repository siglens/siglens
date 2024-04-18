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

package ssa

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/analytics-go/v3"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/localnodeid"
	"github.com/siglens/siglens/pkg/segment/writer"
	segwriter "github.com/siglens/siglens/pkg/segment/writer"
	mmeta "github.com/siglens/siglens/pkg/segment/writer/metrics/meta"
	"github.com/siglens/siglens/pkg/usageStats"
	"github.com/siglens/siglens/pkg/utils"
	"github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
)

const kubernetes = "kubernetes"
const docker = "docker"
const binary = "binary"

var client analytics.Client = nil
var ssaStarted = false
var segmentKey string = "BPDjnefPV0Jc2BRGdGh7CQTnykYKbD8c"
var userId = ""
var IPAddressInfo IPAddressDetails
var source = "computerID"

type IPAddressDetails struct {
	IP        string  `json:"ip"`
	City      string  `json:"city"`
	Region    string  `json:"region"`
	Country   string  `json:"country"`
	Loc       string  `json:"loc"`
	Latitude  float64 `json:"-"`
	Longitude float64 `json:"-"`
	Timezone  string  `json:"timezone"`
}

type silentLogger struct {
}

func (sl *silentLogger) Logf(format string, args ...interface{}) {
}

func (sl *silentLogger) Errorf(format string, args ...interface{}) {
}
func FetchIPAddressDetails() (IPAddressDetails, error) {
	var details IPAddressDetails
	resp, err := http.Get("https://ipinfo.io")
	if err != nil {
		log.Errorf("Failed to fetch IP address details: %v", err)
		return details, err
	}
	defer resp.Body.Close()

	if err := json.NewDecoder(resp.Body).Decode(&details); err != nil {
		log.Errorf("Failed to decode IP address details: %v", err)
		return details, err
	}

	// Parse latitude and longitude from Loc
	locParts := strings.Split(details.Loc, ",")
	if len(locParts) == 2 {
		if lat, err := strconv.ParseFloat(locParts[0], 64); err == nil {
			details.Latitude = lat
		} else {
			log.Errorf("Failed to parse latitude: %v", err)
		}
		if lon, err := strconv.ParseFloat(locParts[1], 64); err == nil {
			details.Longitude = lon
		} else {
			log.Errorf("Failed to parse longitude: %v", err)
		}
	} else {
		log.Errorf("Failed to parse location: %v", details.Loc)
	}

	log.Infof("Successfully fetched and decoded IP address details")

	return details, nil
}
func InitSsa() {

	currClient, err := analytics.NewWithConfig(segmentKey,
		analytics.Config{
			Verbose: false,
			Logger:  &silentLogger{},
		},
	)

	if err != nil {
		log.Errorf("Error initializing ssa: %v", err)
		return
	}
	ipDetails, err := FetchIPAddressDetails()
	if err != nil {
		log.Errorf("Failed to fetch IP address details: %v", err)
	}

	IPAddressInfo = ipDetails
	client = currClient

	timestampFilePath := path.Join(config.GetDataPath(), "install_time.txt")
	_, err = os.Stat(timestampFilePath)
	if os.IsNotExist(err) {
		// If the file does not exist, get the oldest segment epoch
		oldestSegmentEpoch, err := GetOldestSegmentEpoch(config.GetCurrentNodeIngestDir(), 0)
		if err != nil || oldestSegmentEpoch == 0 || oldestSegmentEpoch == uint64(math.MaxUint64) {
			// If there's an error getting the oldest segment epoch or it's 0 or it's MaxUint64, use the current timestamp
			oldestSegmentEpoch = uint64(time.Now().UnixMilli())
		}

		// Write the timestamp to the file (Unix time in milliseconds)
		err = os.WriteFile(timestampFilePath, []byte(strconv.FormatInt(int64(oldestSegmentEpoch), 10)), 0644)
		if err != nil {
			log.Errorf("InitSsa: Failed to write timestamp to file: %v", err)
		}
	} else if err != nil {
		log.Errorf("InitSsa: Failed to check if timestamp file exists: %v", err)
	}

	go waitForInitialEvent()
}

func GetOldestSegmentEpoch(ingestNodeDir string, orgid uint64) (uint64, error) {
	// Read segmeta entries
	currentSegmeta := path.Join(ingestNodeDir, writer.SegmetaSuffix)
	allSegMetas, err := writer.ReadSegmeta(currentSegmeta)
	if err != nil {
		log.Errorf("GetOldestSegmentEpoch: Failed to read segmeta, err: %v", err)
		return 0, err
	}

	// Read metrics meta entries
	currentMetricsMeta := path.Join(ingestNodeDir, mmeta.MetricsMetaSuffix)
	allMetricMetas, err := mmeta.ReadMetricsMeta(currentMetricsMeta)
	if err != nil {
		log.Errorf("GetOldestSegmentEpoch: Failed to get all metric meta entries, err: %v", err)
		return 0, err
	}

	// Combine metrics and segments
	oldest := uint64(math.MaxUint64)
	for _, segMeta := range allSegMetas {
		if segMeta.OrgId == orgid && segMeta.LatestEpochMS < oldest {
			oldest = segMeta.LatestEpochMS
		}
	}

	// Find the oldest metric meta
	for _, metricMeta := range allMetricMetas {
		metricMetaEpochMS := uint64(metricMeta.LatestEpochSec) * 1000 // convert to milliseconds
		if metricMeta.OrgId == orgid && metricMetaEpochMS < oldest {
			oldest = metricMetaEpochMS
		}
	}

	return oldest, nil
}
func waitForInitialEvent() {
	time.Sleep(2 * time.Minute)

	traits := analytics.NewTraits()
	props := analytics.NewProperties()

	// Initialize computer-specific identifier
	if userId = os.Getenv("CSI"); userId != "" {
		source = "CSI"
	} else {
		var err error
		userId, err = utils.GetSpecificIdentifier()
		if err != nil {
			log.Errorf("waitForInitialEvent: %v", err)
			userId = "unknown"
		}
	}
	baseInfo := getBaseInfo()
	for k, v := range baseInfo {
		traits.Set(k, v)
		props.Set(k, v)
	}
	props.Set("id_source", source)
	_ = client.Enqueue(analytics.Identify{
		UserId: userId,
		Traits: traits,
	})
	if localnodeid.IsInitServer() {
		_ = client.Enqueue(analytics.Track{
			Event:      "server startup",
			UserId:     userId,
			Properties: props,
		})
	} else {
		_ = client.Enqueue(analytics.Track{
			Event:      "server restart",
			UserId:     userId,
			Properties: props,
		})
	}
	go startSsa()
}

func StopSsa() {

	if client == nil || !ssaStarted {
		return
	}
	props := make(map[string]interface{})
	props["runtime_os"] = runtime.GOOS
	props["runtime_arch"] = runtime.GOARCH
	populateDeploymentSsa(props)
	populateIngestSsa(props)
	populateQuerySsa(props)
	_ = client.Enqueue(analytics.Track{
		Event:      "server shutdown",
		UserId:     userId,
		Properties: props,
	})
	err := client.Close()
	if err != nil {
		log.Debugf("Failed to stop ssa module! Error: %v", err)
	}
}

func startSsa() {
	ssaStarted = true
	for {
		flushSsa()
		time.Sleep(time.Hour * 3)
	}
}

func flushSsa() {
	// Initialize days with a default value of -1
	days := -1
	allSsa := getSsa()
	props := analytics.NewProperties()
	for k, v := range allSsa {
		props.Set(k, v)
	}
	props.Set("runtime_os", runtime.GOOS)
	props.Set("runtime_arch", runtime.GOARCH)
	props.Set("id_source", source)
	timestampFilePath := path.Join(config.GetDataPath(), "install_time.txt")
	// Read the timestamp from the file (Unix time in milliseconds)
	data, err := os.ReadFile(timestampFilePath)
	if err != nil {
		log.Errorf("Failed to read timestamp from file: %v", err)
	} else {
		timestamp, err := strconv.ParseInt(string(data), 10, 64)
		if err != nil {
			log.Errorf("Failed to parse timestamp: %v", err)
		} else {
			days = int(time.Now().UnixMilli()-timestamp) / (60 * 60 * 24 * 1000)
		}
	}
	props.Set("install_age", days)
	_ = client.Enqueue(analytics.Track{
		Event:      "server status",
		UserId:     userId,
		Properties: props,
	})
}

func getSsa() map[string]interface{} {

	ssa := make(map[string]interface{})
	populateDeploymentSsa(ssa)
	populateIngestSsa(ssa)
	populateQuerySsa(ssa)
	return ssa
}

func getBaseInfo() map[string]interface{} {

	m := make(map[string]interface{})
	mem, _ := mem.VirtualMemory()
	cpuCount, _ := cpu.Counts(true)
	zone, _ := time.Now().Local().Zone()

	m["runtime_os"] = runtime.GOOS
	m["runtime_arch"] = runtime.GOARCH
	m["time_zone"] = zone
	m["cpu_count"] = cpuCount
	m["total_memory_gb"] = mem.Total / (1000 * 1000 * 1000)
	m["company_name"] = "OSS"
	m["ip"] = IPAddressInfo.IP
	m["city"] = IPAddressInfo.City
	m["region"] = IPAddressInfo.Region
	m["country"] = IPAddressInfo.Country
	return m
}

func populateDeploymentSsa(m map[string]interface{}) {
	m["uptime_minutes"] = math.Round(time.Since(utils.GetServerStartTime()).Minutes())
	m["retention_hours"] = config.GetRetentionHours()
	m["company_name"] = "OSS"
	m["version"] = config.SigLensVersion
	m["deployment_type"] = getDeploymentType()
	m["ip"] = IPAddressInfo.IP
	m["city"] = IPAddressInfo.City
	m["region"] = IPAddressInfo.Region
	m["country"] = IPAddressInfo.Country
}

func populateIngestSsa(m map[string]interface{}) {
	allVirtualTableNames, _ := virtualtable.GetVirtualTableNames(0)

	totalEventCount := uint64(0)
	totalOnDiskBytes := uint64(0)
	totalIncomingBytes := uint64(0)

	largestIndexEventCount := uint64(0)
	for indexName := range allVirtualTableNames {
		if indexName == "" {
			log.Debugf("populateIngestSsa: one of nil indexName=%v", indexName)
			continue
		}
		bytesReceivedCount, eventCount, onDiskBytesCount := segwriter.GetVTableCounts(indexName, 0)
		unrotatedBytesCount, unrotatedEventCount, unrotatedOnDiskBytesCount := segwriter.GetUnrotatedVTableCounts(indexName, 0)
		bytesReceivedCount += unrotatedBytesCount
		eventCount += unrotatedEventCount
		onDiskBytesCount += unrotatedOnDiskBytesCount
		totalEventCount += uint64(eventCount)
		totalOnDiskBytes += onDiskBytesCount
		totalIncomingBytes += bytesReceivedCount

		if totalEventCount > largestIndexEventCount {
			largestIndexEventCount = totalEventCount
		}
	}
	m["total_event_count"] = totalEventCount
	m["total_on_disk_bytes"] = totalOnDiskBytes
	m["total_incoming_bytes"] = totalIncomingBytes
	m["total_table_count"] = len(allVirtualTableNames)
	m["largest_index_event_count"] = largestIndexEventCount
	m["ip"] = IPAddressInfo.IP
	m["city"] = IPAddressInfo.City
	m["region"] = IPAddressInfo.Region
	m["country"] = IPAddressInfo.Country
}

func populateQuerySsa(m map[string]interface{}) {
	queryCount, totalResponseTime, querieSinceInstall := usageStats.GetQueryStats(0)
	m["num_queries"] = queryCount
	m["queries_since_install"] = querieSinceInstall
	m["ip"] = IPAddressInfo.IP
	m["city"] = IPAddressInfo.City
	m["region"] = IPAddressInfo.Region
	m["country"] = IPAddressInfo.Country
	if queryCount > 1 {
		m["avg_query_latency_ms"] = fmt.Sprintf("%v", utils.ToFixed(totalResponseTime/float64(queryCount), 3)) + " ms"
	} else {
		m["avg_query_latency_ms"] = fmt.Sprintf("%v", utils.ToFixed(totalResponseTime, 3)) + " ms"
	}
}

func getDeploymentType() string {
	if _, exists := os.LookupEnv("KUBERNETES_SERVICE_HOST"); exists {
		return kubernetes
	}

	if _, exists := os.LookupEnv("DOCKER_HOST"); exists {
		return docker
	}

	return binary
}

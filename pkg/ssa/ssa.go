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

package ssa

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/analytics-go/v3"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/localnodeid"
	segwriter "github.com/siglens/siglens/pkg/segment/writer"
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
		log.Fatalf("Failed to fetch IP address details: %v", err)
	}

	IPAddressInfo = ipDetails
	client = currClient
	go waitForInitialEvent()
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

	allSsa := getSsa()
	props := analytics.NewProperties()
	for k, v := range allSsa {
		props.Set(k, v)
	}
	props.Set("runtime_os", runtime.GOOS)
	props.Set("runtime_arch", runtime.GOARCH)
	props.Set("id_source", source)
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

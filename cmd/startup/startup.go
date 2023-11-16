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

package startup

import (
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"os"
	"time"

	"github.com/siglens/siglens/pkg/alerts/alertsHandler"
	"github.com/siglens/siglens/pkg/ast/pipesearch"
	"github.com/siglens/siglens/pkg/blob"
	local "github.com/siglens/siglens/pkg/blob/local"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/dashboards"
	"github.com/siglens/siglens/pkg/instrumentation"
	"github.com/siglens/siglens/pkg/querytracker"
	"github.com/siglens/siglens/pkg/retention"
	"github.com/siglens/siglens/pkg/scroll"
	"github.com/siglens/siglens/pkg/segment/memory/limit"
	tracinghandler "github.com/siglens/siglens/pkg/segment/tracing/handler"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/segment/writer/metrics"
	ingestserver "github.com/siglens/siglens/pkg/server/ingest"
	queryserver "github.com/siglens/siglens/pkg/server/query"
	"github.com/siglens/siglens/pkg/ssa"
	"github.com/siglens/siglens/pkg/usageStats"
	usq "github.com/siglens/siglens/pkg/usersavedqueries"
	"github.com/siglens/siglens/pkg/utils"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

var StdOutLogger *log.Logger

func init() {
	StdOutLogger = &log.Logger{
		Out:       os.Stderr,
		Formatter: new(log.TextFormatter),
		Hooks:     make(log.LevelHooks),
		Level:     log.InfoLevel,
	}
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	StdOutLogger.SetFormatter(customFormatter)
}

// Licenses should be checked outside of this function
func StartSiglensServer(nodeType config.DeploymentType, nodeID string) error {
	err := alertsHandler.ConnectSiglensDB()
	if err != nil {
		log.Errorf("Failed to connect to siglens database, err: %v", err)
		fmt.Printf("Failed to connect to siglens database, err: %v\n", err)
		return err
	}

	limit.InitMemoryLimiter()
	if nodeID == "" {
		return fmt.Errorf("nodeID cannot be empty")
	}

	usageStats.StartUsageStats()
	ingestNode := config.IsIngestNode()
	queryNode := config.IsQueryNode()
	ingestServer := "0.0.0.0:" + fmt.Sprintf("%d", config.GetIngestPort())
	queryServer := "0.0.0.0:" + fmt.Sprintf("%d", config.GetQueryPort())

	if config.IsTlsEnabled() && config.GetQueryPort() != 443 {
		fmt.Printf("Error starting Query/UI server with TLS, QueryPort should be set to 443 ")
		log.Errorf("Error starting Query/UI server with TLS, QueryPort should be set to 443 ")
		return errors.New("error starting Query/UI server with TLS, QueryPort should be set to 443 ")
	}

	err = vtable.InitVTable()
	if err != nil {
		log.Fatalf("error in InitVTable: %v", err)
	}

	log.Infof("StartSiglensServer: Initialilizing Blob Store")
	err = blob.InitBlobStore()
	if err != nil {
		log.Errorf("StartSiglensServer: Error initializing S3: %v", err)
		return err
	}

	ssa.InitSsa()

	err = usq.InitUsq()
	if err != nil {
		log.Errorf("error in init UserSavedQueries: %v", err)
		return err
	}
	err = retention.InitRetentionCleaner()
	if err != nil {
		log.Errorf("error in init retention cleaner: %v", err)
		return err
	}
	err = dashboards.InitDashboards()
	if err != nil {
		log.Errorf("error in init Dashboards: %v", err)
		return err
	}

	siglensStartupLog := fmt.Sprintf("----- Siglens server type %s starting up ----- \n", nodeType)
	if config.GetLogPrefix() != "" {
		StdOutLogger.Infof(siglensStartupLog)
	}
	log.Infof(siglensStartupLog)
	if queryNode {
		err := usq.InitUsq()
		if err != nil {
			log.Errorf("error in init UserSavedQueries: %v", err)
			return err
		}

		err = dashboards.InitDashboards()
		if err != nil {
			log.Errorf("error in init Dashboards: %v", err)
			return err
		}
	}

	if ingestNode {
		startIngestServer(ingestServer)
	}
	if queryNode {
		startQueryServer(queryServer)
	}
	go makeTracesDependancyGraph()
	go mock()

	instrumentation.InitMetrics()
	querytracker.InitQT()

	alertsHandler.InitAlertingService()
	alertsHandler.InitMinionSearchService()
	go tracinghandler.MonitorSpansHealth()

	return nil
}

func ShutdownSiglensServer() {
	// force write unsaved data to segfile and flush bloom, range, updates to meta
	writer.ForcedFlushToSegfile()
	metrics.ForceFlushMetricsBlock()
	err := vtable.FlushAliasMapToFile()
	if err != nil {
		log.Errorf("flushing of aliasmap file failed, err=%v", err)
	}
	local.ForceFlushSegSetKeysToFile()
	scroll.ForcedFlushToScrollFile()
	ssa.StopSsa()
	usageStats.ForceFlushStatstoFile()
	alertsHandler.Disconnect()
}

type RawSpanData struct {
	Hits RawSpanResponse `json:"hits"`
}

type RawSpanResponse struct {
	Spans []*Span `json:"records"`
}

type Span struct {
	TraceID      string `json:"trace_id"`
	SpanID       string `json:"span_id"`
	ParentSpanID string `json:"parent_span_id"`
	StartTime    uint64 `json:"start_time"`
	EndTime      uint64 `json:"end_time"`
	Duration     uint64 `json:"duration"`
	Status       string `json:"status"`
	Service      string `json:"service"`
}

func FetchSpanData(indexName string, startEpoch, endEpoch uint64, searchText string, myid uint64) ([]*Span, error) {
	requestBody := map[string]interface{}{
		"indexName":  indexName,
		"startEpoch": startEpoch,
		"endEpoch":   endEpoch,
		"searchText": searchText,
	}
	requestBodyJSON, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("Error marshaling request body: %v", err)
	}

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetBody(requestBodyJSON)
	ctx.Request.Header.SetMethod("POST")

	pipesearch.ProcessPipeSearchRequest(ctx, myid)

	var rawSpanData RawSpanData
	if err := json.Unmarshal(ctx.Response.Body(), &rawSpanData); err != nil {
		return nil, fmt.Errorf("could not unmarshal json body: %v", err)
	}
	return rawSpanData.Hits.Spans, nil
}

func CalculateDependencyMatrix(spans []*Span) map[string]map[string]int {
	spanIdToServiceName := make(map[string]string)
	dependencyMatrix := make(map[string]map[string]int)

	for _, span := range spans {
		spanIdToServiceName[span.SpanID] = span.Service
	}
	for _, span := range spans {
		if span.ParentSpanID == "" {
			continue
		}

		parentService, parentExists := spanIdToServiceName[span.ParentSpanID]
		if !parentExists || parentService == span.Service {
			continue
		}

		if dependencyMatrix[parentService] == nil {
			dependencyMatrix[parentService] = make(map[string]int)
		}
		dependencyMatrix[parentService][span.Service]++
	}

	return dependencyMatrix
}
func makeTracesDependancyGraph() {
	time.Sleep(10 * time.Second)
	indexName := "traces"
	nowTs := utils.GetCurrentTimeInMs()
	startEpoch := nowTs - (60 * 60 * 1000)
	endEpoch := nowTs
	searchText := "*"
	var myid uint64

	spans, err := FetchSpanData(indexName, startEpoch, endEpoch, searchText, myid)
	if err != nil {
		fmt.Println(err)
		return
	}

	dependencyMatrix := CalculateDependencyMatrix(spans)
	dependencyMatrixJSON, err := json.Marshal(dependencyMatrix)
	if err != nil {
		fmt.Println("Error marshaling dependency matrix:", err)
		return
	}
	fmt.Println("Dependency Matrix:", string(dependencyMatrixJSON))
}

func mock() {
	time.Sleep(10 * time.Second)
	services := []string{"ServiceA", "ServiceB", "ServiceC"}
	spans := []*Span{
		{SpanID: "1", ParentSpanID: "", Service: services[0]},
		{SpanID: "2", ParentSpanID: "1", Service: services[1]},
		{SpanID: "3", ParentSpanID: "2", Service: services[2]},
		{SpanID: "4", ParentSpanID: "1", Service: services[2]},
	}

	dependencyMatrix := CalculateDependencyMatrix(spans)
	dependencyMatrixJSON, err := json.Marshal(dependencyMatrix)
	if err != nil {
		fmt.Println("Error marshaling dependency matrix:", err)
		return
	}
	fmt.Println("Dependency Matrix:", string(dependencyMatrixJSON))
}

func startIngestServer(serverAddr string) {
	siglensStartupLog := fmt.Sprintf("----- Siglens Ingestion server starting on %s ----- \n", serverAddr)
	if config.GetLogPrefix() != "" {
		StdOutLogger.Infof(siglensStartupLog)
	}
	log.Infof(siglensStartupLog)
	cfg := config.DefaultIngestionHttpConfig()
	s := ingestserver.ConstructIngestServer(cfg, serverAddr)
	if config.IsSafeMode() {
		go func() {
			err := s.RunSafeServer()
			if err != nil {
				log.Errorf("Failed to start mock server! Error: %v", err)
				return
			}
		}()
	} else {
		go func() {
			err := s.Run()
			if err != nil {
				log.Errorf("Failed to start server! Error: %v", err)
			}
		}()
	}
}

func startQueryServer(serverAddr string) {
	siglensStartupLog := fmt.Sprintf("----- Siglens Query server starting on %s ----- \n", serverAddr)
	siglensUIStartupLog := fmt.Sprintf("----- Siglens UI starting on %s ----- \n", serverAddr)
	if config.GetLogPrefix() != "" {
		StdOutLogger.Infof(siglensStartupLog)
		StdOutLogger.Infof(siglensUIStartupLog)
	}
	log.Infof(siglensStartupLog)
	log.Infof(siglensUIStartupLog)
	cfg := config.DefaultQueryServerHttpConfig()
	s := queryserver.ConstructQueryServer(cfg, serverAddr)
	if config.IsSafeMode() {
		go func() {
			err := s.RunSafeServer()
			if err != nil {
				log.Errorf("Failed to start mock server! Error: %v", err)
				return
			}
		}()
	} else {
		go func() {
			tpl := template.Must(template.ParseGlob("./static/*.html"))
			err := s.Run(tpl)
			if err != nil {
				log.Errorf("Failed to start server! Error: %v", err)
			}
		}()
	}
}

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
	"errors"
	"fmt"
	"html/template"
	"os"

	"github.com/siglens/siglens/pkg/alerts/alertsHandler"
	"github.com/siglens/siglens/pkg/blob"
	local "github.com/siglens/siglens/pkg/blob/local"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/dashboards"
	"github.com/siglens/siglens/pkg/instrumentation"
	"github.com/siglens/siglens/pkg/querytracker"
	"github.com/siglens/siglens/pkg/retention"
	"github.com/siglens/siglens/pkg/scroll"
	"github.com/siglens/siglens/pkg/segment/memory/limit"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/segment/writer/metrics"
	ingestserver "github.com/siglens/siglens/pkg/server/ingest"
	queryserver "github.com/siglens/siglens/pkg/server/query"
	"github.com/siglens/siglens/pkg/ssa"
	"github.com/siglens/siglens/pkg/usageStats"
	usq "github.com/siglens/siglens/pkg/usersavedqueries"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
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

	instrumentation.InitMetrics()
	querytracker.InitQT()

	alertsHandler.InitAlertingService()
	alertsHandler.InitMinionSearchService()

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

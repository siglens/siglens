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
	"os/signal"
	"syscall"
	"time"

	"github.com/siglens/siglens/pkg/alerts/alertsHandler"
	"github.com/siglens/siglens/pkg/blob"
	local "github.com/siglens/siglens/pkg/blob/local"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/dashboards"
	"github.com/siglens/siglens/pkg/instrumentation"
	"github.com/siglens/siglens/pkg/localnodeid"
	"github.com/siglens/siglens/pkg/lookuptable"
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
	"gopkg.in/natefinch/lumberjack.v2"
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

func initlogger() {
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	log.SetFormatter(customFormatter)
	customFormatter.FullTimestamp = true
}

func Main() {
	if hook := lookuptable.GlobalLookupTable.StartupHook; hook != nil {
		hook()
	}
	log.Errorf("Finished StartupHook")

	initlogger()
	utils.SetServerStartTime(time.Now())
	err := config.InitConfigurationData()
	if err != nil {
		log.Error("Failed to initialize config! Exiting to avoid misconfigured server...")
		os.Exit(1)
	}

	nodeType, err := config.ValidateDeployment()
	if err != nil {
		log.Errorf("Invalid deployment type! Error=[%+v]", err)
		os.Exit(1)
	}

	nodeID := localnodeid.GetRunningNodeID()
	err = config.InitDerivedConfig(nodeID)
	if err != nil {
		log.Errorf("Error initializing derived configurations! %v", err)
		os.Exit(1)
	}

	serverCfg := *config.GetRunningConfig() // Init the Configuration
	var logOut string
	if config.GetLogPrefix() == "" {
		logOut = "stdout"
	} else {
		logOut = serverCfg.Log.LogPrefix + "siglens.log"
	}
	baseLogDir := serverCfg.Log.LogPrefix
	if baseLogDir == "" {
		log.SetOutput(os.Stdout)
	} else {
		err := os.MkdirAll(baseLogDir, 0764)
		if err != nil {
			log.Fatalf("failed to make log directory at=%v, err=%v", baseLogDir, err)
		}
		log.SetOutput(&lumberjack.Logger{
			Filename:   logOut,
			MaxSize:    serverCfg.Log.LogFileRotationSizeMB,
			MaxBackups: 30,
			MaxAge:     1, //days
			Compress:   serverCfg.Log.CompressLogFile,
		})
	}
	if config.IsDebugMode() {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	log.Infof("----- Siglens server type %s starting up.... ----- \n", nodeType.String())
	log.Infof("----- Siglens server logging to %s ----- \n", logOut)

	configJSON, err := json.MarshalIndent(serverCfg, "", "  ")
	if err != nil {
		log.Errorf("main : Error marshalling config struct %v", err.Error())
	}
	log.Infof("Running config %s", string(configJSON))

	err = StartSiglensServer(nodeType, nodeID)
	if err != nil {
		ShutdownSiglensServer()
		if baseLogDir != "" {
			StdOutLogger.Errorf("siglens main: Error in starting server:%v ", err)
		}
		log.Errorf("siglens main: Error in starting server:%v ", err)
		os.Exit(1)
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	switch <-ch {
	case os.Interrupt, os.Kill, syscall.SIGTERM, syscall.SIGINT:
		log.Errorf("Interrupt signal received. Exiting server...")
		ShutdownSiglensServer()
		log.Errorf("Server shutdown")
		os.Exit(0)
	default:
		log.Errorf("Something went wrong. Exiting server...")
		ShutdownSiglensServer()
		log.Errorf("Server shutdown")
		os.Exit(1)
	}
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
	go tracinghandler.MonitorSpansHealth()
	go tracinghandler.DependencyGraphThread()

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
			templateHook := lookuptable.GlobalLookupTable.TemplateHook
			if templateHook == nil {
				log.Fatalf("startQueryServer: TemplateHook is nil")
			}

			tpl := template.New("html").Funcs(template.FuncMap{
				"safeHTML": func(htmlContent string) template.HTML {
					return template.HTML(htmlContent)
				},
			})
			templateHook(tpl)

			err := s.Run(tpl)
			if err != nil {
				log.Errorf("Failed to start server! Error: %v", err)
			}
		}()
	}
}

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
	htmltemplate "html/template"
	"net"
	"os"
	"os/signal"
	"syscall"
	texttemplate "text/template"
	"time"

	"github.com/siglens/siglens/pkg/alerts/alertsHandler"
	"github.com/siglens/siglens/pkg/blob"
	local "github.com/siglens/siglens/pkg/blob/local"
	"github.com/siglens/siglens/pkg/config"
	commonconfig "github.com/siglens/siglens/pkg/config/common"
	"github.com/siglens/siglens/pkg/dashboards"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/instrumentation"
	"github.com/siglens/siglens/pkg/localnodeid"
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
	if hook := hooks.GlobalHooks.StartupHook; hook != nil {
		hook()
	}

	initlogger()
	utils.SetServerStartTime(time.Now())
	err := config.InitConfigurationData()
	if err != nil {
		log.Error("Failed to initialize config! Exiting to avoid misconfigured server...")
		os.Exit(1)
	}

	validateDeploymentHook := hooks.GlobalHooks.ValidateDeploymentHook
	if validateDeploymentHook == nil {
		validateDeploymentHook = config.ValidateDeployment
	}

	nodeType, err := validateDeploymentHook()
	if err != nil {
		log.Errorf("Invalid deployment type! Error=[%+v]", err)
		os.Exit(1)
	}

	getNodeIdHook := hooks.GlobalHooks.GetNodeIdHook
	if getNodeIdHook == nil {
		getNodeIdHook = localnodeid.GetRunningNodeID
	}

	nodeID := getNodeIdHook()
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

	if hook := hooks.GlobalHooks.CheckLicenseHook; hook != nil {
		hook()
	}

	if hook := hooks.GlobalHooks.LogConfigHook; hook != nil {
		hook()
	} else {
		configJSON, err := json.MarshalIndent(serverCfg, "", "  ")
		if err != nil {
			log.Errorf("main : Error marshalling config struct %v", err.Error())
		}
		log.Infof("Running config %s", string(configJSON))
	}

	if hook := hooks.GlobalHooks.AfterConfigHook; hook != nil {
		hook(baseLogDir)
	}

	err = StartSiglensServer(nodeType, nodeID)
	if err != nil {
		ShutdownSiglensServer()
		if baseLogDir != "" {
			StdOutLogger.Errorf("siglens main: Error in starting server:%v ", err)
		}
		log.Errorf("siglens main: Error in starting server:%v ", err)
		os.Exit(1)
	}
	if hook := hooks.GlobalHooks.CheckOrgValidityHook; hook != nil {
		hook()
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
func StartSiglensServer(nodeType commonconfig.DeploymentType, nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("nodeID cannot be empty")
	}

	if hook := hooks.GlobalHooks.StartSiglensExtrasHook; hook != nil {
		err := hook(nodeID)
		if err != nil {
			return err
		}
	}

	err := alertsHandler.ConnectSiglensDB()
	if err != nil {
		log.Errorf("Failed to connect to siglens database, err: %v", err)
		fmt.Printf("Failed to connect to siglens database, err: %v\n", err)
		return err
	}

	limit.InitMemoryLimiter()

	usageStats.StartUsageStats()
	ingestNode := config.IsIngestNode()
	queryNode := config.IsQueryNode()
	ingestServer := fmt.Sprint(config.GetIngestListenIP()) + ":" + fmt.Sprintf("%d", config.GetIngestPort())
	queryServer := fmt.Sprint(config.GetQueryListenIP()) + ":" + fmt.Sprintf("%d", config.GetQueryPort())

	if config.IsTlsEnabled() && (config.GetTLSCertificatePath() == "" || config.GetTLSPrivateKeyPath() == "") {
		fmt.Println("TLS is enabled but certificate or private key path is not provided")
		log.Fatalf("TLS is enabled but certificate or private key path is not provided")
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
	go func() {
		var err error
		if config.IsSafeMode() {
			err = s.RunSafeServer()
		} else {
			err = s.Run()
		}
		if err != nil {
			var opErr *net.OpError
			if errors.As(err, &opErr) {
				if opErr.Op == "listen" {
					StdOutLogger.Errorf("Failed to start server: %v", err)
					os.Exit(1)
				}
			}
		}
	}()
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
	go func() {
		var err error
		if config.IsSafeMode() {
			err = s.RunSafeServer()
		} else {
			htmlTemplate := htmltemplate.New("html").Funcs(htmltemplate.FuncMap{
				"safeHTML": func(htmlContent string) htmltemplate.HTML {
					return htmltemplate.HTML(htmlContent)
				},
			})
			textTemplate := texttemplate.New("other")

			parseTemplatesHook := hooks.GlobalHooks.ParseTemplatesHook
			if parseTemplatesHook == nil {
				log.Fatalf("startQueryServer: ParseTemplatesHook is nil")
			}
			parseTemplatesHook(htmlTemplate, textTemplate)

			err = s.Run(htmlTemplate, textTemplate)
		}
		if err != nil {
			var opErr *net.OpError
			if errors.As(err, &opErr) {
				if opErr.Op == "listen" {
					StdOutLogger.Errorf("Failed to start server: %v", err)
					os.Exit(1)
				}
			}
		}
	}()
}

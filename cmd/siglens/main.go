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

package main

import (
	"encoding/json"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/siglens/siglens/cmd/startup"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/localnodeid"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

/*

	Main function to start both siglens and ui server with a single command

*/

func initlogger() {
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	log.SetFormatter(customFormatter)
	customFormatter.FullTimestamp = true
}

func main() {
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

	err = startup.StartSiglensServer(nodeType, nodeID)
	if err != nil {
		startup.ShutdownSiglensServer()
		if baseLogDir != "" {
			startup.StdOutLogger.Errorf("siglens main: Error in starting server:%v ", err)
		}
		log.Errorf("siglens main: Error in starting server:%v ", err)
		os.Exit(1)
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	switch <-ch {
	case os.Interrupt, os.Kill, syscall.SIGTERM, syscall.SIGINT:
		log.Errorf("Interrupt signal received. Exiting server...")
		startup.ShutdownSiglensServer()
		log.Errorf("Server shutdown")
		os.Exit(0)
	default:
		log.Errorf("Something went wrong. Exiting server...")
		startup.ShutdownSiglensServer()
		log.Errorf("Server shutdown")
		os.Exit(1)
	}
}

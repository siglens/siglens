// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package fileutils

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	log "github.com/sirupsen/logrus"
)

var (
	QueryLogFile  *os.File
	AccessLogFile *os.File
	fileMutex     sync.Mutex
	columnNames   = "TimeStamp, UserName, QueryID, URI, RequestBody, StatusCode, Duration"
)

func openAndLogRestartMarker(filename string) (*os.File, error) {
	logFile, err := os.OpenFile(config.GetLogPrefix()+filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Errorf("openAndLogRestartMarker: Unable to open file: %s, err: %v", filename, err)
		return nil, err
	}
	logRestartMarker(logFile)
	return logFile, nil
}

func InitLogFiles() {
	var err error
	QueryLogFile, err = openAndLogRestartMarker("query.log")
	if err != nil {
		return
	}

	AccessLogFile, err = openAndLogRestartMarker("access.log")
	if err != nil {
		return
	}
}

// logRestartMarker logs a marker indicating the application has restarted
func logRestartMarker(logFile *os.File) {
	if logFile == nil {
		return
	}
	fileMutex.Lock()
	defer fileMutex.Unlock()

	restartTime := time.Now().Format("2006-01-02 15:04:05")
	_, err := logFile.WriteString(fmt.Sprintf("===== Application Restarted at %s =====\n", restartTime))
	if err != nil {
		log.Errorf("logRestartMarker: Unable to write restart marker to log file, err: %v", err)
	}

	// Write the column names after the restart marker
	_, err = logFile.WriteString(fmt.Sprintf("%s\n", columnNames))
	if err != nil {
		log.Errorf("logRestartMarker: Unable to write column names to log file, err: %v", err)
	}
}

func DeferableAddAccessLogEntry(startTime time.Time, endTimeFunc func() time.Time, user string, qid uint64,
	uri string, requestBody string, statusCodeFunc func() int, allowWebsocket bool, logFile *os.File) {

	// Update the column names const accordingly if you change the data structure
	data := dtypeutils.LogFileData{
		QueryID: qid,
	}
	AddLogEntry(data, allowWebsocket, logFile)
}

// Write to access.log in the following format
// timeStamp <logged-in user> <request URI> <request body> <response status code> <elapsed time in ms>
func AddLogEntry(data dtypeutils.LogFileData, allowWebsocket bool, logFile *os.File) {
	if logFile == nil {
		return
	}
	fileMutex.Lock()
	defer fileMutex.Unlock()

	// Do not log websocket connections, unless explicitly allowed.
	if !allowWebsocket {
		return
	}

	// Update the column names const accordingly if you change the data structure
	_, err := logFile.WriteString(fmt.Sprintf("%d\n", data.QueryID))
	if err != nil {
		log.Errorf("AddLogEntry: Unable to write to access.log file, err: %v", err)
		return
	}
}

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

package utils

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/siglens/siglens/pkg/common/dtypeutils"
	log "github.com/sirupsen/logrus"
)

var (
	QueryLogFile  *os.File
	AccessLogFile *os.File
	mu            sync.Mutex
)

func init() {
	var err error
	QueryLogFile, err = os.OpenFile("query.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Errorf("Unable to open query.log file, err=%v", err)
	} else {
		logRestartMarker(QueryLogFile)
	}

	AccessLogFile, err = os.OpenFile("access.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Errorf("Unable to open access.log file, err=%v", err)
	}
}

// logRestartMarker logs a marker indicating the application has restarted
func logRestartMarker(logFile *os.File) {
	if logFile == nil {
		return
	}
	mu.Lock()
	defer mu.Unlock()

	restartTime := time.Now().Format("2006-01-02 15:04:05")
	_, err := logFile.WriteString(fmt.Sprintf("===== Application Restarted at %s =====\n", restartTime))
	if err != nil {
		log.Errorf("Unable to write restart marker to log file, err=%v", err)
	}
}

func DeferableAddAccessLogEntry(startTime time.Time, endTimeFunc func() time.Time, user string,
	uri string, requestBody string, statusCodeFunc func() int, allowWebsocket bool, logFile *os.File) {

	data := dtypeutils.LogFileData{
		TimeStamp:   startTime.Format("2006-01-02 15:04:05"),
		UserName:    user,
		URI:         uri,
		RequestBody: requestBody,
		StatusCode:  statusCodeFunc(),
		Duration:    endTimeFunc().Sub(startTime).Milliseconds(),
	}
	AddLogEntry(data, allowWebsocket, logFile)
}

// Write to access.log in the following format
// timeStamp <logged-in user> <request URI> <request body> <response status code> <elapsed time in ms>
func AddLogEntry(data dtypeutils.LogFileData, allowWebsocket bool, logFile *os.File) {
	if logFile == nil {
		return
	}
	mu.Lock()
	defer mu.Unlock()

	// Do not log websocket connections, unless explicitly allowed.
	if data.StatusCode == 101 && !allowWebsocket {
		return
	}

	// Do not log internal search requests for trace data
	if (strings.TrimSpace(data.URI) == "http:///" || strings.TrimSpace(data.URI) == "https:///") && strings.Contains(data.RequestBody, "\"indexName\":\"traces\"") {
		return
	}

	_, err := logFile.WriteString(fmt.Sprintf("%s %s %s %s %d %d\n",
		data.TimeStamp,
		data.UserName, // TODO : Add logged in user when user auth is implemented
		data.URI,
		data.RequestBody,
		data.StatusCode,
		data.Duration),
	)
	if err != nil {
		log.Errorf("Unable to write to access.log file, err=%v", err)
		return
	}
}

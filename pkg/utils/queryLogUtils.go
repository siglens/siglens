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
	logFile *os.File
	mu      sync.Mutex
)

func init() {
	var err error
	logFile, err = os.OpenFile("query.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Errorf("Unable to open query.log file, err=%v", err)
	} else {
		logRestartMarker()
	}
}

// logRestartMarker logs a marker indicating the application has restarted
func logRestartMarker() {
	if logFile == nil {
		return
	}
	mu.Lock()
	defer mu.Unlock()

	restartTime := time.Now().Format("2006-01-02 15:04:05")
	_, err := logFile.WriteString(fmt.Sprintf("===== Application Restarted at %s =====\n", restartTime))
	if err != nil {
		log.Errorf("Unable to write restart marker to query.log file, err=%v", err)
	}
}

func AddQueryLogEntry(queryTimestamp time.Time, user string, uri string, requestBody string, allowWebsocket bool) {
	if logFile == nil {
		return
	}
	mu.Lock()
	defer mu.Unlock()

	data := dtypeutils.QueryLogData{
		TimeStamp:   queryTimestamp.Format("2006-01-02 15:04:05"),
		UserName:    user,
		URI:         uri,
		RequestBody: requestBody,
	}

	// Do not log websocket connections, unless explicitly allowed.
	if data.StatusCode == 101 && !allowWebsocket {
		return
	}

	// Do not log internal search requests for trace data
	if (strings.TrimSpace(data.URI) == "http:///" || strings.TrimSpace(data.URI) == "https:///") && strings.Contains(data.RequestBody, "\"indexName\":\"traces\"") {
		return
	}

	_, err := logFile.WriteString(fmt.Sprintf("Timestamp: %s, User: %s, URI: %s, Request Body: %s\n",
		data.TimeStamp,
		data.UserName,
		data.URI,
		data.RequestBody),
	)
	if err != nil {
		log.Errorf("Unable to write to query.log file, err=%v", err)
		return
	}
}

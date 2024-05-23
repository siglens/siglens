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
	"time"

	"github.com/siglens/siglens/pkg/common/dtypeutils"
	log "github.com/sirupsen/logrus"
)

// Write to query.log in the following format
// "Timestamp: <timeStamp>, User: <logged-in user>, URI: <request URI>, Request Body: <request body>"
func AddQueryLogEntry(queryTimestamp time.Time, user string, uri string, requestBody string, allowWebsocket bool) {
	data := dtypeutils.QueryLogData{
		TimeStamp:   queryTimestamp.Format("2006-01-02 15:04:05"),
		UserName:    user,
		URI:         uri,
		RequestBody: requestBody,
	}

	logFile, err := os.OpenFile("query.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Errorf("Unable to write to query.log file, err=%v", err)
	}
	defer logFile.Close()

	// Do not log websocket connections, unless explicitly allowed.
	if data.StatusCode == 101 && !allowWebsocket {
		return
	}

	// Do not log internal search requests for trace data
	if (strings.TrimSpace(data.URI) == "http:///" || strings.TrimSpace(data.URI) == "https:///") && strings.Contains(data.RequestBody, "\"indexName\":\"traces\"") {
		return
	}

	_, err = logFile.WriteString(fmt.Sprintf("Timestamp: %s, User: %s, URI: %s, Request Body: %s\n",
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

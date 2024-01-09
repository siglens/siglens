package utils

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/siglens/siglens/pkg/common/dtypeutils"
	log "github.com/sirupsen/logrus"
)

func DeferableAddAccessLogEntry(startTime time.Time, endTimeFunc func() time.Time, user string,
	uri string, requestBody string, statusCodeFunc func() int, allowWebsocket bool, fileName string) {

	data := dtypeutils.AccessLogData{
		TimeStamp:   startTime.Format("2006-01-02 15:04:05"),
		UserName:    user,
		URI:         uri,
		RequestBody: requestBody,
		StatusCode:  statusCodeFunc(),
		Duration:    endTimeFunc().Sub(startTime).Milliseconds(),
	}
	AddAccessLogEntry(data, allowWebsocket, fileName)
}

// Write to access.log in the following format
// timeStamp <logged-in user> <request URI> <request body> <response status code> <elapsed time in ms>
func AddAccessLogEntry(data dtypeutils.AccessLogData, allowWebsocket bool, fileName string) {
	logFile, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Errorf("Unable to write to access.log file, err=%v", err)
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

	_, err = logFile.WriteString(fmt.Sprintf("%s %s %s %s %d %d\n",
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

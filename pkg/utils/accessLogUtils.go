package utils

import (
	"fmt"
	"os"
	"strings"

	"github.com/siglens/siglens/pkg/common/dtypeutils"
	log "github.com/sirupsen/logrus"
)

func AddAccessLogEntry(data dtypeutils.AccessLogData, fileName string) {
	logFile, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Errorf("Unable to write to access.log file, err=%v", err)
	}
	defer logFile.Close()
	// Do not log websocket connections
	if data.StatusCode == 101 {
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

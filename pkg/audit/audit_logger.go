package audit

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

const (
	DefaultAuditLogFile = "./audit.json"
)

type LogEntry struct {
	Username          string `json:"username"`
	ActionString      string `json:"actionString"`
	ExtraMsg          string `json:"extraMsg"`
	EpochTimestampSec int64  `json:"epochTimestampSec"`
	OrgID             string `json:"orgId"`
}

type Logger struct {
	LogFilePath string
}

func NewLogger(logFilePath string) *Logger {
	if logFilePath == "" {
		logFilePath = DefaultAuditLogFile
	}
	return &Logger{
		LogFilePath: logFilePath,
	}
}

func (l *Logger) CreateAuditEvent(username, actionString, extraMsg string, epochTimestampSec int64, orgID string) error {
	logEntry := LogEntry{
		Username:          username,
		ActionString:      actionString,
		ExtraMsg:          extraMsg,
		EpochTimestampSec: epochTimestampSec,
		OrgID:             orgID,
	}

	logJSON, err := json.Marshal(logEntry)
	if err != nil {
		return fmt.Errorf("error marshaling log entry: %w", err)
	}

	file, err := os.OpenFile(l.LogFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening audit log file: %w", err)
	}
	defer file.Close()

	logJSON = append(logJSON, '\n')
	if _, err := file.Write(logJSON); err != nil {
		return fmt.Errorf("error writing to audit log file: %w", err)
	}

	return nil
}

func (l *Logger) ReadAuditEvents(orgID string, startEpochSec, endEpochSec int64) ([]LogEntry, error) {
	var logs []LogEntry

	if _, err := os.Stat(l.LogFilePath); os.IsNotExist(err) {
		return logs, nil
	}

	file, err := os.Open(l.LogFilePath)
	if err != nil {
		return nil, fmt.Errorf("error opening audit log file: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	for {
		var entry LogEntry
		if err := decoder.Decode(&entry); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, fmt.Errorf("error decoding log entry: %w", err)
		}

		if entry.OrgID == orgID && entry.EpochTimestampSec >= startEpochSec && entry.EpochTimestampSec <= endEpochSec {
			logs = append(logs, entry)
		}
	}

	return logs, nil
}

func (l *Logger) CreateEventNow(username, actionString, extraMsg, orgID string) error {
	return l.CreateAuditEvent(username, actionString, extraMsg, time.Now().Unix(), orgID)
}

func (l *Logger) ReadRecentEvents(orgID string, durationBack time.Duration) ([]LogEntry, error) {
	endTime := time.Now().Unix()
	startTime := time.Now().Add(-durationBack).Unix()
	return l.ReadAuditEvents(orgID, startTime, endTime)
}

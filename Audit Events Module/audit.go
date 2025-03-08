package audit

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// AuditEvent defines the structure for log entries.
type AuditEvent struct {
	Username        string `json:"username"`
	ActionString    string `json:"actionString"`
	ExtraMsg        string `json:"extraMsg,omitempty"`
	EpochTimestamp  int64  `json:"epochTimestampSec"`
	OrgID           int64  `json:"orgId"`
}

// AuditLogger handles logging operations.
type AuditLogger struct {
	mu      sync.Mutex
	logFile string
}

// NewAuditLogger creates a new AuditLogger instance.
func NewAuditLogger(logFile string) *AuditLogger {
	return &AuditLogger{logFile: logFile}
}

// CreateAuditEvent records a user action in the audit log.
func (a *AuditLogger) CreateAuditEvent(username, actionString, extraMsg string, epochTimestampSec, orgId int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	event := AuditEvent{
		Username:       username,
		ActionString:   actionString,
		ExtraMsg:       extraMsg,
		EpochTimestamp: epochTimestampSec,
		OrgID:          orgId,
	}

	file, err := os.OpenFile(a.logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %v", err)
	}
	defer file.Close()

	// Append JSON record to the file
	eventData, _ := json.Marshal(event)
	if _, err := file.WriteString(string(eventData) + "\n"); err != nil {
		return fmt.Errorf("failed to write log: %v", err)
	}

	return nil
}

// ReadAuditEvent retrieves log records for a given orgId within a time range.
func (a *AuditLogger) ReadAuditEvent(orgId, startEpochSec, endEpochSec int64) ([]AuditEvent, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	file, err := os.Open(a.logFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read log file: %v", err)
	}
	defer file.Close()

	var events []AuditEvent
	decoder := json.NewDecoder(file)
	for decoder.More() {
		var event AuditEvent
		if err := decoder.Decode(&event); err != nil {
			continue
		}

		// Filter logs based on orgId and time range
		if event.OrgID == orgId && event.EpochTimestamp >= startEpochSec && event.EpochTimestamp <= endEpochSec {
			events = append(events, event)
		}
	}

	return events, nil
}

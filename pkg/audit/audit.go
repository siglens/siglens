package audit

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

// AuditEvent represents a single audit log entry.
type AuditEvent struct {
	Username          string `json:"username"`
	ActionString      string `json:"actionString"`
	ExtraMsg          string `json:"extraMsg,omitempty"`
	EpochTimestampSec int64  `json:"epochTimestampSec"`
	OrgId             int    `json:"orgId"`
}

const auditLogFile = "audit.log"

// CreateAuditEvent logs a new audit event by appending it as a JSON line to a log file.
func CreateAuditEvent(username, actionString, extraMsg string, epochTimestampSec int64, orgId int) error {
	event := AuditEvent{
		Username:          username,
		ActionString:      actionString,
		ExtraMsg:          extraMsg,
		EpochTimestampSec: epochTimestampSec,
		OrgId:             orgId,
	}
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %v", err)
	}

	f, err := os.OpenFile(auditLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open audit log file: %v", err)
	}
	defer f.Close()

	if _, err := f.Write(append(eventJSON, '\n')); err != nil {
		return fmt.Errorf("failed to write audit event: %v", err)
	}
	return nil
}

// ReadAuditEvent returns all audit events for the specified orgId that have timestamps within the given range.
func ReadAuditEvent(orgId int, startEpochSec, endEpochSec int64) ([]AuditEvent, error) {
	f, err := os.Open(auditLogFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open audit log file: %v", err)
	}
	defer f.Close()

	var events []AuditEvent
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		var event AuditEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			// Skip lines that fail to parse
			continue
		}
		if event.OrgId == orgId && event.EpochTimestampSec >= startEpochSec && event.EpochTimestampSec <= endEpochSec {
			events = append(events, event)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading audit log file: %v", err)
	}
	return events, nil
}
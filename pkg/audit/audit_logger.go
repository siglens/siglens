
// Interview Process for Software Engineering Intern @ SigLens
// Creating Audit Events Assignment

package audit

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

const (
	AuditLogFile = "./audit.json"
)

type LogEntry struct {
	Username          string `json:"username"`
	ActionString      string `json:"actionString"`
	ExtraMsg          string `json:"extraMsg"`
	EpochTimestampSec int64  `json:"epochTimestampSec"`
	OrgID             string `json:"orgId"`
}

// The CreateAuditEvent creates a LogEntry object to record any user related activity in audit.json file
func CreateAuditEvent(username, actionString, extraMsg string, epochTimestampSec int64, orgID string) error {
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

	file, err := os.OpenFile(AuditLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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

// The ReadAuditEvent returns all the log details in a given interval for an orgID
func ReadAuditEvents(orgID string, startEpochSec, endEpochSec int64) ([]LogEntry, error) {
	var logs []LogEntry

	if _, err := os.Stat(AuditLogFile); os.IsNotExist(err) {
		return logs, nil
	}

	file, err := os.Open(AuditLogFile)
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
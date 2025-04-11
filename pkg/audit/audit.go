package audit

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

const auditLogFile = "audit_log.json"

type AuditLog struct {
	Username          string `json:"username"`
	ActionString      string `json:"actionString"`
	ExtraMsg          string `json:"extraMsg:"`
	EpochTimestampSec int64  `json:"epochTimestampSec"`
	OrgId             int64  `json:"orgId"`
}

func CreateAuditEvent(username, actionString, extraMsg string, epochTimestampSec, orgId int64) error {
	auditLog := AuditLog{
		Username:          username,
		ActionString:      actionString,
		ExtraMsg:          extraMsg,
		EpochTimestampSec: epochTimestampSec,
		OrgId:             orgId,
	}

	var logs []AuditLog
	file, err := os.OpenFile(auditLogFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open audit log file: %w", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read audit log file: %w", err)
	}

	if len(data) > 0 {
		if err := json.Unmarshal(data, &logs); err != nil {
			return fmt.Errorf("failed to unmarshal audit logs: %w", err)
		}
	}

	logs = append(logs, auditLog)

	// Truncate file before writing new data
	if err := file.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate audit log file: %w", err)
	}

	// go to the beginning of the file
	if _, err := file.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek audit log file: %w", err)
	}

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(logs); err != nil {
		return fmt.Errorf("failed to encode audit logs: %w", err)
	}

	return nil
}

func ReadAuditEvents(orgId int64, startEpochSec, endEpochSec int64) ([]AuditLog, error) {
	file, err := os.Open(auditLogFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var logs []AuditLog
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var log AuditLog
		if err := json.Unmarshal(scanner.Bytes(), &log); err != nil {
			continue
		}
		if log.OrgId == orgId && log.EpochTimestampSec >= startEpochSec && log.EpochTimestampSec <= endEpochSec {
			logs = append(logs, log)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return logs, nil
}

package audit

import (
	"bufio"
	"encoding/json"
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
	audit_log := AuditLog{
		Username:          username,
		ActionString:      actionString,
		ExtraMsg:          extraMsg,
		EpochTimestampSec: epochTimestampSec,
		OrgId:             orgId,
	}

	// Read existing data
	var logs []AuditLog
	file, err := os.OpenFile(auditLogFile, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err == nil && len(data) > 0 {
		json.Unmarshal(data, &logs)
	}

	logs = append(logs, audit_log)

	file.Truncate(0)
	file.Seek(0, 0)
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(logs); err != nil {
		return err
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

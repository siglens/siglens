package audit

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

const auditLogFile = "audit_log.json"

type AuditLog struct {
	Username          string `json:"username"`
	ActionString      string `json:"actionString"`
	ExtraMsg          string `json:"extraMsg:"`
	EpochTimestampSec int64  `json:"epochTimestampSec"`
	OrgId             string `json:"orgId"`
}

func CreateAuditEvent(username, actionString, extraMsg string, epochTimestampSec int64, orgId string) error {
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

	data, err := ioutil.ReadAll(file)
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

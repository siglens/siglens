package audit

import (
	"encoding/json"
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
	file, err := os.OpenFile(auditLogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(audit_log); err != nil {
		return err
	}
	return nil
}

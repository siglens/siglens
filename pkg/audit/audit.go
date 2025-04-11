package audit

import (
	"encoding/json"
	"errors"
	"os"
	"sync"
)

type AuditEvent struct {
	Username          string `json:"username"`
	ActionString      string `json:"actionString"`
	ExtraMsg          string `json:"extraMsg"`
	EpochTimestampSec int64  `json:"epochTimestampSec"`
	OrgId             int64  `json:"orgId"`
}

var (
	auditFilePath = "audit_log.json"
	mu            sync.Mutex
)

func CreateAuditEvent(username, actionString, extraMsg string, epochTimestampSec, orgId int64) error {
	event := AuditEvent{
		Username:          username,
		ActionString:      actionString,
		ExtraMsg:          extraMsg,
		EpochTimestampSec: epochTimestampSec,
		OrgId:             orgId,
	}

	mu.Lock()
	defer mu.Unlock()

	var existing []AuditEvent
	data, _ := os.ReadFile(auditFilePath)
	if len(data) > 0 {
		_ = json.Unmarshal(data, &existing)
	}

	existing = append(existing, event)
	jsonBytes, err := json.MarshalIndent(existing, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(auditFilePath, jsonBytes, 0644)
}

func ReadAuditEvent(orgId, startEpochSec, endEpochSec int64) ([]AuditEvent, error) {
	mu.Lock()
	defer mu.Unlock()

	data, err := os.ReadFile(auditFilePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []AuditEvent{}, nil
		}
		return nil, err
	}

	var events []AuditEvent
	err = json.Unmarshal(data, &events)
	if err != nil {
		return nil, err
	}

	var filtered []AuditEvent
	for _, e := range events {
		if e.OrgId == orgId && e.EpochTimestampSec >= startEpochSec && e.EpochTimestampSec <= endEpochSec {
			filtered = append(filtered, e)
		}
	}
	return filtered, nil
}

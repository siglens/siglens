// Copyright (c) 2021-2024 SigScalr, Inc.
//
// This file is part of SigLens Observability Solution
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package alertutils

import (
	"crypto/tls"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-co-op/gocron"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type AlertType uint8

const (
	AlertTypeLogs AlertType = iota + 1
	AlertTypeMetrics
	AlertTypeMinion
)

type AlertConfig struct {
	AlertName    string              `json:"alert_name" gorm:"not null;unique"`
	AlertType    AlertType           `json:"alert_type"`
	ContactID    string              `json:"contact_id" gorm:"foreignKey:ContactId;"`
	ContactName  string              `json:"contact_name"`
	Labels       []AlertLabel        `json:"labels" gorm:"many2many:label_alerts"`
	QueryParams  QueryParams         `json:"queryParams" gorm:"embedded"`
	Condition    AlertQueryCondition `json:"condition"`
	Value        float64             `json:"value"`
	EvalWindow   uint64              `json:"eval_for"`      // in minutes; TODO: Rename json field to eval_window
	EvalInterval uint64              `json:"eval_interval"` // in minutes
	Message      string              `json:"message"`
}

type AlertDetails struct {
	AlertConfig
	AlertId                  string     `json:"alert_id" gorm:"primaryKey"`
	State                    AlertState `json:"state"`
	CreateTimestamp          time.Time  `json:"create_timestamp" gorm:"autoCreateTime:milli" `
	SilenceMinutes           uint64     `json:"silence_minutes"`
	SilenceEndTime           uint64     `json:"silence_end_time"`
	MetricsQueryParamsString string     `json:"metricsQueryParams"`
	CronJob                  gocron.Job `json:"cron_job" gorm:"embedded"`
	NodeId                   uint64     `json:"node_id"`
	NotificationID           string     `json:"notification_id" gorm:"foreignKey:NotificationId;"`
	OrgId                    int64      `json:"org_id"`
	NumEvaluationsCount      uint64     `json:"num_evaluations_count"`
}

func (AlertDetails) TableName() string {
	return "all_alerts"
}

type AlertLabel struct {
	LabelName  string `json:"label_name" gorm:"primaryKey;size:256;not null;"` // unique
	LabelValue string `json:"label_value"`
}

// TableName overrides the default tablename generated by GORM
func (AlertLabel) TableName() string {
	return "alert_labels"
}

type AlertHistoryDetails struct {
	ID               uint       `gorm:"primaryKey;autoIncrement:true"`
	AlertId          string     `json:"alert_id"`
	AlertType        AlertType  `json:"alert_type"`
	AlertState       AlertState `json:"alert_state"`
	EventDescription string     `json:"event_description"`
	UserName         string     `json:"user_name"`
	EventTriggeredAt time.Time  `json:"event_triggered_at"`
}

func (AlertHistoryDetails) TableName() string {
	return "alert_history_details"
}

type DB_SORT_ORDER string

const (
	ASC  DB_SORT_ORDER = "ASC"
	DESC DB_SORT_ORDER = "DESC"
)

type AlertHistoryQueryParams struct {
	AlertId   string        `json:"alert_id"` // mandatory
	SortOrder DB_SORT_ORDER `json:"sort_order"`
	Limit     uint64        `json:"limit"`
	Offset    uint64        `json:"offset"`
}

type QueryParams struct {
	DataSource    string `json:"data_source"`
	QueryLanguage string `json:"queryLanguage"`
	QueryText     string `json:"queryText"`
	StartTime     string `json:"startTime"`
	EndTime       string `json:"endTime"`
	Index         string `json:"index"`
	QueryMode     string `json:"queryMode"`
}
type Alert struct {
	Status string
}

type WebhookBody struct {
	Receiver            string
	Status              string
	Title               string
	Body                string
	NumEvaluationsCount uint64
	Alerts              []Alert
}

type Contact struct {
	ContactId   string             `json:"contact_id" gorm:"primaryKey"`
	ContactName string             `json:"contact_name" gorm:"not null;unique"`
	Email       []string           `json:"email" gorm:"type:text[]"`
	Slack       []SlackTokenConfig `json:"slack" gorm:"many2many:slack_contact;auto_preload"`
	PagerDuty   string             `json:"pager_duty"`
	Webhook     []WebHookConfig    `json:"webhook" gorm:"many2many:webhook_contact;auto_preload"`
	OrgId       int64              `json:"org_id"`
}

type SlackTokenConfig struct {
	ID        uint   `gorm:"primaryKey;autoIncrement:true"`
	ChannelId string `json:"channel_id"`
	SlToken   string `json:"slack_token"`
}

func (SlackTokenConfig) TableName() string {
	return "slack_token"
}

type JSONMap map[string]string

func (j JSONMap) Value() (driver.Value, error) {
	return json.Marshal(j)
}

func (j *JSONMap) Scan(value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("JSONMap Scan: failed to assert database value as []byte")
	}
	return json.Unmarshal(bytes, j)
}

type WebHookConfig struct {
	ID      uint    `gorm:"primaryKey;autoIncrement:true"`
	Webhook string  `json:"webhook"`
	Headers JSONMap `json:"headers" gorm:"type:text"` // stored as JSON string in a TEXT column
}

func (WebHookConfig) TableName() string {
	return "webhook"
}

type Notification struct {
	NotificationId string     `json:"notification_id" gorm:"primaryKey"`
	CooldownPeriod uint64     `json:"cooldown_period"`
	LastSentTime   time.Time  `json:"last_sent_time"`
	AlertId        string     `json:"-"`
	LastAlertState AlertState `json:"last_alert_state"`
}

func (Notification) TableName() string {
	return "notification_details"
}

type AlertQueryCondition uint8 // condition for the alert queries
const (
	IsAbove AlertQueryCondition = iota
	IsBelow
	IsEqualTo
	IsNotEqualTo
	HasNoValue
)

type AlertState uint8 // state of the alerts
const (
	Inactive AlertState = iota
	Normal
	Pending
	Firing
	SystemGeneratedAlert = "System Generated"
	UserModified         = "User Modified"
	AlertFiring          = "Alert Firing"
	AlertNormal          = "Alert Normal"
	AlertPending         = "Alert Pending"
	ConfigChange         = "Config Modified"
)

type AlertSilenceRequest struct {
	AlertID        string `json:"alert_id"`
	SilenceMinutes uint64 `json:"silence_minutes"`
}

// This MUST be synced with how https://github.com/sigscalr/logminion structures
// its output JSON.
type LogLinesFile struct {
	Version   string          `json:"version,omitempty"`
	LogAlerts []LogLinesEntry `json:"log_alerts,omitempty"`
}

// This MUST be synced with how https://github.com/sigscalr/logminion structures
// its output JSON.
type LogLinesEntry struct {
	Repository    string `json:"repository,omitempty"`
	Filename      string `json:"filename,omitempty"`
	LineNumber    int    `json:"line_number,omitempty"`
	LogText       string `json:"log_text,omitempty"`
	LogTextHash   string `json:"log_text_hash,omitempty"`
	QueryLanguage string `json:"query_language,omitempty"`
	Query         string `json:"query,omitempty"`
	Condition     string `json:"condition,omitempty"`
	Value         int    `json:"value,omitempty"`
	LogLevel      string `json:"log_level,omitempty"`
}

type MinionSearch struct {
	AlertId         string              `json:"alert_id" gorm:"primaryKey"`
	AlertName       string              `json:"alert_name" gorm:"not null;unique"`
	State           AlertState          `json:"state"`
	CreateTimestamp time.Time           `json:"create_timestamp" gorm:"autoCreateTime:milli" `
	ContactID       string              `json:"contact_id" gorm:"foreignKey:ContactId;"`
	ContactName     string              `json:"contact_name"`
	Labels          []AlertLabel        `json:"labels" gorm:"many2many:label_alerts"`
	SilenceMinutes  uint64              `json:"silence_minutes"`
	SilenceEndTime  uint64              `json:"silence_end_time"`
	QueryParams     QueryParams         `json:"queryParams" gorm:"embedded"`
	Condition       AlertQueryCondition `json:"condition"`
	Value           float64             `json:"value"`
	EvalFor         uint64              `json:"eval_for"`
	EvalInterval    uint64              `json:"eval_interval"`
	Message         string              `json:"message"`
	CronJob         gocron.Job          `json:"cron_job" gorm:"embedded"`
	NodeId          uint64              `json:"node_id"`
	Repository      string              `json:"repository,omitempty"`
	Filename        string              `json:"filename,omitempty"`
	LineNumber      int                 `json:"line_number,omitempty"`
	LogText         string              `json:"log_text,omitempty"`
	LogTextHash     string              `json:"log_text_hash,omitempty"`
	LogLevel        string              `json:"log_level,omitempty"`
	OrgId           int64               `json:"org_id"`
}

type MetricAlertData struct {
	SeriesId  string  `json:"series_id"`
	Timestamp uint32  `json:"timestamp"`
	Value     float64 `json:"value"`
}

func (MinionSearch) TableName() string {
	return "minion_searches"
}

func IsAlertStatePendingOrFiring(alertState AlertState) bool {
	return alertState == Pending || alertState == Firing
}

func (alert *AlertDetails) EncodeQueryParamToBase64() {
	if alert.AlertType == AlertTypeLogs {
		alert.QueryParams.QueryText = utils.EncodeToBase64(alert.QueryParams.QueryText)
	} else if alert.AlertType == AlertTypeMetrics {
		alert.MetricsQueryParamsString = utils.EncodeToBase64(alert.MetricsQueryParamsString)
	}
}

func (alert *AlertDetails) DecodeQueryParamFromBase64() error {
	if alert.AlertType == AlertTypeLogs {
		decoded, err := utils.DecodeFromBase64(alert.QueryParams.QueryText)
		if err != nil {
			err = fmt.Errorf("DecodeQueryParamFromBase64: Error decoding query text:%v from base64, alert_id: %s, err: %v", alert.QueryParams.QueryText, alert.AlertId, err)
			log.Errorf(err.Error())
			return err
		}
		alert.QueryParams.QueryText = decoded
	} else if alert.AlertType == AlertTypeMetrics {
		decoded, err := utils.DecodeFromBase64(alert.MetricsQueryParamsString)
		if err != nil {
			err = fmt.Errorf("DecodeQueryParamFromBase64: Error decoding metrics query params:%v from base64, alert_id: %s, err: %v", alert.MetricsQueryParamsString, alert.AlertId, err)
			log.Errorf(err.Error())
			return err
		}
		alert.MetricsQueryParamsString = decoded
	}

	return nil
}

func GetCertErrorForgivingHttpClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
}

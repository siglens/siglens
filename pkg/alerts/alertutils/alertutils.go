/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package alertutils

import (
	"time"

	"github.com/go-co-op/gocron"
)

type AlertDetails struct {
	AlertInfo    AlertInfo           `json:"alertInfo"`
	QueryParams  QueryParams         `json:"queryParams"`
	Condition    AlertQueryCondition `json:"condition"`
	Value        float32             `json:"value"`
	EvalFor      uint64              `json:"eval_for"`
	EvalInterval uint64              `json:"eval_interval"`
	Message      string              `json:"message"`
	CronJob      gocron.Job          `json:"cron_job"`
	NodeId       uint64              `json:"node_id"`
}

type AlertLabel struct {
	LabelName  string `json:"label_name"`
	LabelValue string `json:"label_value"`
}

type AlertInfo struct {
	AlertId         string       `json:"alert_id"`
	AlertName       string       `json:"alert_name"`
	State           AlertState   `json:"state"`
	CreateTimestamp time.Time    `json:"create_timestamp"`
	ContactId       string       `json:"contact_id"`
	ContactName     string       `json:"contact_name"`
	Labels          []AlertLabel `json:"labels"`
}

type QueryParams struct {
	DataSource    string `json:"data_source"`
	QueryLanguage string `json:"queryLanguage"`
	QueryText     string `json:"queryText"`
	StartTime     string `json:"startTime"`
	EndTime       string `json:"endTime"`
}
type Alert struct {
	Status string
}

type WebhookBody struct {
	Receiver string
	Status   string
	Title    string
	Body     string
	Alerts   []Alert
}

type Contact struct {
	ContactName string             `json:"contact_name"`
	ContactId   string             `json:"contact_id"`
	Email       []string           `json:"email"`
	Slack       []SlackTokenConfig `json:"slack"`
	PagerDuty   string             `json:"pager_duty"`
	Webhook     []string           `json:"webhook"`
}

type SlackTokenConfig struct {
	ChannelId  string `json:"channel_id"`
	SlackToken string `json:"slack_token"`
}

type Notification struct {
	AlertId        string    `json:"alert_id"`
	CooldownPeriod uint64    `json:"cooldown_period"`
	LastSentTime   time.Time `json:"last_sent_time"`
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
	Pending
	Firing
)

// This MUST be synced with how https://github.com/siglens/logminion structures
// its output JSON.
type LogLinesFile struct {
	Version   string          `json:"version,omitempty"`
	LogAlerts []LogLinesEntry `json:"log_alerts,omitempty"`
}

// This MUST be synced with how https://github.com/siglens/logminion structures
// its output JSON.
type LogLinesEntry struct {
	Repository  string             `json:"repository,omitempty"`
	Filename    string             `json:"filename,omitempty"`
	LineNumber  int                `json:"line_number,omitempty"`
	LogText     string             `json:"log_text,omitempty"`
	LogTextHash string             `json:"log_text_hash,omitempty"`
	Alert       LogLinesEntryAlert `json:"alert,omitempty"`
	LogLevel    string             `json:"log_level,omitempty"`
}

// This MUST be synced with how https://github.com/siglens/logminion structures
// its output JSON.
type LogLinesEntryAlert struct {
	QueryLanguage string `json:"query_language,omitempty"`
	Query         string `json:"query,omitempty"`
	Condition     string `json:"condition,omitempty"`
	Value         int    `json:"value,omitempty"`
}

type MinionSearchDetails struct {
	Repository  string `json:"repository,omitempty"`
	Filename    string `json:"filename,omitempty"`
	LineNumber  int    `json:"line_number,omitempty"`
	LogText     string `json:"log_text,omitempty"`
	LogTextHash string `json:"log_text_hash,omitempty"`
	LogLevel    string `json:"log_level,omitempty"`
}

type MinionSearch struct {
	AlertInfo           AlertInfo           `json:"alertInfo"`
	QueryParams         QueryParams         `json:"queryParams"`
	MinionSearchDetails MinionSearchDetails `json:"minionSearchDetails"`
	Condition           AlertQueryCondition `json:"condition"`
	Value1              float32             `json:"value1"`
	Value2              float32             `json:"value2"`
	EvalFor             uint64              `json:"eval_for"`
	EvalInterval        uint64              `json:"eval_interval"`
	Message             string              `json:"message"`
	CronJob             gocron.Job          `json:"cron_job"`
	NodeId              uint64              `json:"node_id"`
}

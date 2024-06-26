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

package alerts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/siglens/siglens/pkg/alerts/alertutils"
	log "github.com/sirupsen/logrus"
)

// End to End Testing for Alerts:
// Create a Contact Point: For this test purpose, we will create a web-hook contact point.
// Verification of Contact Point: Verify that the contact point is created through Get All Contacts API.
// Create an Alert for logs: With 1 min Eval Interval and 1 or 2 mins as Eval window.
// Create an Alert for Metrics: With 1 min Eval Interval and 1 or 2 mins as Eval window.
// Verify the Alerts are created by using the get API
// Verify that the notifications are sent.
// Verify Alert History by calling the Get Alert History API
// Delete the Alerts.
// Delete the Contact Points
// Close

type AllContactResponse struct {
	Contacts []*alertutils.Contact `json:"contacts"`
}

type AllAlertsResponse struct {
	Alerts []*alertutils.AlertDetails `json:"alerts"`
}

type GetAlertHistoryResponse struct {
	AlertHistory []*alertutils.AlertHistoryDetails `json:"alertHistory"`
	Count        uint64                            `json:"count"`
}

const (
	WebhookContactName       = "TestWebhook"
	AlertNamePrefix          = "TestAlert_"
	AlertMessagePrefix       = "Test Alert - "
	LogsString               = "Logs"
	MetricsString            = "Metrics"
	MetricsQueryParamsString = "{\"start\": \"now-24h\", \"end\": \"now\", \"queries\": [{\"name\": \"a\", \"query\": \"avg by (car_type) (testmetric0{car_type=\\\"Passenger car heavy\\\"})\", \"qlType\": \"promql\"}, {\"name\": \"b\", \"query\": \"avg by (car_type) (testmetric1{car_type=\\\"Passenger car heavy\\\"})\", \"qlType\": \"promql\"}], \"formulas\": [{\"formula\": \"a+b\"}]}"
	LogsQueryText            = `app_name=Wheat* AND gender=male | stats count(app_name) by gender`
	LogsQueryLanguage        = "Splunk QL"
	LogsStartTime            = "now-1h"
	LogsEndTime              = "now"
	AlertQueryCondition      = alertutils.IsAbove
	AlertValue               = 10
	EvalWindow               = 1
	EvalInterval             = 1
)

func createContactPoint(host string, webhookUrl string) error {
	url := host + "/api/alerts/createContact"

	contact := &alertutils.Contact{
		ContactName: WebhookContactName,
		Webhook: []alertutils.WebHookConfig{
			{
				Webhook: webhookUrl,
			},
		},
	}

	data, _ := json.Marshal(contact)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("error response status: %v. responseBody=%v", resp.Status, string(bodyBytes))
	}

	return nil
}

func getAllContactPoints(host string) ([]*alertutils.Contact, error) {

	url := host + "/api/alerts/allContacts"

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("error response status: %v. Response Body=%v", resp.Status, string(bodyBytes))
	}

	var allContactsResp AllContactResponse
	if err := json.NewDecoder(resp.Body).Decode(&allContactsResp); err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}

	return allContactsResp.Contacts, nil
}

func createAlert(host string, alertTypeString string, contactId string) error {
	var alertType alertutils.AlertType

	if alertTypeString == "Logs" {
		alertType = alertutils.AlertTypeLogs
	} else if alertTypeString == "Metrics" {
		alertType = alertutils.AlertTypeMetrics
	} else {
		return fmt.Errorf("invalid alert type: %s", alertTypeString)
	}

	alert := &alertutils.AlertDetails{
		AlertName: fmt.Sprintf("%s%s", AlertNamePrefix, alertTypeString),
		AlertType: alertType,
		Labels: []alertutils.AlertLabel{
			{
				LabelName:  "env",
				LabelValue: "test",
			},
		},
		Condition:    AlertQueryCondition,
		Value:        AlertValue,
		EvalWindow:   EvalWindow,
		EvalInterval: EvalInterval,
		Message:      AlertMessagePrefix + alertTypeString,
		ContactID:    contactId,
	}

	if alertType == alertutils.AlertTypeMetrics {
		alert.MetricsQueryParamsString = MetricsQueryParamsString
	} else {
		alert.QueryParams = alertutils.QueryParams{
			DataSource:    LogsString,
			QueryLanguage: LogsQueryLanguage,
			QueryText:     LogsQueryText,
			StartTime:     LogsStartTime,
			EndTime:       LogsEndTime,
		}
	}

	url := host + "/api/alerts/create" // Ensure the URL is correct

	data, err := json.Marshal(alert)
	if err != nil {
		return fmt.Errorf("error marshalling alert: %v", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("error response status: %v. ResponseBody: %v", resp.Status, string(bodyBytes))
	}

	return nil
}

func getAllAlerts(host string) ([]*alertutils.AlertDetails, error) {
	url := host + "/api/allalerts"

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error sending request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("error response status: %v. ResponseBody: %v", resp.Status, string(bodyBytes))
	}

	var alertsResp AllAlertsResponse
	if err := json.NewDecoder(resp.Body).Decode(&alertsResp); err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}

	return alertsResp.Alerts, nil
}

func getAlertHistoryById(host string, alertId string) ([]*alertutils.AlertHistoryDetails, error) {

	url := host + "/api/alerts/" + alertId + "/history"

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error sending request: %v", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("error response status: %v. ResponseBody: %v", resp.Status, string(bodyBytes))
	}

	var alertHistoryResp GetAlertHistoryResponse
	if err := json.NewDecoder(resp.Body).Decode(&alertHistoryResp); err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}

	return alertHistoryResp.AlertHistory, nil
}

func deleteAlert(host string, alertId string) error {
	url := host + "/api/alerts/delete"

	dataBody := map[string]string{
		"alert_id": alertId,
	}

	data, _ := json.Marshal(dataBody)

	req, err := http.NewRequest("DELETE", url, bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error making request: %v", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("error response status: %v. Response Body=%v", resp.Status, string(bodyBytes))
	}

	return nil
}

func deleteContactPoint(host string, contactId string) error {
	url := host + "/api/alerts/deleteContact"

	dataBody := map[string]string{
		"contact_id": contactId,
	}

	data, _ := json.Marshal(dataBody)

	req, err := http.NewRequest("DELETE", url, bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error making request: %v", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("error response status: %v. Response Body=%v", resp.Status, string(bodyBytes))
	}

	return nil
}

func verifyAlertLogsQuery(alert *alertutils.AlertDetails) error {
	if alert.QueryParams.DataSource != LogsString {
		return fmt.Errorf("expected data source to be %s, got %s", LogsString, alert.QueryParams.DataSource)
	}

	if alert.QueryParams.QueryLanguage != LogsQueryLanguage {
		return fmt.Errorf("expected query language to be %s, got %s", LogsQueryLanguage, alert.QueryParams.QueryLanguage)
	}

	if alert.QueryParams.QueryText != LogsQueryText {
		return fmt.Errorf("expected query text to be %s, got %s", LogsQueryText, alert.QueryParams.QueryText)
	}

	if alert.QueryParams.StartTime != LogsStartTime {
		return fmt.Errorf("expected start time to be %s, got %s", LogsStartTime, alert.QueryParams.StartTime)
	}

	if alert.QueryParams.EndTime != LogsEndTime {
		return fmt.Errorf("expected end time to be %s, got %s", LogsEndTime, alert.QueryParams.EndTime)
	}

	return nil
}

func verifyAlertsData(alerts []*alertutils.AlertDetails, contact *alertutils.Contact) (bool, bool) {
	alertLogsFound := false
	alertMetricsFound := false

	for _, alert := range alerts {
		alertTypeString := ""

		if alert.AlertType == alertutils.AlertTypeLogs {
			alertLogsFound = true
			alertTypeString = "Logs"

			err := verifyAlertLogsQuery(alert)
			if err != nil {
				log.Fatalf("Error verifying alert logs query: %v", err)
			}

		} else if alert.AlertType == alertutils.AlertTypeMetrics {
			alertMetricsFound = true
			alertTypeString = "Metrics"

			if alert.MetricsQueryParamsString != MetricsQueryParamsString {
				log.Fatalf("Expected metrics query params to be %s, got %s", MetricsQueryParamsString, alert.MetricsQueryParamsString)
			}

		} else {
			log.Fatalf("Invalid alert type: %v", alert.AlertType)
		}

		expectedAlertName := fmt.Sprintf("%s%s", AlertNamePrefix, alertTypeString)
		if alert.AlertName != expectedAlertName {
			log.Fatalf("Expected alert name to be %s, got %s", expectedAlertName, alert.AlertName)
		}

		expectedAlertMsg := AlertMessagePrefix + alertTypeString
		if alert.Message != expectedAlertMsg {
			log.Fatalf("Expected alert message to be %s, got %s", expectedAlertMsg, alert.Message)
		}

		if alert.ContactID != contact.ContactId {
			log.Fatalf("Expected contact ID to be %s, got %s", contact.ContactId, alert.ContactID)
		}

		if alert.ContactName != contact.ContactName {
			log.Fatalf("Expected contact name to be %s, got %s", contact.ContactName, alert.ContactName)
		}

		if alert.EvalWindow != EvalWindow {
			log.Fatalf("Expected eval window to be %d, got %d", EvalWindow, alert.EvalWindow)
		}

		if alert.EvalInterval != EvalInterval {
			log.Fatalf("Expected eval interval to be %d, got %d", EvalInterval, alert.EvalInterval)
		}

		if alert.Value != AlertValue {
			log.Fatalf("Expected alert value to be %v, got %v", AlertValue, alert.Value)
		}

		if alert.Condition != AlertQueryCondition {
			log.Fatalf("Expected alert condition to be %v, got %v", alertutils.AlertQueryCondition(AlertQueryCondition), alertutils.AlertQueryCondition(alert.Condition))
		}
	}

	return alertLogsFound, alertMetricsFound
}

func verifyAlertHistory(host string, alerts []*alertutils.AlertDetails) error {
	receivedAlertForLogs := false
	receivedAlertForMetrics := false

	for _, alert := range alerts {
		alertHistory, err := getAlertHistoryById(host, alert.AlertId)
		if err != nil {
			return fmt.Errorf("error getting alert history For Alert=%v. Error=%v", alert.AlertName, err)
		}

		if len(alertHistory) == 0 {
			return fmt.Errorf("expected alert history to be non-empty for Alert=%v", alert.AlertName)
		}

		for _, history := range alertHistory {
			if history.AlertState != alertutils.Firing {
				return fmt.Errorf("expected alert state to be %v, got %v", alertutils.Firing, history.AlertState)
			}

			if history.AlertType != alert.AlertType {
				return fmt.Errorf("expected alert type to be %v, got %v", alert.AlertType, history.AlertType)
			}

			if history.EventDescription != alertutils.AlertFiring {
				return fmt.Errorf("expected event description to be %v, got %v", alertutils.AlertFiring, history.EventDescription)
			}
		}

		if alert.AlertType == alertutils.AlertTypeLogs {
			receivedAlertForLogs = true
		} else if alert.AlertType == alertutils.AlertTypeMetrics {
			receivedAlertForMetrics = true
		}
	}

	if !receivedAlertForLogs && !receivedAlertForMetrics {
		return fmt.Errorf("expected alert history for logs and metrics")
	} else if !receivedAlertForLogs {
		return fmt.Errorf("expected alert history for logs")
	} else if !receivedAlertForMetrics {
		return fmt.Errorf("expected alert history for metrics")
	}

	return nil
}

func RunAlertsTest(host string) {

	webhookChan := make(chan alertutils.WebhookBody)

	// Start a Webserver so that we can receive the webhooks
	server := &http.Server{
		Addr: ":4010",
	}

	http.HandleFunc("/webhook", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Webhook Received"))

		var webhookBody alertutils.WebhookBody
		if err := json.NewDecoder(r.Body).Decode(&webhookBody); err != nil {
			bytesBody, _ := io.ReadAll(r.Body)
			log.Errorf("Error decoding webhook body: %v, Error=%v", string(bytesBody), err)
		}

		webhookChan <- webhookBody
	})

	go func() {
		log.Infof("Starting Webserver on port 4010 to Listen for Webhooks")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("Error starting webserver: %v", err)
		}
	}()

	// Give the server a moment to start
	time.Sleep(1 * time.Second)

	// Create a Contact Point
	webhookUrl := "http://localhost:4010/webhook"
	err := createContactPoint(host, webhookUrl)
	if err != nil {
		log.Fatalf("Error creating contact point: %v", err)
		return
	}
	log.Infof("Created Contact Point with Webhook: %v", webhookUrl)

	// Get all Contact Points to verify the creation
	contacts, err := getAllContactPoints(host)
	if err != nil {
		log.Fatalf("Error getting all contacts: %v", err)
		return
	}

	if len(contacts) != 1 {
		log.Fatalf("Expected 1 contact, got %d", len(contacts))
		return
	}

	contact := contacts[0]

	if contact.ContactName != WebhookContactName {
		log.Fatalf("Expected contact name to be %v, got %s", WebhookContactName, contact.ContactName)
		return
	}

	if contact.Webhook[0].Webhook != webhookUrl {
		log.Fatalf("Expected webhook to be %v, got %s", webhookUrl, contact.Webhook[0].Webhook)
		return
	}
	log.Infof("Verified Contact Point: %v", contact.ContactName)

	// Create an Alert for Logs
	err = createAlert(host, "Logs", contact.ContactId)
	if err != nil {
		log.Fatalf("Error creating alert for logs: %v", err)
		return
	}
	log.Infof("Created Alert for Logs")

	// Create an Alert for Metrics
	err = createAlert(host, "Metrics", contact.ContactId)
	if err != nil {
		log.Fatalf("Error creating alert for metrics: %v", err)
		return
	}
	log.Infof("Created Alert for Metrics")

	// Get all Alerts to verify the creation
	alerts, err := getAllAlerts(host)
	if err != nil {
		log.Fatalf("Error getting all alerts: %v", err)
		return
	}

	if len(alerts) != 2 {
		log.Fatalf("Expected 2 alerts, got %d", len(alerts))
		return
	}

	alertLogsFound, alertMetricsFound := verifyAlertsData(alerts, contact)

	if !alertLogsFound {
		log.Fatalf("Expected alert for logs not found. Got Alerts: %v", alerts)
		return
	}

	if !alertMetricsFound {
		log.Fatalf("Expected alert for metrics not found. Got Alerts: %v", alerts)
		return
	}

	log.Infof("Verified Alerts Created for Logs and Metrics")

	// Wait for the Alerts Notifications to be received
	receivedAlertForLogs := false
	receivedAlertForMetrics := false

	// Wait for the Webhook to be received
waitForWebhooks:
	for {
		select {
		case webhookBody := <-webhookChan:
			if webhookBody.Title == AlertNamePrefix+LogsString {
				log.Infof("Received Alert for Logs: %v", webhookBody.Title)
				receivedAlertForLogs = true
			} else if webhookBody.Title == AlertNamePrefix+MetricsString {
				log.Infof("Received Alert for Metrics: %v", webhookBody.Title)
				receivedAlertForMetrics = true
			}

			if receivedAlertForLogs && receivedAlertForMetrics {
				break waitForWebhooks
			}
		case <-time.After(30 * time.Second):
			log.Fatalf("Timed out waiting for webhook")
			return
		}
	}

	// Get Alert History to verify the notifications
	err = verifyAlertHistory(host, alerts)
	if err != nil {
		log.Fatalf("Error verifying alert history: %v", err)
		return
	}
	log.Infof("Verified Alert History")

	// Delete the Alerts
	for _, alert := range alerts {
		err = deleteAlert(host, alert.AlertId)
		if err != nil {
			log.Fatalf("Error deleting alert: %v, Error=%v", alert.AlertName, err)
			return
		}
	}
	log.Infof("Deleted Alerts")

	// Delete the Contact Points
	err = deleteContactPoint(host, contact.ContactId)
	if err != nil {
		log.Fatalf("Error deleting contact point: %v", err)
		return
	}
	log.Infof("Deleted Contact Point")

	// Close the Webserver
	err = server.Shutdown(context.Background())
	if err != nil {
		log.Errorf("Error shutting down server: %v", err)
		return
	}
	log.Infof("Webserver shutdown successfully")
}

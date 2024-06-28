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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/siglens/siglens/pkg/alerts/alertutils"
	log "github.com/sirupsen/logrus"
)

type AlertData struct {
	ReceivedCount int
	TotalDelay    time.Duration
}

type MinuteSummaryData struct {
	TotalAlertsReceived int
	TotalDelay          time.Duration
	AverageDelay        time.Duration
	Alerts              map[string]AlertData
}

type SummaryData struct {
	TotalAlertsReceived    int
	TotalDelay             time.Duration
	AverageDelay           time.Duration
	AverageAlertsPerMinute float64
	MinuteData             map[int]MinuteSummaryData
}

func createMultipleAlerts(host string, contactId string, numAlerts int) error {
	for i := 0; i < numAlerts; i++ {
		alertTypeString := "Logs"
		if i%2 == 0 {
			alertTypeString = "Metrics"
		}
		err := createAlert(host, alertTypeString, contactId)
		if err != nil {
			return fmt.Errorf("error creating alert %d: %v", i, err)
		}
	}
	return nil
}

func startWebhookServer(port int, webhookChan chan alertutils.WebhookBody) *http.Server {
	server := &http.Server{
		Addr: fmt.Sprintf(":%d", port),
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
		log.Infof("Starting Webserver on port %d to Listen for Webhooks", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("Error starting webserver: %v", err)
		}
	}()

	return server
}

func trackNotifications(webhookChan chan alertutils.WebhookBody, numAlerts int, doneChan chan bool) {
	startTime := time.Now()
	summary := SummaryData{
		MinuteData: make(map[int]MinuteSummaryData),
	}

	for {
		currentMinute := int(time.Since(startTime).Minutes())
		select {
		case webhookBody := <-webhookChan:
			alertReceivedTime := time.Now()
			log.Infof("Current Minute:%v, Received Alert: %v", currentMinute, webhookBody.Title)
			minuteData, exists := summary.MinuteData[currentMinute]
			if !exists {
				minuteData = MinuteSummaryData{
					Alerts: make(map[string]AlertData),
				}
			}

			delay := alertReceivedTime.Sub(startTime.Add(time.Duration(currentMinute) * time.Minute))

			alertData := minuteData.Alerts[webhookBody.Title]
			alertData.ReceivedCount++
			alertData.TotalDelay += delay
			minuteData.Alerts[webhookBody.Title] = alertData
			minuteData.TotalAlertsReceived++
			minuteData.TotalDelay += delay

			summary.TotalAlertsReceived++
			summary.TotalDelay += delay

			summary.MinuteData[currentMinute] = minuteData

			if summary.TotalAlertsReceived >= numAlerts {
				summary.AverageDelay = summary.TotalDelay / time.Duration(summary.TotalAlertsReceived)
				summary.AverageAlertsPerMinute = float64(summary.TotalAlertsReceived) / float64(currentMinute+1)
				log.Infof("Received all alerts in %v", time.Since(startTime))
				doneChan <- true
				return
			}
		case <-time.After(1 * time.Minute):
			// Log summary of the current minute
			if minuteData, exists := summary.MinuteData[currentMinute]; exists {
				minuteData.AverageDelay = minuteData.TotalDelay / time.Duration(minuteData.TotalAlertsReceived)
				logMinuteSummary(currentMinute, minuteData, numAlerts)
				summary.MinuteData[currentMinute] = minuteData
			}
		case <-time.After(2 * time.Minute):
			log.Fatalf("Timed out waiting for alerts")
			doneChan <- false
			return
		}
	}
}

func logMinuteSummary(minute int, minuteData MinuteSummaryData, numAlerts int) {
	alertsCount := len(minuteData.Alerts)
	log.Infof("Minute=%d,IsSummary=%v,NumOfAlerts=%d,Pass=%v", minute, true, alertsCount, alertsCount >= numAlerts)
	for title, data := range minuteData.Alerts {
		averageDelay := data.TotalDelay / time.Duration(data.ReceivedCount)
		log.Infof("Alert %s: Received %d times, Average Delay %v", title, data.ReceivedCount, averageDelay)
	}
	log.Infof("Total Alerts Received: %d, Total Delay: %v, Average Delay: %v",
		minuteData.TotalAlertsReceived, minuteData.TotalDelay, minuteData.AverageDelay)
}

func RunAlertsLoadTest(host string, numAlerts uint64) {
	// Remove Trailing Slashes from the Host
	host = removeTrailingSlashes(host)

	webhookChan := make(chan alertutils.WebhookBody)
	doneChan := make(chan bool)

	// Start the webhook server
	server := startWebhookServer(4010, webhookChan)
	defer server.Shutdown(context.Background())

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

	// Create multiple alerts
	err = createMultipleAlerts(host, contact.ContactId, int(numAlerts))
	if err != nil {
		log.Fatalf("Error creating multiple alerts: %v", err)
		return
	}
	log.Infof("Created %d Alerts", numAlerts)

	// Track notifications and measure delays
	go trackNotifications(webhookChan, int(numAlerts), doneChan)

	// Wait for the tracking to complete
	success := <-doneChan
	if !success {
		log.Fatalf("Failed to receive all alerts in time")
		return
	}

	log.Infof("Test completed successfully")
}

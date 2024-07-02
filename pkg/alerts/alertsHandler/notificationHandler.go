package alertsHandler

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

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/smtp"
	"strconv"
	"time"

	"github.com/slack-go/slack"

	"github.com/siglens/siglens/pkg/alerts/alertutils"
	"github.com/siglens/siglens/pkg/config"

	log "github.com/sirupsen/logrus"
)

func NotifyAlertHandlerRequest(alertID string, alertState alertutils.AlertState, alertDataMessage string) error {
	if alertID == "" {
		log.Errorf("NotifyAlertHandlerRequest: Missing alert_id")
		return errors.New("alert ID is empty")
	}

	alertDetails, err := processGetAlertDetailsRequest(alertID)
	if err != nil {
		log.Errorf("NotifyAlertHandlerRequest:Error getting alert details for alert id- %s, err=%v", alertID, err)
		return err
	}

	shouldSend, err := shouldSendNotification(alertID, alertDetails, alertState)
	if err != nil {
		log.Errorf("NotifyAlertHandlerRequest:Error checking if notification should be sent for alert id- %s, err=%v", alertID, err)
		return err
	}
	if !shouldSend {
		return nil
	}

	contact_id, message, subject, err := processGetContactDetails(alertID)
	if err != nil {
		log.Errorf("NotifyAlertHandlerRequest:Error retrieving contact and message for alert id- %s, err=%v", alertID, err)
		return err
	}
	emailIDs, channelIDs, webhooks, err := processGetEmailAndChannelID(contact_id)
	if err != nil {
		log.Errorf("NotifyAlertHandlerRequest:Error retrieving emails or channelIds of slack for contact_id- %s and alert id- %s, err=%v", contact_id, alertID, err)
		return err
	}
	emailSent := false
	slackSent := false
	webhookSent := false
	if len(emailIDs) > 0 {
		for _, emailID := range emailIDs {
			err = sendAlertEmail(emailID, subject, message, alertDataMessage)
			if err != nil {
				log.Errorf("NotifyAlertHandlerRequest: Error sending email to- %s for alert id- %s, err=%v", emailID, alertID, err)
			} else {
				emailSent = true
			}
		}
	}
	if len(channelIDs) > 0 {
		for _, channelID := range channelIDs {
			err = sendSlack(subject, message, channelID, alertState, alertDataMessage)
			if err != nil {
				log.Errorf("NotifyAlertHandlerRequest: Error sending Slack message to channelID- %v for alert id- %v, err=%v", channelID, alertID, err)
			} else {
				slackSent = true
			}
		}
	}
	if len(webhooks) > 0 {
		for _, webhook := range webhooks {
			err = sendWebhooks(webhook.Webhook, subject, message, alertDataMessage, alertDetails.NumEvaluationsCount)
			if err != nil {
				log.Errorf("NotifyAlertHandlerRequest: Error sending Webhook message to webhook- %s for alert id- %s, err=%v", webhook.Webhook, alertID, err)
			} else {
				webhookSent = true
			}
		}
	}

	if !emailSent && !slackSent && !webhookSent {
		return errors.New("neither emails or slack message or webhook sent for this notification")

	}

	err = processUpdateLastSentTimeAndAlertState(alertID, alertState)
	if err != nil {
		log.Errorf("NotifyAlertHandlerRequest:Error updating last sent time for alert_id- %s, err=%v", alertID, err)
		return err
	}
	return nil
}

// shouldSendNotification checks if the notification should be sent based on the cooldown period and silence minutes
// If the last alert state is normal and the current alert state is also normal, then we should not send the notification
func shouldSendNotification(alertID string, alertDetails *alertutils.AlertDetails, currentAlertState alertutils.AlertState) (bool, error) {
	alertNotification, err := processGetAlertNotification(alertID)
	if err != nil {
		log.Errorf("shouldSendNotification:Error getting alert notification details for alert id- %s, err=%v", alertID, err)
		return false, err
	}

	if currentAlertState == alertutils.Normal {
		if alertNotification.LastAlertState == alertutils.Inactive {
			// If the last alert state is inactive and the current alert state is normal, then we should not send the notification
			return false, nil
		}
		if alertNotification.LastAlertState == currentAlertState {
			// If the last alert state is normal and the current alert state is also normal, then we should not send the notification
			return false, nil
		}
	}

	cooldownOver := isCooldownOver(alertNotification.CooldownPeriod, alertNotification.LastSentTime)
	if !cooldownOver {
		return false, nil
	}
	silenceMinutesOver := isSilenceMinutesOver(alertDetails.SilenceMinutes, alertNotification.LastSentTime)
	if !silenceMinutesOver {
		return false, nil
	}

	return true, nil
}

func sendAlertEmail(emailID, subject, message string, alertDataMessage string) error {
	host, port, senderEmail, senderPassword := config.GetEmailConfig()
	auth := smtp.PlainAuth("", senderEmail, senderPassword, host)
	body := "To: " + emailID + "\r\n" +
		"Subject: " + subject + "\r\n" +
		"\r\n" +
		message + "\r\n"
	if alertDataMessage != "" {
		body = body + "Alert Data: " + alertDataMessage + "\r\n"
	}
	err := smtp.SendMail(host+":"+strconv.Itoa(port), auth, senderEmail, []string{emailID}, []byte(body))
	return err
}
func sendWebhooks(webhookUrl, subject, message string, alertDataMessage string, numEvaluationsCount uint64) error {
	if alertDataMessage != "" {
		message = message + "\nAlert Data: " + alertDataMessage
	}

	webhookBody := alertutils.WebhookBody{
		Receiver:            "My Super Webhook",
		Status:              "firing",
		Title:               subject,
		Body:                message,
		NumEvaluationsCount: numEvaluationsCount,
		Alerts: []alertutils.Alert{
			{
				Status: "firing",
			},
		},
	}

	data, _ := json.Marshal(webhookBody)

	r, err := http.NewRequest("POST", webhookUrl, bytes.NewBuffer(data))
	if err != nil {
		log.Errorf("Error creating request: %v", err)
	}

	r.Header.Add("Content-Type", "application/json")
	client := &http.Client{}
	_, err1 := client.Do(r)
	if err1 != nil {
		log.Errorf("Error sending request: %v", err)
	}
	return err
}

func isSilenceMinutesOver(silenceMinutes uint64, lastSendTime time.Time) bool {
	if lastSendTime.IsZero() {
		return true
	}

	currentTimeUTC := time.Now().UTC()
	lastSendTimeUTC := lastSendTime.UTC()
	silenceMinutesUTC := time.Duration(silenceMinutes) * time.Minute

	return currentTimeUTC.Sub(lastSendTimeUTC) >= silenceMinutesUTC
}

func isCooldownOver(cooldownMinutes uint64, lastSendTime time.Time) bool {
	if lastSendTime.IsZero() {
		return true
	}

	currentTimeUTC := time.Now().UTC()
	lastSendTimeUTC := lastSendTime.UTC()
	cooldownDuration := time.Duration(cooldownMinutes) * time.Minute
	return currentTimeUTC.Sub(lastSendTimeUTC) >= cooldownDuration
}

func getSlackMessageColor(alertState alertutils.AlertState) string {
	if alertState == alertutils.Normal {
		return "#00FF00"
	}
	return "#FF0000"
}

func sendSlack(alertName string, message string, channel alertutils.SlackTokenConfig, alertState alertutils.AlertState, alertDataMessage string) error {

	channelID := channel.ChannelId
	token := channel.SlToken
	alert := fmt.Sprintf("Alert Name : '%s'", alertName)
	client := slack.New(token, slack.OptionDebug(false))
	color := getSlackMessageColor(alertState)

	attachment := slack.Attachment{
		Pretext: alert,
		Text:    message,
		Color:   color,
		Fields: []slack.AttachmentField{
			{
				Title: "Date",
				Value: time.Now().String(),
			},
		},
	}

	if alertDataMessage != "" {
		attachment.Fields = append(attachment.Fields, slack.AttachmentField{
			Title: "Alert Data",
			Value: alertDataMessage,
		})
	}

	_, _, err := client.PostMessage(
		channelID,
		slack.MsgOptionText("New message from Alert System", false),
		slack.MsgOptionAttachments(attachment),
	)
	return err
}

func processGetAlertNotification(alert_id string) (*alertutils.Notification, error) {
	alertNotification, err := databaseObj.GetAlertNotification(alert_id)
	if err != nil {
		log.Errorf("ProcessGetAlertNotification:Error getting alert notification details for alert id- %s err=%v", alert_id, err)
		return nil, err
	}
	return alertNotification, nil
}

func processGetAlertDetailsRequest(alert_id string) (*alertutils.AlertDetails, error) {
	alertDataObj, err := databaseObj.GetAlert(alert_id)
	if err != nil {
		log.Errorf("ProcessGetAlertDetailsRequest:Error getting alert details for alert id- %s err=%v", alert_id, err)
		return nil, err
	}
	return alertDataObj, nil
}

func processGetContactDetails(alert_id string) (string, string, string, error) {
	id, message, subject, err := databaseObj.GetContactDetails(alert_id)
	if err != nil {
		log.Errorf("ProcessGetContactDetails: Error getting contact details for alert id- %s, err=%v", alert_id, err)
		return "", "", "", err
	}
	return id, message, subject, nil
}

func processGetEmailAndChannelID(contact_id string) ([]string, []alertutils.SlackTokenConfig, []alertutils.WebHookConfig, error) {
	emails, slacks, webhook, err := databaseObj.GetEmailAndChannelID(contact_id)
	if err != nil {
		log.Errorf("ProcessGetEmailAndChannelID: Error in getting emails and channel_ids for contact_id- %s, err=%v", contact_id, err)
		return nil, nil, nil, err
	}

	return emails, slacks, webhook, nil
}

func processUpdateLastSentTimeAndAlertState(alert_id string, alertState alertutils.AlertState) error {
	err := databaseObj.UpdateLastSentTimeAndAlertState(alert_id, alertState)
	if err != nil {
		log.Errorf("processUpdateLastSentTimeAndAlertState: Unable to update last_sent_time for alert_id- %s, err=%v", alert_id, err)
		return err
	}
	return nil
}

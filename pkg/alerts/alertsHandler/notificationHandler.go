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
	"github.com/siglens/siglens/pkg/utils"

	log "github.com/sirupsen/logrus"
)

func NotifyAlertHandlerRequest(alertID string, alertState alertutils.AlertState, alertDataMessage string) (bool, error) {
	if alertID == "" {
		log.Errorf("NotifyAlertHandlerRequest: Missing alert_id")
		return false, errors.New("alert ID is empty")
	}

	alertDetails, err := processGetAlertDetailsRequest(alertID)
	if err != nil {
		log.Errorf("NotifyAlertHandlerRequest:Error getting alert details for alert id- %s, err=%v", alertID, err)
		return false, err
	}

	shouldSend, err := shouldSendNotification(alertID, alertDetails, alertState)
	if err != nil {
		log.Errorf("NotifyAlertHandlerRequest:Error checking if notification should be sent for alert id- %s, err=%v", alertID, err)
		return false, err
	}
	if !shouldSend {
		return false, nil
	}

	contact_id, message, subject, err := processGetContactDetails(alertID)
	if err != nil {
		log.Errorf("NotifyAlertHandlerRequest:Error retrieving contact and message for alert id- %s, err=%v", alertID, err)
		return false, err
	}
	emailIDs, channelIDs, webhooks, err := processGetEmailAndChannelID(contact_id)
	if err != nil {
		log.Errorf("NotifyAlertHandlerRequest:Error retrieving emails or channelIds of slack for contact_id- %s and alert id- %s, err=%v", contact_id, alertID, err)
		return false, err
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
			err = sendWebhooks(webhook.Webhook, subject, message, alertDataMessage, alertDetails.NumEvaluationsCount, alertState, webhook.Headers)
			if err != nil {
				log.Errorf("NotifyAlertHandlerRequest: Error sending Webhook message to webhook- %s for alert id- %s, err=%v", webhook.Webhook, alertID, err)
			} else {
				webhookSent = true
			}
		}
	}

	if !emailSent && !slackSent && !webhookSent {
		return false, errors.New("neither emails or slack message or webhook sent for this notification")
	}

	return true, nil
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

func sendWebhooks(webhookUrl, subject, message string, alertDataMessage string, numEvaluationsCount uint64,
	alertState alertutils.AlertState, headers map[string]string) error {

	var status string
	switch alertState {
	case alertutils.Normal:
		status = "normal"
	case alertutils.Firing:
		status = "firing"
	case alertutils.Pending, alertutils.Inactive:
		return fmt.Errorf("sendWebhooks: Invalid alert state %v", alertState)
	}

	if alertDataMessage != "" {
		message = message + "\nAlert Data: " + alertDataMessage
	}

	webhookBody := alertutils.WebhookBody{
		Receiver:            "My Super Webhook",
		Status:              status,
		Title:               subject,
		Body:                message,
		NumEvaluationsCount: numEvaluationsCount,
		Alerts: []alertutils.Alert{
			{
				Status: status,
			},
		},
	}

	data, _ := json.Marshal(webhookBody)

	r, err := http.NewRequest("POST", webhookUrl, bytes.NewBuffer(data))
	if err != nil {
		log.Errorf("sendWebhooks: Error creating request. WebhookURL=%v, Error=%v", webhookUrl, err)
		return err
	}

	r.Header.Add("Content-Type", "application/json")

	for key, value := range headers {
		r.Header.Add(key, value)
	}
	client := alertutils.GetCertErrorForgivingHttpClient()
	resp, err := client.Do(r)
	if err != nil {
		log.Errorf("sendWebhooks: Error sending request. WebhookURL=%v, Error=%v", webhookUrl, err)
		return err
	}
	resp.Body.Close()
	return nil
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
	client := slack.New(token, slack.OptionDebug(false))
	color := getSlackMessageColor(alertState)

	attachment := slack.Attachment{
		AuthorName: alertName,
		Text:       message,
		Color:      color,
		Ts:         json.Number(strconv.FormatInt(time.Now().Unix(), 10)),
	}

	if alertDataMessage != "" {
		if utils.IsValidURL(alertDataMessage) {
			attachment.Actions = []slack.AttachmentAction{
				{
					Name:  "view_results",
					Text:  "View Results",
					Type:  "button",
					URL:   alertDataMessage,
					Style: "primary",
				},
			}
		} else {
			attachment.Fields = []slack.AttachmentField{
				{
					Title: "Alert Details",
					Value: alertDataMessage,
				},
			}
		}
	}

	if utils.IsValidURL(message) {
		// If the Message that a user has set while creating is a URL,
		// then we will add a button to view the message
		encodedURL, err := utils.EncodeURL(message)
		if err != nil {
			log.Errorf("sendSlack: Error encoding URL. Error=%v", err)
		} else {
			messageAttachment := slack.AttachmentAction{
				Name:  "view_message_link",
				Text:  "View Message",
				Type:  "button",
				URL:   encodedURL,
				Style: "default",
			}
			if len(attachment.Actions) > 0 {
				attachment.Actions = append(attachment.Actions, messageAttachment)
			} else {
				attachment.Actions = []slack.AttachmentAction{messageAttachment}
			}

			attachment.Text = ""
		}
	}

	_, _, err := client.PostMessage(
		channelID,
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

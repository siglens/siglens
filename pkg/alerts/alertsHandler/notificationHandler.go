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

func NotifyAlertHandlerRequest(alertID string) error {
	if alertID == "" {
		log.Errorf("NotifyAlertHandlerRequest: Missing alert_id")
		return errors.New("Alert ID is empty")
	}
	cooldownOver, err := isCooldownOver(alertID)
	if err != nil {
		log.Errorf("NotifyAlertHandlerRequest:Error checking cooldown period for alert id- %s, err=%v", alertID, err)
		return err
	}
	if !cooldownOver {
		return nil
	}
	silenceMinutesOver, err := isSilenceMinutesOver(alertID)
	if err != nil {
		log.Errorf("NotifyAlertHandlerRequest:Error checking silence period for alert id- %s, err=%v", alertID, err)
		return err
	}
	if !silenceMinutesOver {
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
			err = sendAlertEmail(emailID, subject, message)
			if err != nil {
				log.Errorf("NotifyAlertHandlerRequest: Error sending email to- %s for alert id- %s, err=%v", emailID, alertID, err)
			} else {
				emailSent = true
			}
		}
	}
	if len(channelIDs) > 0 {
		for _, channelID := range channelIDs {
			err = sendSlack(subject, message, channelID)
			if err != nil {
				log.Errorf("NotifyAlertHandlerRequest: Error sending Slack message to channelID- %v for alert id- %v, err=%v", channelID, alertID, err)
			} else {
				slackSent = true
			}
		}
	}
	if len(webhooks) > 0 {
		for _, webhook := range webhooks {
			err = sendWebhooks(webhook.Webhook, subject, message)
			if err != nil {
				log.Errorf("NotifyAlertHandlerRequest: Error sending Webhook message to webhook- %s for alert id- %s, err=%v", webhook.Webhook, alertID, err)
			} else {
				webhookSent = true
			}
		}
	}

	if !emailSent && !slackSent && !webhookSent {
		return errors.New("Neither emails or slack message or webhook sent for this notification")

	}

	err = processUpdateLastSentTime(alertID)
	if err != nil {
		log.Errorf("NotifyAlertHandlerRequest:Error updating last sent time for alert_id- %s, err=%v", alertID, err)
		return err
	}
	return nil
}

func sendAlertEmail(emailID, subject, message string) error {
	host, port, senderEmail, senderPassword := config.GetEmailConfig()
	auth := smtp.PlainAuth("", senderEmail, senderPassword, host)
	body := "To: " + emailID + "\r\n" +
		"Subject: " + subject + "\r\n" +
		"\r\n" +
		message + "\r\n"
	err := smtp.SendMail(host+":"+strconv.Itoa(port), auth, senderEmail, []string{emailID}, []byte(body))
	return err
}
func sendWebhooks(webhookUrl, subject, message string) error {
	webhookBody := alertutils.WebhookBody{
		Receiver: "My Super Webhook",
		Status:   "firing",
		Title:    subject,
		Body:     message,
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

func isSilenceMinutesOver(alertID string) (bool, error) {
	silenceMinutes, lastSendTime, err := processGetSilenceMinutesRequest(alertID)

	if lastSendTime.IsZero() {
		return true, nil
	}

	if err != nil {
		return true, err
	}

	currentTimeUTC := time.Now().UTC()
	lastSendTimeUTC := lastSendTime.UTC()
	silenceMinutesUTC := time.Duration(silenceMinutes) * time.Minute
	if currentTimeUTC.Sub(lastSendTimeUTC) >= silenceMinutesUTC {
		return true, nil
	}
	return false, nil
}

func isCooldownOver(alertID string) (bool, error) {
	cooldownMinutes, lastSendTime, err := processGetCooldownRequest(alertID)

	if lastSendTime.IsZero() {
		return true, nil
	}

	if err != nil {
		return false, err
	}

	currentTimeUTC := time.Now().UTC()
	lastSendTimeUTC := lastSendTime.UTC()
	cooldownDuration := time.Duration(cooldownMinutes) * time.Minute
	if currentTimeUTC.Sub(lastSendTimeUTC) >= cooldownDuration {
		return true, nil
	}
	return false, nil
}

func sendSlack(alertName string, message string, channel alertutils.SlackTokenConfig) error {

	channelID := channel.ChannelId
	token := channel.SlToken
	alert := fmt.Sprintf("Alert Name : '%s'", alertName)
	client := slack.New(token, slack.OptionDebug(false))

	attachment := slack.Attachment{
		Pretext: alert,
		Text:    message,
		Color:   "#FF0000",
		Fields: []slack.AttachmentField{
			{
				Title: "Date",
				Value: time.Now().String(),
			},
		},
	}
	_, _, err := client.PostMessage(
		channelID,
		slack.MsgOptionText("New message from Alert System", false),
		slack.MsgOptionAttachments(attachment),
	)
	return err
}

func processGetCooldownRequest(alert_id string) (uint64, time.Time, error) {
	period, last_time, err := databaseObj.GetCoolDownDetails(alert_id)
	if err != nil {
		log.Errorf("ProcessGetCooldownRequest:Error getting cooldown details for alert id- %s err=%v", alert_id, err)
		return 0, time.Time{}, err
	}
	return period, last_time, nil
}

func processGetSilenceMinutesRequest(alert_id string) (uint64, time.Time, error) {
	alertDataObj, err := databaseObj.GetAlert(alert_id)
	if err != nil {
		log.Errorf("ProcessGetSilenceMinutesRequest:Error getting alert details for alert id- %s err=%v", alert_id, err)
		return 0, time.Time{}, err
	}

	_, last_time, err := databaseObj.GetCoolDownDetails(alert_id)
	if err != nil {
		log.Errorf("ProcessGetSilenceMinutesRequest:Error getting cooldown details for alert id- %s err=%v", alert_id, err)
		return 0, time.Time{}, err
	}

	return alertDataObj.SilenceMinutes, last_time, nil
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

func processUpdateLastSentTime(alert_id string) error {
	err := databaseObj.UpdateLastSentTime(alert_id)
	if err != nil {
		log.Errorf("ProcessUpdateLastSentTime: Unable to update last_sent_time for alert_id- %s, err=%v", alert_id, err)
		return err
	}
	return nil
}

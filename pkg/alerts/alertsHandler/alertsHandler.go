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

package alertsHandler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
	alertsqlite "github.com/siglens/siglens/pkg/alerts/alertsqlite"
	"github.com/siglens/siglens/pkg/config"
	"gorm.io/gorm"

	"github.com/siglens/siglens/pkg/alerts/alertutils"
	"github.com/siglens/siglens/pkg/utils"
	"github.com/valyala/fasthttp"

	log "github.com/sirupsen/logrus"
)

type database interface {
	Connect() error
	CloseDb()
	SetDB(db *gorm.DB)
	CreateAlert(alertInfo *alertutils.AlertDetails) (alertutils.AlertDetails, error)
	GetAlert(alert_id string) (*alertutils.AlertDetails, error)
	CreateAlertHistory(alertHistoryDetails *alertutils.AlertHistoryDetails) (*alertutils.AlertHistoryDetails, error)
	GetAlertHistory(alertId string) ([]*alertutils.AlertHistoryDetails, error)
	GetAllAlerts(orgId uint64) ([]alertutils.AlertDetails, error)
	CreateMinionSearch(alertInfo *alertutils.MinionSearch) (alertutils.MinionSearch, error)
	GetMinionSearch(alert_id string) (*alertutils.MinionSearch, error)
	GetAllMinionSearches(orgId uint64) ([]alertutils.MinionSearch, error)
	UpdateMinionSearchStateByAlertID(alertId string, alertState alertutils.AlertState) error
	UpdateAlert(*alertutils.AlertDetails) error
	UpdateSilenceMinutes(*alertutils.AlertDetails) error
	DeleteAlert(alert_id string) error
	CreateContact(*alertutils.Contact) error
	GetAllContactPoints(orgId uint64) ([]alertutils.Contact, error)
	UpdateContactPoint(contact *alertutils.Contact) error
	GetCoolDownDetails(alert_id string) (uint64, time.Time, error)
	GetContactDetails(alert_id string) (string, string, string, error)
	GetEmailAndChannelID(contact_id string) ([]string, []alertutils.SlackTokenConfig, []alertutils.WebHookConfig, error)
	UpdateLastSentTime(alert_id string) error
	UpdateAlertStateByAlertID(alertId string, alertState alertutils.AlertState) error
	DeleteContactPoint(contact_id string) error
}

var databaseObj database

var invalidDatabaseProvider = "database provider is not configured in server.yaml"

type TestContactPointRequest struct {
	Type     string                 `json:"type"`
	Settings map[string]interface{} `json:"settings"`
}

func ConnectSiglensDB() error {
	databaseObj = &alertsqlite.Sqlite{}
	if databaseObj == nil {
		log.Errorf("ConnectSiglensDB: %v", invalidDatabaseProvider)
		return errors.New("ConnectSiglensDB: database provider is not configured in server.yaml")
	}
	err := databaseObj.Connect()
	if err != nil {
		return err
	}
	return nil
}

func Disconnect() {
	if databaseObj == nil {
		return
	}
	databaseObj.CloseDb()
}

func ProcessVersionInfo(ctx *fasthttp.RequestCtx) {
	responseBody := make(map[string]interface{})
	ctx.SetStatusCode(fasthttp.StatusOK)
	responseBody["version"] = config.SigLensVersion
	utils.WriteJsonResponse(ctx, responseBody)
}

func ProcessCreateAlertRequest(ctx *fasthttp.RequestCtx, org_id uint64) {
	if databaseObj == nil {
		utils.SendError(ctx, invalidDatabaseProvider, "", nil)
		return
	}
	responseBody := make(map[string]interface{})
	var alertToBeCreated alertutils.AlertDetails

	rawJSON := ctx.PostBody()
	if len(rawJSON) == 0 {
		utils.SendError(ctx, "Received empty request", "", nil)
		return
	}
	alertToBeCreated.OrgId = org_id
	err := json.Unmarshal(rawJSON, &alertToBeCreated)
	if err != nil {
		utils.SendError(ctx, "Failed to unmarshal json", "", err)
		return
	}
	alertDataObj, err := databaseObj.CreateAlert(&alertToBeCreated)
	if err != nil {
		utils.SendError(ctx, "Failed to create alert", fmt.Sprintf("alert name: %v", alertToBeCreated.AlertName), err)
		return
	}

	_, err = AddCronJob(&alertDataObj)
	if err != nil {
		utils.SendError(ctx, "Failed to add CronJob for alert", fmt.Sprintf("alert name: %v", alertDataObj.AlertName), err)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	responseBody["message"] = "Successfully created an alert"
	utils.WriteJsonResponse(ctx, responseBody)
}

func ProcessSilenceAlertRequest(ctx *fasthttp.RequestCtx) {
	responseBody := make(map[string]interface{})

	// Check if databaseObj is nil
	if databaseObj == nil {
		utils.SendError(ctx, invalidDatabaseProvider, "", nil)
		return
	}

	// Check if request body is empty
	if string(ctx.PostBody()) == "" {
		utils.SendError(ctx, "Received empty request", "", nil)
		return
	}

	// Parse request
	var silenceRequest struct {
		AlertID        string `json:"alert_id"`
		SilenceMinutes uint64 `json:"silence_minutes"`
	}
	if err := json.Unmarshal(ctx.PostBody(), &silenceRequest); err != nil {
		utils.SendError(ctx, "Failed to unmarshal json", "", err)
		return
	}

	// Find alert and update SilenceMinutes
	alertDataObj, err := databaseObj.GetAlert(silenceRequest.AlertID)
	if err != nil {
		utils.SendError(ctx, "Failed to find alert", fmt.Sprintf("alert ID: %v", silenceRequest.AlertID), err)
		return
	}

	alertDataObj.SilenceMinutes = silenceRequest.SilenceMinutes
	// Update the SilenceMinutes
	err = databaseObj.UpdateAlert(alertDataObj)
	if err != nil {
		utils.SendError(ctx, "Failed to update alert", fmt.Sprintf("alert name: %v", alertDataObj.AlertName), err)
		return
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
	responseBody["message"] = "Successfully updated silence period"
	utils.WriteJsonResponse(ctx, responseBody)
}

func ProcessTestContactPointRequest(ctx *fasthttp.RequestCtx) {
	var testContactRequest TestContactPointRequest
	if err := json.Unmarshal(ctx.PostBody(), &testContactRequest); err != nil {
		utils.SendError(ctx, "Failed to unmarshal json", "ProcessTestContactPointRequest: Request Body: "+string(ctx.PostBody()), err)
		return
	}

	switch testContactRequest.Type {
	case "slack":
		channelID, ok := testContactRequest.Settings["channel_id"].(string)
		if !ok {
			utils.SendError(ctx, "channel_id is required but is missing", "ProcessTestContactPointRequest: Slack Channel ID is missing. Request Body: "+string(ctx.PostBody()), nil)
			return
		}
		slackToken, ok := testContactRequest.Settings["slack_token"].(string)
		if !ok {
			utils.SendError(ctx, "slack_token is required but is missing", "ProcessTestContactPointRequest: Slack token is missing. Request Body: "+string(ctx.PostBody()), nil)
			return
		}
		err := sendTestMessageToSlack(channelID, slackToken)
		if err != nil {
			utils.SendError(ctx, err.Error(), "ProcessTestContactPointRequest: Error sending test message to slack. Request Body:"+string(ctx.PostBody()), err)
			return
		}
	case "webhook":
		webhookURL, ok := testContactRequest.Settings["webhook"].(string)
		if !ok {
			utils.SendError(ctx, "webhook is required but is missing", "ProcessTestContactPointRequest: Webhook URL is missing Request Body: "+string(ctx.PostBody()), nil)
			return
		}
		if err := testWebhookURL(webhookURL); err != nil {
			utils.SendError(ctx, "Failed to verify webhook URL", "", err)
			return
		}
	default:
		utils.SendError(ctx, "Invalid type", "ProcessTestContactPointRequest: Received unsupported test contact request type Request Body:"+string(ctx.PostBody()), nil)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, map[string]interface{}{"message": "Successfully verified contact point"})
}

func sendTestMessageToSlack(channelID, slackToken string) error {
	url := "https://slack.com/api/chat.postMessage"
	data := map[string]string{
		"channel": channelID,
		"text":    "This is a test message to verify the Slack integration.",
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Errorf("sendTestMessageToSlack: failed to marshal json data: %v err: %v", data, err)
		return err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+slackToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Errorf("sendTestMessageToSlack: failed to send test message to Slack: %v", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to send test message to Slack. Received status code: %v", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("sendTestMessageToSlack: failed to read response body. Response Body: %v Err: %v", resp.Body, err)
		return err
	}

	var result map[string]interface{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		log.Errorf("sendTestMessageToSlack: failed to unmarshal response body. Body: %v err:%v", body, err)
		return err
	}

	if !result["ok"].(bool) {
		return fmt.Errorf("failed to send test message to Slack: %v", result["error"])
	}

	return nil
}

func testWebhookURL(webhookURL string) error {
	resp, err := http.Get(webhookURL)
	if err != nil {
		log.Errorf("testWebhookURL: failed to test webhook URL. URL: %v err: %v", webhookURL, err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to test webhook URL: %v", resp.Status)
	}

	return nil
}

func ProcessGetAlertRequest(ctx *fasthttp.RequestCtx) {
	if databaseObj == nil {
		utils.SendError(ctx, invalidDatabaseProvider, "", nil)
		return
	}

	responseBody := make(map[string]interface{})
	alert_id := utils.ExtractParamAsString(ctx.UserValue("alertID"))
	alert, err := databaseObj.GetAlert(alert_id)
	if err != nil {
		utils.SendError(ctx, "Failed to get alert", fmt.Sprintf("alert ID: %v", alert_id), err)
		return
	}

	responseBody["alert"] = alert
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, responseBody)
}

func ProcessGetAllAlertsRequest(ctx *fasthttp.RequestCtx, org_id uint64) {
	if databaseObj == nil {
		utils.SendError(ctx, invalidDatabaseProvider, "", nil)
		return
	}

	responseBody := make(map[string]interface{})
	alerts, err := databaseObj.GetAllAlerts(org_id)
	if err != nil {
		utils.SendError(ctx, "Failed to get alerts", "", err)
		return
	}

	responseBody["alerts"] = alerts
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, responseBody)
}

func ProcessGetAllMinionSearchesRequest(ctx *fasthttp.RequestCtx, orgID uint64) {
	if databaseObj == nil {
		utils.SendError(ctx, invalidDatabaseProvider, "", nil)
		return
	}

	responseBody := make(map[string]interface{})
	minionSearches, err := databaseObj.GetAllMinionSearches(orgID)
	if err != nil {
		utils.SendError(ctx, "Failed to get all alerts", "", err)
		return
	}

	responseBody["minionSearches"] = minionSearches
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, responseBody)
}

func ProcessUpdateAlertRequest(ctx *fasthttp.RequestCtx) {
	if databaseObj == nil {
		utils.SendError(ctx, invalidDatabaseProvider, "", nil)
		return
	}
	responseBody := make(map[string]interface{})
	rawJSON := ctx.PostBody()
	if len(rawJSON) == 0 {
		utils.SendError(ctx, "Received empty request", "", nil)
		return
	}

	var alertToBeUpdated *alertutils.AlertDetails
	err := json.Unmarshal(rawJSON, &alertToBeUpdated)
	if err != nil {
		utils.SendError(ctx, "Failed to unmarshal json", "", err)
		return
	}

	err = databaseObj.UpdateAlert(alertToBeUpdated)
	if err != nil {
		utils.SendError(ctx, "Failed to update alert", fmt.Sprintf("alert name: %v", alertToBeUpdated.AlertName), err)
		return
	}

	// TODO: Update Username with specific user who changed the config. Username can be fetched from "ctx" when the authentication is implemented.
	alertEvent := alertutils.AlertHistoryDetails{
		AlertId:          alertToBeUpdated.AlertId,
		EventDescription: alertutils.ConfigChange,
		UserName:         alertutils.UserModified,
		EventTriggeredAt: time.Now().UTC(),
	}
	_, err = databaseObj.CreateAlertHistory(&alertEvent)
	if err != nil {
		log.Errorf("ProcessUpdateAlertRequest: could not create alert event in alert history. found error = %v", err)
	}

	err = RemoveCronJob(alertToBeUpdated.AlertId)

	if err != nil {
		utils.SendError(ctx, "Failed to remove cron job for alert", fmt.Sprintf("alert name: %v", alertToBeUpdated.AlertName), err)
		return
	}
	_, err = AddCronJob(alertToBeUpdated)
	if err != nil {
		utils.SendError(ctx, "Failed to add new cron job for alert", fmt.Sprintf("alert name: %v", alertToBeUpdated.AlertName), err)
		return
	}

	responseBody["message"] = "Alert updated successfully"
	utils.WriteJsonResponse(ctx, responseBody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessAlertHistoryRequest(ctx *fasthttp.RequestCtx) {
	if databaseObj == nil {
		utils.SendError(ctx, invalidDatabaseProvider, "", nil)
		return
	}

	responseBody := make(map[string]interface{})
	alertId := utils.ExtractParamAsString(ctx.UserValue("alertID"))
	alertHistory, err := databaseObj.GetAlertHistory(alertId)
	if err != nil {
		utils.SendError(ctx, "Failed to get alert history", fmt.Sprintf("alert ID: %v", alertId), err)
		return
	}

	responseBody["alertHistory"] = alertHistory
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, responseBody)
}

// request body should contain alert_id only
func ProcessDeleteAlertRequest(ctx *fasthttp.RequestCtx) {
	if databaseObj == nil {
		utils.SendError(ctx, invalidDatabaseProvider, "", nil)
		return
	}
	responseBody := make(map[string]interface{})
	rawJSON := ctx.PostBody()
	if len(rawJSON) == 0 {
		utils.SendError(ctx, "Received empty request", "", nil)
		return
	}

	var alertToBeRemoved *alertutils.AlertDetails
	err := json.Unmarshal(rawJSON, &alertToBeRemoved)
	if err != nil {
		utils.SendError(ctx, "Failed to unmarshal json", "", err)
		return
	}
	err = RemoveCronJob(alertToBeRemoved.AlertId)
	if err != nil {
		utils.SendError(ctx, "Failed to remove cron job for alert", fmt.Sprintf("alert name: %v", alertToBeRemoved.AlertName), err)
		return
	}

	err = databaseObj.DeleteAlert(alertToBeRemoved.AlertId)
	if err != nil {
		utils.SendError(ctx, "Failed to delete alert", fmt.Sprintf("alert name: %v", alertToBeRemoved.AlertName), err)
		return
	}

	responseBody["message"] = "Alert deleted successfully"
	utils.WriteJsonResponse(ctx, responseBody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessCreateContactRequest(ctx *fasthttp.RequestCtx, org_id uint64) {
	if databaseObj == nil {
		utils.SendError(ctx, invalidDatabaseProvider, "", nil)
		return
	}
	responseBody := make(map[string]interface{})
	var contactToBeCreated *alertutils.Contact

	rawJSON := ctx.PostBody()
	if len(rawJSON) == 0 {
		utils.SendError(ctx, "Received emtpy request", "", nil)
		return
	}
	err := json.Unmarshal(rawJSON, &contactToBeCreated)
	if err != nil {
		utils.SendError(ctx, "Failed to unmarshal json", "", err)
		return
	}
	contactToBeCreated.OrgId = org_id
	err = databaseObj.CreateContact(contactToBeCreated)
	if err != nil {
		utils.SendError(ctx, "Failed to create contact", fmt.Sprintf("contact name: %v", contactToBeCreated.ContactName), err)
		return
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
	responseBody["message"] = "Successfully created a contact point"
	utils.WriteJsonResponse(ctx, responseBody)
}

func ProcessGetAllContactsRequest(ctx *fasthttp.RequestCtx, org_id uint64) {
	if databaseObj == nil {
		utils.SendError(ctx, invalidDatabaseProvider, "", nil)
		return
	}

	responseBody := make(map[string]interface{})
	contacts, err := databaseObj.GetAllContactPoints(org_id)
	if err != nil {
		utils.SendError(ctx, "Failed get get all contact points", "", err)
		return
	}
	responseBody["contacts"] = contacts
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, responseBody)
}

func ProcessUpdateContactRequest(ctx *fasthttp.RequestCtx) {
	if databaseObj == nil {
		utils.SendError(ctx, invalidDatabaseProvider, "", nil)
		return
	}

	responseBody := make(map[string]interface{})
	rawJSON := ctx.PostBody()
	if len(rawJSON) == 0 {
		utils.SendError(ctx, "Received empty request", "", nil)
		return
	}

	var contactToBeUpdated *alertutils.Contact
	err := json.Unmarshal(rawJSON, &contactToBeUpdated)
	if err != nil {
		utils.SendError(ctx, "Failed to unmarshal json", "", err)
		return
	}
	err = databaseObj.UpdateContactPoint(contactToBeUpdated)
	if err != nil {
		utils.SendError(ctx, "Failed to update contact", fmt.Sprintf("contact name: %v", contactToBeUpdated.ContactName), err)
		return
	}
	responseBody["message"] = "Contact details updated successfully"
	utils.WriteJsonResponse(ctx, responseBody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessDeleteContactRequest(ctx *fasthttp.RequestCtx) {
	if databaseObj == nil {
		utils.SendError(ctx, invalidDatabaseProvider, "", nil)
		return
	}

	responseBody := make(map[string]interface{})
	rawJSON := ctx.PostBody()
	if len(rawJSON) == 0 {
		utils.SendError(ctx, "Received empty request", "", nil)
		return
	}

	var contact *alertutils.Contact
	err := json.Unmarshal(rawJSON, &contact)
	if err != nil {
		utils.SendError(ctx, "Failed to unmarshal json", "", err)
		return
	}

	err = databaseObj.DeleteContactPoint(contact.ContactId)
	if err != nil {
		utils.SendError(ctx, "Failed to delete contact", fmt.Sprintf("contact ID: %v", contact.ContactId), err)
		return
	}

	responseBody["message"] = "Contact point deleted successfully"
	utils.WriteJsonResponse(ctx, responseBody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func InitAlertingService(getMyIds func() []uint64) {
	if databaseObj == nil {
		log.Errorf("InitAlertingService, err = %+v", invalidDatabaseProvider)
		return
	}
	myids := getMyIds()
	for _, myid := range myids {

		//get all alerts from database
		allAlerts, err := databaseObj.GetAllAlerts(myid)
		if err != nil {
			log.Errorf("InitAlertingService: unable to GetAllAlerts: ,err: %+v", err)
		}
		for _, alertInfo := range allAlerts {
			alertDataObj, err := databaseObj.GetAlert(alertInfo.AlertId)
			if err != nil {
				log.Errorf("InitAlertingService: unable to GetAlert with alertId %v: ,err: %+v", alertInfo.AlertId, err)
			}
			if alertDataObj != nil {
				_, err = AddCronJob(alertDataObj)
				if err != nil {
					log.Errorf("InitAlertingService: could not add a new CronJob corresponding to alert=%+v, err=%+v", alertDataObj.AlertName, err)
					return
				}
			}
		}
	}
}

func InitMinionSearchService(getMyIds func() []uint64) {
	if databaseObj == nil {
		log.Errorf("InitMinionSearchService, err = %+v", invalidDatabaseProvider)
		return
	}
	myids := getMyIds()
	for _, myid := range myids {

		//get all alerts from database
		allMinionSearches, err := databaseObj.GetAllMinionSearches(myid)
		if err != nil {
			log.Errorf("InitMinionSearchService: unable to GetAllAlerts: ,err: %+v", err)
		}
		for _, minionSearch := range allMinionSearches {
			_, err = AddMinionSearchCronJob(&minionSearch)
			if err != nil {
				log.Errorf("InitMinionSearchService: could not add a new CronJob corresponding to alert=%+v, err=%+v", minionSearch.AlertName, err)
				return
			}

		}
	}
}

func ProcessCreateLogMinionSearchRequest(ctx *fasthttp.RequestCtx, org_id uint64) {
	if databaseObj == nil {
		utils.SendError(ctx, invalidDatabaseProvider, "", nil)
		return
	}
	responseBody := make(map[string]interface{})
	var LogLinesEntry alertutils.LogLinesFile

	rawJSON := ctx.PostBody()
	if len(rawJSON) == 0 {
		utils.SendError(ctx, "Received empty request", "", nil)
		return
	}
	err := json.Unmarshal(rawJSON, &LogLinesEntry)
	if err != nil {
		utils.SendError(ctx, "Failed to unmarshal json", "", err)
		return
	}
	minionSearches := convertToSiglensAlert(LogLinesEntry)
	for _, searchToBeCreated := range minionSearches {
		searchToBeCreated.OrgId = org_id
		searchDataObj, err := databaseObj.CreateMinionSearch(searchToBeCreated)
		if err != nil {
			utils.SendError(ctx, "Failed to create alert", fmt.Sprintf("alert name: %v", searchToBeCreated.AlertName), err)
			return
		}
		_, err = AddMinionSearchCronJob(&searchDataObj)
		if err != nil {
			utils.SendError(ctx, "Failed to create cron job for alert", fmt.Sprintf("alert name: %v", searchDataObj.AlertName), err)
			return
		}
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	responseBody["message"] = "Successfully created an minion search"
	utils.WriteJsonResponse(ctx, responseBody)
}

func convertToSiglensAlert(lmDetails alertutils.LogLinesFile) []*alertutils.MinionSearch {
	var minionSearches []*alertutils.MinionSearch
	for _, entry := range lmDetails.LogAlerts {
		alert_id := uuid.New().String()
		alert_name := entry.LogTextHash

		queryParams := alertutils.QueryParams{
			DataSource:    "Logs",
			QueryLanguage: entry.QueryLanguage,
			StartTime:     time.Now().Add(time.Duration(-15) * time.Minute).String(),
			EndTime:       time.Now().String(),
			QueryText:     entry.Query,
		}
		siglensAlert := &alertutils.MinionSearch{
			AlertName:       alert_name,
			AlertId:         alert_id,
			State:           alertutils.Inactive,
			CreateTimestamp: time.Now(),
			QueryParams:     queryParams,
			Repository:      entry.Repository,
			Filename:        entry.Filename,
			LineNumber:      entry.LineNumber,
			LogText:         entry.LogText,
			LogTextHash:     entry.LogTextHash,
			LogLevel:        entry.LogLevel,
			Condition:       alertutils.IsAbove,
			Value:           float32(entry.Value),
			EvalFor:         0,
			EvalInterval:    1,
			Message:         "Minion search " + alert_name,
		}
		minionSearches = append(minionSearches, siglensAlert)
	}
	return minionSearches
}

func ProcessGetMinionSearchRequest(ctx *fasthttp.RequestCtx) {
	if databaseObj == nil {
		utils.SendError(ctx, invalidDatabaseProvider, "", nil)
		return
	}

	responseBody := make(map[string]interface{})
	alert_id := utils.ExtractParamAsString(ctx.UserValue("alertID"))
	msearch, err := databaseObj.GetMinionSearch(alert_id)
	if err != nil {
		utils.SendError(ctx, "Failed to get alert", fmt.Sprintf("alert ID: %v", alert_id), err)
		return
	}

	responseBody["minionsearch"] = msearch
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, responseBody)
}

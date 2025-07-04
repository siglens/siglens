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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	alertsqlite "github.com/siglens/siglens/pkg/alerts/alertsqlite"
	"github.com/siglens/siglens/pkg/ast/pipesearch"
	"github.com/siglens/siglens/pkg/integrations/prometheus/promql"
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
	GetAlertHistoryByAlertID(alertHistoryParams *alertutils.AlertHistoryQueryParams) ([]*alertutils.AlertHistoryDetails, error)
	GetAllAlerts(orgId int64) ([]*alertutils.AlertDetails, error)
	CreateMinionSearch(alertInfo *alertutils.MinionSearch) (alertutils.MinionSearch, error)
	GetMinionSearch(alert_id string) (*alertutils.MinionSearch, error)
	GetAllMinionSearches(orgId int64) ([]alertutils.MinionSearch, error)
	UpdateMinionSearchStateByAlertID(alertId string, alertState alertutils.AlertState) error
	UpdateAlert(*alertutils.AlertDetails) error
	UpdateSilenceMinutes(*alertutils.AlertDetails) error
	DeleteAlert(alert_id string) error
	CreateContact(*alertutils.Contact) error
	GetAllContactPoints(orgId int64) ([]alertutils.Contact, error)
	UpdateContactPoint(contact *alertutils.Contact) error
	GetCoolDownDetails(alert_id string) (uint64, time.Time, error)
	GetAlertNotification(alert_id string) (*alertutils.Notification, error)
	GetContactDetails(alert_id string) (string, string, string, error)
	GetEmailAndChannelID(contact_id string) ([]string, []alertutils.SlackTokenConfig, []alertutils.WebHookConfig, error)
	UpdateAlertStateAndNotificationDetails(alertId string, alertState alertutils.AlertState, updateNotificationState bool) error
	DeleteContactPoint(contact_id string) error
}

var databaseObj database

var invalidDatabaseProvider = "database provider is not configured in server.yaml"

type TestContactPointRequest struct {
	Type     string                 `json:"type"`
	Settings map[string]interface{} `json:"settings"`
	Headers  map[string]string      `json:"headers"`
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

func validateAlertTypeAndQuery(alertToBeCreated *alertutils.AlertDetails) (string, error) {
	switch alertToBeCreated.AlertType {
	case alertutils.AlertTypeLogs:
		_, queryAggs, _, err := pipesearch.ParseQuery(alertToBeCreated.QueryParams.QueryText, 0, alertToBeCreated.QueryParams.QueryLanguage)
		if err != nil {
			return queryTextAndLanguage(alertToBeCreated), fmt.Errorf("error Parsing logs Query. Error=%v", err)
		}

		if queryAggs == nil {
			return queryTextAndLanguage(alertToBeCreated), fmt.Errorf("query does not contain any aggregation. Expected Stats Query")
		}

		isStatsQuery := queryAggs.IsStatsAggPresentInChain()
		if !isStatsQuery {
			return queryTextAndLanguage(alertToBeCreated), fmt.Errorf("query does not contain any aggregation. Expected Stats Query")
		}

		allMeasureAggGroups := queryAggs.GetAllMeasureAggsInChain()
		if len(allMeasureAggGroups) == 0 || len(allMeasureAggGroups[0]) == 0 {
			return queryTextAndLanguage(alertToBeCreated), fmt.Errorf("query does not contain any measure aggregation")
		}

		if len(allMeasureAggGroups) > 1 || len(allMeasureAggGroups[0]) > 1 {
			return queryTextAndLanguage(alertToBeCreated), fmt.Errorf("query contains more than one measure aggregation")
		}
	case alertutils.AlertTypeMetrics:
		_, _, _, _, errorLog, _, err := promql.ParseMetricTimeSeriesRequest([]byte(alertToBeCreated.MetricsQueryParamsString))
		if err != nil {
			return errorLog, err
		}
	case alertutils.AlertTypeMinion:
		return fmt.Sprintf("Alert Type: %v", alertToBeCreated.AlertType), fmt.Errorf("minion alerts are not supported")
	}

	return "", nil
}

func queryTextAndLanguage(alert *alertutils.AlertDetails) string {
	params := alert.QueryParams
	return fmt.Sprintf("QuerySearchText: %v, QueryLanguage: %v", params.QueryText, params.QueryLanguage)
}

func ProcessCreateAlertRequest(ctx *fasthttp.RequestCtx, org_id int64) {
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
		utils.SendError(ctx, fmt.Sprintf("Failed to unmarshal json. Error=%v", err), "", err)
		return
	}

	if alertToBeCreated.EvalWindow < alertToBeCreated.EvalInterval {
		utils.SendError(ctx, "EvalWindow should be greater than or equal to EvalInterval", fmt.Sprintf("EvalWindow: %v, EvalInterval:%v", alertToBeCreated.EvalWindow, alertToBeCreated.EvalInterval), nil)
		return
	}

	// Validate Alert Type and Query
	extraMsgToLog, err := validateAlertTypeAndQuery(&alertToBeCreated)
	if err != nil {
		utils.SendError(ctx, fmt.Sprintf("Failed to Create Alert. Error=%v", err), extraMsgToLog, err)
		return
	}

	alertDataObj, err := databaseObj.CreateAlert(&alertToBeCreated)
	if err != nil {
		utils.SendError(ctx, fmt.Sprintf("Failed to Create Alert. Error=%v", err), fmt.Sprintf("alert name: %v", alertToBeCreated.AlertName), err)
		return
	}

	_, err = AddCronJob(&alertDataObj)
	if err != nil {
		utils.SendError(ctx, fmt.Sprintf("Failed to add CronJob for alert. Error=%v", err), fmt.Sprintf("alert name: %v", alertDataObj.AlertName), err)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	responseBody["message"] = "Successfully created an alert"
	utils.WriteJsonResponse(ctx, responseBody)
}

func processAlertSilence(ctx *fasthttp.RequestCtx, isSilence bool) {
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
	var request alertutils.AlertSilenceRequest
	if err := json.Unmarshal(ctx.PostBody(), &request); err != nil {
		utils.SendError(ctx, fmt.Sprintf("Failed to unmarshal json. Error=%v", err), "", err)
		return
	}

	// Find alert in the database
	alertDataObj, err := databaseObj.GetAlert(request.AlertID)
	if err != nil {
		utils.SendError(ctx, fmt.Sprintf("Failed to find alert. Error=%v", err), fmt.Sprintf("alert ID: %v", request.AlertID), err)
		return
	}

	now := time.Now()

	if isSilence {
		if request.SilenceMinutes == 0 {
			utils.SendError(ctx, "SilenceMinutes must be greater than zero", "", nil)
			return
		}
		alertDataObj.SilenceMinutes = request.SilenceMinutes
		alertDataObj.SilenceEndTime = uint64(now.Add(time.Duration(request.SilenceMinutes) * time.Minute).Unix())
	} else {
		alertDataObj.SilenceMinutes = 0
		alertDataObj.SilenceEndTime = 0
	}

	// Update alert in the database
	err = databaseObj.UpdateAlert(alertDataObj)
	if err != nil {
		utils.SendError(ctx, fmt.Sprintf("Failed to update alert. Error=%v", err), fmt.Sprintf("alert name: %v", alertDataObj.AlertName), err)
		return
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
	if isSilence {
		responseBody["message"] = "Successfully updated silence period"
	} else {
		responseBody["message"] = "Successfully unsilenced the alert"
	}
	utils.WriteJsonResponse(ctx, responseBody)
}

func ProcessSilenceAlertRequest(ctx *fasthttp.RequestCtx) {
	processAlertSilence(ctx, true)
}

func ProcessUnsilenceAlertRequest(ctx *fasthttp.RequestCtx) {
	processAlertSilence(ctx, false)
}

func ProcessTestContactPointRequest(ctx *fasthttp.RequestCtx) {
	var testContactRequest TestContactPointRequest
	if err := json.Unmarshal(ctx.PostBody(), &testContactRequest); err != nil {
		utils.SendError(ctx, fmt.Sprintf("Failed to unmarshal json. Error=%v", err), "Request Body: "+string(ctx.PostBody()), err)
		return
	}

	switch testContactRequest.Type {
	case "slack":
		channelID, ok := testContactRequest.Settings["channel_id"].(string)
		if !ok {
			utils.SendError(ctx, "channel_id is required but is missing", "Request Body: "+string(ctx.PostBody()), nil)
			return
		}
		slackToken, ok := testContactRequest.Settings["slack_token"].(string)
		if !ok {
			utils.SendError(ctx, "slack_token is required but is missing", "Request Body: "+string(ctx.PostBody()), nil)
			return
		}
		channel := alertutils.SlackTokenConfig{
			ChannelId: channelID,
			SlToken:   slackToken,
		}
		err := sendSlack("Test Alert", "This is a test message to verify the Slack integration.", channel, alertutils.Normal, "")
		if err != nil {
			utils.SendError(ctx, err.Error(), "Error sending test message to slack. Request Body:"+string(ctx.PostBody()), err)
			return
		}
	case "webhook":
		webhookURL, ok := testContactRequest.Settings["webhook"].(string)
		if !ok {
			utils.SendError(ctx, "webhook is required but is missing", "Request Body: "+string(ctx.PostBody()), nil)
			return
		}
		if err := testWebhookURL(webhookURL, testContactRequest.Headers); err != nil {
			utils.SendError(ctx, fmt.Sprintf("Failed to verify webhook URL. Error=%v", err), "", err)
			return
		}
	default:
		utils.SendError(ctx, "Invalid type", "Request Body:"+string(ctx.PostBody()), nil)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, map[string]interface{}{"message": "Successfully verified contact point"})
}

func testWebhookURL(webhookURL string, headers map[string]string) error {
	client := alertutils.GetCertErrorForgivingHttpClient()
	req, err := http.NewRequest("GET", webhookURL, nil)
	if err != nil {
		log.Errorf("testWebhookURL: failed to create a test webhook request. URL: %v err: %v", webhookURL, err)
		return err
	}
	for key, value := range headers {
		req.Header.Add(key, value)
	}
	resp, err := client.Do(req)
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
		utils.SendError(ctx, fmt.Sprintf("Failed to get alert. Error=%v", err), fmt.Sprintf("alert ID: %v", alert_id), err)
		return
	}

	responseBody["alert"] = alert
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, responseBody)
}

func ProcessGetAllAlertsRequest(ctx *fasthttp.RequestCtx, org_id int64) {
	if databaseObj == nil {
		utils.SendError(ctx, invalidDatabaseProvider, "", nil)
		return
	}

	responseBody := make(map[string]interface{})
	alerts, err := databaseObj.GetAllAlerts(org_id)
	if err != nil {
		utils.SendError(ctx, fmt.Sprintf("Failed to get alerts. Error=%v", err), "", err)
		return
	}

	responseBody["alerts"] = alerts
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, responseBody)
}

func ProcessGetAllMinionSearchesRequest(ctx *fasthttp.RequestCtx, orgID int64) {
	if databaseObj == nil {
		utils.SendError(ctx, invalidDatabaseProvider, "", nil)
		return
	}

	responseBody := make(map[string]interface{})
	minionSearches, err := databaseObj.GetAllMinionSearches(orgID)
	if err != nil {
		utils.SendError(ctx, fmt.Sprintf("Failed to get all alerts. Error=%v", err), "", err)
		return
	}

	responseBody["minionSearches"] = minionSearches
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, responseBody)
}

func ProcessUpdateAlertRequest(ctx *fasthttp.RequestCtx) {
	type Input struct {
		alertutils.AlertConfig
		AlertId string `json:"alert_id"`
	}

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

	var input Input
	err := json.Unmarshal(rawJSON, &input)
	if err != nil {
		utils.SendError(ctx, fmt.Sprintf("Failed to unmarshal json. Error=%v", err), "", err)
		return
	}

	// Find alert in the database
	alertToBeUpdated, err := databaseObj.GetAlert(input.AlertId)
	if err != nil {
		utils.SendError(ctx, fmt.Sprintf("Failed to find alert. Error=%v", err), fmt.Sprintf("alert ID: %v", input.AlertId), err)
		return
	}

	alertToBeUpdated.AlertName = input.AlertName
	alertToBeUpdated.AlertType = input.AlertType
	alertToBeUpdated.ContactID = input.ContactID
	alertToBeUpdated.ContactName = input.ContactName
	alertToBeUpdated.Labels = input.Labels
	alertToBeUpdated.QueryParams = input.QueryParams
	alertToBeUpdated.Condition = input.Condition
	alertToBeUpdated.Value = input.Value
	alertToBeUpdated.EvalWindow = input.EvalWindow
	alertToBeUpdated.EvalInterval = input.EvalInterval
	alertToBeUpdated.Message = input.Message

	if alertToBeUpdated.EvalWindow < alertToBeUpdated.EvalInterval {
		utils.SendError(ctx, "EvalWindow should be greater than or equal to EvalInterval", fmt.Sprintf("EvalWindow: %v, EvalInterval:%v", alertToBeUpdated.EvalWindow, alertToBeUpdated.EvalInterval), nil)
		return
	}

	// Validate Alert Type and Query
	extraMsgToLog, err := validateAlertTypeAndQuery(alertToBeUpdated)
	if err != nil {
		utils.SendError(ctx, fmt.Sprintf("Failed to update alert. Error=%v", err), extraMsgToLog, err)
		return
	}

	err = databaseObj.UpdateAlert(alertToBeUpdated)
	if err != nil {
		utils.SendError(ctx, fmt.Sprintf("Failed to update alert. Error=%v", err), fmt.Sprintf("alert name: %v", alertToBeUpdated.AlertName), err)
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
		utils.SendError(ctx, fmt.Sprintf("Failed to remove cron job for alert. Error=%v", err), fmt.Sprintf("alert name: %v", alertToBeUpdated.AlertName), err)
		return
	}
	_, err = AddCronJob(alertToBeUpdated)
	if err != nil {
		utils.SendError(ctx, fmt.Sprintf("Failed to add new cron job for alert. Error=%v", err), fmt.Sprintf("alert name: %v", alertToBeUpdated.AlertName), err)
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
	limit := ctx.QueryArgs().GetUintOrZero("limit")
	offset := ctx.QueryArgs().GetUintOrZero("offset")
	sortOrder := string(ctx.QueryArgs().Peek("sort_order"))

	if sortOrder != string(alertutils.ASC) && sortOrder != string(alertutils.DESC) {
		sortOrder = string(alertutils.DESC)
	}

	alertHistory, err := databaseObj.GetAlertHistoryByAlertID(&alertutils.AlertHistoryQueryParams{
		AlertId:   alertId,
		SortOrder: alertutils.DB_SORT_ORDER(sortOrder),
		Limit:     uint64(limit),
		Offset:    uint64(offset),
	})
	if err != nil {
		utils.SendError(ctx, fmt.Sprintf("Failed to get alert history. Error=%v", err), fmt.Sprintf("alert ID: %v", alertId), err)
		return
	}

	responseBody["count"] = len(alertHistory)
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
		utils.SendError(ctx, fmt.Sprintf("Failed to unmarshal json. Error=%v", err), "", err)
		return
	}
	err = RemoveCronJob(alertToBeRemoved.AlertId)
	if err != nil {
		utils.SendError(ctx, fmt.Sprintf("Failed to remove cron job for alert. Error=%v", err), fmt.Sprintf("alert name: %v", alertToBeRemoved.AlertName), err)
		return
	}

	err = databaseObj.DeleteAlert(alertToBeRemoved.AlertId)
	if err != nil {
		utils.SendError(ctx, fmt.Sprintf("Failed to delete alert. Error=%v", err), fmt.Sprintf("alert name: %v", alertToBeRemoved.AlertName), err)
		return
	}

	responseBody["message"] = "Alert deleted successfully"
	utils.WriteJsonResponse(ctx, responseBody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessCreateContactRequest(ctx *fasthttp.RequestCtx, org_id int64) {
	if databaseObj == nil {
		utils.SendError(ctx, invalidDatabaseProvider, "", nil)
		return
	}
	responseBody := make(map[string]interface{})
	var contactToBeCreated *alertutils.Contact

	rawJSON := ctx.PostBody()
	if len(rawJSON) == 0 {
		utils.SendError(ctx, "Received empty request", "", nil)
		return
	}
	err := json.Unmarshal(rawJSON, &contactToBeCreated)
	if err != nil {
		utils.SendError(ctx, fmt.Sprintf("Failed to unmarshal json. Error=%v", err), "", err)
		return
	}
	contactToBeCreated.OrgId = org_id
	err = databaseObj.CreateContact(contactToBeCreated)
	if err != nil {
		utils.SendError(ctx, fmt.Sprintf("Failed to create contact. Error=%v", err), fmt.Sprintf("contact name: %v", contactToBeCreated.ContactName), err)
		return
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
	responseBody["message"] = "Successfully created a contact point"
	utils.WriteJsonResponse(ctx, responseBody)
}

func ProcessGetAllContactsRequest(ctx *fasthttp.RequestCtx, org_id int64) {
	if databaseObj == nil {
		utils.SendError(ctx, invalidDatabaseProvider, "", nil)
		return
	}

	responseBody := make(map[string]interface{})
	contacts, err := databaseObj.GetAllContactPoints(org_id)
	if err != nil {
		utils.SendError(ctx, fmt.Sprintf("Failed get get all contact points. Error=%v", err), "", err)
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
		utils.SendError(ctx, fmt.Sprintf("Failed to unmarshal json. Error=%v", err), "", err)
		return
	}
	err = databaseObj.UpdateContactPoint(contactToBeUpdated)
	if err != nil {
		utils.SendError(ctx, fmt.Sprintf("Failed to update contact. Error=%v", err), fmt.Sprintf("contact name: %v", contactToBeUpdated.ContactName), err)
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
		utils.SendError(ctx, fmt.Sprintf("Failed to unmarshal json. Error=%v", err), "", err)
		return
	}

	err = databaseObj.DeleteContactPoint(contact.ContactId)
	if err != nil {
		utils.SendError(ctx, fmt.Sprintf("Failed to delete contact. Error=%v", err), fmt.Sprintf("contact ID: %v", contact.ContactId), err)
		return
	}

	responseBody["message"] = "Contact point deleted successfully"
	utils.WriteJsonResponse(ctx, responseBody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func InitAlertingService(getMyIds func() []int64) {
	if databaseObj == nil {
		log.Errorf("InitAlertingService, err = %+v", invalidDatabaseProvider)
		return
	}
	myids := getMyIds()
	for _, myid := range myids {

		// get all alerts from database
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

func InitMinionSearchService(getMyIds func() []int64) {
	if databaseObj == nil {
		log.Errorf("InitMinionSearchService, err = %+v", invalidDatabaseProvider)
		return
	}
	myids := getMyIds()
	for _, myid := range myids {

		// get all alerts from database
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

func ProcessCreateLogMinionSearchRequest(ctx *fasthttp.RequestCtx, org_id int64) {
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
		utils.SendError(ctx, fmt.Sprintf("Failed to unmarshal json. Error=%v", err), "", err)
		return
	}
	minionSearches := convertToSiglensAlert(LogLinesEntry)
	for _, searchToBeCreated := range minionSearches {
		searchToBeCreated.OrgId = org_id
		searchDataObj, err := databaseObj.CreateMinionSearch(searchToBeCreated)
		if err != nil {
			utils.SendError(ctx, fmt.Sprintf("Failed to create alert. Error=%v", err), fmt.Sprintf("alert name: %v", searchToBeCreated.AlertName), err)
			return
		}
		_, err = AddMinionSearchCronJob(&searchDataObj)
		if err != nil {
			utils.SendError(ctx, fmt.Sprintf("Failed to create cron job for alert. Error=%v", err), fmt.Sprintf("alert name: %v", searchDataObj.AlertName), err)
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
			Value:           float64(entry.Value),
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
		utils.SendError(ctx, fmt.Sprintf("Failed to get alert. Error=%v", err), fmt.Sprintf("alert ID: %v", alert_id), err)
		return
	}

	responseBody["minionsearch"] = msearch
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, responseBody)
}

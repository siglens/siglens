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

package alertsHandler

import (
	"encoding/json"
	"errors"
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
	GetEmailAndChannelID(contact_id string) ([]string, []alertutils.SlackTokenConfig, []string, error)
	UpdateLastSentTime(alert_id string) error
	UpdateAlertStateByAlertID(alertId string, alertState alertutils.AlertState) error
	DeleteContactPoint(contact_id string) error
}

var databaseObj database

var invalidDatabaseProvider = "database provider is not configured in server.yaml"

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
		responseBody := make(map[string]interface{})
		log.Errorf("ProcessCreateAlertRequest: failed to create alert, err = %+v", invalidDatabaseProvider)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = invalidDatabaseProvider
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}
	responseBody := make(map[string]interface{})
	var alertToBeCreated alertutils.AlertDetails

	rawJSON := ctx.PostBody()
	if len(rawJSON) == 0 {
		log.Errorf("ProcessCreateAlertRequest: empty json body received")
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = "empty json body received"
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}
	alertToBeCreated.OrgId = org_id
	err := json.Unmarshal(rawJSON, &alertToBeCreated)
	if err != nil {
		log.Errorf("ProcessCreateAlertRequest: could not unmarshal json body, err=%v", err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}
	alertDataObj, err := databaseObj.CreateAlert(&alertToBeCreated)
	if err != nil {
		log.Errorf("ProcessCreateAlertRequest: could not create alert=%v, err=%v", alertToBeCreated.AlertName, err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	_, err = AddCronJob(&alertDataObj)
	if err != nil {
		log.Errorf("ProcessCreateAlertRequest: could not add a new CronJob corresponding to alert=%+v, err=%+v", alertDataObj.AlertName, err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
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
		log.Error("ProcessSilenceAlertRequest: databaseObj is nil")
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		responseBody["error"] = "Internal server error"
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	// Check if request body is empty
	if string(ctx.PostBody()) == "" {
		log.Error("ProcessSilenceAlertRequest: request body is empty")
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = "Request body is empty"
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	// Parse request
	var silenceRequest struct {
		AlertID        string `json:"alert_id"`
		SilenceMinutes uint64 `json:"silence_minutes"`
	}
	if err := json.Unmarshal(ctx.PostBody(), &silenceRequest); err != nil {
		log.Errorf("ProcessSilenceAlertRequest: could not parse request body, err=%+v", err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	// Find alert and update SilenceMinutes
	alertDataObj, err := databaseObj.GetAlert(silenceRequest.AlertID)
	if err != nil {
		log.Errorf("ProcessSilenceAlertRequest: could not find alert, err=%+v", err)
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	alertDataObj.SilenceMinutes = silenceRequest.SilenceMinutes
	// Update the SilenceMinutes
	err = databaseObj.UpdateAlert(alertDataObj)
	if err != nil {
		log.Errorf("ProcessUpdateSilenceRequestRequest: could not update alert=%+v, err=%+v", alertDataObj.AlertName, err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
	responseBody["message"] = "Successfully updated silence period"
	utils.WriteJsonResponse(ctx, responseBody)
}

func ProcessGetAlertRequest(ctx *fasthttp.RequestCtx) {
	if databaseObj == nil {
		responseBody := make(map[string]interface{})
		log.Errorf("ProcessGetAlertRequest: failed to get alert, err = %+v", invalidDatabaseProvider)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = invalidDatabaseProvider
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	responseBody := make(map[string]interface{})
	alert_id := utils.ExtractParamAsString(ctx.UserValue("alertID"))
	alert, err := databaseObj.GetAlert(alert_id)
	if err != nil {
		log.Errorf("ProcessGetAlertRequest: failed to get alert with alertId = %+v, err = %+v", alert_id, err.Error())
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	responseBody["alert"] = alert
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, responseBody)
}

func ProcessGetAllAlertsRequest(ctx *fasthttp.RequestCtx, org_id uint64) {
	if databaseObj == nil {
		responseBody := make(map[string]interface{})
		log.Errorf("ProcessGetAllAlertsRequest: failed to get all alerts, err = %+v", invalidDatabaseProvider)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = invalidDatabaseProvider
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	responseBody := make(map[string]interface{})
	alerts, err := databaseObj.GetAllAlerts(org_id)
	if err != nil {
		log.Errorf("ProcessGetAllAlertsRequest: could not get all alerts, err: %+v", err.Error())
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	responseBody["alerts"] = alerts
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, responseBody)
}

func ProcessGetAllMinionSearchesRequest(ctx *fasthttp.RequestCtx, orgID uint64) {
	if databaseObj == nil {
		responseBody := make(map[string]interface{})
		log.Errorf("ProcessGetAllMinionSearchesRequest: failed to get all alerts, err = %+v", invalidDatabaseProvider)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = invalidDatabaseProvider
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	responseBody := make(map[string]interface{})
	minionSearches, err := databaseObj.GetAllMinionSearches(orgID)
	if err != nil {
		log.Errorf("ProcessGetAllMinionSearchesRequest: could not get all alerts, err: %+v", err.Error())
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	responseBody["minionSearches"] = minionSearches
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, responseBody)
}

func ProcessUpdateAlertRequest(ctx *fasthttp.RequestCtx) {
	if databaseObj == nil {
		responseBody := make(map[string]interface{})
		log.Errorf("ProcessUpdateAlertRequest: failed to update alert, err = %+v", invalidDatabaseProvider)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = invalidDatabaseProvider
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}
	responseBody := make(map[string]interface{})
	rawJSON := ctx.PostBody()
	if len(rawJSON) == 0 {
		log.Errorf("ProcessUpdateAlertRequest: empty json body received")
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = "empty json body received"
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	var alertToBeUpdated *alertutils.AlertDetails
	err := json.Unmarshal(rawJSON, &alertToBeUpdated)
	if err != nil {
		log.Errorf("ProcessUpdateAlertRequest: could not unmarshal json body, err=%v", err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	err = databaseObj.UpdateAlert(alertToBeUpdated)
	if err != nil {
		log.Errorf("ProcessUpdateAlertRequest: could not update alert=%+v, err=%+v", alertToBeUpdated.AlertName, err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
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
		log.Errorf("ProcessUpdateAlertRequest: could not remove old cron job corresponding to alert=%+v, err=%+v", alertToBeUpdated.AlertName, err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}
	_, err = AddCronJob(alertToBeUpdated)
	if err != nil {
		log.Errorf("ProcessUpdateAlertRequest: could not add a new cron job corresponding to alert=%+v, err=%+v", alertToBeUpdated.AlertName, err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	responseBody["message"] = "Alert updated successfully"
	utils.WriteJsonResponse(ctx, responseBody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessAlertHistoryRequest(ctx *fasthttp.RequestCtx) {
	if databaseObj == nil {
		responseBody := make(map[string]interface{})
		log.Errorf("ProcessAlertHistoryRequest: failed to get alert history, err = %+v", invalidDatabaseProvider)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = invalidDatabaseProvider
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	responseBody := make(map[string]interface{})
	alertId := utils.ExtractParamAsString(ctx.UserValue("alertID"))
	alertHistory, err := databaseObj.GetAlertHistory(alertId)
	if err != nil {
		log.Errorf("ProcessAlertHistoryRequest: failed to get alert history with alertId = %+v, err = %+v", alertId, err.Error())
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	responseBody["alertHistory"] = alertHistory
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, responseBody)
}

// request body should contain alert_id only
func ProcessDeleteAlertRequest(ctx *fasthttp.RequestCtx) {
	if databaseObj == nil {
		responseBody := make(map[string]interface{})
		log.Errorf("ProcessDeleteAlertRequest: failed to delete alert, err = %+v", invalidDatabaseProvider)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = invalidDatabaseProvider
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}
	responseBody := make(map[string]interface{})
	rawJSON := ctx.PostBody()
	if len(rawJSON) == 0 {
		log.Errorf("ProcessDeleteAlertRequest: empty json body received")
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = "empty json body received"
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	var alertToBeRemoved *alertutils.AlertDetails
	err := json.Unmarshal(rawJSON, &alertToBeRemoved)
	if err != nil {
		log.Errorf("ProcessDeleteAlertRequest: could not unmarshal json body, err=%v", err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}
	err = RemoveCronJob(alertToBeRemoved.AlertId)
	if err != nil {
		log.Errorf("ProcessDeleteAlertRequest: RemoveCronJob failed , alert id=%v, err=%v", alertToBeRemoved.AlertId, err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	err = databaseObj.DeleteAlert(alertToBeRemoved.AlertId)
	if err != nil {
		log.Errorf("ProcessDeleteAlertRequest: failed to delete alert with id=%v, err=%v", alertToBeRemoved.AlertId, err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	responseBody["message"] = "Alert deleted successfully"
	utils.WriteJsonResponse(ctx, responseBody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessCreateContactRequest(ctx *fasthttp.RequestCtx, org_id uint64) {
	if databaseObj == nil {
		responseBody := make(map[string]interface{})
		log.Errorf("ProcessCreateContactRequest: failed to create a contact point, err = %+v", invalidDatabaseProvider)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = invalidDatabaseProvider
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}
	responseBody := make(map[string]interface{})
	var contactToBeCreated *alertutils.Contact

	rawJSON := ctx.PostBody()
	if len(rawJSON) == 0 {
		log.Errorf("ProcessCreateContactRequest: empty json body received")
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = "empty json body received"
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}
	err := json.Unmarshal(rawJSON, &contactToBeCreated)
	if err != nil {
		log.Errorf("ProcessCreateContactRequest: could not unmarshal json body, err=%v", err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = "could not unmarshal json body"
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}
	contactToBeCreated.OrgId = org_id
	err = databaseObj.CreateContact(contactToBeCreated)
	if err != nil {
		log.Errorf("ProcessCreateContactRequest: could not create contact with name=%v, err=%v", contactToBeCreated.ContactName, err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
	responseBody["message"] = "Successfully created a contact point"
	utils.WriteJsonResponse(ctx, responseBody)
}

func ProcessGetAllContactsRequest(ctx *fasthttp.RequestCtx, org_id uint64) {
	if databaseObj == nil {
		responseBody := make(map[string]interface{})
		log.Errorf("ProcessGetAllContactsRequest: failed to get all contacts, err = %+v", invalidDatabaseProvider)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = invalidDatabaseProvider
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	responseBody := make(map[string]interface{})
	contacts, err := databaseObj.GetAllContactPoints(org_id)
	if err != nil {
		log.Errorf("ProcessGetAllContactsRequest: could not get all contact points, err = %+v", err.Error())
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}
	responseBody["contacts"] = contacts
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, responseBody)
}

func ProcessUpdateContactRequest(ctx *fasthttp.RequestCtx) {
	if databaseObj == nil {
		responseBody := make(map[string]interface{})
		log.Errorf("ProcessUpdateContactRequest: failed to update contact, err = %+v", invalidDatabaseProvider)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = invalidDatabaseProvider
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	responseBody := make(map[string]interface{})
	rawJSON := ctx.PostBody()
	if len(rawJSON) == 0 {
		log.Errorf("ProcessUpdateContactRequest: empty json body received")
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = "empty json body received"
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	var contactToBeUpdated *alertutils.Contact
	err := json.Unmarshal(rawJSON, &contactToBeUpdated)
	if err != nil {
		log.Errorf("ProcessUpdateContactRequest: could not unmarshal json body, err=%v", err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}
	err = databaseObj.UpdateContactPoint(contactToBeUpdated)
	if err != nil {
		log.Errorf("ProcessUpdateContactRequest: could not update contact = %+v, err = %+v", contactToBeUpdated.ContactName, err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}
	responseBody["message"] = "Contact details updated successfully"
	utils.WriteJsonResponse(ctx, responseBody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessDeleteContactRequest(ctx *fasthttp.RequestCtx) {
	if databaseObj == nil {
		responseBody := make(map[string]interface{})
		log.Errorf("ProcessDeleteContactRequest: failed to delete contact, err = %+v", invalidDatabaseProvider)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = invalidDatabaseProvider
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	responseBody := make(map[string]interface{})
	rawJSON := ctx.PostBody()
	if len(rawJSON) == 0 {
		log.Errorf("ProcessDeleteContactRequest: empty json body received")
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = "empty json body received"
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	var contact *alertutils.Contact
	err := json.Unmarshal(rawJSON, &contact)
	if err != nil {
		log.Errorf("ProcessDeleteContactRequest: could not unmarshal json body, err=%v", err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	err = databaseObj.DeleteContactPoint(contact.ContactId)
	if err != nil {
		log.Errorf("ProcessDeleteContactRequest: could not delete contact=%+v, err=%+v", contact.ContactId, err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
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
		responseBody := make(map[string]interface{})
		log.Errorf("ProcessCreateLogMinionSearchRequest: failed to create alert, err = %+v", invalidDatabaseProvider)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = invalidDatabaseProvider
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}
	responseBody := make(map[string]interface{})
	var LogLinesEntry alertutils.LogLinesFile

	rawJSON := ctx.PostBody()
	if len(rawJSON) == 0 {
		log.Errorf("ProcessCreateLogMinionSearchRequest: empty json body received")
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = "empty json body received"
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}
	err := json.Unmarshal(rawJSON, &LogLinesEntry)
	if err != nil {
		log.Errorf("ProcessCreateLogMinionSearchRequest: could not unmarshal json body, err=%v", err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}
	minionSearches := convertToSiglensAlert(LogLinesEntry)
	for _, searchToBeCreated := range minionSearches {
		searchToBeCreated.OrgId = org_id
		searchDataObj, err := databaseObj.CreateMinionSearch(searchToBeCreated)
		if err != nil {
			log.Errorf("ProcessCreateLogMinionSearchRequest: could not create alert=%v, err=%v", searchToBeCreated.AlertName, err)
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			responseBody["error"] = err.Error()
			utils.WriteJsonResponse(ctx, responseBody)
			return
		}
		_, err = AddMinionSearchCronJob(&searchDataObj)
		if err != nil {
			log.Errorf("ProcessCreateLogMinionSearchRequest: could not add a new CronJob corresponding to alert=%+v, err=%+v", searchDataObj.AlertName, err)
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			responseBody["error"] = err.Error()
			utils.WriteJsonResponse(ctx, responseBody)
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
		responseBody := make(map[string]interface{})
		log.Errorf("ProcessGetMinionSearchRequest: failed to get alert, err = %+v", invalidDatabaseProvider)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = invalidDatabaseProvider
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	responseBody := make(map[string]interface{})
	alert_id := utils.ExtractParamAsString(ctx.UserValue("alertID"))
	msearch, err := databaseObj.GetMinionSearch(alert_id)
	if err != nil {
		log.Errorf("ProcessGetMinionSearchRequest: failed to get alert with alertId = %+v, err = %+v", alert_id, err.Error())
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = err.Error()
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	responseBody["minionsearch"] = msearch
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, responseBody)
}

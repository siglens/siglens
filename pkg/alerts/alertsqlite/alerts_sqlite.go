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

package alertsqlite

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/siglens/siglens/pkg/alerts/alertutils"
	log "github.com/sirupsen/logrus"
)

type Sqlite struct {
	db  *gorm.DB
	ctx context.Context
}

func (p *Sqlite) SetDB(dbConnection *gorm.DB) {
	p.db = dbConnection
}

func (p *Sqlite) CloseDb() {
	sqlDB, err := p.db.DB()
	if err != nil {
		log.Errorf("CloseDb: Error occurred while closing a DB connection, Error=%v", err)
	}
	defer sqlDB.Close()
}

func (p *Sqlite) Connect() error {
	dbname := "siglens.db"
	logger := log.New()
	dbConnection, err := gorm.Open(sqlite.Open(dbname), &gorm.Config{
		Logger: alertutils.NewGormLogrusLogger(logger.WithField("component", "gorm"), 100*time.Millisecond),
	})
	if err != nil {
		log.Errorf("Connect: error in opening sqlite connection, Error=%+v", err)
		return err
	}

	p.SetDB(dbConnection)

	err = dbConnection.AutoMigrate(&alertutils.AlertDetails{})
	if err != nil {
		return err
	}
	err = dbConnection.AutoMigrate(&alertutils.AlertLabel{})
	if err != nil {
		return err
	}
	err = dbConnection.AutoMigrate(&alertutils.AlertHistoryDetails{})
	if err != nil {
		return err
	}
	err = dbConnection.AutoMigrate(&alertutils.Notification{})
	if err != nil {
		return err
	}
	err = dbConnection.AutoMigrate(&alertutils.Contact{})
	if err != nil {
		return err
	}
	err = dbConnection.AutoMigrate(&alertutils.SlackTokenConfig{})
	if err != nil {
		return err
	}
	err = dbConnection.AutoMigrate(&alertutils.WebHookConfig{})
	if err != nil {
		return err
	}
	err = dbConnection.AutoMigrate(&alertutils.MinionSearch{})
	if err != nil {
		return err
	}
	p.ctx = context.Background()
	return nil
}

func isValid(str string) bool {
	return str != "" && str != "*"
}

// checks whether the alert name exists
func (p Sqlite) isNewAlertName(alertName string) (bool, error) {
	if !isValid(alertName) {
		err := fmt.Errorf("isNewAlertName: Data Validation Check Failed: Alert Name given is not valid. Alert Name given : %v", alertName)
		log.Error(err.Error())
		return false, err
	}
	if err := p.db.Where("alert_name = ?", alertName).First(&alertutils.AlertDetails{}).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return true, nil
		} else {
			return false, err
		}
	}
	return true, nil
}

// checks based on alert_id, returns true and alertDetails if alert exists
func (p Sqlite) verifyAlertExists(alert_id string) (bool, *alertutils.AlertDetails, error) {
	if !isValid(alert_id) {
		err := fmt.Errorf("verifyAlertExists: Data Validation Check Failed: Alert Id given is not valid. AlertId=%v", alert_id)
		log.Error(err.Error())
		return false, nil, err
	}
	var alert alertutils.AlertDetails

	if err := p.db.Where("alert_id = ?", alert_id).First(&alert).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return false, nil, nil
		} else {
			return false, nil, err
		}
	}

	return true, &alert, nil
}

func (p Sqlite) verifyContactExists(contact_id string) (bool, *alertutils.Contact, error) {
	if !isValid(contact_id) {
		err := fmt.Errorf("verifyContactExists: Data Validation Check Failed: Contact ID given is not Valid. ContactId=%v", contact_id)
		log.Error(err.Error())
		return false, nil, err
	}

	var contact alertutils.Contact
	if err := p.db.Where("contact_id = ?", contact_id).First(&contact).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return false, nil, nil
		}
		return false, nil, err
	}
	return true, &contact, nil
}

// Generates uniq uuid for alert, contact point
func CreateUniqId() string {
	newAlertId := uuid.New().String()
	return newAlertId
}

// Creates a new record in all_alerts table and notification_details table
// In notification_details table, cooldown period will be set as 0 for now
// Starts a new cron job for the alert
func (p Sqlite) CreateAlert(alertDetails *alertutils.AlertDetails) (alertutils.AlertDetails, error) {
	if !isValid(alertDetails.AlertName) {
		err := fmt.Errorf("CreateAlert: Data Validation Check Failed: Alert Name given is not Valid. AlertName=%v", alertDetails.AlertName)
		log.Error(err.Error())
		return alertutils.AlertDetails{}, err
	}
	isNewAlertName, _ := p.isNewAlertName(alertDetails.AlertName)

	if !isNewAlertName {
		err := fmt.Errorf("CreateAlert: Alert Name=%v already exists", alertDetails.AlertName)
		log.Error(err.Error())
		return alertutils.AlertDetails{}, err
	}

	exists, contactData, err := p.verifyContactExists(alertDetails.ContactID)
	if err != nil {
		err = fmt.Errorf("CreateAlert: Error ocurred while fetching contact data with contactId: %v, for Alert: %v, Error=%+v", alertDetails.ContactID, alertDetails.AlertName, err)
		log.Error(err.Error())
		return alertutils.AlertDetails{}, err
	}

	if !exists {
		err := fmt.Errorf("CreateAlert: Contact does not exist with contactId: %v, for alert: %v", alertDetails.ContactID, alertDetails.AlertName)
		log.Error(err.Error())
		return alertutils.AlertDetails{}, err
	}

	alertDetails.ContactName = contactData.ContactName

	alert_id := CreateUniqId()
	state := alertutils.Inactive
	alertDetails.State = state
	alertDetails.AlertId = alert_id
	result := p.db.Create(alertDetails)
	if result.Error != nil && result.RowsAffected != 1 {
		err := fmt.Errorf("CreateAlert: unable to create alert for Alert: %v, Error=%v", alertDetails.AlertName, result.Error)
		log.Error(err.Error())
		return alertutils.AlertDetails{}, err
	}

	var notification alertutils.Notification
	notification.CooldownPeriod = 0
	notification.AlertId = alert_id
	notification.NotificationId = CreateUniqId()
	result = p.db.Create(&notification)
	if result.Error != nil && result.RowsAffected != 1 {
		err := fmt.Errorf("CreateAlert: unable to update notification details for Alert: %v, Error=%v", alertDetails.AlertName, result.Error)
		log.Error(err.Error())
		return alertutils.AlertDetails{}, err
	}
	return *alertDetails, nil
}

func (p Sqlite) GetAlert(alert_id string) (*alertutils.AlertDetails, error) {
	if !isValid(alert_id) {
		err := fmt.Errorf("GetAlert: Data Validation Check Failed: Alert Id given is not valid. AlertId=%v", alert_id)
		log.Error(err.Error())
		return nil, err
	}
	var alert alertutils.AlertDetails
	if err := p.db.Preload("Labels").Where(&alertutils.AlertDetails{AlertId: alert_id}).Find(&alert).Error; err != nil {
		return nil, err
	}
	return &alert, nil

}

func (p Sqlite) GetAllAlerts(orgId uint64) ([]alertutils.AlertDetails, error) {
	alerts := make([]alertutils.AlertDetails, 0)
	err := p.db.Model(&alerts).Preload("Labels").Where("org_id = ?", orgId).Find(&alerts).Error
	return alerts, err
}

func (p Sqlite) UpdateSilenceMinutes(updatedSilenceMinutes *alertutils.AlertDetails) error {
	if !isValid(updatedSilenceMinutes.AlertName) || !isValid(updatedSilenceMinutes.QueryParams.QueryText) {
		err := fmt.Errorf("UpdateSilenceMinutes: Data  Validation check Failed: AlertName or QueryText given is not valid. AlertName=%v, QueryText=%v", updatedSilenceMinutes.AlertName, updatedSilenceMinutes.QueryParams.QueryText)
		log.Error(err.Error())
		return err
	}
	alertExists, _, err := p.verifyAlertExists(updatedSilenceMinutes.AlertId)
	if err != nil {
		err = fmt.Errorf("UpdateSilenceMinutes: unable to verify if alert: %v exists, Error=%+v", updatedSilenceMinutes.AlertName, err)
		log.Error(err.Error())
		return err
	}
	if !alertExists {
		err := fmt.Errorf("UpdateSilenceMinutes: alert: %v does not exist", updatedSilenceMinutes.AlertName)
		log.Error(err.Error())
		return err
	}
	result := p.db.Save(&updatedSilenceMinutes)
	if result.Error != nil && result.RowsAffected != 1 {
		err := fmt.Errorf("UpdateSilenceMinutes: unable to update silence minutes details for Alert: %v, Error=%v", updatedSilenceMinutes.AlertName, result.Error)
		log.Error(err.Error())
		return err
	}

	return nil
}

// Deletes cron job associated with the alert
// Updates the db
// Starts a new cron job with a new cron job id
// updates alert details except state & cron_job_id
func (p Sqlite) UpdateAlert(editedAlert *alertutils.AlertDetails) error {
	// update alert can update alert name -> still id will remain same
	// todo: check if contact_id exists
	if !isValid(editedAlert.AlertName) {
		err := fmt.Errorf("UpdateAlert: Data Validation Check Failed: AlertName=%v is not valid", editedAlert.AlertName)
		log.Error(err.Error())
		return err
	}

	alertExists, currentAlertData, err := p.verifyAlertExists(editedAlert.AlertId)
	if err != nil {
		err = fmt.Errorf("UpdateAlert: unable to verify if alert: %v exists, Error=%+v", editedAlert.AlertName, err)
		log.Error(err.Error())
		return err
	}
	// new alert means id in request body is incorrect
	if !alertExists {
		err := fmt.Errorf("UpdateAlert: Alert: %v does not exist", editedAlert.AlertName)
		log.Error(err.Error())
		return err
	}
	// if alert name in request body is same as that present in db, allow update
	if currentAlertData.AlertName != editedAlert.AlertName {
		isNewAlertName, err := p.isNewAlertName(editedAlert.AlertName)
		if err != nil {
			err = fmt.Errorf("UpdateAlert: unable to verify if Alert Name=%v is unique, Error=%+v", editedAlert.AlertName, err)
			log.Error(err.Error())
			return err
		}
		if !isNewAlertName {
			err := fmt.Errorf("UpdateAlert: Alert Name=%v already exists", editedAlert.AlertName)
			log.Error(err.Error())
			return err
		}
	}

	if editedAlert.AlertType == alertutils.AlertTypeLogs {
		if !isValid(editedAlert.QueryParams.QueryText) {
			err := fmt.Errorf("UpdateAlert: data validation check failed for alert: %v. Alert Query is not Valid: %v", editedAlert.AlertName, editedAlert.QueryParams.QueryText)
			log.Error(err.Error())
			return err
		}
	}

	if editedAlert.ContactID != currentAlertData.ContactID {
		exists, contactData, err := p.verifyContactExists(editedAlert.ContactID)
		if err != nil {
			err := fmt.Errorf("UpdateAlert: unable to verify if contact: %v exists, Error=%+v", editedAlert.ContactID, err)
			log.Error(err.Error())
			return err
		}
		if !exists {
			err := fmt.Errorf("UpdateAlert: contact: %v does not exist", editedAlert.ContactID)
			log.Error(err.Error())
			return err
		}
		editedAlert.ContactName = contactData.ContactName
	}

	result := p.db.Set("gorm:association_autoupdate", true).Save(&editedAlert)
	if result.Error != nil && result.RowsAffected != 1 {
		err := fmt.Errorf("UpdateAlert: unable to update details for alert: %v, Error=%v", editedAlert.AlertName, result.Error)
		log.Error(err.Error())
		return err
	}
	return nil
}

func (p Sqlite) DeleteAlert(alert_id string) error {
	if !isValid(alert_id) {
		err := fmt.Errorf("DeleteAlert: Data Validation Check Failed: AlertId=%v is not valid", alert_id)
		log.Error(err.Error())
		return err
	}
	var alert alertutils.AlertDetails
	result := p.db.First(&alert, "alert_id = ?", alert_id)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			err := fmt.Errorf("DeleteAlert: error deleting alert, alert does not exist, Alert Name=%v, Error=%v", alert.AlertName, result.Error)
			log.Error(err.Error())
			return err
		} else {
			err := fmt.Errorf("DeleteAlert: error deleting alert, Alert Name=%v, Error=%v", alert.AlertName, result.Error)
			log.Error(err.Error())
			return err
		}
	}
	err := p.db.Model(&alert).Association("Labels").Clear()
	if err != nil {
		err := fmt.Errorf("DeleteAlert: unable to delete alert, Alert Name=%v, Error=%v", alert.AlertName, err)
		log.Error(err.Error())
		return err
	}

	result = p.db.Delete(&alert)
	if result.Error != nil && result.RowsAffected != 1 {
		err := fmt.Errorf("DeleteAlert: unable to delete alert, AlertId=%v, Error=%v", alert.AlertName, err)
		log.Error(err.Error())
		return err
	}

	return nil
}

func (p Sqlite) CreateContact(newContact *alertutils.Contact) error {
	var contact alertutils.Contact
	result := p.db.First(&contact, "contact_name = ?", newContact.ContactName)
	if result.Error != nil {
		if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
			err := fmt.Errorf("CreateContact: contact name: %v already exist, Error=%v", newContact.ContactName, result.Error)
			log.Error(err.Error())
			return err
		} else {
			contact_id := CreateUniqId()
			newContact.ContactId = contact_id
			result = p.db.Create(&newContact)
			if result.Error != nil && result.RowsAffected != 1 {
				err := fmt.Errorf("CreateContact: unable to create contact: %v, Error=%v", newContact.ContactName, result.Error)
				log.Error(err.Error())
				return err
			}
		}
	}
	return nil
}

func (p Sqlite) GetAllContactPoints(org_id uint64) ([]alertutils.Contact, error) {
	contacts := make([]alertutils.Contact, 0)
	if err := p.db.Preload("Slack").Preload("Webhook").Where("org_id = ?", org_id).Find(&contacts).Error; err != nil {
		return nil, err
	}

	return contacts, nil
}

func (p Sqlite) UpdateContactPoint(contact *alertutils.Contact) error {
	if !isValid(contact.ContactId) {
		err := fmt.Errorf("UpdateContactPoint: invalid contact id: %v, contact name: %v", contact.ContactId, contact.ContactName)
		log.Error(err.Error())
		return err
	}

	contactExists, _, err := p.verifyContactExists(contact.ContactId)
	if err != nil {
		err = fmt.Errorf("UpdateContactPoint: unable to verify if contact exists, contact name: %v, Error=%+v", contact.ContactName, err)
		log.Error(err.Error())
		return err
	}
	// contact does not exist, that means id in request body is incorrect
	if !contactExists {
		err := fmt.Errorf("UpdateContactPoint: contact does not exist, contact name: %v", contact.ContactName)
		log.Error(err.Error())
		return err
	}

	if len(contact.Slack) != 0 {
		err := p.db.Model(&alertutils.Contact{ContactId: contact.ContactId}).Association("Slack").Clear()
		if err != nil {
			err = fmt.Errorf("UpdateContactPoint: unable to update contact : %v, Error=%+v", contact.ContactName, err)
			log.Error(err.Error())
			return err
		}
	}
	if len(contact.Webhook) != 0 {
		err := p.db.Model(&alertutils.Contact{ContactId: contact.ContactId}).Association("Webhook").Clear()
		if err != nil {
			err = fmt.Errorf("UpdateContactPoint: unable to update contact: %v, Error=%+v", contact.ContactName, err)
			log.Error(err.Error())
			return err
		}
	}
	result := p.db.Session(&gorm.Session{FullSaveAssociations: true}).Save(&contact)
	if result.Error != nil && result.RowsAffected != 1 {
		err := fmt.Errorf("UpdateContactPoint: unable to update contact: %v, Error=%+v", contact.ContactName, err)
		log.Error(err.Error())
		return err
	}
	return nil

}

// get contact_id and message from all_alerts table using alert_id
func (p Sqlite) GetContactDetails(alert_id string) (string, string, string, error) {

	var alert alertutils.AlertDetails
	if err := p.db.Where("alert_id = ?", alert_id).First(&alert).Error; err != nil {
		return "", "", "", err
	}

	alert_name := alert.AlertName
	contact_id := alert.ContactID
	message := alert.Message
	condition := alert.Condition
	value := alert.Value

	newMessage := strings.ReplaceAll(message, "{{alert_rule_name}}", alert_name)
	newMessage = strings.ReplaceAll(newMessage, "{{query_string}}", alert.QueryParams.QueryLanguage)
	if condition == 0 {
		val := "above " + fmt.Sprintf("%1.0f", value)
		newMessage = strings.ReplaceAll(newMessage, "{{condition}}", val)
	} else if condition == 1 {
		val := "below " + fmt.Sprintf("%1.0f", value)
		newMessage = strings.ReplaceAll(newMessage, "{{condition}}", val)
	} else if condition == 2 {
		val := "is equal to " + fmt.Sprintf("%1.0f", value)
		newMessage = strings.ReplaceAll(newMessage, "{{condition}}", val)
	} else if condition == 3 {
		val := "is not equal to " + fmt.Sprintf("%1.0f", value)
		newMessage = strings.ReplaceAll(newMessage, "{{condition}}", val)
	} else if condition == 4 {
		newMessage = strings.ReplaceAll(newMessage, "{{condition}}", "has no value")
	}
	newMessage = strings.ReplaceAll(newMessage, "{{queryLanguage}}", alert.QueryParams.QueryLanguage)
	return contact_id, newMessage, alert_name, nil
}

func (p Sqlite) GetCoolDownDetails(alert_id string) (uint64, time.Time, error) {
	var notification alertutils.Notification
	if err := p.db.Where("alert_id = ?", alert_id).First(&notification).Error; err != nil {
		return 0, time.Time{}, err
	}
	cooldown_period := notification.CooldownPeriod
	last_sent_time := notification.LastSentTime

	return cooldown_period, last_sent_time, nil
}

func (p Sqlite) GetAlertNotification(alert_id string) (*alertutils.Notification, error) {
	var notification alertutils.Notification
	if err := p.db.Where("alert_id = ?", alert_id).First(&notification).Error; err != nil {
		return nil, err
	}
	return &notification, nil
}

func (p Sqlite) DeleteContactPoint(contact_id string) error {
	if !isValid(contact_id) {
		err := fmt.Errorf("DeleteContactPoint: Data Validation Check Failed: contact id=%v is not Valid", contact_id)
		log.Error(err.Error())
		return err
	}

	contactExists, _, err := p.verifyContactExists(contact_id)
	if err != nil {
		err = fmt.Errorf("DeleteContactPoint: unable to verify if contact exists, contact id: %v, Error=%+v", contact_id, err)
		log.Error(err.Error())
		return err
	}
	// contact does not exist, that means id in request body is incorrect
	if !contactExists {
		err := fmt.Errorf("DeleteContactPoint: contact does not exist, contact id: %v", contact_id)
		log.Error(err.Error())
		return err
	}

	var contact alertutils.Contact

	result := p.db.First(&contact, "contact_id = ?", contact_id)
	if result.Error != nil {
		err := fmt.Errorf("DeleteContactPoint: error deleting contact, contact: %v, Error=%v", contact.ContactName, result.Error)
		log.Error(err.Error())
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return fmt.Errorf("DeleteContactPoint: contact: %v does not exist", contact.ContactName)
		} else {
			return err
		}
	}
	err = p.db.Model(&contact).Association("Slack").Clear()
	if err != nil {
		err = fmt.Errorf("DeleteContactPoint: unable to delete contact: %v, Error=%v", contact.ContactName, err)
		log.Error(err.Error())
		return err
	}

	result = p.db.Delete(&contact)
	if result.Error != nil && result.RowsAffected != 1 {
		err := fmt.Errorf("DeleteContactPoint: unable to delete contact: %v, Error=%v", contact.ContactName, err)
		log.Error(err.Error())
		return err
	}

	return nil
}

// update last_sent_time and last_alert_state in notification_details table
func (p Sqlite) UpdateLastSentTimeAndAlertState(alert_id string, alertState alertutils.AlertState) error {
	currentTime := time.Now().UTC()

	if err := p.db.Model(&alertutils.Notification{}).Where("alert_id = ?", alert_id).
		Updates(map[string]interface{}{
			"last_sent_time":   currentTime,
			"last_alert_state": alertState,
		}).Error; err != nil {
		err = fmt.Errorf("UpdateLastSentTimeAndAlertState: unable to update, AlertId=%v, Error=%+v", alert_id, err)
		log.Errorf(err.Error())
		return err
	}

	return nil
}

func (p Sqlite) UpdateAlertStateAndIncrementNumEvaluations(alert_id string, alertState alertutils.AlertState) error {
	if !isValid(alert_id) {
		err := fmt.Errorf("UpdateAlertStateAndIncrementNumEvaluations: Data Validation Check Failed: AlertId=%v is not valid", alert_id)
		log.Error(err.Error())
		return err
	}
	alertExists, _, err := p.verifyAlertExists(alert_id)
	if err != nil {
		err = fmt.Errorf("UpdateAlertStateAndIncrementNumEvaluations: unable to verify if alert name exists, AlertId=%v, Error=%+v", alert_id, err)
		log.Error(err.Error())
		return err
	}
	if !alertExists {
		err := fmt.Errorf("UpdateAlertStateAndIncrementNumEvaluations: alert does not exist, AlertId=%v", alert_id)
		log.Error(err.Error())
		return err
	}

	if err := p.db.Model(&alertutils.AlertDetails{}).Where("alert_id = ?", alert_id).
		Updates(map[string]interface{}{
			"state":                 alertState,
			"num_evaluations_count": gorm.Expr("num_evaluations_count + ?", 1),
		}).Error; err != nil {
		err = fmt.Errorf("UpdateAlertStateAndIncrementNumEvaluations: unable to update alert state and increment evaluations count, with AlertId=%v, Error=%+v", alert_id, err)
		log.Error(err.Error())
		return err
	}
	return nil
}

func (p Sqlite) GetEmailAndChannelID(contact_id string) ([]string, []alertutils.SlackTokenConfig, []alertutils.WebHookConfig, error) {

	var contact = &alertutils.Contact{}
	if err := p.db.Preload("Slack").Preload("Webhook").Where("contact_id = ?", contact_id).First(contact).Error; err != nil {
		err = fmt.Errorf("GetEmailAndChannelID: unable to update contact, contact id: %v, Error=%+v", contact_id, err)
		log.Error(err.Error())
		return nil, nil, nil, err
	}
	emailArray := contact.Email
	slackArray := contact.Slack
	webhookArray := contact.Webhook

	return emailArray, slackArray, webhookArray, nil
}

func (p Sqlite) GetAllMinionSearches(orgId uint64) ([]alertutils.MinionSearch, error) {

	alerts := make([]alertutils.MinionSearch, 0)
	err := p.db.Model(&alerts).Where("org_id = ?", orgId).Find(&alertutils.MinionSearch{}).Error
	return alerts, err
}

// Creates a new record in all_alerts table
func (p Sqlite) CreateMinionSearch(minionSearchDetails *alertutils.MinionSearch) (alertutils.MinionSearch, error) {
	if !isValid(minionSearchDetails.AlertName) {
		err := fmt.Errorf("CreateMinionSearch: Data Validation Check Failed: Alert Name: %v is not Valid", minionSearchDetails.AlertName)
		log.Error(err.Error())
		return alertutils.MinionSearch{}, err
	}
	isNewAlertName, _ := p.isNewAlertName(minionSearchDetails.AlertName)

	if !isNewAlertName {
		err := fmt.Errorf("CreateMinionSearch: Alert Name=%v already exists", minionSearchDetails.AlertName)
		log.Error(err.Error())
		return alertutils.MinionSearch{}, err
	}
	minionSearchDetails.CreateTimestamp = time.Now()
	minionSearchDetails.State = alertutils.Inactive

	result := p.db.Create(minionSearchDetails)
	if result.Error != nil && result.RowsAffected != 1 {
		err := fmt.Errorf("CreateAlert: unable to create alert, Alert Name=%v Error=%v", minionSearchDetails.AlertName, result.Error)
		log.Error(err.Error())
		return alertutils.MinionSearch{}, err
	}

	return *minionSearchDetails, nil
}

func (p Sqlite) GetMinionSearch(alert_id string) (*alertutils.MinionSearch, error) {
	if !isValid(alert_id) {
		err := fmt.Errorf("GetMinionSearch: Data Validation Check Failed: AlertId=%v is not valid", alert_id)
		log.Error(err.Error())
		return nil, err
	}

	var alert alertutils.MinionSearch
	if err := p.db.Preload("Labels").Where(&alertutils.AlertDetails{AlertId: alert_id}).Find(&alert).Error; err != nil {
		return nil, err
	}
	return &alert, nil

}

func (p Sqlite) UpdateMinionSearchStateByAlertID(alertId string, alertState alertutils.AlertState) error {
	if !isValid(alertId) {
		err := fmt.Errorf("UpdateMinionSearchStateByAlertID: Data Validation Check Failed: AlertId=%v is not valid", alertId)
		log.Error(err.Error())
		return err
	}
	searchExists, _, err := p.verifyMinionSearchExists(alertId)
	if err != nil {
		err = fmt.Errorf("UpdateMinionSearchStateByAlertID: unable to verify if alert name exists, AlertId=%v, Error=%+v", alertId, err)
		log.Error(err.Error())
		return err
	}
	if !searchExists {
		err := fmt.Errorf("UpdateMinionSearchStateByAlertID: alert does not exist, AlertId=%v", alertId)
		log.Error(err.Error())
		return err
	}
	if err := p.db.Model(&alertutils.MinionSearch{}).Where("alert_id = ?", alertId).Update("state", alertState).Error; err != nil {
		err = fmt.Errorf("UpdateAlertStateByAlertID: unable to update alert state, with AlertId=%v, Error=%+v", alertId, err)
		log.Error(err.Error())
		return err
	}
	return nil
}

func (p Sqlite) verifyMinionSearchExists(alert_id string) (bool, string, error) {
	if !isValid(alert_id) {
		err := fmt.Errorf("verifyMinionSearchExists: Data Validation Check Failed: AlertId=%v is not valid", alert_id)
		log.Error(err.Error())
		return false, "", err
	}
	var alert alertutils.MinionSearch

	if err := p.db.Where("alert_id = ?", alert_id).Find(&alert).First(&alertutils.AlertDetails{}).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return true, alert.AlertName, nil
		} else {
			return false, "", err
		}
	}
	return true, "", nil
}

func (p Sqlite) CreateAlertHistory(alertHistoryDetails *alertutils.AlertHistoryDetails) (*alertutils.AlertHistoryDetails, error) {
	if !isValid(alertHistoryDetails.AlertId) || !isValid(alertHistoryDetails.EventDescription) || !isValid(alertHistoryDetails.UserName) {
		err := fmt.Errorf("CreateAlertHistory: data validation check failed. AlertId=%v or Event Description=%v or Username=%v is/are not valid", alertHistoryDetails.AlertId, alertHistoryDetails.EventDescription, alertHistoryDetails.UserName)
		log.Error(err.Error())
		return nil, err
	}

	result := p.db.Create(alertHistoryDetails)
	if result.Error != nil && result.RowsAffected != 1 {
		err := fmt.Errorf("CreateAlert: unable to create alert, AlertId=%v, Alert Name=%v, Error=%v", alertHistoryDetails.AlertId, alertHistoryDetails.UserName, result.Error)
		log.Error(err.Error())
		return &alertutils.AlertHistoryDetails{}, err
	}
	return alertHistoryDetails, nil
}

func (p Sqlite) GetAlertHistoryByAlertID(alertHistoryParams *alertutils.AlertHistoryQueryParams) ([]*alertutils.AlertHistoryDetails, error) {
	if !isValid(alertHistoryParams.AlertId) {
		err := fmt.Errorf("GetAlertHistory: Data Validation Check Failed: Alert Id: %v is not valid", alertHistoryParams.AlertId)
		log.Error(err.Error())
		return nil, err
	}

	alertExists, _, err := p.verifyAlertExists(alertHistoryParams.AlertId)
	if err != nil {
		err = fmt.Errorf("GetAlertHistory: unable to verify if alert exists, AlertId=%v, Error=%+v", alertHistoryParams.AlertId, err)
		log.Error(err.Error())
		return nil, err
	}

	if !alertExists {
		err := fmt.Errorf("GetAlertHistory: alert does not exist, AlertId=%v", alertHistoryParams.AlertId)
		log.Error(err.Error())
		return nil, err
	}

	if alertHistoryParams.Limit == 0 {
		alertHistoryParams.Limit = 20
	}

	if alertHistoryParams.SortOrder == "" {
		alertHistoryParams.SortOrder = alertutils.DESC
	}

	alertHistory := make([]*alertutils.AlertHistoryDetails, 0)

	query := p.db.Where("alert_id = ?", alertHistoryParams.AlertId).Order(
		clause.OrderByColumn{Column: clause.Column{Name: clause.PrimaryColumn.Name}, Desc: alertHistoryParams.SortOrder == alertutils.DESC}).Offset(int(alertHistoryParams.Offset)).Limit(int(alertHistoryParams.Limit))

	err = query.Find(&alertHistory).Error
	if err != nil {
		err = fmt.Errorf("GetAlertHistory: unable to fetch alert history for Alert Query Params: %v, Error=%+v", *alertHistoryParams, err)
		log.Error(err.Error())
		return nil, err
	}

	return alertHistory, nil
}

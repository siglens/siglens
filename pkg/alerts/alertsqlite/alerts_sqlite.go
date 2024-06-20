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
	_ "gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/siglens/siglens/pkg/alerts/alertutils"
	"github.com/sirupsen/logrus"
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
		log.Errorf("CloseDb: Error occurred while closing a DB connection, err: %v", err)
	}
	defer sqlDB.Close()
}

func (p *Sqlite) Connect() error {
	dbname := "siglens.db"
	logger := logrus.New()
	dbConnection, err := gorm.Open(sqlite.Open(dbname), &gorm.Config{
		Logger: alertutils.NewGormLogrusLogger(logger.WithField("component", "gorm"), 100*time.Millisecond),
	})
	if err != nil {
		log.Errorf("Connect: error in opening sqlite connection, err: %+v", err)
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
		log.Errorf("isNewAlertName: data validation check failed for alertName: %v", alertName)
		return false, fmt.Errorf("isNewAlertName: data validation check failed for alertName: %v", alertName)
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

// checks based on alert_id, returns true and alert_name if alert exists
func (p Sqlite) verifyAlertExists(alert_id string) (bool, string, error) {
	if !isValid(alert_id) {
		log.Errorf("verifyAlertExists: data validation check failed %v", alert_id)
		return false, "", fmt.Errorf("verifyAlertExists: data validation check failed %v", alert_id)
	}
	var alert alertutils.AlertDetails

	if err := p.db.Where("alert_id = ?", alert_id).Find(&alert).First(&alertutils.AlertDetails{}).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return true, alert.AlertName, nil
		} else {
			return false, "", err
		}
	}

	return true, "", nil
}

func (p Sqlite) verifyContactExists(contact_id string) (bool, error) {
	if !isValid(contact_id) {
		log.Errorf("verifyContactExists: data validation check failed for contact_id: %v", contact_id)
		return false, fmt.Errorf("verifyContactExists: data validation check failed for contact_id: %v", contact_id)
	}
	var contact alertutils.Contact
	if err := p.db.Where("contact_id = ?", contact_id).Find(&contact).First(&alertutils.Contact{}).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return true, nil
		} else {
			return false, err
		}
	}
	return true, nil
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
		log.Errorf("CreateAlert: data validation check failed for alert: %v", alertDetails.AlertName)
		return alertutils.AlertDetails{}, fmt.Errorf("createAlert: data validation check failed for alert: %v", alertDetails.AlertName)
	}
	isNewAlertName, _ := p.isNewAlertName(alertDetails.AlertName)

	if !isNewAlertName {
		log.Errorf("CreateAlert: alert name: %v already exists", alertDetails.AlertName)
		return alertutils.AlertDetails{}, fmt.Errorf("createAlert: alert name: %v already exists", alertDetails.AlertName)
	}
	alert_id := CreateUniqId()
	state := alertutils.Inactive
	alertDetails.State = state
	alertDetails.AlertId = alert_id
	result := p.db.Create(alertDetails)
	if result.Error != nil && result.RowsAffected != 1 {
		log.Errorf("CreateAlert: unable to create alert for alert: %v, err: %v", alertDetails.AlertName, result.Error)
		return alertutils.AlertDetails{}, result.Error
	}

	var notification alertutils.Notification
	notification.CooldownPeriod = 0
	notification.AlertId = alert_id
	notification.NotificationId = CreateUniqId()
	result = p.db.Create(&notification)
	if result.Error != nil && result.RowsAffected != 1 {
		log.Errorf("CreateAlert: unable to update notification details for alert: %v, err= %v", alertDetails.AlertName, result.Error)
		return alertutils.AlertDetails{}, result.Error
	}
	return *alertDetails, nil
}

func (p Sqlite) GetAlert(alert_id string) (*alertutils.AlertDetails, error) {
	if !isValid(alert_id) {
		log.Errorf("GetAlert: data validation check failed for alert id: %v", alert_id)
		return nil, fmt.Errorf("GetAlert: data validation check failed for alert id: %v", alert_id)
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
		log.Errorf("UpdateSilenceMinutes: data validation check failed for alert: %v", updatedSilenceMinutes.AlertName)
		return fmt.Errorf("UpdateSilenceMinutes: data validation check failed for alert: %v", updatedSilenceMinutes.AlertName)
	}
	alertExists, _, err := p.verifyAlertExists(updatedSilenceMinutes.AlertId)
	if err != nil {
		log.Errorf("UpdateSilenceMinutes: unable to verify if alert: %v exists, err: %+v", updatedSilenceMinutes.AlertName, err)
		return fmt.Errorf("UpdateSilenceMinutes: unable to verify if alert: %v exists, err: %+v", updatedSilenceMinutes.AlertName, err)
	}
	if !alertExists {
		log.Errorf("UpdateSilenceMinutes: alert: %v does not exist", updatedSilenceMinutes.AlertName)
		return fmt.Errorf("UpdateSilenceMinutes: alert: %v does not exist", updatedSilenceMinutes.AlertName)
	}
	result := p.db.Save(&updatedSilenceMinutes)
	if result.Error != nil && result.RowsAffected != 1 {
		log.Errorf("UpdateSilenceMinutes: unable to update silence minutes details for alert: %v, err: %v", updatedSilenceMinutes.AlertName, result.Error)
		return fmt.Errorf("UpdateSilenceMinutes: unable to update silence minutes details for alert: %v, err: %v", updatedSilenceMinutes.AlertName, result.Error)
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
	if !isValid(editedAlert.AlertName) || !isValid(editedAlert.QueryParams.QueryText) {
		log.Errorf("UpdateAlert: data validation check failed for alert: %v", editedAlert.AlertName)
		return fmt.Errorf("UpdateAlert: data validation check failed for alert: %v", editedAlert.AlertName)
	}
	alertExists, alertName, err := p.verifyAlertExists(editedAlert.AlertId)
	if err != nil {
		log.Errorf("UpdateAlert: unable to verify if alert: %v exists, err: %+v", editedAlert.AlertName, err)
		return fmt.Errorf("UpdateAlert: unable to verify if alert: %v exists, err: %+v", editedAlert.AlertName, err)
	}
	// new alert means id in request body is incorrect
	if !alertExists {
		log.Errorf("UpdateAlert: alert: %v does not exist", editedAlert.AlertName)
		return fmt.Errorf("UpdateAlert: alert: %v does not exist", editedAlert.AlertName)
	}
	// if alert name in request body is same as that present in db, allow update
	if alertName != editedAlert.AlertName {
		isNewAlertName, err := p.isNewAlertName(editedAlert.AlertName)
		if err != nil {
			log.Errorf("UpdateAlert: unable to verify if alert name: %v is new, err: %+v", editedAlert.AlertName, err)
			return fmt.Errorf("UpdateAlert: unable to verify if alert name: %v is new, err: %+v", editedAlert.AlertName, err)
		}
		if !isNewAlertName {
			log.Errorf("UpdateAlert: alert name: %v already exists", editedAlert.AlertName)
			return fmt.Errorf("UpdateAlert: alert name: %v already exists", editedAlert.AlertName)
		}
	}
	result := p.db.Set("gorm:association_autoupdate", true).Save(&editedAlert)
	if result.Error != nil && result.RowsAffected != 1 {
		log.Errorf("UpdateAlert: unable to update details for alert: %v, err: %v", editedAlert.AlertName, result.Error)
		return fmt.Errorf("UpdateAlert: unable to update details for alert: %v, err: %v", editedAlert.AlertName, result.Error)
	}
	return nil
}

func (p Sqlite) DeleteAlert(alert_id string) error {
	if !isValid(alert_id) {
		log.Errorf("DeleteAlert: data validation check failed for alert id: %v", alert_id)
		return fmt.Errorf("DeleteAlert: data validation check failed for alert id: %v", alert_id)
	}
	var alert alertutils.AlertDetails
	result := p.db.First(&alert, "alert_id = ?", alert_id)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			log.Errorf("DeleteAlert: error deleting alert, alert does not exist, alert name: %v, err: %v", alert.AlertName, result.Error)
			return fmt.Errorf("DeleteAlert: error deleting alert, alert does not exist, alert name: %v, err: %v", alert.AlertName, result.Error)
		} else {
			log.Errorf("DeleteAlert: error deleting alert, alert name: %v, err: %v", alert.AlertName, result.Error)
			return fmt.Errorf("DeleteAlert: error deleting alert, alert name: %v, err: %v", alert.AlertName, result.Error)
		}
	}
	err := p.db.Model(&alert).Association("Labels").Clear()
	if err != nil {
		log.Errorf("DeleteAlert: unable to delete alert, alert name: %v, err: %v", alert.AlertName, err)
		return fmt.Errorf("DeleteAlert: unable to delete alert, alert name: %v, err: %v", alert.AlertName, err)
	}

	result = p.db.Delete(&alert)
	if result.Error != nil && result.RowsAffected != 1 {
		log.Errorf("DeleteAlert: unable to delete alert, alert id: %v, err: %v", alert.AlertName, err)
		return fmt.Errorf("DeleteAlert: unable to delete alert, alert name: %v, err: %v", alert.AlertName, err)
	}

	return nil
}

func (p Sqlite) CreateContact(newContact *alertutils.Contact) error {
	var contact alertutils.Contact
	result := p.db.First(&contact, "contact_name = ?", newContact.ContactName)
	if result.Error != nil {
		if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
			log.Errorf("CreateContact: contact name: %v already exist, err: %v", newContact.ContactName, result.Error)
			return fmt.Errorf("CreateContact: contact name: %v already exist, err: %v", newContact.ContactName, result.Error)
		} else {
			contact_id := CreateUniqId()
			newContact.ContactId = contact_id
			result = p.db.Create(&newContact)
			if result.Error != nil && result.RowsAffected != 1 {
				log.Errorf("CreateContact: unable to create contact: %v, err: %v", newContact.ContactName, result.Error)
				return fmt.Errorf("CreateContact: unable to create contact: %v, err: %v", newContact.ContactName, result.Error)
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
		log.Errorf("UpdateContactPoint: invalid contact id: %v, contact name: %v", contact.ContactId, contact.ContactName)
		return fmt.Errorf("UpdateContactPoint: invalid contact id: %v, contact name: %v", contact.ContactId, contact.ContactName)
	}

	contactExists, err := p.verifyContactExists(contact.ContactId)
	if err != nil {
		log.Errorf("UpdateContactPoint: unable to verify if contact exists, contact name: %v, err: %+v", contact.ContactName, err)
		return fmt.Errorf("UpdateContactPoint: unable to verify if contact exists, contact name: %v, err: %+v", contact.ContactName, err)
	}
	// contact does not exist, that means id in request body is incorrect
	if !contactExists {
		log.Errorf("UpdateContactPoint: contact does not exist, contact name: %v", contact.ContactName)
		return fmt.Errorf("UpdateContactPoint: contact does not exist, contact name: %v", contact.ContactName)
	}

	if len(contact.Slack) != 0 {
		err := p.db.Model(&alertutils.Contact{ContactId: contact.ContactId}).Association("Slack").Clear()
		if err != nil {
			log.Errorf("UpdateContactPoint: unable to update contact : %v, err: %+v", contact.ContactName, err)
			return fmt.Errorf("UpdateContactPoint: unable to update contact : %v, err: %+v", contact.ContactName, err)
		}
	}
	if len(contact.Webhook) != 0 {
		err := p.db.Model(&alertutils.Contact{ContactId: contact.ContactId}).Association("Webhook").Clear()
		if err != nil {
			log.Errorf("UpdateContactPoint: unable to update contact: %v, err: %+v", contact.ContactName, err)
			return fmt.Errorf("UpdateContactPoint: unable to update contact: %v, err: %+v", contact.ContactName, err)
		}
	}
	result := p.db.Session(&gorm.Session{FullSaveAssociations: true}).Save(&contact)
	if result.Error != nil && result.RowsAffected != 1 {
		log.Errorf("UpdateContactPoint: unable to update contact: %v, err: %+v", contact.ContactName, err)
		return fmt.Errorf("UpdateContactPoint: unable to update contact: %v, err: %+v", contact.ContactName, err)
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

func (p Sqlite) DeleteContactPoint(contact_id string) error {
	if !isValid(contact_id) {
		log.Errorf("DeleteContactPoint: data validation check failed, contact id: %v", contact_id)
		return fmt.Errorf("DeleteContactPoint: data validation check failed, contact id: %v", contact_id)
	}

	contactExists, err := p.verifyContactExists(contact_id)
	if err != nil {
		log.Errorf("DeleteContactPoint: unable to verify if contact exists, contact id: %v, err: %+v", contact_id, err)
		return fmt.Errorf("DeleteContactPoint: unable to verify if contact exists, contact id: %v, err: %+v", contact_id, err)
	}
	// contact does not exist, that means id in request body is incorrect
	if !contactExists {
		log.Errorf("DeleteContactPoint: contact does not exist, contact id: %v", contact_id)
		return fmt.Errorf("DeleteContactPoint: contact does not exist, contact id: %v", contact_id)
	}

	var contact alertutils.Contact

	result := p.db.First(&contact, "contact_id = ?", contact_id)
	if result.Error != nil {
		log.Errorf("DeleteContactPoint: error deleting contact, contact: %v, err: %v", contact.ContactName, result.Error)
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			log.Errorf("DeleteContactPoint: contact: %v does not exist", contact.ContactName)
			return result.Error
		} else {
			return fmt.Errorf("DeleteContactPoint: error deleting contact, contact: %v, err: %v", contact.ContactName, result.Error)
		}
	}
	err = p.db.Model(&contact).Association("Slack").Clear()
	if err != nil {
		log.Errorf("DeleteContactPoint: unable to delete contact: %v, err: %v", contact.ContactName, err)
		return fmt.Errorf("DeleteContactPoint: unable to delete contact: %v, err: %v", contact.ContactName, err)
	}

	result = p.db.Delete(&contact)
	if result.Error != nil && result.RowsAffected != 1 {
		log.Errorf("DeleteContactPoint: unable to delete contact: %v, err: %v", contact.ContactName, err)
		return fmt.Errorf("DeleteContactPoint: unable to delete contact: %v, err: %v", contact.ContactName, err)
	}

	return nil
}

// update last_sent_time in notification_details table
func (p Sqlite) UpdateLastSentTime(alert_id string) error {
	currentTime := time.Now().UTC()
	if err := p.db.Model(&alertutils.Notification{}).Where("alert_id = ?", alert_id).Update("last_sent_time", currentTime).Error; err != nil {
		log.Errorf("UpdateLastSentTime: unable to UpdateLastSentTime, alert id: %v, err: %+v", alert_id, err)
		return fmt.Errorf("UpdateLastSentTime: unable to UpdateLastSentTime, alert id: %v, err: %+v", alert_id, err)
	}
	return nil
}

func (p Sqlite) UpdateAlertStateByAlertID(alert_id string, alertState alertutils.AlertState) error {
	if !isValid(alert_id) {
		log.Errorf("UpdateAlertStateByAlertID: data validation check failed, alert id: %v", alert_id)
		return fmt.Errorf("UpdateAlertStateByAlertID: data validation check failed, alert id: %v", alert_id)
	}
	alertExists, _, err := p.verifyAlertExists(alert_id)
	if err != nil {
		log.Errorf("UpdateAlertStateByAlertID: unable to verify if alert name exists, alert id: %v, err: %+v", alert_id, err)
		return fmt.Errorf("UpdateAlertStateByAlertID: unable to verify if alert name exists, alert id: %v, err: %+v", alert_id, err)
	}
	if !alertExists {
		log.Errorf("UpdateAlertStateByAlertID: alert does not exist, alert id: %v", alert_id)
		return fmt.Errorf("UpdateAlertStateByAlertID: alert does not exist, alert id: %v", alert_id)
	}

	if err := p.db.Model(&alertutils.AlertDetails{}).Where("alert_id = ?", alert_id).Update("state", alertState).Error; err != nil {
		log.Errorf("UpdateAlertStateByAlertID: unable to update alert state, with alert id: %v, err: %+v", alert_id, err)
		return fmt.Errorf("UpdateAlertStateByAlertID: unable to update alert state, with alert id: %v, err: %+v", alert_id, err)
	}
	return nil
}

func (p Sqlite) GetEmailAndChannelID(contact_id string) ([]string, []alertutils.SlackTokenConfig, []alertutils.WebHookConfig, error) {

	var contact = &alertutils.Contact{}
	if err := p.db.Preload("Slack").Preload("Webhook").Where("contact_id = ?", contact_id).First(contact).Error; err != nil {
		log.Errorf("GetEmailAndChannelID: unable to update contact, contact id: %v, err: %+v", contact_id, err)
		return nil, nil, nil, fmt.Errorf("GetEmailAndChannelID: unable to update contact, contact id: %v, err: %+v", contact_id, err)
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
		log.Errorf("CreateMinionSearch: data validation check failed for alert: %v", minionSearchDetails.AlertName)
		return alertutils.MinionSearch{}, fmt.Errorf("CreateMinionSearch: data validation check failed for alert: %v", minionSearchDetails.AlertName)
	}
	isNewAlertName, _ := p.isNewAlertName(minionSearchDetails.AlertName)

	if !isNewAlertName {
		log.Errorf("CreateMinionSearch: alert name: %v already exists", minionSearchDetails.AlertName)
		return alertutils.MinionSearch{}, fmt.Errorf("CreateMinionSearch: alert name: %v already exists", minionSearchDetails.AlertName)
	}
	minionSearchDetails.CreateTimestamp = time.Now()
	minionSearchDetails.State = alertutils.Inactive

	result := p.db.Create(minionSearchDetails)
	if result.Error != nil && result.RowsAffected != 1 {
		log.Errorf("CreateAlert: unable to create alert, alert name: %v err: %v", minionSearchDetails.AlertName, result.Error)
		return alertutils.MinionSearch{}, fmt.Errorf("CreateAlert: unable to create alert, alert name: %v err: %v", minionSearchDetails.AlertName, result.Error)
	}

	return *minionSearchDetails, nil
}

func (p Sqlite) GetMinionSearch(alert_id string) (*alertutils.MinionSearch, error) {
	if !isValid(alert_id) {
		log.Errorf("GetMinionSearch: data validation check failed for alert id: %v", alert_id)
		return nil, fmt.Errorf("GetMinionSearch: data validation check failed for alert id: %v", alert_id)
	}

	var alert alertutils.MinionSearch
	if err := p.db.Preload("Labels").Where(&alertutils.AlertDetails{AlertId: alert_id}).Find(&alert).Error; err != nil {
		return nil, err
	}
	return &alert, nil

}

func (p Sqlite) UpdateMinionSearchStateByAlertID(alertId string, alertState alertutils.AlertState) error {
	if !isValid(alertId) {
		log.Errorf("UpdateMinionSearchStateByAlertID: data validation check failed for alert id: %v", alertId)
		return fmt.Errorf("UpdateMinionSearchStateByAlertID: data validation check failed for alert id: %v", alertId)
	}
	searchExists, _, err := p.verifyMinionSearchExists(alertId)
	if err != nil {
		log.Errorf("UpdateMinionSearchStateByAlertID: unable to verify if alert name exists, alert id: %v, err: %+v", alertId, err)
		return fmt.Errorf("UpdateMinionSearchStateByAlertID: unable to verify if alert name exists, alert id: %v, err: %+v", alertId, err)
	}
	if !searchExists {
		log.Errorf("UpdateMinionSearchStateByAlertID: alert does not exist, alert id: %v", alertId)
		return fmt.Errorf("UpdateMinionSearchStateByAlertID: alert does not exist, alert id: %v", alertId)
	}
	if err := p.db.Model(&alertutils.MinionSearch{}).Where("alert_id = ?", alertId).Update("state", alertState).Error; err != nil {
		log.Errorf("UpdateAlertStateByAlertID: unable to update alert state, with alert id: %v, err: %+v", alertId, err)
		return fmt.Errorf("UpdateAlertStateByAlertID: unable to update alert state, with alert id: %v, err: %+v", alertId, err)
	}
	return nil
}

func (p Sqlite) verifyMinionSearchExists(alert_id string) (bool, string, error) {
	if !isValid(alert_id) {
		log.Errorf("verifyMinionSearchExists: data validation check failed, alert id: %v", alert_id)
		return false, "", fmt.Errorf("verifyMinionSearchExists: data validation check failed, alert id: %v", alert_id)
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
		log.Errorf("CreateAlertHistory: data validation check failed, alert id: %v, alert name: %v", alertHistoryDetails.AlertId, alertHistoryDetails.UserName)
		return nil, fmt.Errorf("CreateAlertHistory: data validation check failed, alert id: %v, alert name: %v", alertHistoryDetails.AlertId, alertHistoryDetails.UserName)
	}

	result := p.db.Create(alertHistoryDetails)
	if result.Error != nil && result.RowsAffected != 1 {
		log.Errorf("CreateAlert: unable to create alert, alert id: %v, alert name: %v, err: %v", alertHistoryDetails.AlertId, alertHistoryDetails.UserName, result.Error)
		return &alertutils.AlertHistoryDetails{}, fmt.Errorf("CreateAlert: unable to create alert, alert id: %v, alert name: %v, err: %v", alertHistoryDetails.AlertId, alertHistoryDetails.UserName, result.Error)
	}
	return alertHistoryDetails, nil
}

func (p Sqlite) GetAlertHistory(alertId string) ([]*alertutils.AlertHistoryDetails, error) {
	if !isValid(alertId) {
		log.Errorf("GetAlertHistory: data validation check failed for alert id: %v", alertId)
		return nil, fmt.Errorf("GetAlertHistory: data validation check failed for alert id: %v", alertId)
	}

	alertExists, _, err := p.verifyAlertExists(alertId)
	if err != nil {
		log.Errorf("GetAlertHistory: unable to verify if alert exists, alert id: %v, err: %+v", alertId, err)
		return nil, fmt.Errorf("GetAlertHistory: unable to verify if alert exists, alert id: %v, err: %+v", alertId, err)
	}

	if !alertExists {
		log.Errorf("GetAlertHistory: alert does not exist, alert id: %v", alertId)
		return nil, fmt.Errorf("GetAlertHistory: alert does not exist, alert id: %v", alertId)
	}

	alertHistory := make([]*alertutils.AlertHistoryDetails, 0)

	err = p.db.Where("alert_id = ?", alertId).First(&alertHistory).Error
	return alertHistory, err

}

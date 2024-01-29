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

func (p *Sqlite) Connect() error {
	dbname := "siglens.db"
	logger := logrus.New()
	dbConnection, err := gorm.Open(sqlite.Open(dbname), &gorm.Config{
		Logger: alertutils.NewGormLogrusLogger(logger.WithField("component", "gorm"), 100*time.Millisecond),
	})
	if err != nil {
		log.Errorf("connectAlertDB: error in opening sqlite connection, err: %+v", err)
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
	p.ctx = context.Background()
	return nil
}

func isValid(str string) bool {
	return str != "" && str != "*"
}

// checks whether the alert name exists
func (p Sqlite) isNewAlertName(alertName string) (bool, error) {
	if !isValid(alertName) {
		log.Errorf("isNewAlertName: data validation check failed")
		return false, errors.New("isNewAlertName: data validation check failed")
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
		return false, "", errors.New("verifyAlertExists: data validation check failed")
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
		log.Errorf("verifyContactExists: data validation check failed")
		return false, errors.New("verifyContactExists: data validation check failed")
	}
	var contact alertutils.Contact
	if err := p.db.Where("contact_id = ?", contact_id).Find(&contact).First(&alertutils.Contact{}).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return true, nil
		} else {
			return false, err
		}
	}
	return false, nil
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
		log.Errorf("createAlert: data validation check failed")
		return alertutils.AlertDetails{}, errors.New("createAlert: data validation check failed")
	}
	isNewAlertName, _ := p.isNewAlertName(alertDetails.AlertName)

	if !isNewAlertName {
		log.Errorf("createAlert: alert name already exists")
		return alertutils.AlertDetails{}, errors.New("alert name already exists")
	}
	alert_id := CreateUniqId()
	state := alertutils.Inactive
	alertDetails.State = state
	alertDetails.AlertId = alert_id
	result := p.db.Create(alertDetails)
	if result.Error != nil && result.RowsAffected != 1 {
		log.Errorf("createAlert: unable to create alert:%v", result.Error)
		return alertutils.AlertDetails{}, result.Error
	}

	var notification alertutils.Notification
	notification.CooldownPeriod = 0
	notification.AlertId = alert_id
	result = p.db.Create(&notification)
	if result.Error != nil && result.RowsAffected != 1 {
		log.Errorf("createAlert: unable to update notification details:%v", result.Error)
		return alertutils.AlertDetails{}, result.Error
	}
	return *alertDetails, nil
}

func (p Sqlite) GetAlert(alert_id string) (*alertutils.AlertDetails, error) {
	if !isValid(alert_id) {
		log.Errorf("getAlert: data validation check failed")
		return nil, errors.New("getAlert: data validation check failed")
	}
	var alert alertutils.AlertDetails
	if err := p.db.Preload("Labels").Where(&alertutils.AlertDetails{AlertId: alert_id}).Find(&alert).Error; err != nil {
		return nil, err
	}
	return &alert, nil

}

func (p Sqlite) GetAllAlerts() ([]alertutils.AlertDetails, error) {
	alerts := make([]alertutils.AlertDetails, 0)
	if err := p.db.Preload("Labels").Find(&alerts).Error; err != nil {
		return nil, err
	}
	return alerts, nil
}

func (p Sqlite) UpdateSilenceMinutes(updatedSilenceMinutes *alertutils.AlertDetails) error {
	if !isValid(updatedSilenceMinutes.AlertName) || !isValid(updatedSilenceMinutes.QueryParams.QueryText) {
		log.Errorf("updateSilenceMinutes: data validation check failed")
		return errors.New("updateSilenceMinutesupdateSilenceMinutes: data validation check failed")
	}
	alertExists, _, err := p.verifyAlertExists(updatedSilenceMinutes.AlertId)
	if err != nil {
		log.Errorf("updateSilenceMinutes: unable to verify if alert exists, err: %+v", err)
		return err
	}
	if !alertExists {
		log.Errorf("updateSilenceMinutes: alert does not exist")
		return errors.New("alert does not exist")
	}
	result := p.db.Save(&updatedSilenceMinutes)
	if result.Error != nil && result.RowsAffected != 1 {
		log.Errorf("UpdateSilenceMinutes: unable to update silence minutes details:%v", result.Error)
		return result.Error
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
		log.Errorf("updateAlert: data validation check failed")
		return errors.New("updateAlert: data validation check failed")
	}
	alertExists, alertName, err := p.verifyAlertExists(editedAlert.AlertId)
	if err != nil {
		log.Errorf("updateAlert: unable to verify if alert exists, err: %+v", err)
		return err
	}
	// new alert means id in request body is incorrect
	if !alertExists {
		log.Errorf("updateAlert: alert does not exist")
		return errors.New("alert does not exist")
	}
	// if alert name in request body is same as that present in db, allow update
	if alertName != editedAlert.AlertName {
		isNewAlertName, err := p.isNewAlertName(editedAlert.AlertName)
		if err != nil {
			log.Errorf("updateAlert: unable to verify if alert name is new, err: %+v", err)
			return err
		}
		if !isNewAlertName {
			log.Errorf("updateAlert: alert name already exists")
			return errors.New("alert name already exists")
		}
	}
	result := p.db.Save(&editedAlert)
	if result.Error != nil && result.RowsAffected != 1 {
		log.Errorf("UpdateAlert: unable to update alert details:%v", result.Error)
		return result.Error
	}
	return nil
}

func (p Sqlite) DeleteAlert(alert_id string) error {
	if !isValid(alert_id) {
		log.Errorf("deleteAlert: data validation check failed")
		return errors.New("deleteAlert: data validation check failed")
	}
	var alert alertutils.AlertDetails
	result := p.db.First(&alert, "alert_id = ?", alert_id)
	if result.Error != nil {
		log.Errorf("deleteAlert: error deleting alert %v", result.Error)
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			log.Errorf("deleteAlert: alert does not exist")
			return result.Error
		} else {
			return result.Error
		}
	}

	result = p.db.Delete(&alert)
	if result.Error != nil && result.RowsAffected != 1 {
		log.Errorf("deleteAlert: unable to delete alert :%v", result.Error)
		return result.Error
	}

	return nil
}

func (p Sqlite) CreateContact(newContact *alertutils.Contact) error {
	var contact alertutils.Contact
	log.Info(newContact, "newContact")
	result := p.db.First(&contact, "contact_name = ?", newContact.ContactName)
	if result.Error != nil {
		if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
			log.Errorf("CreateContact: contact name already exist")
			return result.Error
		} else {
			contact_id := CreateUniqId()
			log.Info(contact_id, "Contact")
			newContact.ContactId = contact_id
			result = p.db.Create(&newContact)
			if result.Error != nil && result.RowsAffected != 1 {
				log.Errorf("CreateContact: unable to create contact:%v", result.Error)
				return result.Error
			}
		}
	}
	return nil
}

func (p Sqlite) GetLabels(alertDetails *alertutils.AlertDetails) ([]alertutils.AlertLabel, error) {
	labels := make([]alertutils.AlertLabel, 0)
	err := p.db.Model(alertDetails).Association(alertutils.LabelAssociation).Find(&labels)
	return labels, err
}

func (p Sqlite) GetAllContactPoints() ([]alertutils.Contact, error) {
	contacts := make([]alertutils.Contact, 0)
	if err := p.db.Preload("Slack").Find(&contacts).Error; err != nil {
		return nil, err
	}

	return contacts, nil
}

func (p Sqlite) UpdateContactPoint(contact *alertutils.Contact) error {
	if !isValid(contact.ContactId) {
		log.Errorf("updateContactPoint: invalid contact id")
		return errors.New("invalid contact id")
	}

	contactExists, err := p.verifyContactExists(contact.ContactId)
	if err != nil {
		log.Errorf("updateContactPoint: unable to verify if contact exists, err: %+v", err)
		return err
	}
	// contact does not exist, that means id in request body is incorrect
	if !contactExists {
		log.Errorf("updateContactPoint: contact does not exist")
		return errors.New("contact does not exist")
	}

	result := p.db.Save(&contact)
	if result.Error != nil && result.RowsAffected != 1 {
		log.Errorf("updateContactPoint: unable to update contact : %v, err: %+v", contact.ContactName, err)
		return result.Error
	}
	return nil

}

// get contact_id and message from all_alerts table using alert_id
func (p Sqlite) GetContactDetails(alert_id string) (string, string, string, error) {

	var alert alertutils.AlertDetails

	if err := p.db.First(alert, "alert_id = ?", alert_id).Error; err != nil {
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
	if err := p.db.Preload("Notification").Where(&alertutils.AlertDetails{AlertId: alert_id}).Find(&notification).Error; err != nil {
		return 0, time.Time{}, err
	}
	cooldown_period := notification.CooldownPeriod
	last_sent_time := notification.LastSentTime

	return cooldown_period, last_sent_time, nil
}

func (p Sqlite) DeleteContactPoint(contact_id string) error {
	if !isValid(contact_id) {
		log.Errorf("deleteContactPoint: data validation check failed")
		return errors.New("deleteContactPoint: data validation check failed")
	}

	contactExists, err := p.verifyContactExists(contact_id)
	if err != nil {
		log.Errorf("deleteContactPoint: unable to verify if contact exists, err: %+v", err)
		return err
	}
	// contact does not exist, that means id in request body is incorrect
	if !contactExists {
		log.Errorf("deleteContactPoint: contact does not exist")
		return errors.New("contact does not exist")
	}

	var contact alertutils.Contact
	if err := p.db.Model(&contact).Where("contact_id = ?", contact_id).Delete(contact).Error; err != nil {
		log.Errorf("deleteContactPoint: unable to delete contact: %v, err: %+v", contact_id, err)

		return err
	}

	return nil
}

// update last_sent_time in notification_details table
func (p Sqlite) UpdateLastSentTime(alert_id string) error {
	currentTime := time.Now().UTC()
	if err := p.db.Model(&alertutils.Notification{}).Where("alert_id = ?", alert_id).Update("last_sent_time", currentTime).Error; err != nil {
		log.Errorf("updateLastSentTime: unable to UpdateLastSentTime, err: %+v", err)
		return err
	}
	return nil
}

func (p Sqlite) UpdateAlertStateByAlertID(alert_id string, alertState alertutils.AlertState) error {
	if !isValid(alert_id) {
		log.Errorf("UpdateAlertStateByAlertID: data validation check failed")
		return errors.New("UpdateAlertStateByAlertID: data validation check failed")
	}
	alertExists, _, err := p.verifyAlertExists(alert_id)
	if err != nil {
		log.Errorf("UpdateAlertStateByAlertID: unable to verify if alert name exists, err: %+v", err)
		return err
	}
	if !alertExists {
		log.Errorf("UpdateAlertStateByAlertID: alert does not exist")
		return errors.New("alert does not exist")
	}

	if err := p.db.Model(&alertutils.AlertDetails{}).Where("alert_id = ?", alert_id).Update("state", alertState).Error; err != nil {
		log.Errorf("UpdateAlertStateByAlertID: unable to update alert state, with alert id: %v, err: %+v", alert_id, err)
		return err
	}
	return nil
}

func (p Sqlite) GetEmailAndChannelID(contact_id string) ([]string, []alertutils.SlackTokenConfig, []string, error) {

	var contact = &alertutils.Contact{}
	if err := p.db.Model(&contact).Where("contact_id = ?", contact_id).Error; err != nil {
		log.Errorf("GetEmailAndChannelID: unable to update contact, err: %+v", err)
		return nil, nil, nil, err
	}
	emailArray := contact.Email
	slackArray := contact.Slack
	webhookArray := contact.Webhook

	return emailArray, slackArray, webhookArray, nil
}

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
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"

	"github.com/siglens/siglens/pkg/alerts/alertutils"
	log "github.com/sirupsen/logrus"
)

type Sqlite struct {
	db  *sql.DB
	ctx context.Context
}

const createDBQuery = `ATTACH DATABASE 'siglens.db' AS siglens;`

const allAlertsTableQuery = `CREATE TABLE IF NOT EXISTS siglens.all_alerts (
	alert_id TEXT NOT NULL PRIMARY KEY UNIQUE,
	alert_name TEXT NOT NULL UNIQUE,
	query_params JSONB NOT NULL,
	condition INT NOT NULL,
	value FLOAT NOT NULL,
	eval_for INT NOT NULL,
	eval_interval INT,
	state INT NOT NULL,
	create_timestamp TIMESTAMP NOT NULL,
	message TEXT,
	contact_id TEXT NOT NULL,
	contact_name TEXT NOT NULL,
	cron_job JSONB,
	labels JSONB,
	node_id INT,
	silence_minutes INT DEFAULT 0
  );`

const minionSearchesTableQuery = `CREATE TABLE IF NOT EXISTS siglens.minion_searches (
	alert_id TEXT NOT NULL PRIMARY KEY UNIQUE,
	alert_name TEXT NOT NULL UNIQUE,
	minion_search_details JSONB NOT NULL,
	query_params JSONB NOT NULL,
	condition INT NOT NULL,
	value1 FLOAT NOT NULL,
	value2 FLOAT,
	eval_for INT NOT NULL,
	eval_interval INT,
	state INT NOT NULL,
	create_timestamp TIMESTAMP NOT NULL,
	message TEXT,
	contact_id TEXT NOT NULL,
	contact_name TEXT NOT NULL,
	cron_job JSONB,
	node_id INT
  );`

const allContactsTableQuery = `CREATE TABLE IF NOT EXISTS siglens.all_contacts (
	contact_id TEXT NOT NULL UNIQUE,
	contact_name TEXT NOT NULL PRIMARY KEY UNIQUE,
	email JSONB,
	slack JSONB,
	pager_duty TEXT,
	webhook JSONB
  );`

const allNotifsTableQuery = `CREATE TABLE IF NOT EXISTS siglens.notification_details (
	alert_id TEXT NOT NULL PRIMARY KEY,
	cooldown_period INT NOT NULL,
	last_sent_time TIMESTAMP,
	FOREIGN KEY (alert_id) REFERENCES all_alerts(alert_id) ON DELETE CASCADE
  );`

func (p *Sqlite) SetDB(dbConnection *sql.DB) {
	p.db = dbConnection
}

func (p *Sqlite) Connect() error {
	dbname := "siglens.db"
	dbConnection, err := sql.Open("sqlite3", dbname)
	if err != nil {
		log.Errorf("connectAlertDB: error in opening sqlite connection, err: %+v", err)
		return err
	}

	p.SetDB(dbConnection)
	err = p.db.Ping()
	if err != nil {
		log.Errorf("connectAlertDB: error in pinging sqlite connection, err: %+v", err)
		return err
	}
	p.ctx = context.Background()
	return nil
}

func (p *Sqlite) InitializeDB() error {
	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("initializeDB: unable to begin transaction, err: %+v", err)
		return err
	}
	_, err = tx.ExecContext(p.ctx, createDBQuery)
	if err != nil {
		log.Errorf("initializeDB: unable to execute query: %v, err: %+v", createDBQuery, err)
		_ = tx.Rollback()
		return err
	}
	_, err = tx.ExecContext(p.ctx, allAlertsTableQuery)
	if err != nil {
		log.Errorf("initializeDB: unable to execute query: %v, err: %+v", allAlertsTableQuery, err)
		_ = tx.Rollback()
		return err
	}
	_, err = tx.ExecContext(p.ctx, allContactsTableQuery)
	if err != nil {
		log.Errorf("initializeDB: unable to execute query: %v, err: %+v", allContactsTableQuery, err)
		_ = tx.Rollback()
		return err
	}
	_, err = tx.ExecContext(p.ctx, allNotifsTableQuery)
	if err != nil {
		log.Errorf("initializeDB: unable to execute query: %v, err: %+v", allNotifsTableQuery, err)
		_ = tx.Rollback()
		return err
	}
	_, err = tx.ExecContext(p.ctx, minionSearchesTableQuery)
	if err != nil {
		log.Errorf("initializeDB: unable to execute query: %v, err: %+v", minionSearchesTableQuery, err)
		_ = tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		log.Errorf("initializeDB: unable to execute transaction, err: %+v", err)
		return err
	}
	return nil
}

func (p *Sqlite) CloseDb() {
	p.db.Close()
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
	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("isNewAlertName: unable to begin transaction, err: %+v", err)
		return false, err
	}
	sqlStatement := "SELECT EXISTS (SELECT * FROM all_alerts WHERE alert_name=$1);"
	row := tx.QueryRow(sqlStatement, alertName)
	var alertNameFound bool
	err = row.Scan(&alertNameFound)
	if err != nil {
		log.Errorf("isNewAlertName: unable to execute query: %v, with parameters: %v, err: %+v", sqlStatement, alertName, err)
		_ = tx.Rollback()
		return true, err
	}
	err = tx.Commit()
	if err != nil {
		log.Errorf("isNewAlertName: unable to execute transaction, err: %+v", err)
		return false, err
	}
	// if alertNameFound in database, alert name is not new
	return !alertNameFound, nil
}

// checks based on alert_id, returns true and alert_name if alert exists
func (p Sqlite) verifyAlertExists(alert_id string) (bool, string, error) {
	if !isValid(alert_id) {
		log.Errorf("verifyAlertExists: data validation check failed %v", alert_id)
		return false, "", errors.New("verifyAlertExists: data validation check failed")
	}
	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("verifyAlertExists: unable to begin transaction, err: %+v", err)
		return false, "", err
	}
	sqlStatement := "SELECT EXISTS (SELECT * FROM all_alerts WHERE alert_id=$1);"
	row := tx.QueryRow(sqlStatement, alert_id)
	var alertExists bool
	err = row.Scan(&alertExists)
	if err != nil {
		log.Errorf("verifyAlertExists: unable to execute query: %v, with parameters: %v, err: %+v", sqlStatement, alert_id, err)
		_ = tx.Rollback()
		return false, "", err
	}
	sqlStatement = "SELECT alert_name FROM all_alerts WHERE alert_id=$1;"
	row = tx.QueryRow(sqlStatement, alert_id)
	var alert_name string
	err = row.Scan(&alert_name)
	if err != nil {
		log.Errorf("verifyAlertExists: unable to execute query: %v, with parameters: %v, err: %+v", sqlStatement, alert_id, err)
		_ = tx.Rollback()
		return false, "", err
	}

	err = tx.Commit()
	if err != nil {
		log.Errorf("verifyAlertExists: unable to execute transaction, err: %+v", err)
		return false, "", err
	}
	return alertExists, alert_name, nil
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
	if !isValid(alertDetails.AlertInfo.AlertName) {
		log.Errorf("createAlert: data validation check failed")
		return alertutils.AlertDetails{}, errors.New("createAlert: data validation check failed")
	}
	isNewAlertName, err := p.isNewAlertName(alertDetails.AlertInfo.AlertName)
	if err != nil {
		log.Errorf("createAlert: unable to verify if alert name is new, err: %+v", err)
		return alertutils.AlertDetails{}, err
	}
	if !isNewAlertName {
		log.Errorf("createAlert: alert name already exists")
		return alertutils.AlertDetails{}, errors.New("alert name already exists")
	}
	alert_id := CreateUniqId()
	create_timestamp := time.Now()
	state := alertutils.Inactive
	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("createAlert: unable to begin transaction, err: %+v", err)
		return alertutils.AlertDetails{}, err
	}
	queryParamsJSON, err := json.Marshal(alertDetails.QueryParams)
	if err != nil {
		log.Errorf("createAlert: unable to marshal queryParams to JSON, err: %+v", err)
		return alertutils.AlertDetails{}, err
	}
	labelJson, err := json.Marshal(alertDetails.AlertInfo.Labels)
	if err != nil {
		log.Errorf("createAlert: unable to marshal Labels to JSON, err: %+v", err)
		return alertutils.AlertDetails{}, err
	}
	sqlStatement := "INSERT INTO all_alerts(alert_name, alert_id, query_params, condition, value, eval_for, eval_interval, state, create_timestamp, message, contact_id, contact_name, labels) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);"
	_, err = tx.ExecContext(p.ctx, sqlStatement, alertDetails.AlertInfo.AlertName, alert_id, queryParamsJSON, alertDetails.Condition, alertDetails.Value, alertDetails.EvalFor, alertDetails.EvalInterval, state, create_timestamp, alertDetails.Message, alertDetails.AlertInfo.ContactId, alertDetails.AlertInfo.ContactName, labelJson)
	if err != nil {
		log.Errorf("createAlert: unable to execute query: %v, with parameters: %v %+v %v %v, err: %v ", sqlStatement, alert_id, alertDetails, state, create_timestamp, err)
		_ = tx.Rollback()
		return alertutils.AlertDetails{}, err
	}
	err = tx.Commit()
	if err != nil {
		log.Errorf("createAlert: unable to execute transaction, err: %+v", err)
		return alertutils.AlertDetails{}, err
	}
	alertDetails.AlertInfo.AlertId = alert_id

	var notification alertutils.Notification
	notification.CooldownPeriod = 0
	notification.AlertId = alert_id
	err = p.CreateNotificationDetails(&notification)
	if err != nil {
		log.Errorf("createAlert: unable to update notification details, err: %+v", err)
		return alertutils.AlertDetails{}, err
	}

	return *alertDetails, nil
}

func (p Sqlite) GetAlert(alert_id string) (*alertutils.AlertDetails, error) {
	if !isValid(alert_id) {
		log.Errorf("getAlert: data validation check failed")
		return nil, errors.New("getAlert: data validation check failed")
	}
	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("getAlert: unable to begin transaction, err: %+v", err)
		return nil, err
	}
	var (
		alert_name       string
		queryParams      string
		condition        alertutils.AlertQueryCondition
		value            float32
		eval_for         uint64
		eval_interval    uint64
		state            alertutils.AlertState
		create_timestamp time.Time
		message          string
		contact_id       string
		contact_name     string
		labels           []byte
	)

	sqlStatement := "SELECT alert_name, query_params,  condition, value, eval_for, eval_interval, state, create_timestamp, message, contact_id, contact_name, labels FROM all_alerts WHERE alert_id=$1;"
	row := tx.QueryRow(sqlStatement, alert_id)
	err = row.Scan(&alert_name, &queryParams, &condition, &value, &eval_for, &eval_interval, &state, &create_timestamp, &message, &contact_id, &contact_name, &labels)
	if err != nil {
		log.Errorf("getAlert: unable to execute query: %v, err: %+v", sqlStatement, err)
		_ = tx.Rollback()
		return nil, err
	}

	var queryParamsStruct alertutils.QueryParams
	err = json.Unmarshal([]byte(queryParams), &queryParamsStruct)
	if err != nil {
		log.Errorf("GetAlert: unable to unmarshal queryParams: %v", err)
		_ = tx.Rollback()
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		log.Errorf("getAlert: unable to execute transaction, err: %+v", err)
		return nil, err
	}
	var alertLabels []alertutils.AlertLabel
	err = json.Unmarshal(labels, &alertLabels)
	if err != nil {
		log.Errorf("getAlert: unable to Unmarshal alertLabels, err: %+v", err)
		return nil, err
	}
	alertInfoObj := alertutils.AlertInfo{
		AlertName:       alert_name,
		AlertId:         alert_id,
		State:           state,
		CreateTimestamp: create_timestamp,
		ContactId:       contact_id,
		ContactName:     contact_name,
		Labels:          alertLabels,
	}

	return &alertutils.AlertDetails{AlertInfo: alertInfoObj, QueryParams: queryParamsStruct,
		Condition: condition, Value: value,
		EvalFor: eval_for, EvalInterval: eval_interval, Message: message}, nil
}

func (p Sqlite) GetAllAlerts() ([]alertutils.AlertInfo, error) {
	var alerts []alertutils.AlertInfo
	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("getAllAlerts: unable to begin transaction, err: %+v", err)
		return nil, err
	}
	sqlSta := "ALTER TABLE all_alerts ADD COLUMN silence_minutes INTEGER DEFAULT 0;"
	_, errorAlter := tx.Exec(sqlSta)
	if err != nil {
		log.Errorf("getAllAlerts: Error adding column: %v, err: %+v", sqlSta, errorAlter)
		return nil, err
	}

	sqlStatement := "SELECT alert_id, alert_name, state, create_timestamp, contact_id, labels, silence_minutes FROM all_alerts;"
	rows, err := tx.Query(sqlStatement)
	if err != nil {
		log.Errorf("getAllAlerts: unable to execute query: %v, err: %+v", sqlStatement, err)
		_ = tx.Rollback()
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			alert_id         string
			alert_name       string
			state            alertutils.AlertState
			create_timestamp time.Time
			contact_id       string
			labels           []byte
			silence_minutes  uint64
		)
		err := rows.Scan(&alert_id, &alert_name, &state, &create_timestamp, &contact_id, &labels, &silence_minutes)
		if err != nil {
			log.Errorf("getAllAlerts: uanble to scan row: %+v", err)
			_ = tx.Rollback()
			return nil, err
		}
		var labels_array []alertutils.AlertLabel
		err = json.Unmarshal(labels, &labels_array)
		if err != nil {
			log.Errorf("getAllAlerts: unable to unmarshal labels: %+v", err)
			_ = tx.Rollback()
			return nil, err
		}
		alerts = append(alerts, alertutils.AlertInfo{AlertId: alert_id, AlertName: alert_name, State: state, CreateTimestamp: create_timestamp, ContactId: contact_id, Labels: labels_array, SilenceMinutes: silence_minutes})
	}
	err = tx.Commit()
	if err != nil {
		log.Errorf("getAllAlerts: unable to execute transaction, err: %+v", err)
		return nil, err
	}
	return alerts, nil
}
func (p Sqlite) UpdateSilenceMinutes(updatedSilenceMinutes *alertutils.AlertDetails) error {
	if !isValid(updatedSilenceMinutes.AlertInfo.AlertName) || !isValid(updatedSilenceMinutes.AlertInfo.ContactName) || !isValid(updatedSilenceMinutes.QueryParams.QueryText) {
		log.Errorf("updateSilenceMinutes: data validation check failed")
		return errors.New("updateSilenceMinutesupdateSilenceMinutes: data validation check failed")
	}
	alertExists, _, err := p.verifyAlertExists(updatedSilenceMinutes.AlertInfo.AlertId)
	if err != nil {
		log.Errorf("updateSilenceMinutes: unable to verify if alert exists, err: %+v", err)
		return err
	}
	if !alertExists {
		log.Errorf("updateSilenceMinutes: alert does not exist")
		return errors.New("alert does not exist")
	}

	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("updateSilenceMinutes: unable to begin transaction, err: %+v", err)
		return err
	}
	sqlStatement := "UPDATE all_alerts SET silence_minutes=$1 WHERE alert_id=$2;"
	_, err = tx.ExecContext(p.ctx, sqlStatement, updatedSilenceMinutes.AlertInfo.SilenceMinutes, updatedSilenceMinutes.AlertInfo.AlertId)
	if err != nil {
		log.Errorf("updateSilenceMinutes: unable to execute query: %v, with alert name: %v, err: %+v", sqlStatement, updatedSilenceMinutes.AlertInfo.AlertName, err)
		_ = tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		log.Errorf("updateSilenceMinutes: unable to execute transaction, err: %+v", err)
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
	if !isValid(editedAlert.AlertInfo.AlertName) || !isValid(editedAlert.AlertInfo.ContactName) || !isValid(editedAlert.QueryParams.QueryText) {
		log.Errorf("updateAlert: data validation check failed")
		return errors.New("updateAlert: data validation check failed")
	}
	alertExists, alertName, err := p.verifyAlertExists(editedAlert.AlertInfo.AlertId)
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
	if alertName != editedAlert.AlertInfo.AlertName {
		isNewAlertName, err := p.isNewAlertName(editedAlert.AlertInfo.AlertName)
		if err != nil {
			log.Errorf("updateAlert: unable to verify if alert name is new, err: %+v", err)
			return err
		}
		if !isNewAlertName {
			log.Errorf("updateAlert: alert name already exists")
			return errors.New("alert name already exists")
		}
	}

	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("updateAlert: unable to begin transaction, err: %+v", err)
		return err
	}
	queryParamsJSON, err := json.Marshal(editedAlert.QueryParams)
	if err != nil {
		log.Errorf("UpdateAlert: unable to marshal queryParams to JSON, err: %+v", err)
		return err
	}
	labelJson, err := json.Marshal(editedAlert.AlertInfo.Labels)
	if err != nil {
		log.Errorf("createAlert: unable to marshal Labels to JSON, err: %+v", err)
		return err
	}
	sqlStatement := "UPDATE all_alerts SET alert_name=$1, query_params=$2, condition=$3, value=$4, eval_for=$5, eval_interval=$6, message=$7, contact_id=$8, contact_name=$9, labels=$10 WHERE alert_id=$11;"
	_, err = tx.ExecContext(p.ctx, sqlStatement, editedAlert.AlertInfo.AlertName, queryParamsJSON, editedAlert.Condition, editedAlert.Value, editedAlert.EvalFor, editedAlert.EvalInterval, editedAlert.Message, editedAlert.AlertInfo.ContactId, editedAlert.AlertInfo.ContactName, labelJson, editedAlert.AlertInfo.AlertId)
	if err != nil {
		log.Errorf("updateAlert: unable to execute query: %v, with alert name: %v, err: %+v", sqlStatement, editedAlert.AlertInfo.AlertName, err)
		_ = tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		log.Errorf("updateAlert: unable to execute transaction, err: %+v", err)
		return err
	}
	return nil
}

func (p Sqlite) DeleteAlert(alert_id string) error {
	if !isValid(alert_id) {
		log.Errorf("deleteAlert: data validation check failed")
		return errors.New("deleteAlert: data validation check failed")
	}
	alertExists, _, err := p.verifyAlertExists(alert_id)
	if err != nil {
		log.Errorf("deleteAlert: unable to verify if alert exists, err: %+v", err)
		return err
	}
	if !alertExists {
		log.Errorf("deleteAlert: alert does not exist")
		return errors.New("alert does not exist")
	}

	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("deleteAlert: unable to begin transaction, err: %+v", err)
		return err
	}

	sqlStatement := "DELETE FROM all_alerts WHERE alert_id=$1;"
	_, err = tx.ExecContext(p.ctx, sqlStatement, alert_id)
	if err != nil {
		log.Errorf("deleteAlert: unable to execute query: %v, with parameters: %v, err: %+v", sqlStatement, alert_id, err)
		_ = tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Errorf("deleteAlert: unable to execute transaction, err: %+v", err)
		return err
	}
	return nil
}

func (p Sqlite) CreateContact(newContact *alertutils.Contact) error {
	isNewContactName, err := p.isNewContactName(newContact.ContactName)
	if err != nil {
		log.Errorf("createContact: unable to verify if contact name is new, err: %+v", err)
		return err
	}
	if !isNewContactName {
		log.Errorf("createContact: contact name already exists")
		return errors.New("contact name already exists")
	} else {
		contact_id := CreateUniqId()
		newContact.ContactId = contact_id
		emailJSON, err := json.Marshal(newContact.Email)
		if err != nil {
			log.Errorf("createContact: unable to marshal email to JSON, err: %+v", err)
			return err
		}
		if len(newContact.Email) == 0 && len(newContact.Slack) == 0 && len(newContact.Webhook) == 0 {
			log.Errorf("createContact: Please set contact email / Slack channel")
			return errors.New("Please set contact email / Slack channel")
		}
		slackJSON, err := json.Marshal(newContact.Slack)
		if err != nil {
			log.Errorf("createContact: unable to marshal slack to JSON, err: %+v", err)
			return err
		}
		webhookJSON, err := json.Marshal(newContact.Webhook)
		if err != nil {
			log.Errorf("createContact: unable to marshal webhook to JSON, err: %+v", err)
			return err
		}
		tx, err := p.db.BeginTx(p.ctx, nil)
		if err != nil {
			log.Errorf("createContact: unable to begin transaction, err: %+v", err)
			return err
		}
		sqlStatement := "INSERT INTO all_contacts(contact_name, contact_id, email, slack, pager_duty, webhook) VALUES($1, $2, $3::jsonb, $4::jsonb, $5, $6::jsonb);"
		_, err = tx.ExecContext(p.ctx, sqlStatement, newContact.ContactName, newContact.ContactId, emailJSON, slackJSON, newContact.PagerDuty, webhookJSON)
		if err != nil {
			log.Errorf("createContact: unable to execute query: %v, with parameters: %v %v %v %v %v, err: %+v", sqlStatement, newContact.ContactName, newContact.ContactId, newContact.Email, newContact.Slack, newContact.PagerDuty, err)
			_ = tx.Rollback()
			return err
		}
		err = tx.Commit()
		if err != nil {
			log.Errorf("createContact: unable to execute transaction, err: %+v", err)
			return err
		}
	}
	return nil
}

func (p Sqlite) isNewContactName(contact_name string) (bool, error) {
	if !isValid(contact_name) {
		log.Errorf("isNewContactName: data validation check failed")
		return false, errors.New("isNewContactName: data validation check failed")
	}
	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("isNewContactName: unable to begin transaction, err: %+v", err)
		return false, err
	}
	sqlStatement := "SELECT EXISTS (SELECT * FROM all_contacts WHERE contact_name=$1);"
	row := tx.QueryRow(sqlStatement, contact_name)
	var contactNameFound bool
	err = row.Scan(&contactNameFound)
	if err != nil {
		log.Errorf("isNewContactName: unable to execute query: %v, with parameters: %v, err: %+v", sqlStatement, contact_name, err)
		_ = tx.Rollback()
		return true, err
	}
	err = tx.Commit()
	if err != nil {
		log.Errorf("isNewContactName: unable to execute transaction, err: %+v", err)
		return false, err
	}
	// if contactName not found in database, given contactName is new
	return !contactNameFound, nil
}

// checks based on contact_id, returns true and contact_name if contact exists
func (p Sqlite) verifyContactExists(contact_id string) (bool, error) {
	if !isValid(contact_id) {
		log.Errorf("verifyContactExists: data validation check failed")
		return false, errors.New("verifyContactExists: data validation check failed")
	}
	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("verifyContactExists: unable to begin transaction, err: %+v", err)
		return false, err
	}
	sqlStatement := "SELECT EXISTS (SELECT * FROM all_contacts WHERE contact_id=$1);"
	row := tx.QueryRow(sqlStatement, contact_id)
	var contactExists bool
	err = row.Scan(&contactExists)
	if err != nil {
		log.Errorf("verifyContactExists: unable to execute query: %v, with parameters: %v, err: %+v", sqlStatement, contact_id, err)
		_ = tx.Rollback()
		return false, err
	}
	_ = tx.Rollback()
	return contactExists, nil
}

func (p Sqlite) CreateNotificationDetails(newNotif *alertutils.Notification) error {
	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("createNotificationDetails: unable to begin transaction, err: %+v", err)
		return err
	}
	sqlStatement := "INSERT INTO notification_details(alert_id, cooldown_period, last_sent_time) VALUES($1, $2, $3);"
	_, err = tx.ExecContext(p.ctx, sqlStatement, newNotif.AlertId, newNotif.CooldownPeriod, newNotif.LastSentTime)
	if err != nil {
		log.Errorf("createNotificationDetails: unable to execute query: %v, with parameters: %v %v %v, err: %+v", sqlStatement, newNotif.AlertId, newNotif.CooldownPeriod, newNotif.LastSentTime, err)
		_ = tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		log.Errorf("createNotificationDetails: unable to execute transaction, err: %+v", err)
		return err
	}
	return nil
}

func (p Sqlite) GetAllContactPoints() ([]alertutils.Contact, error) {
	var contacts []alertutils.Contact
	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("getAllContactPoints: unable to begin transaction, err: %+v", err)
		return nil, err
	}
	sqlStatement := "SELECT contact_id, contact_name, email, slack, pager_duty,webhook FROM all_contacts;"
	rows, err := tx.Query(sqlStatement)
	if err != nil {
		log.Errorf("getAllContactPoints: unable to execute query: %v, err: %+v", sqlStatement, err)
		_ = tx.Rollback()
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			contact_id   string
			contact_name string
			email        []byte
			slack        []byte
			webhook      []byte
			pager_duty   string
		)
		err := rows.Scan(&contact_id, &contact_name, &email, &slack, &pager_duty, &webhook)
		if err != nil {
			log.Errorf("getAllContactPoints: unable to scan row: %+v", err)
			_ = tx.Rollback()
			return nil, err
		}
		var emailArray []string
		err = json.Unmarshal(email, &emailArray)
		if err != nil {
			log.Errorf("getAllContactPoints: unable to unmarshal email: %+v", err)
			_ = tx.Rollback()
			return nil, err
		}
		var slackArray []alertutils.SlackTokenConfig
		err = json.Unmarshal(slack, &slackArray)
		if err != nil {
			log.Errorf("getAllContactPoints: unable to unmarshal slack: %+v", err)
			_ = tx.Rollback()
			return nil, err
		}
		var webhookArray []string
		err = json.Unmarshal(webhook, &webhookArray)
		if err != nil {
			log.Errorf("getAllContactPoints: unable to unmarshal webhook: %+v", err)
			_ = tx.Rollback()
			return nil, err
		}
		contacts = append(contacts, alertutils.Contact{ContactName: contact_name, ContactId: contact_id, Email: emailArray, Slack: slackArray, PagerDuty: pager_duty, Webhook: webhookArray})
	}
	err = tx.Commit()
	if err != nil {
		log.Errorf("getAllContactPoints: unable to execute transaction, err: %+v", err)
		return nil, err
	}
	return contacts, nil
}

// functions used by notification system

func (p Sqlite) GetCoolDownDetails(alert_id string) (uint64, time.Time, error) {
	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("GetCoolDownDetails: unable to begin transaction, err: %+v", err)
		return 0, time.Time{}, err
	}

	var cooldown_period uint64
	var last_sent_time time.Time

	sqlStatement := "SELECT cooldown_period, last_sent_time FROM notification_details WHERE alert_id=$1;"
	row := tx.QueryRow(sqlStatement, alert_id)
	err = row.Scan(&cooldown_period, &last_sent_time)
	if err != nil {
		log.Errorf("GetCoolDownDetails: unable to execute query: %v, err: %+v", sqlStatement, err)
		_ = tx.Rollback()
		return 0, time.Time{}, err
	}

	err = tx.Commit()
	if err != nil {
		log.Errorf("GetCoolDownDetails: unable to execute transaction, err: %+v", err)
		return 0, time.Time{}, err
	}

	return cooldown_period, last_sent_time, nil
}

// get contact_id and message from all_alerts table using alert_id
func (p Sqlite) GetContactDetails(alert_id string) (string, string, string, error) {
	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("getContactDetails: unable to begin transaction, err: %+v", err)
		return "", "", "", err
	}

	var contact_id string
	var message string
	var alert_name string
	var queryParams string
	var condition int
	var value float32

	sqlStatement := "SELECT contact_id, message, query_params, alert_name, condition, value FROM all_alerts WHERE alert_id=$1;"
	row := tx.QueryRow(sqlStatement, alert_id)
	err = row.Scan(&contact_id, &message, &queryParams, &alert_name, &condition, &value)
	if err != nil {
		log.Errorf("getContactDetails: unable to execute query: %v, err: %+v", sqlStatement, err)
		_ = tx.Rollback()
		return "", "", "", err
	}

	err = tx.Commit()
	if err != nil {
		log.Errorf("getContactDetails: unable to execute transaction, err: %+v", err)
		return "", "", "", err
	}

	var queryParamsStruct alertutils.QueryParams
	err = json.Unmarshal([]byte(queryParams), &queryParamsStruct)
	if err != nil {
		log.Errorf("GetAlert: unable to unmarshal queryParams: %v", err)
		_ = tx.Rollback()
		return "", "", "", err
	}
	newMessage := strings.ReplaceAll(message, "{{alert_rule_name}}", alert_name)
	newMessage = strings.ReplaceAll(newMessage, "{{query_string}}", queryParamsStruct.QueryText)
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
	newMessage = strings.ReplaceAll(newMessage, "{{queryLanguage}}", queryParamsStruct.QueryLanguage)
	return contact_id, newMessage, alert_name, nil
}

// get all emails and slack channelIDs from all_contacts table using contact_id
func (p Sqlite) GetEmailAndChannelID(contact_id string) ([]string, []alertutils.SlackTokenConfig, []string, error) {
	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("GetEmailAndChannelID: unable to begin transaction, err: %+v", err)
		return nil, nil, nil, err
	}

	var email, slack, webhook []byte
	sqlStatement := "SELECT email, slack, webhook FROM all_contacts WHERE contact_id=$1;"

	row := tx.QueryRow(sqlStatement, contact_id)
	err = row.Scan(&email, &slack, &webhook)
	if err != nil {
		log.Errorf("GetEmailAndChannelID: unable to execute query: %v, err: %+v", sqlStatement, err)
		_ = tx.Rollback()
		return nil, nil, nil, err
	}
	var emailArray []string
	err = json.Unmarshal(email, &emailArray)
	if err != nil {
		log.Errorf("GetEmailAndChannelID: unable to unmarshal email: %+v", err)
		_ = tx.Rollback()
		return nil, nil, nil, err
	}
	var slackArray []alertutils.SlackTokenConfig
	err = json.Unmarshal(slack, &slackArray)
	if err != nil {
		log.Errorf("GetEmailAndChannelID: unable to unmarshal slack: %+v", err)
		_ = tx.Rollback()
		return nil, nil, nil, err
	}
	var webhookArray []string
	err = json.Unmarshal(webhook, &webhookArray)
	if err != nil {
		log.Errorf("GetEmailAndChannelID: unable to unmarshal webhook: %+v", err)
		_ = tx.Rollback()
		return nil, nil, nil, err
	}

	err = tx.Commit()
	if err != nil {
		log.Errorf("GetEmailAndChannelID: unable to execute transaction, err: %+v", err)
		return nil, nil, nil, err
	}
	return emailArray, slackArray, webhookArray, nil
}

// update last_sent_time in notification_details table
func (p Sqlite) UpdateLastSentTime(alert_id string) error {

	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("updateLastSentTime: unable to begin transaction, err: %+v", err)
		return err
	}
	currentTime := time.Now().UTC()
	sqlStatement := "UPDATE notification_details SET last_sent_time = $1 WHERE alert_id = $2;"
	_, err = tx.ExecContext(p.ctx, sqlStatement, currentTime, alert_id)
	if err != nil {
		log.Errorf("updateLastSentTime: unable to execute query: %v, err: %+v", sqlStatement, err)
		_ = tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		log.Errorf("updateLastSentTime: unable to execute transaction, err: %+v", err)
		return err
	}
	return nil
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

	emailJSON, err := json.Marshal(contact.Email)
	if err != nil {
		log.Fatal(err)
	}
	slackJSON, err := json.Marshal(contact.Slack)
	if err != nil {
		log.Fatal(err)
	}
	webhookJSON, err := json.Marshal(contact.Webhook)
	if err != nil {
		log.Fatal(err)
	}
	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("updateContactPoint: unable to begin transaction, err: %+v", err)
		return err
	}
	sqlStatement := "UPDATE all_contacts SET contact_name=$1, email=$2, slack=$3, pager_duty=$4, webhook=$5 WHERE contact_id=$6;"
	_, err = tx.ExecContext(p.ctx, sqlStatement, contact.ContactName, emailJSON, slackJSON, contact.PagerDuty, webhookJSON, contact.ContactId)
	if err != nil {
		log.Errorf("updateContactPoint: unable to execute query: %v, with contact name: %v, err: %+v", sqlStatement, contact.ContactName, err)
		_ = tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		log.Errorf("updateContactPoint: unable to execute transaction, err: %+v", err)
		return err
	}
	return nil
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

	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("deleteContactPoint: unable to begin transaction, err: %+v", err)
		return err
	}

	sqlStatement := "DELETE FROM all_contacts WHERE contact_id=$1;"
	_, err = tx.ExecContext(p.ctx, sqlStatement, contact_id)
	if err != nil {
		log.Errorf("deleteContactPoint: unable to execute query: %v, with parameters: %v, err: %+v", sqlStatement, contact_id, err)
		_ = tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Errorf("deleteContactPoint: unable to execute transaction, err: %+v", err)
		return err
	}
	return nil
}

func (p Sqlite) UpdateAlertStateByAlertID(alertId string, alertState alertutils.AlertState) error {
	if !isValid(alertId) {
		log.Errorf("UpdateAlertStateByAlertID: data validation check failed")
		return errors.New("UpdateAlertStateByAlertID: data validation check failed")
	}
	alertExists, _, err := p.verifyAlertExists(alertId)
	if err != nil {
		log.Errorf("UpdateAlertStateByAlertID: unable to verify if alert name exists, err: %+v", err)
		return err
	}
	if !alertExists {
		log.Errorf("UpdateAlertStateByAlertID: alert does not exist")
		return errors.New("alert does not exist")
	}

	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("UpdateAlertStateByAlertID: unable to begin transaction, err: %+v", err)
		return err
	}
	sqlStatement := "UPDATE all_alerts SET state=$1 WHERE alert_id=$2;"
	_, err = tx.ExecContext(p.ctx, sqlStatement, alertState, alertId)
	if err != nil {
		log.Errorf("UpdateAlertStateByAlertID: unable to execute query: %v, with alert id: %v, err: %+v", sqlStatement, alertId, err)
		_ = tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		log.Errorf("UpdateAlertStateByAlertID: unable to execute transaction, err: %+v", err)
		return err
	}
	return nil
}

// Creates a new record in all_alerts table
func (p Sqlite) CreateMinionSearch(minionSearchDetails *alertutils.MinionSearch) (alertutils.MinionSearch, error) {
	if !isValid(minionSearchDetails.AlertInfo.AlertName) {
		log.Errorf("CreateMinionSearch: data validation check failed")
		return alertutils.MinionSearch{}, errors.New("CreateMinionSearch: data validation check failed")
	}
	isNewAlertName, err := p.isNewAlertName(minionSearchDetails.AlertInfo.AlertName)
	if err != nil {
		log.Errorf("CreateMinionSearch: unable to verify if alert name is new, err: %+v", err)
		return alertutils.MinionSearch{}, err
	}
	if !isNewAlertName {
		log.Errorf("CreateMinionSearch: alert name already exists")
		return alertutils.MinionSearch{}, errors.New("alert name already exists")
	}
	create_timestamp := time.Now()
	state := alertutils.Inactive
	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("CreateMinionSearch: unable to begin transaction, err: %+v", err)
		return alertutils.MinionSearch{}, err
	}
	queryParamsJSON, err := json.Marshal(minionSearchDetails.QueryParams)
	if err != nil {
		log.Errorf("CreateMinionSearch: unable to marshal queryParams to JSON, err: %+v", err)
		return alertutils.MinionSearch{}, err
	}
	minionSearchDetailsJSON, err := json.Marshal(minionSearchDetails.MinionSearchDetails)
	if err != nil {
		log.Errorf("CreateMinionSearch: unable to marshal queryParams to JSON, err: %+v", err)
		return alertutils.MinionSearch{}, err
	}
	sqlStatement := "INSERT INTO minion_searches(alert_name, alert_id, query_params, minion_search_details, condition, value1, value2, eval_for, eval_interval, state, create_timestamp, message, contact_id, contact_name) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14);"
	_, err = tx.ExecContext(p.ctx, sqlStatement, minionSearchDetails.AlertInfo.AlertName, minionSearchDetails.AlertInfo.AlertId, queryParamsJSON, minionSearchDetailsJSON, minionSearchDetails.Condition, minionSearchDetails.Value1, minionSearchDetails.Value2, minionSearchDetails.EvalFor, minionSearchDetails.EvalInterval, state, create_timestamp, minionSearchDetails.Message, minionSearchDetails.AlertInfo.ContactId, minionSearchDetails.AlertInfo.ContactName)
	if err != nil {
		log.Errorf("CreateMinionSearch: unable to execute query: %v, with parameters: %v %+v %v %v, err: %v ", sqlStatement, minionSearchDetails.AlertInfo.AlertId, minionSearchDetails, state, create_timestamp, err)
		_ = tx.Rollback()
		return alertutils.MinionSearch{}, err
	}
	err = tx.Commit()
	if err != nil {
		log.Errorf("CreateMinionSearch: unable to execute transaction, err: %+v", err)
		return alertutils.MinionSearch{}, err
	}
	return *minionSearchDetails, nil
}

func (p Sqlite) GetAllMinionSearches() ([]alertutils.MinionSearch, error) {
	var alerts []alertutils.MinionSearch
	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("GetAllMinionSearches: unable to begin transaction, err: %+v", err)
		return nil, err
	}

	sqlStatement := "SELECT alert_id, alert_name, query_params, condition, value1, value2, eval_for, eval_interval, state, create_timestamp, message, contact_id, contact_name, minion_search_details FROM minion_searches;"
	rows, err := tx.Query(sqlStatement)
	if err != nil {
		log.Errorf("GetAllMinionSearches: unable to execute query: %v, err: %+v", sqlStatement, err)
		_ = tx.Rollback()
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			alert_id            string
			alert_name          string
			state               alertutils.AlertState
			create_timestamp    time.Time
			contact_id          string
			contact_name        string
			minionSearchDetails string
			queryParams         string
			condition           alertutils.AlertQueryCondition
			value1              float32
			value2              float32
			eval_for            uint64
			eval_interval       uint64
			message             string
		)

		err := rows.Scan(&alert_id, &alert_name, &queryParams, &condition, &value1, &value2, &eval_for, &eval_interval, &state, &create_timestamp, &message, &contact_id, &contact_name, &minionSearchDetails)
		if err != nil {
			log.Errorf("GetAllMinionSearches: uanble to scan row: %+v", err)
			_ = tx.Rollback()
			return nil, err
		}

		var minionSearchStruct alertutils.MinionSearchDetails
		err = json.Unmarshal([]byte(minionSearchDetails), &minionSearchStruct)
		if err != nil {
			log.Errorf("GetAllMinionSearches: unable to unmarshal MinionAlertDetails: %v", err)
			_ = tx.Rollback()
			return nil, err
		}
		var queryParamsStruct alertutils.QueryParams
		err = json.Unmarshal([]byte(queryParams), &queryParamsStruct)
		if err != nil {
			log.Errorf("GetAllMinionSearches: unable to unmarshal QueryParams: %v", err)
			_ = tx.Rollback()
			return nil, err
		}
		alertinfo := alertutils.AlertInfo{AlertId: alert_id, AlertName: alert_name, State: state,
			CreateTimestamp: create_timestamp, ContactId: contact_id, ContactName: contact_name}

		minion_search_details := alertutils.MinionSearchDetails{Repository: minionSearchStruct.Repository,
			Filename: minionSearchStruct.Filename, LineNumber: minionSearchStruct.LineNumber,
			LogText: minionSearchStruct.LogText, LogTextHash: minionSearchStruct.LogTextHash, LogLevel: minionSearchStruct.LogLevel}
		alerts = append(alerts, alertutils.MinionSearch{AlertInfo: alertinfo, MinionSearchDetails: minion_search_details,
			Condition: condition, Value1: value1, Value2: value2,
			EvalFor: eval_for, EvalInterval: eval_interval, Message: message,
			QueryParams: queryParamsStruct})
	}
	err = tx.Commit()
	if err != nil {
		log.Errorf("GetAllMinionSearches: unable to execute transaction, err: %+v", err)
		return nil, err
	}
	return alerts, nil

}

func (p Sqlite) GetMinionSearch(alert_id string) (*alertutils.MinionSearch, error) {
	if !isValid(alert_id) {
		log.Errorf("GetMinionSearch: data validation check failed")
		return nil, errors.New("GetMinionSearch: data validation check failed")
	}
	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("GetMinionSearch: unable to begin transaction, err: %+v", err)
		return nil, err
	}
	var (
		alert_name            string
		queryParams           string
		minionSearchDetails   string
		condition             alertutils.AlertQueryCondition
		value1                float32
		value2                float32
		eval_for              uint64
		eval_interval         uint64
		state                 alertutils.AlertState
		create_timestamp      time.Time
		message               string
		contact_id            string
		contact_name          string
		minion_search_details alertutils.MinionSearchDetails
	)
	sqlStatement := "SELECT alert_name, query_params, minion_search_details,  condition, value1, value2, eval_for, eval_interval, state, create_timestamp, message, contact_id, contact_name FROM minion_searches WHERE alert_id=$1;"
	row := tx.QueryRow(sqlStatement, alert_id)
	err = row.Scan(&alert_name, &queryParams, &minionSearchDetails, &condition, &value1, &value2, &eval_for, &eval_interval, &state, &create_timestamp, &message, &contact_id, &contact_name)
	if err != nil {
		log.Errorf("GetMinionSearch: unable to execute query: %v, err: %+v", sqlStatement, err)
		_ = tx.Rollback()
		return nil, err
	}

	var queryParamsStruct alertutils.QueryParams
	err = json.Unmarshal([]byte(queryParams), &queryParamsStruct)
	if err != nil {
		log.Errorf("GetMinionSearch: unable to unmarshal queryParams: %v", err)
		_ = tx.Rollback()
		return nil, err
	}
	var minionAlertStruct alertutils.MinionSearchDetails
	err = json.Unmarshal([]byte(minionSearchDetails), &minionAlertStruct)
	if err != nil {
		log.Errorf("GetMinionSearch: unable to unmarshal minion_search_details: %v", err)
		_ = tx.Rollback()
		return nil, err
	}

	minion_search_details = alertutils.MinionSearchDetails{Repository: minionAlertStruct.Repository,
		Filename: minionAlertStruct.Filename, LineNumber: minionAlertStruct.LineNumber,
		LogText: minionAlertStruct.LogText, LogTextHash: minionAlertStruct.LogTextHash, LogLevel: minionAlertStruct.LogLevel}

	err = tx.Commit()
	if err != nil {
		log.Errorf("GetMinionSearch: unable to execute transaction, err: %+v", err)
		return nil, err
	}

	alertInfoObj := alertutils.AlertInfo{
		AlertName:       alert_name,
		AlertId:         alert_id,
		State:           state,
		CreateTimestamp: create_timestamp,
		ContactId:       contact_id,
		ContactName:     contact_name,
	}
	return &alertutils.MinionSearch{AlertInfo: alertInfoObj, QueryParams: queryParamsStruct,
		MinionSearchDetails: minion_search_details, Condition: condition, Value1: value1, Value2: value2,
		EvalFor: eval_for, EvalInterval: eval_interval, Message: message}, nil
}

func (p Sqlite) UpdateMinionSearchStateByAlertID(alertId string, alertState alertutils.AlertState) error {
	if !isValid(alertId) {
		log.Errorf("UpdateMinionSearchStateByAlertID: data validation check failed")
		return errors.New("UpdateMinionSearchStateByAlertID: data validation check failed")
	}
	searchExists, _, err := p.verifyMinionSearchExists(alertId)
	if err != nil {
		log.Errorf("UpdateMinionSearchStateByAlertID: unable to verify if alert name exists, err: %+v", err)
		return err
	}
	if !searchExists {
		log.Errorf("UpdateMinionSearchStateByAlertID: alert does not exist")
		return errors.New("MinionSearch does not exist")
	}

	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("UpdateMinionSearchStateByAlertID: unable to begin transaction, err: %+v", err)
		return err
	}
	sqlStatement := "UPDATE minion_searches SET state=$1 WHERE alert_id=$2;"
	_, err = tx.ExecContext(p.ctx, sqlStatement, alertState, alertId)
	if err != nil {
		log.Errorf("UpdateMinionSearchStateByAlertID: unable to execute query: %v, with alert id: %v, err: %+v", sqlStatement, alertId, err)
		_ = tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		log.Errorf("UpdateMinionSearchStateByAlertID: unable to execute transaction, err: %+v", err)
		return err
	}
	return nil
}

func (p Sqlite) verifyMinionSearchExists(alert_id string) (bool, string, error) {
	if !isValid(alert_id) {
		log.Errorf("verifyMinionSearchExists: data validation check failed %v", alert_id)
		return false, "", errors.New("verifyMinionSearchExists: data validation check failed")
	}
	tx, err := p.db.BeginTx(p.ctx, nil)
	if err != nil {
		log.Errorf("verifyMinionSearchExists: unable to begin transaction, err: %+v", err)
		return false, "", err
	}
	sqlStatement := "SELECT EXISTS (SELECT * FROM minion_searches WHERE alert_id=$1);"
	row := tx.QueryRow(sqlStatement, alert_id)
	var searchExists bool
	err = row.Scan(&searchExists)
	if err != nil {
		log.Errorf("verifyMinionSearchExists: unable to execute query: %v, with parameters: %v, err: %+v", sqlStatement, alert_id, err)
		_ = tx.Rollback()
		return false, "", err
	}
	sqlStatement = "SELECT alert_name FROM minion_searches WHERE alert_id=$1;"
	row = tx.QueryRow(sqlStatement, alert_id)
	var alert_name string
	err = row.Scan(&alert_name)
	if err != nil {
		log.Errorf("verifyMinionSearchExists: unable to execute query: %v, with parameters: %v, err: %+v", sqlStatement, alert_id, err)
		_ = tx.Rollback()
		return false, "", err
	}

	err = tx.Commit()
	if err != nil {
		log.Errorf("verifyMinionSearchExists: unable to execute transaction, err: %+v", err)
		return false, "", err
	}
	return searchExists, alert_name, nil
}

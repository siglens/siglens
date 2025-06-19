// Copyright (c) 2021-2025 SigScalr, Inc.
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
	"testing"
	"time"

	"github.com/siglens/siglens/pkg/alerts/alertutils"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
	"gorm.io/gorm"
)

// mockDatabase implements the database interface for contact point methods only
type mockDatabase struct {
	contacts map[int64][]alertutils.Contact // org_id -> contacts
	nextID   int64
}

func newMockDatabase() *mockDatabase {
	return &mockDatabase{
		contacts: make(map[int64][]alertutils.Contact),
		nextID:   1,
	}
}

func (m *mockDatabase) CreateContact(contact *alertutils.Contact) error {
	if contact.ContactName == "fail" {
		return errors.New("forced create error")
	}
	contact.ContactId = fmt.Sprintf("%d", m.nextID)
	m.nextID++
	m.contacts[contact.OrgId] = append(m.contacts[contact.OrgId], *contact)
	return nil
}

func (m *mockDatabase) GetAllContactPoints(orgId int64) ([]alertutils.Contact, error) {
	return m.contacts[orgId], nil
}

func (m *mockDatabase) UpdateContactPoint(contact *alertutils.Contact) error {
	for org, contacts := range m.contacts {
		for i, c := range contacts {
			if c.ContactId == contact.ContactId {
				m.contacts[org][i] = *contact
				return nil
			}
		}
	}
	return errors.New("contact not found")
}

func (m *mockDatabase) DeleteContactPoint(contact_id string) error {
	for org, contacts := range m.contacts {
		for i, c := range contacts {
			if c.ContactId == contact_id {
				m.contacts[org] = append(contacts[:i], contacts[i+1:]...)
				return nil
			}
		}
	}
	return errors.New("contact not found")
}

// The following methods are not used in these tests
func (m *mockDatabase) Connect() error    { return nil }
func (m *mockDatabase) CloseDb()          {}
func (m *mockDatabase) SetDB(db *gorm.DB) {}
func (m *mockDatabase) CreateAlert(alertInfo *alertutils.AlertDetails) (alertutils.AlertDetails, error) {
	return alertutils.AlertDetails{}, nil
}
func (m *mockDatabase) GetAlert(alert_id string) (*alertutils.AlertDetails, error) { return nil, nil }
func (m *mockDatabase) CreateAlertHistory(alertHistoryDetails *alertutils.AlertHistoryDetails) (*alertutils.AlertHistoryDetails, error) {
	return nil, nil
}
func (m *mockDatabase) GetAlertHistoryByAlertID(alertHistoryParams *alertutils.AlertHistoryQueryParams) ([]*alertutils.AlertHistoryDetails, error) {
	return nil, nil
}
func (m *mockDatabase) GetAllAlerts(orgId int64) ([]*alertutils.AlertDetails, error) { return nil, nil }
func (m *mockDatabase) CreateMinionSearch(alertInfo *alertutils.MinionSearch) (alertutils.MinionSearch, error) {
	return alertutils.MinionSearch{}, nil
}
func (m *mockDatabase) GetMinionSearch(alert_id string) (*alertutils.MinionSearch, error) {
	return nil, nil
}
func (m *mockDatabase) GetAllMinionSearches(orgId int64) ([]alertutils.MinionSearch, error) {
	return nil, nil
}
func (m *mockDatabase) UpdateMinionSearchStateByAlertID(alertId string, alertState alertutils.AlertState) error {
	return nil
}
func (m *mockDatabase) UpdateAlert(*alertutils.AlertDetails) error          { return nil }
func (m *mockDatabase) UpdateSilenceMinutes(*alertutils.AlertDetails) error { return nil }
func (m *mockDatabase) DeleteAlert(alert_id string) error                   { return nil }
func (m *mockDatabase) GetCoolDownDetails(alert_id string) (uint64, time.Time, error) {
	return 0, time.Time{}, nil
}
func (m *mockDatabase) GetAlertNotification(alert_id string) (*alertutils.Notification, error) {
	return nil, nil
}
func (m *mockDatabase) GetContactDetails(alert_id string) (string, string, string, error) {
	return "", "", "", nil
}
func (m *mockDatabase) GetEmailAndChannelID(contact_id string) ([]string, []alertutils.SlackTokenConfig, []alertutils.WebHookConfig, error) {
	return nil, nil, nil, nil
}
func (m *mockDatabase) UpdateAlertStateAndNotificationDetails(alertId string, alertState alertutils.AlertState, updateNotificationState bool) error {
	return nil
}

func Test_ContactPointCRUD(t *testing.T) {
	// Setup
	mockDB := newMockDatabase()
	databaseObj = mockDB
	orgID := int64(42)

	// --- Create Contact ---
	contact := &alertutils.Contact{
		ContactName: "test-contact",
		Email:       []string{"test@example.com"},
	}
	body, _ := json.Marshal(contact)
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetBody(body)

	ProcessCreateContactRequest(ctx, orgID)
	assert.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
	var resp map[string]interface{}
	_ = json.Unmarshal(ctx.Response.Body(), &resp)
	assert.Contains(t, resp["message"], "Successfully created")

	// --- Create Contact: Empty Body ---
	ctx2 := &fasthttp.RequestCtx{}
	ctx2.Request.SetBody([]byte{})
	ProcessCreateContactRequest(ctx2, orgID)
	assert.NotEqual(t, fasthttp.StatusOK, ctx2.Response.StatusCode())
	assert.Contains(t, string(ctx2.Response.Body()), "Received empty request")

	// --- Get All Contacts ---
	ctx3 := &fasthttp.RequestCtx{}
	ProcessGetAllContactsRequest(ctx3, orgID)
	assert.Equal(t, fasthttp.StatusOK, ctx3.Response.StatusCode())
	var getResp map[string]interface{}
	_ = json.Unmarshal(ctx3.Response.Body(), &getResp)
	contacts, ok := getResp["contacts"].([]interface{})
	assert.True(t, ok)
	assert.Len(t, contacts, 1)
	// --- Update Contact ---
	updatedContact := mockDB.contacts[orgID][0]
	updatedContact.ContactName = "updated-contact"
	updateBody, _ := json.Marshal(&updatedContact)
	ctx4 := &fasthttp.RequestCtx{}
	ctx4.Request.SetBody(updateBody)
	ProcessUpdateContactRequest(ctx4)
	assert.Equal(t, fasthttp.StatusOK, ctx4.Response.StatusCode())
	var updateResp map[string]interface{}
	_ = json.Unmarshal(ctx4.Response.Body(), &updateResp)
	assert.Contains(t, updateResp["message"], "updated successfully")
	assert.Equal(t, "updated-contact", mockDB.contacts[orgID][0].ContactName)

	// --- Update Contact: Empty Body ---
	ctx5 := &fasthttp.RequestCtx{}
	ctx5.Request.SetBody([]byte{})
	ProcessUpdateContactRequest(ctx5)
	assert.NotEqual(t, fasthttp.StatusOK, ctx5.Response.StatusCode())
	assert.Contains(t, string(ctx5.Response.Body()), "Received empty request")

	// --- Delete Contact ---
	delContact := mockDB.contacts[orgID][0]
	delBody, _ := json.Marshal(&delContact)
	ctx6 := &fasthttp.RequestCtx{}
	ctx6.Request.SetBody(delBody)
	ProcessDeleteContactRequest(ctx6)
	assert.Equal(t, fasthttp.StatusOK, ctx6.Response.StatusCode())
	var delResp map[string]interface{}
	_ = json.Unmarshal(ctx6.Response.Body(), &delResp)
	assert.Contains(t, delResp["message"], "deleted successfully")
	assert.Len(t, mockDB.contacts[orgID], 0)

	// --- Delete Contact: Empty Body ---
	ctx7 := &fasthttp.RequestCtx{}
	ctx7.Request.SetBody([]byte{})
	ProcessDeleteContactRequest(ctx7)
	assert.NotEqual(t, fasthttp.StatusOK, ctx7.Response.StatusCode())
	assert.Contains(t, string(ctx7.Response.Body()), "Received empty request")
}

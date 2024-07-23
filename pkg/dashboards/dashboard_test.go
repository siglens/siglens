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

package dashboards

import (
	"os"
	"strings"
	"testing"

	"github.com/siglens/siglens/pkg/config"
	"github.com/stretchr/testify/assert"
)

func Test_dashboard_storage_methods(t *testing.T) {

	//Create the defaultDB directory
	defaultDBPath := "defaultDBs"
	e := os.Mkdir(defaultDBPath, 0755)
	if e != nil {
		t.Fatalf("Failed to create defaultDB directory: %v", e)
	}

	//Create the file with JSON content inside defaultDB
	jsonContent := `{
		"10329b95-47a8-48df-8b1d-0a0a01ec6c42": "Siglens Ingestion DB",
		"a28f485c-4747-4024-bb6b-d230f101f852": "Siglens Query DB",
		"bd74f11e-26c8-4827-bf65-c0b464e1f2a4": "Siglens Data DB",
		"53cb3dde-fd78-4253-808c-18e4077ef0f1": "Sample Dashboard"
	}`
	filePath := defaultDBPath + "/allids.json"
	e = os.WriteFile(filePath, []byte(jsonContent), 0644)
	if e != nil {
		t.Fatalf("Failed to write JSON content to file: %v", e)
	}

	config.InitializeDefaultConfig(t.TempDir())

	_ = InitDashboards()

	//Delete the defaultDB directory
	defer func() {
		err := os.RemoveAll(defaultDBPath)
		if err != nil {
			t.Logf("Failed to remove defaultDB directory: %v", err)
		}
	}()

	_, err := createDashboard("dashboard-1", 0)
	assert.Nil(t, err)

	_, err = createDashboard("dashboard-2", 0)
	assert.Nil(t, err)

	dIds, err := getAllDashboardIds(0)
	expected := []string{}

	for id := range dIds {
		expected = append(expected, id)
	}

	assert.Nil(t, err)
	assert.Equal(t, len(expected), len(dIds))

	for id := range dIds {
		assert.Contains(t, expected, id)
	}

	eDashboardDetails := make(map[string]interface{})

	eDashboardDetails["description"] = "Dashboard type"
	eDashboardDetails["note"] = "mydashboard"
	dashboardId := expected[0]
	dashboardName := "upd-dashboard-1"

	err = updateDashboard(dashboardId, dashboardName, eDashboardDetails, 0)
	assert.Nil(t, err)

	aDashBoardDetails, err := getDashboard(expected[0])

	assert.Nil(t, err)
	assert.Equal(t, eDashboardDetails["description"], aDashBoardDetails["description"])
	assert.Equal(t, eDashboardDetails["note"], aDashBoardDetails["note"])

	// Test toggleFavorite with non-existing dashboard
	_, err = toggleFavorite("non-existing-id")
	assert.NotNil(t, err)

	// Test isDashboardFavorite with non-existing dashboard
	_, err = isDashboardFavorite("non-existing-id")
	assert.NotNil(t, err)

	isFavorite, err := toggleFavorite(expected[0])
	assert.Nil(t, err)
	assert.True(t, isFavorite)

	// Test isDashboardFavorite
	isFavorite, err = isDashboardFavorite(expected[0])
	assert.Nil(t, err)
	assert.True(t, isFavorite)

	// Test isDashboardFavorite for non-favorite dashboard
	isFavorite, err = isDashboardFavorite(expected[1])
	assert.Nil(t, err)
	assert.False(t, isFavorite)

	// Test getAllFavoriteDashboardIds
	favoriteDashboards, err := getAllFavoriteDashboardIds(0)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(favoriteDashboards))
	assert.Contains(t, favoriteDashboards, expected[0])

	// Toggle favorite off
	isFavorite, err = toggleFavorite(expected[0])
	assert.Nil(t, err)
	assert.False(t, isFavorite)

	// Test isDashboardFavorite
	isFavorite, err = isDashboardFavorite(expected[0])
	assert.Nil(t, err)
	assert.False(t, isFavorite)

	// Test getAllFavoriteDashboardIds
	favoriteDashboards, err = getAllFavoriteDashboardIds(0)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(favoriteDashboards))

	// Test toggleFavorite with multiple dashboards
	isFavorite, err = toggleFavorite(expected[0])
	assert.Nil(t, err)
	assert.True(t, isFavorite)

	isFavorite, err = toggleFavorite(expected[1])
	assert.Nil(t, err)
	assert.True(t, isFavorite)

	// Test getAllFavoriteDashboardIds with multiple favorite dashboards
	favoriteDashboards, err = getAllFavoriteDashboardIds(0)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(favoriteDashboards))
	assert.Contains(t, favoriteDashboards, expected[0])
	assert.Contains(t, favoriteDashboards, expected[1])

	err = deleteDashboard(expected[0], 0)
	assert.Nil(t, err)

	dIds, err = getAllDashboardIds(0)
	assert.Nil(t, err)
	assert.Equal(t, len(expected)-1, len(dIds))

	// Delete the files created in the test
	var sb strings.Builder
	sb.WriteString(config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards")
	baseDir := sb.String()
	err = os.RemoveAll(baseDir)
	assert.Nil(t, err)
}

func Test_parseUpdateDashboardRequest(t *testing.T) {
	// Test with empty request
	req := make(map[string]interface{})
	_, _, _, err := parseUpdateDashboardRequest(req)
	assert.NotNil(t, err)

	// Test with invalid request
	req["id"] = "122"
	req["name"] = "Dashboard Name"
	_, _, _, err = parseUpdateDashboardRequest(req)
	assert.NotNil(t, err)

	// Test with invalid request sending value other than string
	req["id"] = 122
	req["name"] = "Dashboard Name"
	_, _, _, err = parseUpdateDashboardRequest(req)
	assert.NotNil(t, err)

	// Test with valid request
	req["id"] = "dashboard-1"
	req["name"] = "Dashboard"
	req["details"] = make(map[string]interface{})
	req["details"].(map[string]interface{})["description"] = "Dashboard type"
	req["details"].(map[string]interface{})["note"] = "mydashboard"
	id, name, details, err := parseUpdateDashboardRequest(req)
	assert.Nil(t, err)
	assert.Equal(t, "dashboard-1", id)
	assert.Equal(t, "Dashboard", name)
	assert.Equal(t, "Dashboard type", details["description"])
	assert.Equal(t, "mydashboard", details["note"])
}

// TestCreateDashboardWithDefaultName tests that creating a dashboard with a default name is not allowed
func TestCreateDashboardWithDefaultName(t *testing.T) {

	defaultName := "Sample Dashboard"

	_, err := createDashboard(defaultName, 1)

	// We expect an error because default names should not be allowed
	if err == nil {
		t.Errorf("Expected an error when creating a dashboard with the default name '%s', but got none", defaultName)
	}

	//Check if the error was the expected one
	expectedErrMsg := "dashboard name already exists"
	if err != nil && err.Error() != expectedErrMsg {
		t.Errorf("Expected error message '%s', but got '%s'", expectedErrMsg, err.Error())
	}
}

/*
func Test_dashboard_storage_methods_multiple_orgs(t *testing.T) {
	config.InitializeDefaultConfig()
	_ = InitDashboards()

	_, err := createDashboard("new-dashboard-1", 0)
	assert.Nil(t, err)

	_, err = createDashboard("new-dashboard-1", 1)
	assert.Nil(t, err)

	dIds, err := getAllDashboardIds(0)
	expected := []string{}

	for id := range dIds {
		expected = append(expected, id)
	}

	assert.Nil(t, err)
	assert.Equal(t, len(expected), len(dIds))

	for id := range dIds {
		assert.Contains(t, expected, id)
	}

	eDashboardDetails := make(map[string]interface{})

	eDashboardDetails["description"] = "Dashboard type"
	eDashboardDetails["note"] = "mydashboard"
	dashboardId := expected[0]
	dashboardName := "updated-dashboard-1"

	err = updateDashboard(dashboardId, dashboardName, eDashboardDetails, 0)
	assert.Nil(t, err)

	aDashBoardDetails, err := getDashboard(expected[0])

	assert.Nil(t, err)
	assert.Equal(t, eDashboardDetails["description"], aDashBoardDetails["description"])
	assert.Equal(t, eDashboardDetails["note"], aDashBoardDetails["note"])

	err = deleteDashboard(expected[0], 0)
	assert.Nil(t, err)

	dIds, err = getAllDashboardIds(0)
	assert.Nil(t, err)
	assert.Equal(t, len(expected)-1, len(dIds))

	dIds, err = getAllDashboardIds(1)
	expected = []string{}

	for id := range dIds {
		expected = append(expected, id)
	}

	assert.Nil(t, err)
	assert.Equal(t, len(expected), len(dIds))

	for id := range dIds {
		assert.Contains(t, expected, id)
	}

	dashboardId = expected[0]
	dashboardName = "updated-dashboard"
	err = updateDashboard(dashboardId, dashboardName, eDashboardDetails, 1)
	assert.Nil(t, err)

	aDashBoardDetails, err = getDashboard(expected[0])

	assert.Nil(t, err)
	assert.Equal(t, eDashboardDetails["description"], aDashBoardDetails["description"])
	assert.Equal(t, eDashboardDetails["note"], aDashBoardDetails["note"])

	err = deleteDashboard(expected[0], 1)
	assert.Nil(t, err)

	dIds, err = getAllDashboardIds(1)
	assert.Nil(t, err)
	assert.Equal(t, len(expected)-1, len(dIds))

	// Delete the files created in the test
	var sb strings.Builder
	sb.WriteString(config.GetDataPath() + "querynodes/" + config.GetHostname() + "/dashboards")
	baseDir := sb.String()
	err = os.RemoveAll(baseDir)
	assert.Nil(t, err)
}
*/

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

package dashboards

import (
	"os"
	"strings"
	"testing"

	"github.com/siglens/siglens/pkg/config"
	"github.com/stretchr/testify/assert"
)

func Test_dashboard_storage_methods(t *testing.T) {

	config.InitializeDefaultConfig()

	_ = InitDashboards()

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
	dashboardName := "dashboard-1"

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

	// Delete the files created in the test
	var sb strings.Builder
	sb.WriteString(config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards")
	baseDir := sb.String()
	err = os.RemoveAll(baseDir)
	assert.Nil(t, err)
}

func Test_dashboard_storage_methods_multiple_orgs(t *testing.T) {
	config.InitializeDefaultConfig()
	_ = InitDashboards()

	_, err := createDashboard("dashboard-1", 0)
	assert.Nil(t, err)

	_, err = createDashboard("dashboard-1", 1)
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
	dashboardName := "dashboard-1"

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
	dashboardName = "dashboard-1"
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

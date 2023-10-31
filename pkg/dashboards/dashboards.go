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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/valyala/fasthttp"

	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

var allidsBaseFname string
var allDashIdsLock map[uint64]*sync.Mutex = make(map[uint64]*sync.Mutex)
var latestDashboardReadTimeMillis map[uint64]uint64

// map of "orgid" => "dashboardId" ==> "dashboardName"
// e.g. "1234567890" => "11812083241622924684" => "dashboard-1"
var allDashboardsIds map[uint64]map[string]string = make(map[uint64]map[string]string)
var allDashboardsIdsLock *sync.RWMutex = &sync.RWMutex{}

func readSavedDashboards(orgid uint64) ([]byte, error) {
	var dashboardData []byte
	allidsFname := getDashboardFileName(orgid)
	_, err := os.Stat(allidsFname)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
	}

	dashboardData, err = os.ReadFile(allidsFname)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		log.Errorf("readSavedDashboards: Failed to read dashboard file fname=%v, err=%v", allidsFname, err)
		return nil, err
	}

	allDashboardsIdsLock.Lock()
	if _, ok := allDashboardsIds[orgid]; !ok {
		allDashboardsIds[orgid] = make(map[string]string)
	}
	var dashboardDetails map[string]string
	err = json.Unmarshal(dashboardData, &dashboardDetails)
	if err != nil {
		allDashboardsIdsLock.Unlock()
		log.Errorf("readSavedDashboards: Failed to unmarshall dashboard file fname=%v, err=%v", allidsFname, err)
		return nil, err
	}
	allDashboardsIds[orgid] = dashboardDetails
	latestDashboardReadTimeMillis[orgid] = utils.GetCurrentTimeInMs()
	allDashboardsIdsLock.Unlock()
	return dashboardData, nil
}

func readSavedDefaultDashboards(orgid uint64) ([]byte, error) {
	var dashboardData []byte
	allidsFname := getDefaultDashboardFileName(orgid)
	_, err := os.Stat(allidsFname)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		log.Errorf("readSavedDashboards: Failed to stat dashboard file fname=%v, err=%v", allidsFname, err)
		return nil, err
	}

	dashboardData, err = os.ReadFile(allidsFname)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		log.Errorf("readSavedDashboards: Failed to read dashboard file fname=%v, err=%v", allidsFname, err)
		return nil, err
	}

	allDashboardsIdsLock.Lock()
	if _, ok := allDashboardsIds[orgid]; !ok {
		allDashboardsIds[orgid] = make(map[string]string)
	}
	var dashboardDetails map[string]string
	err = json.Unmarshal(dashboardData, &dashboardDetails)
	if err != nil {
		allDashboardsIdsLock.Unlock()
		log.Errorf("readSavedDashboards: Failed to unmarshall dashboard file fname=%v, err=%v", allidsFname, err)
		return nil, err
	}
	allDashboardsIds[orgid] = dashboardDetails
	latestDashboardReadTimeMillis[orgid] = utils.GetCurrentTimeInMs()
	allDashboardsIdsLock.Unlock()
	return dashboardData, nil
}

func getDashboardFileName(orgid uint64) string {
	var allidsFname string
	if orgid == 0 {
		allidsFname = allidsBaseFname + ".json"
	} else {
		allidsFname = allidsBaseFname + "-" + strconv.FormatUint(orgid, 10) + ".json"
	}
	return allidsFname
}

func getDefaultDashboardFileName(orgid uint64) string {
	var allidsFname string
	var defaultDBsAllidsBaseFname string = "defaultDBs/allids"
	if orgid == 0 {
		allidsFname = defaultDBsAllidsBaseFname + ".json"
	} else {
		allidsFname = defaultDBsAllidsBaseFname + "-" + strconv.FormatUint(orgid, 10) + ".json"
	}
	return allidsFname
}

func InitDashboards() error {
	var sb strings.Builder

	sb.WriteString(config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards")
	baseDir := sb.String()
	allidsBaseFname = baseDir + "/allids"
	latestDashboardReadTimeMillis = make(map[uint64]uint64)

	err := os.MkdirAll(baseDir, 0764)
	if err != nil {
		log.Errorf("InitDashboard: failed to create basedir=%v, err=%v", baseDir, err)
		return err
	}

	err = os.MkdirAll(baseDir+"/details", 0764)
	if err != nil {
		log.Errorf("InitDashboard: failed to create basedir=%v, err=%v", baseDir, err)
		return err
	}

	createOrAcquireLock(0)
	_, err = readSavedDashboards(0)
	if err != nil {
		releaseLock(0)
		log.Errorf("InitDashboard: failed to read saved dashboards, err=%v", err)
		return err
	}
	releaseLock(0)

	return nil
}

func createOrAcquireLock(orgid uint64) {
	if _, ok := allDashIdsLock[orgid]; !ok {
		allDashIdsLock[orgid] = &sync.Mutex{}
	}
	allDashIdsLock[orgid].Lock()
}

func releaseLock(orgid uint64) {
	allDashIdsLock[orgid].Unlock()
}

func getAllDashboardIds(orgid uint64) (map[string]string, error) {
	createOrAcquireLock(orgid)
	_, err := readSavedDashboards(orgid)
	if err != nil {
		releaseLock(orgid)
		log.Errorf("getAllDashboardIds: failed to read, err=%v", err)
		return nil, err
	}
	releaseLock(orgid)
	allDashboardsIdsLock.RLock()
	defer allDashboardsIdsLock.RUnlock()
	return allDashboardsIds[orgid], nil
}

func getAllDefaultDashboardIds(orgid uint64) (map[string]string, error) {
	createOrAcquireLock(orgid)
	_, err := readSavedDefaultDashboards(orgid)
	if err != nil {
		releaseLock(orgid)
		log.Errorf("getAllDashboardIds: failed to read, err=%v", err)
		return nil, err
	}
	releaseLock(orgid)
	allDashboardsIdsLock.RLock()
	defer allDashboardsIdsLock.RUnlock()
	return allDashboardsIds[orgid], nil
}

// Generate the uniq uuid for the dashboard
func createUniqId(dname string) string {
	newId := uuid.New().String()
	return newId
}

func createDashboard(dname string, orgid uint64) (map[string]string, error) {
	if dname == "" {
		log.Errorf("createDashboard: failed to create Dashboard, with empty dashboard name")
		return nil, errors.New("createDashboard: failed to create Dashboard, with empty dashboard name")
	}

	newId := createUniqId(dname)

	dashBoardIds, err := getAllDashboardIds(orgid)
	if err != nil {
		log.Errorf("createDashboard: Failed to get all dashboard ids err=%v", err)
		return nil, err
	}
	for _, dId := range dashBoardIds {
		if dId == newId {
			log.Errorf("createDashboard: Failed to create dashboard, dashboard id already exists=%v", dname)
			return nil, errors.New("createDashboard: Failed to create dashboard, dashboard id already exists")
		}
	}

	allDashboardsIdsLock.Lock()
	if _, ok := allDashboardsIds[orgid]; !ok {
		allDashboardsIds[orgid] = make(map[string]string)
	}
	allDashboardsIds[orgid][newId] = dname
	orgDashboards := allDashboardsIds[orgid]
	jdata, err := json.Marshal(&orgDashboards)
	allDashboardsIdsLock.Unlock()
	if err != nil {
		log.Errorf("createDashboard: Failed to marshall err=%v", err)
		return nil, err
	}

	allidsFname := getDashboardFileName(orgid)
	err = os.WriteFile(allidsFname, jdata, 0644)
	if err != nil {
		log.Errorf("createDashboard: Failed to write file=%v, err=%v", allidsFname, err)
		return nil, err
	}

	dashboardDetailsFname := config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards/details/" + newId + ".json"

	err = os.WriteFile(dashboardDetailsFname, []byte("{}"), 0644)
	if err != nil {
		log.Errorf("createDashboard: Error creating empty local file %s: %v", dashboardDetailsFname, err)
		return nil, err
	}

	log.Infof("createDashboard: Successfully created file %v", dashboardDetailsFname)
	err = blob.UploadQueryNodeDir()
	if err != nil {
		log.Errorf("createDashboard: Failed to upload query nodes dir  err=%v", err)
		return nil, err
	}

	retval := make(map[string]string)
	allDashboardsIdsLock.RLock()
	orgDashboardsIds := allDashboardsIds[orgid]
	allDashboardsIdsLock.RUnlock()
	for k, v := range orgDashboardsIds {
		if k == newId {
			retval[k] = v
			break
		}
	}

	return retval, nil
}

func getDashboard(id string) (map[string]interface{}, error) {
	var dashboardDetailsFname string
	if id == "10329b95-47a8-48df-8b1d-0a0a01ec6c42" || id == "a28f485c-4747-4024-bb6b-d230f101f852" || id == "bd74f11e-26c8-4827-bf65-c0b464e1f2a4" {
		dashboardDetailsFname = "defaultDBs/details/" + id + ".json"
	} else {
		dashboardDetailsFname = config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards/details/" + id + ".json"
	}
	rdata, err := os.ReadFile(dashboardDetailsFname)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		log.Errorf("getDashboard: Failed to read dashboard file fname=%v, err=%v", dashboardDetailsFname, err)
		return nil, err
	}

	var detailDashboardInfo map[string]interface{} = make(map[string]interface{})

	err = json.Unmarshal(rdata, &detailDashboardInfo)
	if err != nil {
		log.Errorf("getDashboard: Failed to unmarshall dashboard file fname=%v, err=%v", dashboardDetailsFname, err)
		return nil, err
	}

	retval := detailDashboardInfo
	return retval, nil
}

func updateDashboard(id string, dName string, dashboardDetails map[string]interface{}, orgid uint64) error {

	// Check if the dashboard exists
	allDashboards, err := getAllDashboardIds(orgid)
	if err != nil {
		log.Errorf("updateDashboard: Failed to get all dashboard ids err=%v", err)
		return err
	}
	_, ok := allDashboards[id]
	if !ok {
		log.Errorf("updateDashboard: Dashboard id %v does not exist", id)
		return errors.New("updateDashboard: Dashboard id does not exist")
	}

	// Update the dashboard name if it is different
	if allDashboards[id] != dName {
		allDashboards[id] = dName
	}
	allDashboardsIdsLock.RLock()
	orgDashboards := allDashboardsIds[orgid]
	allDashboardsIdsLock.RUnlock()
	jdata, err := json.Marshal(&orgDashboards)
	if err != nil {
		log.Errorf("updateDashboard: Failed to marshall err=%v", err)
		return err
	}

	allidsFname := getDashboardFileName(orgid)
	err = os.WriteFile(allidsFname, jdata, 0644)
	if err != nil {
		log.Errorf("updateDashboard: Failed to write file=%v, err=%v", allidsFname, err)
		return err
	}

	dashboardDetailsFname := config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards/details/" + id + ".json"

	jdata, err = json.Marshal(&dashboardDetails)
	if err != nil {
		log.Errorf("updateDashboard: Failed to marshall err=%v", err)
		return err
	}

	err = os.WriteFile(dashboardDetailsFname, jdata, 0644)
	if err != nil {
		log.Errorf("updateDashboard: Failed to writefile fullname=%v, err=%v", dashboardDetailsFname, err)
		return err
	}
	log.Infof("updateDashboard: Successfully updated dashboard details in file %v", dashboardDetailsFname)

	// Update the query node dir
	err = blob.UploadQueryNodeDir()
	if err != nil {
		log.Errorf("updateDashboard: Failed to upload query nodes dir  err=%v", err)
		return err
	}

	return nil
}

func deleteDashboard(id string, orgid uint64) error {

	createOrAcquireLock(orgid)
	dashboardData, err := readSavedDashboards(orgid)
	if err != nil {
		releaseLock(orgid)
		log.Errorf("deleteDashboard: failed to read saved dashboards, err=%v", err)
		return err
	}
	releaseLock(orgid)

	var dashboardDetails map[string]string
	err = json.Unmarshal(dashboardData, &dashboardDetails)
	if err != nil {
		log.Errorf("deleteDashboard: Failed to unmarshall dashboard file for orgid=%v, err=%v", orgid, err)
		return err
	}

	// Delete entry from dashboardInfo and write to file allids.json
	allDashboardsIdsLock.Lock()
	delete(allDashboardsIds[orgid], id)
	allDashboardsIdsLock.Unlock()

	// Update the file with latest dashboard info
	allDashboardsIdsLock.RLock()
	orgDashboardIds := allDashboardsIds[orgid]
	allDashboardsIdsLock.RUnlock()
	jdata, err := json.Marshal(&orgDashboardIds)
	if err != nil {
		log.Errorf("deleteDashboard: Failed to marshall err=%v", err)
		return err
	}

	allidsFname := getDashboardFileName(orgid)
	err = os.WriteFile(allidsFname, jdata, 0644)
	if err != nil {
		log.Errorf("deleteDashboard: Failed to write file=%v, err=%v", allidsFname, err)
		return err
	}

	// Delete dashboard details file
	dashboardDetailsFname := config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards/details/" + id + ".json"
	err = os.Remove(dashboardDetailsFname)
	if err != nil {
		log.Errorf("deleteDashboard:  Error deleting file %s: %v", dashboardDetailsFname, err)
		return err
	}

	// Update the query node dir
	err = blob.UploadQueryNodeDir()
	if err != nil {
		log.Errorf("deleteDashboard: Failed to upload query nodes dir  err=%v", err)
		return err
	}

	return nil
}

func setBadMsg(ctx *fasthttp.RequestCtx) {
	var httpResp utils.HttpServerResponse
	ctx.SetStatusCode(fasthttp.StatusBadRequest)
	httpResp.Message = "Bad Request"
	httpResp.StatusCode = fasthttp.StatusBadRequest
	utils.WriteResponse(ctx, httpResp)
}

func ProcessCreateDashboardRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	rawJSON := ctx.PostBody()
	if rawJSON == nil {
		log.Errorf("ProcessCreateDashboardRequest: received empty user query")
		setBadMsg(ctx)
		return
	}

	var dname string

	err := json.Unmarshal(rawJSON, &dname)
	if err != nil {
		log.Errorf("ProcessCreateDashboardRequest: could not unmarshall user query, err=%v", err)
		setBadMsg(ctx)
		return
	}
	dashboardInfo, err := createDashboard(dname, myid)

	if err != nil {
		log.Errorf("ProcessCreateDashboardRequest: could not create Dashboard=%v, err=%v", dname, err)
		setBadMsg(ctx)
		return
	}

	utils.WriteJsonResponse(ctx, dashboardInfo)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessListAllRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	dIds, err := getAllDashboardIds(myid)

	if err != nil {
		log.Errorf("ProcessListAllRequest: could not get dashboard ids, err=%v", err)
		setBadMsg(ctx)
		return
	}
	utils.WriteJsonResponse(ctx, dIds)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessListAllDefaultDBRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	dIds, err := getAllDefaultDashboardIds(myid)

	if err != nil {
		log.Errorf("ProcessListAllRequest: could not get dashboard ids, err=%v", err)
		setBadMsg(ctx)
		return
	}
	utils.WriteJsonResponse(ctx, dIds)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessUpdateDashboardRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	rawJSON := ctx.PostBody()
	if rawJSON == nil {
		log.Errorf("ProcessCreateDashboardRequest: received empty user query")
		setBadMsg(ctx)
		return
	}

	readJSON := make(map[string]interface{})

	err := json.Unmarshal(rawJSON, &readJSON)
	if err != nil {
		log.Errorf("ProcessCreateDashboardRequest: could not unmarshall user query, err=%v", err)
		setBadMsg(ctx)
		return
	}

	dId := readJSON["id"].(string)
	dName := readJSON["name"].(string)
	dashboardDetails := readJSON["details"].(map[string]interface{})
	err = updateDashboard(dId, dName, dashboardDetails, myid)
	if err != nil {
		log.Errorf("ProcessCreateDashboardRequest: could not create Dashboard=%v, err=%v", dId, err)
		setBadMsg(ctx)
		return
	}
	response := "Dashboard updated successfully"
	utils.WriteJsonResponse(ctx, response)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessGetDashboardRequest(ctx *fasthttp.RequestCtx) {
	dId := utils.ExtractParamAsString(ctx.UserValue("dashboard-id"))
	dashboardDetails, err := getDashboard(dId)
	if err != nil {
		log.Errorf("ProcessGetDashboardRequest: could not get Dashboard=%v, err=%v", dId, err)
		setBadMsg(ctx)
		return
	}
	utils.WriteJsonResponse(ctx, dashboardDetails)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessDeleteDashboardRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	dId := utils.ExtractParamAsString(ctx.UserValue("dashboard-id"))
	err := deleteDashboard(dId, myid)
	if err != nil {
		log.Errorf("ProcessDeleteDashboardRequest: Failed to delete dashboard=%v, err=%v", dId, err)
		setBadMsg(ctx)
		return
	}

	log.Infof("ProcessDeleteDashboardRequest: Successfully deleted dashboard %v", dId)
	err = blob.UploadQueryNodeDir()
	if err != nil {
		log.Errorf("ProcessDeleteDashboardRequest: Failed to upload query nodes dir  err=%v", err)
		return
	}
	response := "Dashboard deleted successfully"
	utils.WriteJsonResponse(ctx, response)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessDeleteDashboardsByOrgId(orgid uint64) error {
	dIds, err := getAllDashboardIds(orgid)
	if err != nil {
		log.Errorf("ProcessDeleteDashboardsByOrgId: Failed to get all dashboard ids err=%v", err)
		return err
	}
	for dId := range dIds {
		err = deleteDashboard(dId, orgid)
		if err != nil {
			log.Errorf("ProcessDeleteDashboardsByOrgId: Failed to delete dashboard=%v, err=%v", dId, err)
		}

		log.Infof("ProcessDeleteDashboardsByOrgId: Successfully deleted dashboard %v", dId)
		err = blob.UploadQueryNodeDir()
		if err != nil {
			log.Errorf("ProcessDeleteDashboardsByOrgId: Failed to upload query nodes dir, err=%v", err)
			// Move on to the next dashboard for now
		}
	}

	dashboardAllIdsFilename := config.GetDataPath() + "querynodes/" + config.GetHostname() + "/dashboards/allids-" + fmt.Sprint(orgid) + ".json"

	err = os.Remove(dashboardAllIdsFilename)
	if err != nil {
		log.Warnf("ProcessDeleteDashboardsByOrgId: Failed to delete the dashboard allids file: %v", dashboardAllIdsFilename)
	}
	return nil
}

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

package usersavedqueries

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/imdario/mergo"

	jsoniter "github.com/json-iterator/go"
	"github.com/siglens/siglens/pkg/audit"
	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

var usqBaseFilename string

// map of orgid => usq lock
var usqLock map[int64]*sync.Mutex
var usqLastReadTime map[int64]uint64
var localUSQInfoLock *sync.RWMutex = &sync.RWMutex{}
var externalUSQInfoLock *sync.RWMutex = &sync.RWMutex{}

// map of "orgid" => queryName" ==> fieldname => fieldvalue
// e.g. "123456789" => mysave1" => {"searchText":"...", "indexName": "..."}
var localUSQInfo map[int64]map[string]map[string]interface{} = make(map[int64]map[string]map[string]interface{})
var externalUSQInfo map[int64]map[string]map[string]interface{} = make(map[int64]map[string]map[string]interface{})

func InitUsq() error {
	var sb strings.Builder
	sb.WriteString(config.GetDataPath() + "querynodes/" + config.GetHostID() + "/usersavedqueries")
	baseDir := sb.String()
	usqBaseFilename = baseDir + "/usqinfo"
	err := os.MkdirAll(baseDir, 0764)
	if err != nil {
		log.Errorf("InitUsq: failed to create basedir=%v, err=%v", baseDir, err)
		return err
	}
	usqLock = make(map[int64]*sync.Mutex)
	usqLastReadTime = make(map[int64]uint64)

	acquireOrCreateLock(0)
	err = readSavedQueries(0)
	if err != nil {
		releaseLock(0)
		log.Errorf("InitUsq: failed to read saved queries, err=%v", err)
		return err
	}
	releaseLock(0)
	return nil
}

func acquireOrCreateLock(myid int64) {
	if _, ok := usqLock[myid]; !ok {
		usqLock[myid] = &sync.Mutex{}
	}
	usqLock[myid].Lock()
}

func releaseLock(myid int64) {
	usqLock[myid].Unlock()
}

func writeUsq(qname string, uq map[string]interface{}, myid int64) error {

	if qname == "" {
		log.Errorf("writeUsq: failed to save query data, query name is empty")
		return errors.New("writeUsq: failed to save query data, query name is empty")
	}

	acquireOrCreateLock(myid)
	err := readSavedQueries(myid)
	if err != nil {
		releaseLock(myid)
		log.Errorf("writeUsq: failed to read save queries, err=%v", err)
		return errors.New("internal server error, failed to read saved queries")
	}
	releaseLock(myid)
	localUSQInfoLock.Lock()
	if _, ok := localUSQInfo[myid]; !ok {
		localUSQInfo[myid] = make(map[string]map[string]interface{})
	}
	localUSQInfo[myid][qname] = uq
	localUSQInfoLock.Unlock()

	err = writeSavedQueries(myid)
	if err != nil {
		log.Errorf("writeUsq: failed to write the saved queries into a file, err=%v", err)
		return errors.New("internal server error, cant write data")
	}
	return nil
}

/*
	   takes the queryname
	   returns:
		  bool : if found true else false
		  map[string]interface: the savequerydata
		  error: any error
*/
func getUsqOne(qname string, myid int64) (bool, map[string]map[string]interface{}, error) {
	var allUSQInfo map[string]map[string]interface{} = make(map[string]map[string]interface{})
	retval := make(map[string]map[string]interface{})
	found := false
	acquireOrCreateLock(myid)
	err := readSavedQueries(myid)
	if err != nil {
		releaseLock(myid)
		log.Errorf("GetUsqOne: failed to read, err=%v", err)
		return false, nil, err
	}
	releaseLock(myid)

	localUSQInfoLock.RLock()
	defer localUSQInfoLock.RUnlock()
	if orgMap, ok := localUSQInfo[myid]; ok {
		err = mergo.Merge(&allUSQInfo, &orgMap)
		if err != nil {
			log.Errorf("getUsqOne: failed to merge local saved queries, err=%v", err)
			return false, localUSQInfo[myid], err
		}
	}

	externalUSQInfoLock.RLock()
	defer externalUSQInfoLock.RUnlock()
	if orgExternalMap, ok := externalUSQInfo[myid]; ok {
		err = mergo.Merge(&allUSQInfo, &orgExternalMap)
		if err != nil {
			log.Errorf("getUsqOne: failed to merge external saved queries, err=%v", err)
			return false, localUSQInfo[myid], err
		}
	}
	for k, v := range allUSQInfo {
		i := strings.Index(k, qname)
		if i != -1 {
			retval[k] = v
			found = true
		}
	}
	return found, retval, nil
}

func getUsqAll(myid int64) (bool, map[string]map[string]interface{}, error) {
	var allUSQInfo map[string]map[string]interface{} = make(map[string]map[string]interface{})

	acquireOrCreateLock(myid)
	err := readSavedQueries(myid)
	if err != nil {
		releaseLock(myid)
		log.Errorf("GetUsqAll: failed to read, err=%v", err)
		return false, nil, err
	}
	releaseLock(myid)

	localUSQInfoLock.RLock()
	defer localUSQInfoLock.RUnlock()
	if orgMap, ok := localUSQInfo[myid]; ok {
		err = mergo.Merge(&allUSQInfo, &orgMap)
		if err != nil {
			log.Errorf("GetUsqAll: failed to merge local saved queries, err=%v", err)
			return false, localUSQInfo[myid], err
		}
	}

	externalUSQInfoLock.RLock()
	defer externalUSQInfoLock.RUnlock()
	if orgExternalMap, ok := externalUSQInfo[myid]; ok {
		err = mergo.Merge(&allUSQInfo, &orgExternalMap)
		if err != nil {
			log.Errorf("GetUsqAll: failed to merge external saved queries, err=%v", err)
			return false, localUSQInfo[myid], err
		}
	}

	// todo should we make a deep copy here
	return true, allUSQInfo, nil
}

/*
	   takes the orgid whose usq are to be deleted
	   returns:
		  error: any error
*/
func deleteAllUsq(myid int64) error {
	acquireOrCreateLock(myid)
	err := readSavedQueries(myid)
	if err != nil {
		releaseLock(myid)
		log.Errorf("deleteAllUsq: failed to read, err=%v", err)
		return err
	}
	releaseLock(myid)
	localUSQInfoLock.RLock()
	if _, ok := localUSQInfo[myid]; !ok {
		localUSQInfoLock.RUnlock()
		return nil

	}
	localUSQInfoLock.RUnlock()

	localUSQInfoLock.Lock()
	delete(localUSQInfo, myid)
	localUSQInfoLock.Unlock()

	userSavedQueriesFilename := getUsqFileName(myid)
	err = os.Remove(userSavedQueriesFilename)
	if err != nil {
		log.Errorf("deleteAllUsq: failed to delete usersavedqueries file: %v", userSavedQueriesFilename)
		return err
	}

	return nil
}

/*
	   takes the queryname to be deleted
	   returns:
		  bool : if deleted true else false
		  error: any error
*/
func deleteUsq(qname string, myid int64) (bool, error) {

	acquireOrCreateLock(myid)
	err := readSavedQueries(myid)
	if err != nil {
		releaseLock(myid)
		log.Errorf("DeleteUsq: failed to read, err=%v", err)
		return false, err
	}
	releaseLock(myid)
	localUSQInfoLock.RLock()
	if _, ok := localUSQInfo[myid]; !ok {
		localUSQInfoLock.RUnlock()
		return false, nil

	}
	_, ok := localUSQInfo[myid][qname]
	if !ok {
		localUSQInfoLock.RUnlock()
		return false, nil
	}
	localUSQInfoLock.RUnlock()
	localUSQInfoLock.Lock()
	delete(localUSQInfo[myid], qname)
	localUSQInfoLock.Unlock()

	err = writeSavedQueries(myid)
	if err != nil {
		log.Errorf("DeleteUsq: failed to write file, err=%v", err)
		return false, errors.New("internal server error, cant write data")
	}

	return true, nil
}

/*
Caller must call this via a lock
*/
func readSavedQueries(myid int64) error {

	// first see if on disk is newer than memory
	usqFilename := getUsqFileName(myid)
	fileInfo, err := os.Stat(usqFilename)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Errorf("readSavedQueries: failed to stat file=%v, err=%v", usqFilename, err)
			return errors.New("internal server error, can't read mtime")
		}
		return nil // if doesnt exist then cant really do anything
	}

	modifiedTime := uint64(fileInfo.ModTime().UTC().Unix() * 1000)
	lastReadTime, ok := usqLastReadTime[myid]
	if ok && modifiedTime <= lastReadTime {
		return nil
	}

	rdata, err := os.ReadFile(usqFilename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		log.Errorf("readSavedQueries: Failed to read usersavdeequries file fname=%v, err=%v", usqFilename, err)
		return err
	}

	var orgSavedQueriesMap map[string]map[string]interface{}
	err = json.Unmarshal(rdata, &orgSavedQueriesMap)
	if err != nil {
		log.Errorf("readSavedQueries: Failed to unmarshall usqFilename=%v, err=%v", usqFilename, err)
		return err
	}
	localUSQInfoLock.Lock()
	localUSQInfo[myid] = orgSavedQueriesMap
	localUSQInfoLock.Unlock()
	usqLastReadTime[myid] = utils.GetCurrentTimeInMs()

	return nil
}

func getUsqFileName(myid int64) string {
	if myid != 0 {
		usqFilename := usqBaseFilename + "-" + strconv.FormatInt(myid, 10) + ".bin"
		return usqFilename
	} else {
		return usqBaseFilename + ".bin"
	}
}

/*
Caller must call this via a lock
*/
func writeSavedQueries(myid int64) error {

	usqFilename := getUsqFileName(myid)
	localUSQInfoLock.RLock()
	orgMap := localUSQInfo[myid]
	localUSQInfoLock.RUnlock()
	jdata, err := json.Marshal(orgMap)
	if err != nil {
		log.Errorf("writeSavedQueries: Failed to marshall orgMap=%v, err=%v", orgMap, err)
		return err
	}

	err = os.WriteFile(usqFilename, jdata, 0644)
	if err != nil {
		log.Errorf("writeSavedQueries: Failed to writefile filename=%v, err=%v", usqFilename, err)
		return err
	}
	err = blob.UploadQueryNodeDir()
	if err != nil {
		log.Errorf("DeleteUserSavedQuery: Failed to upload query nodes dir  err=%v", err)
		return err
	}
	return nil
}

func DeleteAllUserSavedQueries(myid int64) error {
	err := deleteAllUsq(myid)
	if err != nil {
		log.Errorf("DeleteAllUserSavedQueries: Failed to delete user saved queries for orgid %d, err=%v", myid, err)
		return err
	}
	log.Infof("DeleteAllUserSavedQueries: Successfully deleted user saved queries for orgid: %d", myid)
	err = blob.UploadQueryNodeDir()
	if err != nil {
		log.Errorf("DeleteAllUserSavedQueries: Failed to upload query nodes dir, err=%v", err)
		return err
	}

	return nil
}

func DeleteUserSavedQuery(ctx *fasthttp.RequestCtx, myid int64) {
	queryName := utils.ExtractParamAsString(ctx.UserValue("qname"))
	deleted, err := deleteUsq(queryName, myid)
	if err != nil {
		log.Errorf("DeleteUserSavedQuery: Failed to delete user saved query %v, err=%v", queryName, err)
		utils.SetBadMsg(ctx, "")
		return
	}
	if !deleted {
		utils.SetBadMsg(ctx, "")
		return
	}
	log.Infof("DeleteUserSavedQuery: Successfully deleted user saved query %v", queryName)
	err = blob.UploadQueryNodeDir()
	if err != nil {
		log.Errorf("DeleteUserSavedQuery: Failed to upload query nodes dir  err=%v", err)
		return
	}

	// Audit log
	username := "No-user" // TODO: Add logged in user when user auth is implemented
	var orgId int64
	if hook := hooks.GlobalHooks.MiddlewareExtractOrgIdHook; hook != nil {
		orgId, err = hook(ctx)
		if err != nil {
			log.Errorf("DeleteUserSavedQuery: failed to extract orgId from context. Err=%+v", err)
			utils.SetBadMsg(ctx, "")
			return
		}
	}
	epochTimestampSec := time.Now().Unix()
	actionString := "Deleted user saved query"
	extraMsg := fmt.Sprintf("Query Name: %s", queryName)

	audit.CreateAuditEvent(username, actionString, extraMsg, epochTimestampSec, orgId)

	ctx.SetStatusCode(fasthttp.StatusOK)
}

func GetUserSavedQueriesAll(ctx *fasthttp.RequestCtx, myid int64) {
	httpResp := make(utils.AllSavedQueries)
	found, savedQueriesAll, err := getUsqAll(myid)
	if err != nil {
		log.Errorf("GetUserSavedQueriesAll: Failed to read user saved queries, err=%v", err)
		utils.SetBadMsg(ctx, "")
		return
	}
	if !found {
		return
	}
	for name, usquery := range savedQueriesAll {
		httpResp[name] = usquery
	}
	utils.WriteJsonResponse(ctx, httpResp)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func SaveUserQueries(ctx *fasthttp.RequestCtx, myid int64) {
	rawJSON := ctx.PostBody()
	if rawJSON == nil {
		log.Errorf("SaveUserQueries: received empty user query. Postbody is nil")
		utils.SetBadMsg(ctx, "")
		return
	}

	readJSON := make(map[string]interface{})
	var jsonc = jsoniter.ConfigCompatibleWithStandardLibrary
	decoder := jsonc.NewDecoder(bytes.NewReader(rawJSON))
	decoder.UseNumber()
	err := decoder.Decode(&readJSON)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		_, err = ctx.WriteString(err.Error())
		if err != nil {
			log.Errorf("SaveUserQueries: could not write error message err=%v", err)
		}
		log.Errorf("SaveUserQueries: failed to decode user query body=%v, Err=%+v", string(rawJSON), err)
	}

	var qname string
	usQueryMap := make(map[string]interface{})
	for key, value := range readJSON {
		switch valtype := value.(type) {
		case string:
			switch key {
			case "queryName":
				{
					qname = valtype
				}
			case "queryDescription":
				{
					usQueryMap["description"] = valtype
				}
			case "searchText":
				{
					usQueryMap["searchText"] = valtype
				}
			case "indexName":
				{
					usQueryMap["indexName"] = valtype
				}
			case "filterTab":
				{
					usQueryMap["filterTab"] = valtype
				}
			case "queryLanguage":
				{
					usQueryMap["queryLanguage"] = valtype
				}
			case "dataSource":
				{
					usQueryMap["dataSource"] = valtype
				}
			case "startTime":
				{
					usQueryMap["startTime"] = valtype
				}
			case "endTime":
				{
					usQueryMap["endTime"] = valtype
				}
			case "metricsQueryParams":
				{
					usQueryMap["metricsQueryParams"] = valtype
				}
			}
		default:
			log.Errorf("SaveUserQueries: Invalid save query key=[%v]", key)
			utils.SetBadMsg(ctx, "")
			return
		}
	}
	err = writeUsq(qname, usQueryMap, myid)
	if err != nil {
		log.Errorf("SaveUserQueries: could not write query with query name=%v, to file err=%v", qname, err)
		utils.SetBadMsg(ctx, "")
		return
	}
	log.Infof("SaveUserQueries: successfully written query %v to file ", qname)

	// Audit log
	username := "No-user" // TODO: Add logged in user when user auth is implemented
	var orgId int64
	if hook := hooks.GlobalHooks.MiddlewareExtractOrgIdHook; hook != nil {
		orgId, err = hook(ctx)
		if err != nil {
			log.Errorf("SaveUserQueries: failed to extract orgId from context. Err=%+v", err)
			utils.SetBadMsg(ctx, "")
			return
		}
	}
	epochTimestampSec := time.Now().Unix()
	actionString := "Saved user query"
	extraMsg := fmt.Sprintf("Query Name: %s", qname)

	audit.CreateAuditEvent(username, actionString, extraMsg, epochTimestampSec, orgId)

	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ReadExternalUSQInfo(fName string, myid int64) error {
	var tempUSQInfo map[string]map[string]interface{} = make(map[string]map[string]interface{})
	content, err := os.ReadFile(fName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		log.Errorf("ReadExternalUSQInfo: error in reading fname=%v, err=%v", fName, err)
		return err
	}

	err = json.Unmarshal(content, &tempUSQInfo)
	if err != nil {
		log.Errorf("ReadExternalUSQInfo: json unmarshall failed fname=%v, err=%v", fName, err)
		return err
	}
	externalUSQInfoLock.Lock()
	if _, ok := externalUSQInfo[myid]; !ok {
		externalUSQInfo[myid] = make(map[string]map[string]interface{})
	}
	for usQuery, usQueryInfo := range tempUSQInfo {
		externalUSQInfo[myid][usQuery] = usQueryInfo
	}
	externalUSQInfoLock.Unlock()
	return nil
}

func SearchUserSavedQuery(ctx *fasthttp.RequestCtx, myid int64) {
	queryName := utils.ExtractParamAsString(ctx.UserValue("qname"))

	found, usqInfo, err := getUsqOne(queryName, myid)

	log.Infof("SearchUserSavedQuery: Found=%v, usqInfo=%v", found, usqInfo)
	if err != nil {
		log.Errorf("SearchUserSavedQuery: Failed to get user saved query %v, err=%v", queryName, err)
		utils.SetBadMsg(ctx, "")
		return
	}
	if !found {
		response := "Query not found"
		utils.WriteJsonResponse(ctx, response)
		ctx.SetStatusCode(fasthttp.StatusNotFound)
		return
	}

	utils.WriteJsonResponse(ctx, usqInfo)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

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

package usersavedqueries

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/imdario/mergo"

	jsoniter "github.com/json-iterator/go"
	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

var usqBaseFilename string

// map of orgid => usq lock
var usqLock map[uint64]*sync.Mutex
var usqLastReadTime map[uint64]uint64
var localUSQInfoLock *sync.RWMutex = &sync.RWMutex{}
var externalUSQInfoLock *sync.RWMutex = &sync.RWMutex{}

// map of "orgid" => queryName" ==> fieldname => fieldvalue
// e.g. "123456789" => mysave1" => {"searchText":"...", "indexName": "..."}
var localUSQInfo map[uint64]map[string]map[string]interface{} = make(map[uint64]map[string]map[string]interface{})
var externalUSQInfo map[uint64]map[string]map[string]interface{} = make(map[uint64]map[string]map[string]interface{})

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
	usqLock = make(map[uint64]*sync.Mutex)
	usqLastReadTime = make(map[uint64]uint64)

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

func acquireOrCreateLock(orgid uint64) {
	if _, ok := usqLock[orgid]; !ok {
		usqLock[orgid] = &sync.Mutex{}
	}
	usqLock[orgid].Lock()
}

func releaseLock(orgid uint64) {
	usqLock[orgid].Unlock()
}

func writeUsq(qname string, uq map[string]interface{}, orgid uint64) error {

	if qname == "" {
		log.Errorf("writeUsq: failed to save query data, with empty query name")
		return errors.New("writeUsq: failed to save query data, with empty query name")
	}

	acquireOrCreateLock(orgid)
	err := readSavedQueries(orgid)
	if err != nil {
		releaseLock(orgid)
		log.Errorf("writeUsq: failed to read sdata, err=%v", err)
		return errors.New("Internal server error, failed to read sdata")
	}
	releaseLock(orgid)
	localUSQInfoLock.Lock()
	if _, ok := localUSQInfo[orgid]; !ok {
		localUSQInfo[orgid] = make(map[string]map[string]interface{})
	}
	localUSQInfo[orgid][qname] = uq
	localUSQInfoLock.Unlock()

	err = writeSavedQueries(orgid)
	if err != nil {
		log.Errorf("writeUsq: failed to stat file, err=%v", err)
		return errors.New("Internal server error, cant write data")
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
func getUsqOne(qname string, orgid uint64) (bool, map[string]map[string]interface{}, error) {
	var allUSQInfo map[string]map[string]interface{} = make(map[string]map[string]interface{})
	retval := make(map[string]map[string]interface{})
	found := false
	acquireOrCreateLock(orgid)
	err := readSavedQueries(orgid)
	if err != nil {
		releaseLock(orgid)
		log.Errorf("GetUsqOne: failed to read, err=%v", err)
		return false, nil, err
	}
	releaseLock(orgid)

	localUSQInfoLock.RLock()
	defer localUSQInfoLock.RUnlock()
	if orgMap, ok := localUSQInfo[orgid]; ok {
		err = mergo.Merge(&allUSQInfo, &orgMap)
		if err != nil {
			log.Errorf("getUsqOne: failed to merge local saved queries, err=%v", err)
			return false, localUSQInfo[orgid], err
		}
	}

	externalUSQInfoLock.RLock()
	defer externalUSQInfoLock.RUnlock()
	if orgExternalMap, ok := externalUSQInfo[orgid]; ok {
		err = mergo.Merge(&allUSQInfo, &orgExternalMap)
		if err != nil {
			log.Errorf("getUsqOne: failed to merge external saved queries, err=%v", err)
			return false, localUSQInfo[orgid], err
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

func getUsqAll(orgid uint64) (bool, map[string]map[string]interface{}, error) {
	var allUSQInfo map[string]map[string]interface{} = make(map[string]map[string]interface{})

	acquireOrCreateLock(orgid)
	err := readSavedQueries(orgid)
	if err != nil {
		releaseLock(orgid)
		log.Errorf("GetUsqAll: failed to read, err=%v", err)
		return false, nil, err
	}
	releaseLock(orgid)

	localUSQInfoLock.RLock()
	defer localUSQInfoLock.RUnlock()
	if orgMap, ok := localUSQInfo[orgid]; ok {
		err = mergo.Merge(&allUSQInfo, &orgMap)
		if err != nil {
			log.Errorf("GetUsqAll: failed to merge local saved queries, err=%v", err)
			return false, localUSQInfo[orgid], err
		}
	}

	externalUSQInfoLock.RLock()
	defer externalUSQInfoLock.RUnlock()
	if orgExternalMap, ok := externalUSQInfo[orgid]; ok {
		err = mergo.Merge(&allUSQInfo, &orgExternalMap)
		if err != nil {
			log.Errorf("GetUsqAll: failed to merge external saved queries, err=%v", err)
			return false, localUSQInfo[orgid], err
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
func deleteAllUsq(orgid uint64) error {
	acquireOrCreateLock(orgid)
	err := readSavedQueries(orgid)
	if err != nil {
		releaseLock(orgid)
		log.Errorf("deleteAllUsq: failed to read, err=%v", err)
		return err
	}
	releaseLock(orgid)
	localUSQInfoLock.RLock()
	if _, ok := localUSQInfo[orgid]; !ok {
		localUSQInfoLock.RUnlock()
		return nil

	}
	localUSQInfoLock.RUnlock()

	localUSQInfoLock.Lock()
	delete(localUSQInfo, orgid)
	localUSQInfoLock.Unlock()

	userSavedQueriesFilename := getUsqFileName(orgid)
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
func deleteUsq(qname string, orgid uint64) (bool, error) {

	acquireOrCreateLock(orgid)
	err := readSavedQueries(orgid)
	if err != nil {
		releaseLock(orgid)
		log.Errorf("DeleteUsq: failed to read, err=%v", err)
		return false, err
	}
	releaseLock(orgid)
	localUSQInfoLock.RLock()
	if _, ok := localUSQInfo[orgid]; !ok {
		localUSQInfoLock.RUnlock()
		return false, nil

	}
	_, ok := localUSQInfo[orgid][qname]
	if !ok {
		localUSQInfoLock.RUnlock()
		return false, nil
	}
	localUSQInfoLock.RUnlock()
	localUSQInfoLock.Lock()
	delete(localUSQInfo[orgid], qname)
	localUSQInfoLock.Unlock()

	err = writeSavedQueries(orgid)
	if err != nil {
		log.Errorf("DeleteUsq: failed to write file, err=%v", err)
		return false, errors.New("Internal server error, cant write data")
	}

	return true, nil
}

/*
Caller must call this via a lock
*/
func readSavedQueries(orgid uint64) error {

	// first see if on disk is newer than memory
	usqFilename := getUsqFileName(orgid)
	fileInfo, err := os.Stat(usqFilename)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Errorf("readSavedQueries: failed to stat file, err=%v", err)
			return errors.New("Internal server error, can't read mtime")
		}
		return nil // if doesnt exist then cant really do anything
	}

	modifiedTime := uint64(fileInfo.ModTime().UTC().Unix() * 1000)
	lastReadTime, ok := usqLastReadTime[orgid]
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
	localUSQInfo[orgid] = orgSavedQueriesMap
	localUSQInfoLock.Unlock()
	usqLastReadTime[orgid] = utils.GetCurrentTimeInMs()

	return nil
}

func getUsqFileName(orgid uint64) string {
	if orgid != 0 {
		usqFilename := usqBaseFilename + "-" + strconv.FormatUint(orgid, 10) + ".bin"
		return usqFilename
	} else {
		return usqBaseFilename + ".bin"
	}
}

/*
Caller must call this via a lock
*/
func writeSavedQueries(orgid uint64) error {

	usqFilename := getUsqFileName(orgid)
	localUSQInfoLock.RLock()
	orgMap := localUSQInfo[orgid]
	localUSQInfoLock.RUnlock()
	jdata, err := json.Marshal(orgMap)
	if err != nil {
		log.Errorf("writeSavedQueries: Failed to marshall err=%v", err)
		return err
	}

	err = os.WriteFile(usqFilename, jdata, 0644)
	if err != nil {
		log.Errorf("writeSavedQueries: Failed to writefile fullname=%v, err=%v", usqFilename, err)
		return err
	}
	err = blob.UploadQueryNodeDir()
	if err != nil {
		log.Errorf("DeleteUserSavedQuery: Failed to upload query nodes dir  err=%v", err)
		return err
	}
	return nil
}

func DeleteAllUserSavedQueries(orgid uint64) error {
	err := deleteAllUsq(orgid)
	if err != nil {
		log.Errorf("DeleteAllUserSavedQueries: Failed to delete user saved queries for orgid %d, err=%v", orgid, err)
		return err
	}
	log.Infof("DeleteAllUserSavedQueries: Successfully deleted user saved queries for orgid: %d", orgid)
	err = blob.UploadQueryNodeDir()
	if err != nil {
		log.Errorf("DeleteAllUserSavedQueries: Failed to upload query nodes dir, err=%v", err)
		return err
	}

	return nil
}

func DeleteUserSavedQuery(ctx *fasthttp.RequestCtx) {
	queryName := utils.ExtractParamAsString(ctx.UserValue("qname"))
	deleted, err := deleteUsq(queryName, 0)
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
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func GetUserSavedQueriesAll(ctx *fasthttp.RequestCtx) {
	httpResp := make(utils.AllSavedQueries)
	found, savedQueriesAll, err := getUsqAll(0)
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

func SaveUserQueries(ctx *fasthttp.RequestCtx) {
	rawJSON := ctx.PostBody()
	if rawJSON == nil {
		log.Errorf("SaveUserQueries: received empty user query")
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
		log.Errorf("SaveUserQueries: failed to decode user query body! Err=%+v", err)
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
			}
		default:
			log.Errorf("SaveUserQueries: Invalid save query key=[%v]", key)
			utils.SetBadMsg(ctx, "")
			return
		}
	}
	err = writeUsq(qname, usQueryMap, 0)
	if err != nil {
		log.Errorf("SaveUserQueries: could not write query to file err=%v", err)
		utils.SetBadMsg(ctx, "")
		return
	}
	log.Infof("SaveUserQueries: successfully written query %v to file ", qname)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ReadExternalUSQInfo(fName string, orgid uint64) error {
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
	if _, ok := externalUSQInfo[orgid]; !ok {
		externalUSQInfo[orgid] = make(map[string]map[string]interface{})
	}
	for usQuery, usQueryInfo := range tempUSQInfo {
		externalUSQInfo[orgid][usQuery] = usQueryInfo
	}
	externalUSQInfoLock.Unlock()
	return nil
}

func SearchUserSavedQuery(ctx *fasthttp.RequestCtx) {
	queryName := utils.ExtractParamAsString(ctx.UserValue("qname"))

	found, usqInfo, err := getUsqOne(queryName, 0)

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

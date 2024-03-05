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

package querytracker

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"encoding/json"

	"github.com/imdario/mergo"
	jsoniter "github.com/json-iterator/go"
	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/utils"

	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

const MAX_QUERIES_TO_TRACK = 100     // this limits how many PQS searches we are doing
const MAX_CANDIDATE_QUERIES = 10_000 // this limits how many unique queries we use in our stats calculations

const STALE_QUERIES_EXPIRY_SECS = 21600 // queries will get booted out if they have not been seen in 6 hours
const STALE_SLEEP_SECS = 1800

const FLUSH_SLEEP_SECS = 120

const MAX_NUM_GROUPBY_COLS = 10

var localPersistentQueries = map[string]*PersistentSearchNode{} // map[pqid] ==> *PersistentQuery
var allNodesPQsSorted = []*PersistentSearchNode{}
var persistentInfoLock = sync.RWMutex{}
var groupByOverrideLock = sync.RWMutex{}
var localPersistentAggs = map[string]*PersistentAggregation{} // map[pqid] ==> *PersistentAggregation
var allPersistentAggsSorted = []*PersistentAggregation{}
var localGroupByOverride = map[string]*PersistentGroupBy{}

type PersistentSearchNode struct {
	SearchNode *structs.SearchNode
	PersistentInfo
}

type PersistentAggregation struct {
	QueryAggs *structs.QueryAggregators
	PersistentInfo
}

type PersistentGroupBy struct {
	GroupByCols map[string]bool
	MeasureCols map[string]bool
}

type PersistentInfo struct {
	AllTables     map[string]bool
	LocalUsage    uint32
	TotalUsage    uint32 `json:"-"`
	LastUsedEpoch uint64
	Pqid          string
}

func InitQT() {
	readSavedQueryInfo()
	go removeStaleEntries()
	go runFlushLoop()
}

func runFlushLoop() {
	for {
		time.Sleep(FLUSH_SLEEP_SECS * time.Second)
		persistentInfoLock.Lock()
		flushPQueriesToDisk()
		persistentInfoLock.Unlock()
		err := blob.UploadQueryNodeDir()
		if err != nil {
			log.Errorf("runFlushLoop: Error in uploading the query nodes dir, err: %v", err)
			continue
		}
	}
}

func removeStaleEntries() {
	for {
		time.Sleep(STALE_SLEEP_SECS * time.Second)
		removeOldEntries()
	}
}

func removeOldEntries() {
	persistentInfoLock.Lock()
	defer persistentInfoLock.Unlock()
	now := uint64(time.Now().Unix())
	totalQueries := len(allNodesPQsSorted)
	removed := uint32(0)
	for i := totalQueries - 1; i >= 0; i-- {
		if now-allNodesPQsSorted[i].LastUsedEpoch > STALE_QUERIES_EXPIRY_SECS {
			removed++
			delete(localPersistentQueries, allNodesPQsSorted[i].Pqid)
			allNodesPQsSorted = append(allNodesPQsSorted[:i], allNodesPQsSorted[i+1:]...)
		}
	}

	totalAggs := len(allPersistentAggsSorted)
	for i := totalAggs - 1; i >= 0; i-- {
		if now-allPersistentAggsSorted[i].LastUsedEpoch > STALE_QUERIES_EXPIRY_SECS {
			removed++
			delete(localPersistentQueries, allPersistentAggsSorted[i].Pqid)
			allPersistentAggsSorted = append(allPersistentAggsSorted[:i], allPersistentAggsSorted[i+1:]...)
		}
	}
	if removed > 0 {
		log.Infof("RemoveStaleEntries: removed %v stale entries, query len=%v, aggs len=%v", removed, len(allNodesPQsSorted),
			len(allPersistentAggsSorted))

		sort.Slice(allNodesPQsSorted, func(i, j int) bool {
			return allNodesPQsSorted[i].TotalUsage > allNodesPQsSorted[j].TotalUsage
		})
		sort.Slice(allPersistentAggsSorted, func(i, j int) bool {
			return allPersistentAggsSorted[i].TotalUsage > allPersistentAggsSorted[j].TotalUsage
		})
	} else {
		log.Infof("RemoveStaleEntries: removed criteria not met, query len=%v, aggs len=%+v", len(allNodesPQsSorted),
			len(allPersistentAggsSorted))
	}

}

func GetTopNPersistentSearches(intable string, orgid uint64) (map[string]*structs.SearchNode, error) {

	res := make(map[string]*structs.SearchNode)
	if !config.IsPQSEnabled() {
		return res, nil
	}

	persistentInfoLock.Lock()
	defer persistentInfoLock.Unlock()

	for pqNum, pqinfo := range allNodesPQsSorted {
		if pqNum > MAX_QUERIES_TO_TRACK {
			break
		}
		if _, ok := pqinfo.AllTables[intable]; ok {
			res[pqinfo.Pqid] = pqinfo.SearchNode
		} else {
			// if during qtupdate insertion the indexnames contained wildcard, and there was no index created
			// at the time, then that would have not expanded to real indexnames, we do it now
			found := false
			for idxname := range pqinfo.AllTables {
				indexNamesRetrieved := vtable.ExpandAndReturnIndexNames(idxname, orgid, false)
				for _, t := range indexNamesRetrieved {
					pqinfo.AllTables[t] = true // for future so that we don't enter this idxname expansion block
					if t == intable {
						res[pqinfo.Pqid] = pqinfo.SearchNode
						found = true
						break // inner for loop exit
					}
				}
				if found {
					break // outer for loop exit
				}
			}
		}
	}

	return res, nil
}

func GetPersistentColumns(intable string, orgid uint64) (map[string]bool, error) {
	persistentQueries, err := GetTopNPersistentSearches(intable, orgid)

	if err != nil {
		log.Errorf("GetPersistentColumns: error getting persistent queries: %v", err)
		return map[string]bool{}, err
	}

	pqsCols := make(map[string]bool)
	for _, searchNode := range persistentQueries {
		allColumns, _ := searchNode.GetAllColumnsToSearch()
		for col := range allColumns {
			pqsCols[col] = true
		}
	}

	return pqsCols, nil
}

type colUsage struct {
	col   string
	usage int
}

// returns a sorted slice of most used group by columns, and all measure columns.
func GetTopPersistentAggs(table string) ([]string, map[string]bool) {
	groupByColsUsage := make(map[string]int)
	measureInfoUsage := make(map[string]bool)

	if !config.IsPQSEnabled() {
		return []string{}, measureInfoUsage
	}
	overrideGroupByCols := make([]string, 0)
	persistentInfoLock.Lock()
	defer persistentInfoLock.Unlock()

	if strings.HasPrefix(table, "jaeger-") {
		overrideGroupByCols = append(overrideGroupByCols, "traceID", "serviceName", "operationName")
		measureInfoUsage["startTime"] = true
	}

	if _, ok := localGroupByOverride[table]; ok {
		if localGroupByOverride[table].GroupByCols != nil {
			cols := localGroupByOverride[table].GroupByCols
			for col := range cols {
				overrideGroupByCols = append(overrideGroupByCols, col)
			}
		}
		if localGroupByOverride[table].MeasureCols != nil {
			mcols := localGroupByOverride[table].MeasureCols
			for m := range mcols {
				measureInfoUsage[m] = true
			}
		}
	}

	for idx, agginfo := range allPersistentAggsSorted {
		if idx > MAX_QUERIES_TO_TRACK {
			break
		}
		if _, ok := agginfo.AllTables[table]; !ok {
			continue
		}
		queryAggs := agginfo.QueryAggs
		if queryAggs == nil || queryAggs.GroupByRequest == nil {
			continue
		}
		cols := queryAggs.GroupByRequest.GroupByColumns
		for _, col := range cols {
			// groupby columns from more popular queries should get more preference, so use usage count
			groupByColsUsage[col] += int(agginfo.TotalUsage)
		}
		measureInfo := queryAggs.GroupByRequest.MeasureOperations
		for _, m := range measureInfo {
			measureInfoUsage[m.MeasureCol] = true
		}
	}
	var ss []colUsage
	for k, v := range groupByColsUsage {
		ss = append(ss, colUsage{k, v})
	}
	sort.Slice(ss, func(i, j int) bool {
		return ss[i].usage > ss[j].usage
	})
	var finalCols []string
	if len(overrideGroupByCols) >= MAX_NUM_GROUPBY_COLS {
		finalCols = make([]string, MAX_NUM_GROUPBY_COLS)
		finalCols = append(finalCols, overrideGroupByCols[:MAX_NUM_GROUPBY_COLS]...)
	} else {
		finalCols = append(finalCols, overrideGroupByCols[:]...)
		for _, s := range ss {
			if len(finalCols) <= MAX_NUM_GROUPBY_COLS {
				finalCols = append(finalCols, s.col)
			} else {
				break
			}
		}
	}
	return finalCols, measureInfoUsage
}

func UpdateQTUsage(tableName []string, sn *structs.SearchNode, aggs *structs.QueryAggregators) {

	if len(tableName) == 0 {
		return
	}

	persistentInfoLock.Lock()
	defer persistentInfoLock.Unlock()
	updateSearchNodeUsage(tableName, sn)
	updateAggsUsage(tableName, aggs)
}

func updateSearchNodeUsage(tableName []string, sn *structs.SearchNode) {

	if sn == nil {
		return
	}
	if sn.NodeType == structs.MatchAllQuery {
		return
	}

	pqid := GetHashForQuery(sn)

	var pqinfo *PersistentSearchNode
	var ok bool
	pqinfo, ok = localPersistentQueries[pqid]
	if !ok {
		if len(localPersistentQueries) >= MAX_CANDIDATE_QUERIES {
			log.Infof("updateSearchNodeUsage: reached limit %v for candidate queries, booting last one", MAX_CANDIDATE_QUERIES)
			delete(localPersistentQueries, allNodesPQsSorted[len(allNodesPQsSorted)-1].Pqid)
			allNodesPQsSorted = allNodesPQsSorted[:len(allNodesPQsSorted)-1]
		}
		pInfo := PersistentInfo{AllTables: make(map[string]bool), Pqid: pqid}
		pqinfo = &PersistentSearchNode{SearchNode: sn}
		pqinfo.PersistentInfo = pInfo
		localPersistentQueries[pqid] = pqinfo
		allNodesPQsSorted = append(allNodesPQsSorted, pqinfo)
		log.Infof("updateSearchNodeUsage: added pqid %v, total=%v, tableName=%v",
			pqid, len(localPersistentQueries), tableName)

	}

	pqinfo.LastUsedEpoch = uint64(time.Now().Unix())
	pqinfo.TotalUsage++
	pqinfo.LocalUsage++
	for _, tName := range tableName {
		pqinfo.AllTables[tName] = true
	}

	sort.Slice(allNodesPQsSorted, func(i, j int) bool {
		return allNodesPQsSorted[i].TotalUsage > allNodesPQsSorted[j].TotalUsage
	})
}

func updateAggsUsage(tableName []string, aggs *structs.QueryAggregators) {

	if aggs == nil || aggs.IsAggsEmpty() {
		return
	}

	pqid := GetHashForAggs(aggs)

	var pqinfo *PersistentAggregation
	var ok bool
	pqinfo, ok = localPersistentAggs[pqid]
	if !ok {
		if len(localPersistentAggs) >= MAX_CANDIDATE_QUERIES {
			log.Infof("updateAggsUsage: reached limit %v for candidate queries, booting last one", MAX_CANDIDATE_QUERIES)
			delete(localPersistentAggs, allPersistentAggsSorted[len(allPersistentAggsSorted)-1].Pqid)
			allPersistentAggsSorted = allPersistentAggsSorted[:len(allPersistentAggsSorted)-1]
		}
		pInfo := PersistentInfo{AllTables: make(map[string]bool), Pqid: pqid}
		pqinfo = &PersistentAggregation{QueryAggs: aggs}
		pqinfo.PersistentInfo = pInfo
		localPersistentAggs[pqid] = pqinfo
		allPersistentAggsSorted = append(allPersistentAggsSorted, pqinfo)
		log.Infof("updateAggsUsage: added pqid %v, total=%v, tableName=%v",
			pqid, len(localPersistentAggs), tableName)

	}

	pqinfo.LastUsedEpoch = uint64(time.Now().Unix())
	pqinfo.TotalUsage++
	pqinfo.LocalUsage++
	for _, tName := range tableName {
		pqinfo.AllTables[tName] = true
	}

	sort.Slice(allPersistentAggsSorted, func(i, j int) bool {
		return allPersistentAggsSorted[i].TotalUsage > allPersistentAggsSorted[j].TotalUsage
	})
}

func GetQTUsageInfo(tableName []string, sn *structs.SearchNode) (*PersistentSearchNode, error) {

	if sn == nil {
		return nil, errors.New("sn was nil")
	}

	pqid := GetHashForQuery(sn)

	persistentInfoLock.RLock()
	defer persistentInfoLock.RUnlock()

	pqinfo, ok := localPersistentQueries[pqid]
	if ok {
		return pqinfo, nil
	} else {
		for _, pqinfo := range allNodesPQsSorted {
			if pqinfo.Pqid == pqid {
				return pqinfo, nil
			}
		}
	}

	return nil, errors.New("pqid not found")
}

func IsQueryPersistent(tableName []string, sn *structs.SearchNode) (bool, error) {

	if sn == nil {
		return false, errors.New("sn was nil")
	}

	pqid := GetHashForQuery(sn)

	persistentInfoLock.RLock()
	defer persistentInfoLock.RUnlock()
	pqInfo, ok := localPersistentQueries[pqid]

	if !ok {
		for _, pqinfo := range allNodesPQsSorted {
			if pqinfo.Pqid == pqid {
				return true, nil
			}
		}
		return false, nil
	}

	found := false
	for _, idx := range tableName {
		if _, ok := pqInfo.AllTables[idx]; ok {
			found = true
			break
		}
	}

	if found {
		// we found it but make sure it is in top 100 queries
		totallen := len(allNodesPQsSorted)
		for i := 0; i < MAX_QUERIES_TO_TRACK && i < totallen; i++ {
			if allNodesPQsSorted[i].Pqid == pqid {
				return true, nil
			}
		}
	}

	return false, nil
}

func flushPQueriesToDisk() {
	var sb strings.Builder
	sb.WriteString(config.GetDataPath() + "querynodes/" + config.GetHostID() + "/pqueries/")
	baseDir := sb.String()

	err := os.MkdirAll(baseDir, 0764)
	if err != nil {
		log.Errorf("flushPQueriesToDisk: failed to create basedir=%v, err=%v", baseDir, err)
		return
	}

	queryfName := baseDir + "pqinfo.bin"
	queryFD, err := os.OpenFile(queryfName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("flushPQueriesToDisk: Failed to open pqinfo file=%v, err=%v", queryfName, err)
		return
	}
	defer queryFD.Close()
	jdata, err := json.Marshal(&localPersistentQueries)
	if err != nil {
		log.Errorf("flushPQueriesToDisk: json marshalling failed fname=%v, err=%v", queryfName, err)
		return
	}
	// todo encode in binary form before writing
	if _, err = queryFD.Write(jdata); err != nil {
		log.Errorf("flushPQueriesToDisk: write failed fname=%v, err=%v", queryfName, err)
		return
	}

	aggsfName := baseDir + "aggsinfo.bin"
	aggsFD, err := os.OpenFile(aggsfName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("flushPQueriesToDisk: Failed to open pqinfo file=%v, err=%v", aggsfName, err)
		return
	}
	defer aggsFD.Close()
	adata, err := json.Marshal(localPersistentAggs)
	if err != nil {
		log.Errorf("flushPQueriesToDisk: json marshalling failed fname=%v, err=%v", aggsfName, err)
		return
	}
	// todo encode in binary form before writing
	if _, err = aggsFD.Write(adata); err != nil {
		log.Errorf("flushPQueriesToDisk: write failed fname=%v, err=%v", aggsfName, err)
		return
	}

	groupbyAggsFName := baseDir + "groupinfo.bin"
	fd, err := os.OpenFile(groupbyAggsFName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("flushPQueriesToDisk: Failed to open  file=%v, err=%v", groupbyAggsFName, err)
		return
	}
	defer fd.Close()
	gdata, err := json.Marshal(localGroupByOverride)
	if err != nil {
		log.Errorf("flushPQueriesToDisk: json marshalling failed fname=%v, err=%v", groupbyAggsFName, err)
		return
	}
	// todo encode in binary form before writing
	if _, err = fd.Write(gdata); err != nil {
		log.Errorf("flushPQueriesToDisk: write failed fname=%v, err=%v", groupbyAggsFName, err)
		return
	}
}

func readSavedQueryInfo() {

	var sb strings.Builder
	sb.WriteString(config.GetDataPath() + "querynodes/" + config.GetHostID() + "/pqueries/")
	baseDir := sb.String()

	persistentInfoLock.Lock()
	defer persistentInfoLock.Unlock()

	queryfName := baseDir + "pqinfo.bin"
	content, err := os.ReadFile(queryfName)
	if err != nil {
		return
	}
	err = json.Unmarshal(content, &localPersistentQueries)
	if err != nil {
		log.Errorf("readSavedPQueries: json unmarshall failed fname=%v, err=%v", queryfName, err)
		localPersistentQueries = make(map[string]*PersistentSearchNode)
		return
	}

	allNodesPQsSorted = make([]*PersistentSearchNode, 0)
	for _, pqinfo := range localPersistentQueries {
		allNodesPQsSorted = append(allNodesPQsSorted, pqinfo)
	}

	for _, pqinfo := range allNodesPQsSorted {
		pqinfo.SearchNode.AddQueryInfoForNode()
		localPersistentQueries[pqinfo.Pqid] = pqinfo
	}

	sort.Slice(allNodesPQsSorted, func(i, j int) bool {
		return allNodesPQsSorted[i].TotalUsage > allNodesPQsSorted[j].TotalUsage
	})

	log.Infof("readSavedPQueries: read %v queries into pqinfo", len(allNodesPQsSorted))

	aggsfName := baseDir + "aggsinfo.bin"
	content, err = os.ReadFile(aggsfName)
	if err != nil {
		return
	}
	err = json.Unmarshal(content, &localPersistentAggs)
	if err != nil {
		log.Errorf("readSavedPQueries: json unmarshall failed fname=%v, err=%v", aggsfName, err)
		localPersistentAggs = make(map[string]*PersistentAggregation)
		return
	}

	allPersistentAggsSorted = make([]*PersistentAggregation, 0)
	for _, pqinfo := range localPersistentAggs {
		allPersistentAggsSorted = append(allPersistentAggsSorted, pqinfo)
	}

	for _, pqinfo := range allPersistentAggsSorted {
		localPersistentAggs[pqinfo.Pqid] = pqinfo
	}

	sort.Slice(allPersistentAggsSorted, func(i, j int) bool {
		return allPersistentAggsSorted[i].TotalUsage > allPersistentAggsSorted[j].TotalUsage
	})

	log.Infof("readSavedPQueries: read %v aggs into pqinfo", len(allPersistentAggsSorted))

	groupByfName := baseDir + "groupinfo.bin"
	content, err = os.ReadFile(groupByfName)
	if err != nil {
		return
	}
	err = json.Unmarshal(content, &localGroupByOverride)
	if err != nil {
		log.Errorf("readSavedPQueries: json unmarshall failed fname=%v, err=%v", groupByfName, err)
		localGroupByOverride = make(map[string]*PersistentGroupBy)
		return
	}
	log.Infof("readSavedPQueries: read %v groupby aggs", len(localGroupByOverride))
}

func GetPQSSummary(ctx *fasthttp.RequestCtx) {
	response := getPQSSummary()
	utils.WriteJsonResponse(ctx, response)
	ctx.Response.Header.Set("Content-Type", "application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func getPQSSummary() map[string]interface{} {
	persistentInfoLock.RLock()
	defer persistentInfoLock.RUnlock()

	response := make(map[string]interface{})
	numQueriesInPQS := len(allNodesPQsSorted)
	response["total_tracked_queries"] = numQueriesInPQS
	pqidUsageCount := make(map[string]int)
	for idx, pqinfo := range allNodesPQsSorted {
		if idx > MAX_QUERIES_TO_TRACK {
			continue
		}
		pqidUsageCount[pqinfo.Pqid] = int(pqinfo.TotalUsage)
	}
	response["promoted_searches"] = pqidUsageCount
	aggsUsageCount := make(map[string]int)
	for idx, pqinfo := range allPersistentAggsSorted {
		if idx > MAX_QUERIES_TO_TRACK {
			continue
		}
		aggsUsageCount[pqinfo.Pqid] = int(pqinfo.TotalUsage)
	}
	response["promoted_aggregations"] = aggsUsageCount
	return response
}

// writes the json converted search node
func GetPQSById(ctx *fasthttp.RequestCtx) {
	pqid := utils.ExtractParamAsString(ctx.UserValue("pqid"))
	finalResult := getPqsById(pqid)
	if finalResult == nil {
		err := getAggPQSById(ctx, pqid)
		if err != nil {
			var httpResp utils.HttpServerResponse
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			httpResp.Message = fmt.Sprintf("pqid %+v does not exist", pqid)
			httpResp.StatusCode = fasthttp.StatusBadRequest
			utils.WriteResponse(ctx, httpResp)
		}
		return
	}

	utils.WriteJsonResponse(ctx, &finalResult)
	ctx.Response.Header.Set("Content-Type", "application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func getPqsById(pqid string) map[string]interface{} {
	persistentInfoLock.RLock()
	defer persistentInfoLock.RUnlock()
	// TODO: aggs support
	pqinfo, exists := localPersistentQueries[pqid]
	if !exists {
		for _, info := range allNodesPQsSorted {
			if info.Pqid == pqid {
				pqinfo = info
			}
		}
	}

	var finalResult map[string]interface{}
	if pqinfo != nil {
		sNode := pqinfo.SearchNode
		var convertedSNode map[string]interface{}
		converted, _ := json.Marshal(sNode)
		_ = json.Unmarshal(converted, &convertedSNode)

		finalResult = make(map[string]interface{})
		finalResult["pqid"] = pqinfo.Pqid
		finalResult["last_used_epoch"] = pqinfo.LastUsedEpoch
		finalResult["total_usage"] = pqinfo.TotalUsage
		finalResult["virtual_tables"] = pqinfo.AllTables
		finalResult["search_node"] = convertedSNode
	}
	return finalResult
}

func getAggPQSById(ctx *fasthttp.RequestCtx, pqid string) error {
	pqinfo, exists := localPersistentAggs[pqid]
	if !exists {
		for _, info := range allPersistentAggsSorted {
			if info.Pqid == pqid {
				pqinfo = info
			}
		}
	}

	if pqinfo == nil {
		return fmt.Errorf("pqid %+s does not exist in aggs", pqid)
	}
	sNode := pqinfo.QueryAggs
	var convertedAggs map[string]interface{}
	converted, _ := json.Marshal(sNode)
	_ = json.Unmarshal(converted, &convertedAggs)

	finalResult := make(map[string]interface{})
	finalResult["pqid"] = pqinfo.Pqid
	finalResult["last_used_epoch"] = pqinfo.LastUsedEpoch
	finalResult["total_usage"] = pqinfo.TotalUsage
	finalResult["virtual_tables"] = pqinfo.AllTables
	finalResult["search_aggs"] = convertedAggs

	utils.WriteJsonResponse(ctx, &finalResult)
	ctx.Response.Header.Set("Content-Type", "application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
	return nil
}

func RefreshExternalPQInfo(fNames []string) error {

	allNodesPQs := make(map[string]*PersistentSearchNode)
	persistentInfoLock.Lock()
	defer persistentInfoLock.Unlock()

	for _, file := range fNames {
		var tempPersistentQueries = map[string]*PersistentSearchNode{}
		content, err := os.ReadFile(file)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			log.Errorf("RefreshExternalPQInfo: error in reading fname=%v, err=%v", file, err)
			return err
		}

		err = json.Unmarshal(content, &tempPersistentQueries)
		if err != nil {
			log.Errorf("RefreshExternalPQInfo: json unmarshall failed fname=%v, err=%v", file, err)
			return err
		}

		for pqid, pqinfo := range tempPersistentQueries {
			val, present := allNodesPQs[pqid]

			if !present {
				pqinfo.TotalUsage = pqinfo.LocalUsage
				allNodesPQs[pqid] = pqinfo
			} else {
				val.TotalUsage = val.TotalUsage + pqinfo.LocalUsage

				// merge Alltables
				err = mergo.Merge(&val.AllTables, pqinfo.AllTables)
				if err != nil {
					log.Errorf("RefreshExternalPQInfo: error in merging Alltables, err=%v", err)
					return err
				}
			}
		}
	}
	allNodesPQsSorted = make([]*PersistentSearchNode, 0)
	for _, pqinfo := range localPersistentQueries {
		allNodesPQsSorted = append(allNodesPQsSorted, pqinfo)
	}

	for pqid, pqinfo := range allNodesPQs {
		val, present := localPersistentQueries[pqid]
		if present {
			val.TotalUsage = val.LocalUsage + pqinfo.TotalUsage
		} else {
			allNodesPQsSorted = append(allNodesPQsSorted, pqinfo)
		}
	}

	//Sort the slice in descending order of TotalUsage
	sort.Slice(allNodesPQsSorted, func(i, j int) bool {
		return allNodesPQsSorted[i].TotalUsage > allNodesPQsSorted[j].TotalUsage
	})
	return nil
}

func RefreshExternalAggsInfo(fNames []string) error {
	allNodesAggs := make(map[string]*PersistentAggregation)
	persistentInfoLock.Lock()
	defer persistentInfoLock.Unlock()

	for _, file := range fNames {
		var tempAggs = map[string]*PersistentAggregation{}
		content, err := os.ReadFile(file)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			log.Errorf("RefreshExternalAggsInfo: error in reading fname=%v, err=%v", file, err)
			return err
		}

		err = json.Unmarshal(content, &tempAggs)
		if err != nil {
			log.Errorf("RefreshExternalAggsInfo: json unmarshall failed fname=%v, err=%v", file, err)
			return err
		}

		for pqid, pqinfo := range tempAggs {
			val, present := allNodesAggs[pqid]

			if !present {
				pqinfo.TotalUsage = pqinfo.LocalUsage
				allNodesAggs[pqid] = pqinfo
			} else {
				val.TotalUsage = val.TotalUsage + pqinfo.LocalUsage

				// merge Alltables
				err = mergo.Merge(&val.AllTables, pqinfo.AllTables)
				if err != nil {
					log.Errorf("RefreshExternalAggsInfo: error in merging Alltables, err=%v", err)
					return err
				}
			}
		}
	}
	allPersistentAggsSorted = make([]*PersistentAggregation, 0)
	for _, pqinfo := range localPersistentAggs {
		allPersistentAggsSorted = append(allPersistentAggsSorted, pqinfo)
	}

	for pqid, aggsInfo := range allNodesAggs {
		val, present := localPersistentAggs[pqid]
		if present {
			val.TotalUsage = val.LocalUsage + aggsInfo.TotalUsage
		} else {
			allPersistentAggsSorted = append(allPersistentAggsSorted, aggsInfo)
		}
	}

	//Sort the slice in descending order of TotalUsage
	sort.Slice(allNodesPQsSorted, func(i, j int) bool {
		return allNodesPQsSorted[i].TotalUsage > allNodesPQsSorted[j].TotalUsage
	})
	return nil
}

func PostPqsClear(ctx *fasthttp.RequestCtx) {
	ClearPqs()
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ClearPqs() {
	persistentInfoLock.Lock()
	localPersistentQueries = make(map[string]*PersistentSearchNode)
	allNodesPQsSorted = make([]*PersistentSearchNode, 0)

	localPersistentAggs = make(map[string]*PersistentAggregation)
	allPersistentAggsSorted = make([]*PersistentAggregation, 0)
	persistentInfoLock.Unlock()

	groupByOverrideLock.Lock()
	localGroupByOverride = make(map[string]*PersistentGroupBy)
	groupByOverrideLock.Unlock()

	flushPQueriesToDisk()
}

func PostPqsAggCols(ctx *fasthttp.RequestCtx) {
	var httpResp utils.HttpServerResponse
	rawJSON := ctx.PostBody()
	if rawJSON == nil {
		log.Errorf("PostPqsAggCols: received empty request")
		utils.SetBadMsg(ctx, "Empty post body")
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
			log.Errorf("PostPqsAggCols: could not write error message err=%v", err)
		}
		log.Errorf("PostPqsAggCols: failed to decode request body! Err=%+v", err)
	}

	err = parsePostPqsAggBody(readJSON)

	if err != nil {
		utils.SetBadMsg(ctx, err.Error())
	} else {
		ctx.SetStatusCode(fasthttp.StatusOK)
		httpResp.Message = "All OK"
		httpResp.StatusCode = fasthttp.StatusOK
	}
	utils.WriteResponse(ctx, httpResp)
}

func parsePostPqsAggBody(jsonSource map[string]interface{}) error {
	var tableName string
	var err error
	groupByColsMap := make(map[string]bool)
	measureColsMaps := make(map[string]bool)
	groupByOverrideLock.Lock()
	defer groupByOverrideLock.Unlock()
	for key, value := range jsonSource {
		switch valtype := value.(type) {
		case string:
			if key == "tableName" {
				tableName = valtype
				if tableName == "*" {
					err := errors.New("PostPqsAggCols: tableName can not be *")
					log.Errorf("%+v", err)
					return err
				}
			}
		case []interface{}:
			switch key {
			case "groupByColumns":
				{
					groupByColsMap, err = processPostAggs(valtype)
					if err != nil {
						log.Errorf("PostPqsAggCols:processPostAggs error %v", err)
						return err
					}
				}
			case "measureColumns":
				{
					measureColsMaps, err = processPostAggs(valtype)
					if err != nil {
						log.Errorf("PostPqsAggCols:processPostAggs error %v", err)
						return err
					}
				}
			}
		default:
			log.Errorf("PostPqsAggCols: Invalid key=[%v]", key)
			err := fmt.Sprintf("PostPqsAggCols: Invalid key=[%v]", key)
			return errors.New(err)
		}
	}
	if _, ok := localGroupByOverride[tableName]; ok {
		entry := localGroupByOverride[tableName]
		for cname := range entry.GroupByCols {
			groupByColsMap[cname] = true
		}
		for mcname := range entry.MeasureCols {
			measureColsMaps[mcname] = true
		}

	}
	pqsAggs := &PersistentGroupBy{GroupByCols: groupByColsMap, MeasureCols: measureColsMaps}
	localGroupByOverride[tableName] = pqsAggs
	return nil
}
func processPostAggs(inputValueParam interface{}) (map[string]bool, error) {
	switch inputValueParam.(type) {
	case []interface{}:
		break
	default:
		err := fmt.Errorf("processPostAggs type = %T not accepted", inputValueParam)
		return nil, err
	}
	evMap := make(map[string]bool)
	for _, element := range inputValueParam.([]interface{}) {
		switch element := element.(type) {
		case string:
			evMap[element] = true
		default:
			err := fmt.Errorf("processPostAggs type = %T not accepted", element)
			return nil, err
		}
	}
	return evMap, nil
}

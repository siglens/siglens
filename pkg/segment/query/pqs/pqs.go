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

package pqs

import (
	"errors"
	"fmt"
	"sync"

	blob "github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/blob/ssutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	"github.com/siglens/siglens/pkg/segment/structs"
	log "github.com/sirupsen/logrus"
)

type PersistentQueryResults struct {
	segKey       string // segment key
	tableName    string // table name for segKey
	pqid         string // persistent query id
	pqmrFilePath string // raw file path for pqmr file
}

// segKey -> [qid -> PersistentQueryResults]
var allPersistentQueryResults map[string]map[string]*PersistentQueryResults
var allPersistentQueryResultsLock sync.RWMutex

func init() {
	allPersistentQueryResults = make(map[string]map[string]*PersistentQueryResults)
	allPersistentQueryResultsLock = sync.RWMutex{}
}

// base func to add & read from segmeta updates
func AddPersistentQueryResult(segKey string, tableName string, pqid string) {

	if !config.IsPQSEnabled() {
		return
	}
	ssFile := structs.SegSetFile{
		SegKey:     segKey,
		Identifier: "",
		FileType:   structs.Pqmr,
	}
	fName := ssutils.GetFileNameFromSegSetFile(ssFile)
	allPersistentQueryResultsLock.Lock()
	if _, ok := allPersistentQueryResults[segKey]; !ok {
		allPersistentQueryResults[segKey] = make(map[string]*PersistentQueryResults)
	}

	allPersistentQueryResults[segKey][pqid] = &PersistentQueryResults{
		segKey:       segKey,
		pqid:         pqid,
		pqmrFilePath: fName,
		tableName:    tableName,
	}
	allPersistentQueryResultsLock.Unlock()
}

func getPQResults(segKey string, pqid string) (*PersistentQueryResults, error) {
	allPersistentQueryResultsLock.RLock()
	defer allPersistentQueryResultsLock.RUnlock()
	if _, ok := allPersistentQueryResults[segKey]; !ok {
		return nil, errors.New("segKey does not have any persistent query results")
	}

	var pqResults *PersistentQueryResults
	var ok bool
	if pqResults, ok = allPersistentQueryResults[segKey][pqid]; !ok {
		return nil, errors.New("pqid does not exist for segKey")
	}
	return pqResults, nil
}

func GetAllPersistentQueryResults(segKey string, pqid string) (*pqmr.SegmentPQMRResults, error) {

	pqResults, err := getPQResults(segKey, pqid)
	if err != nil {
		return nil, err
	}
	fName := fmt.Sprintf("%v/pqmr/%v.pqmr", segKey, pqResults.pqid)
	err = blob.DownloadSegmentBlob(fName, false)
	if err != nil {
		log.Errorf("Failed to download PQMR results! SegKey: %+v, pqid: %+v", segKey, pqResults.pqid)
		return nil, errors.New("failed to download PQMR results")
	}

	pqmrResults, err := pqmr.ReadPqmr(&fName)
	if err != nil {
		log.Errorf("Failed to read PQMR results! From file %+v. Error: %+v", fName, err)
		return nil, err
	}
	return pqmrResults, nil
}

// Returns if segKey, pqid combination exists
func DoesSegKeyHavePqidResults(segKey string, pqid string) bool {

	_, err := getPQResults(segKey, pqid)
	return err == nil
}

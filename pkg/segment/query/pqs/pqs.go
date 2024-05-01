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

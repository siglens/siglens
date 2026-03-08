// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package pqs

import (
	"errors"
	"fmt"
	"sync"

	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

// segKey -> [pqid] -> bool
var allPersistentQueryResults map[string]map[string]bool
var allPersistentQueryResultsLock sync.RWMutex

func init() {
	allPersistentQueryResults = make(map[string]map[string]bool)
	allPersistentQueryResultsLock = sync.RWMutex{}
}

// base func to add & read from segmeta updates
func AddPersistentQueryResult(segKey string, pqid string) {

	isPqidPresent := DoesSegKeyHavePqidResults(segKey, pqid)
	if isPqidPresent {
		return
	}

	allPersistentQueryResultsLock.Lock()
	if _, ok := allPersistentQueryResults[segKey]; !ok {
		allPersistentQueryResults[segKey] = make(map[string]bool)
	}
	allPersistentQueryResults[segKey][pqid] = true
	allPersistentQueryResultsLock.Unlock()
}

func GetAllPersistentQueryResults(segKey string, pqid string) (*pqmr.SegmentPQMRResults, error) {

	isPqidPresent := DoesSegKeyHavePqidResults(segKey, pqid)
	if !isPqidPresent {
		return nil, errors.New("segKey does not have any persistent query results")
	}

	fName := fmt.Sprintf("%v/pqmr/%v.pqmr", segKey, pqid)
	err := blob.DownloadSegmentBlob(fName, false)
	if err != nil {
		log.Errorf("GetAllPersistentQueryResults: Failed to download PQMR results! SegKey: %+v, pqid: %+v, file name: %v", segKey, pqid, fName)
		return nil, errors.New("failed to download PQMR results")
	}

	pqmrResults, err := pqmr.ReadPqmr(&fName)
	if err != nil {
		log.Errorf("GetAllPersistentQueryResults: Failed to read PQMR results! From file %+v. Error: %+v", fName, err)
		return nil, err
	}
	return pqmrResults, nil
}

// Returns if segKey, pqid combination exists
func DoesSegKeyHavePqidResults(segKey string, pqid string) bool {

	if !config.IsPQSEnabled() {
		return false
	}

	isPqidPresent := false
	allPersistentQueryResultsLock.RLock()
	_, ok := allPersistentQueryResults[segKey]
	if ok {
		_, ok := allPersistentQueryResults[segKey][pqid]
		if ok {
			isPqidPresent = true
		}
	}
	allPersistentQueryResultsLock.RUnlock()
	return isPqidPresent
}

func AddAllPqidResults(segKey string, allPqids map[string]bool) {
	if !config.IsPQSEnabled() {
		return
	}

	allPersistentQueryResultsLock.Lock()
	if _, ok := allPersistentQueryResults[segKey]; !ok {
		allPersistentQueryResults[segKey] = make(map[string]bool)
	}
	utils.MergeMapsRetainingFirst(allPersistentQueryResults[segKey], allPqids)
	allPersistentQueryResultsLock.Unlock()
}

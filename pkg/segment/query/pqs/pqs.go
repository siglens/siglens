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
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	log "github.com/sirupsen/logrus"
)

// segKey -> [qid] -> struct{}
var allPersistentQueryResults map[string]map[string]struct{}
var allPersistentQueryResultsLock sync.RWMutex

func init() {
	allPersistentQueryResults = make(map[string]map[string]struct{})
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
		allPersistentQueryResults[segKey] = make(map[string]struct{})
	}
	allPersistentQueryResults[segKey][pqid] = struct{}{}
	allPersistentQueryResultsLock.Unlock()
}

func GetAllPersistentQueryResults(segKey string, pqid string) (*pqmr.SegmentPQMRResults, error) {

	isPqidPresent := DoesSegKeyHavePqidResults(segKey, pqid)
	if isPqidPresent {
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

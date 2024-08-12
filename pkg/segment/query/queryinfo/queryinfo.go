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

package queryinfo

import (
	"sync"

	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type queryInfo struct {
	lock                     sync.RWMutex
	dictColsToNoResultHashes map[string]map[uint64]struct{}
}

var qidToQueryInfo = make(map[uint64]*queryInfo)
var mapLock = sync.RWMutex{}

func AddQueryInfo(qid uint64) {
	mapLock.Lock()
	defer mapLock.Unlock()

	qidToQueryInfo[qid] = &queryInfo{
		dictColsToNoResultHashes: make(map[string]map[uint64]struct{}),
	}
}

func DeleteQueryInfo(qid uint64) {
	mapLock.Lock()
	defer mapLock.Unlock()

	delete(qidToQueryInfo, qid)
}

func getQueryInfo(qid uint64) *queryInfo {
	mapLock.RLock()
	defer mapLock.RUnlock()

	return qidToQueryInfo[qid]
}

func AddDictValuesHashGivingNoResults(qid uint64, colName string, hash uint64) {
	queryInfo := getQueryInfo(qid)
	if queryInfo == nil {
		log.Errorf("AddDictValuesHashGivingNoResults: qid %+v does not exist!", qid)
		return
	}

	queryInfo.lock.Lock()
	defer queryInfo.lock.Unlock()

	if _, ok := queryInfo.dictColsToNoResultHashes[colName]; !ok {
		queryInfo.dictColsToNoResultHashes[colName] = make(map[uint64]struct{})
	}

	queryInfo.dictColsToNoResultHashes[colName][hash] = struct{}{}
}

func GetDictValuesHashGivingNoResults(qid uint64, colName string) (map[uint64]struct{}, error) {
	queryInfo := getQueryInfo(qid)
	if queryInfo == nil {
		return nil, utils.TeeErrorf("GetDictValuesHashGivingNoResults: qid %+v does not exist!", qid)
	}

	queryInfo.lock.Lock()
	defer queryInfo.lock.Unlock()

	if _, ok := queryInfo.dictColsToNoResultHashes[colName]; !ok {
		return nil, nil
	}

	return queryInfo.dictColsToNoResultHashes[colName], nil
}

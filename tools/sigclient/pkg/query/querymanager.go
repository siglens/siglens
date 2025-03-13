// Copyright (c) 2021-2025 SigScalr, Inc.
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

package query

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"verifier/pkg/utils"

	log "github.com/sirupsen/logrus"
)

const delayForFlush = 30 * time.Second

type QueryTemplate struct {
	validator        queryValidator
	timeRangeSeconds uint64
	maxInProgress    int
}

type queryManager struct {
	templates         []*QueryTemplate
	inProgressQueries []queryValidator
	runnableQueries   []queryValidator
	runnableLock      sync.Mutex
	templateChan      chan *QueryTemplate

	maxConcurrentQueries int32
	numRunningQueries    atomic.Int32

	lastLogEpochMs int64

	url string
}

func NewQueryTemplate(validator queryValidator, timeRangeSeconds uint64, maxInProgress int) *QueryTemplate {
	return &QueryTemplate{
		validator:        validator,
		timeRangeSeconds: timeRangeSeconds,
		maxInProgress:    maxInProgress,
	}
}

func NewQueryManager(templates []*QueryTemplate, maxConcurrentQueries int32, url string) *queryManager {
	manager := &queryManager{
		templates:            templates,
		inProgressQueries:    make([]queryValidator, 0),
		runnableQueries:      make([]queryValidator, 0),
		templateChan:         make(chan *QueryTemplate),
		maxConcurrentQueries: maxConcurrentQueries,
		url:                  url,
	}

	manager.spawnTemplateAdders()

	return manager
}

func (qm *queryManager) spawnTemplateAdders() {
	for _, template := range qm.templates {
		// Space out the queries.
		go func(template *QueryTemplate) {
			seconds := template.timeRangeSeconds / uint64(template.maxInProgress)
			seconds = max(seconds, 1)
			ticker := time.NewTicker(time.Duration(seconds) * time.Second)
			for range ticker.C {
				qm.templateChan <- template
			}
		}(template)
	}
}

func (qm *queryManager) HandleIngestedLogs(logs []map[string]interface{}) {
	qm.addInProgessQueries()
	qm.sendToValidators(logs)

	if lastEpoch, ok := qm.getLastEpoch(logs); ok {
		qm.lastLogEpochMs = int64(lastEpoch)
		qm.moveToRunnable(lastEpoch)
	}

	if qm.canRunMore() {
		qm.startQueries()
	}
}

func (qm *queryManager) addInProgessQueries() {
	for {
		select {
		case template := <-qm.templateChan:
			validator := template.validator.Copy()
			startEpochMs := qm.lastLogEpochMs + 1
			endEpochMs := startEpochMs + int64(template.timeRangeSeconds*1000)
			validator.SetTimeRange(uint64(startEpochMs), uint64(endEpochMs))

			qm.inProgressQueries = append(qm.inProgressQueries, validator)
		default:
			return
		}
	}
}

func (qm *queryManager) sendToValidators(logs []map[string]interface{}) {
	// Just forward to the in progress queries. We don't need to send the logs
	// to the runnable queries because they don't get marked as runnable until
	// we've reached an epoch where the time filtering means they won't accept
	// any more logs.
	for _, validator := range qm.inProgressQueries {
		for _, log := range logs {
			validator.HandleLog(log)
		}
	}
}

func (qm *queryManager) moveToRunnable(epoch uint64) {
	for i, validator := range qm.inProgressQueries {
		_, _, endEpoch := validator.GetQuery()
		if endEpoch < epoch {
			// Move it to runnable, since no future logs will affect the results.
			qm.inProgressQueries = append(qm.inProgressQueries[:i], qm.inProgressQueries[i+1:]...)

			go func(validator queryValidator) {
				time.Sleep(delayForFlush)

				qm.runnableLock.Lock()
				qm.runnableQueries = append(qm.runnableQueries, validator)
				qm.runnableLock.Unlock()
			}(validator)
		}
	}
}

func (qm *queryManager) getLastEpoch(logs []map[string]interface{}) (uint64, bool) {
	if len(logs) == 0 {
		return 0, false
	}

	lastLog := logs[len(logs)-1]
	timestamp, ok := lastLog[timestampCol]
	if !ok {
		return 0, false
	}

	return utils.AsUint64(timestamp)
}

func (qm *queryManager) canRunMore() bool {
	qm.runnableLock.Lock()
	defer qm.runnableLock.Unlock()

	return qm.numRunningQueries.Load() < qm.maxConcurrentQueries && len(qm.runnableQueries) > 0
}

func (qm *queryManager) startQueries() {
	qm.runnableLock.Lock()
	defer qm.runnableLock.Unlock()

	maxToStart := int(qm.maxConcurrentQueries - qm.numRunningQueries.Load())
	numToStart := min(maxToStart, len(qm.runnableQueries))

	qm.numRunningQueries.Add(int32(numToStart))

	for i := 0; i < numToStart; i++ {
		validator := qm.runnableQueries[i]
		go qm.runQuery(validator)
	}

	qm.runnableQueries = qm.runnableQueries[numToStart:]
}

func (qm *queryManager) runQuery(validator queryValidator) {
	query, startEpoch, endEpoch := validator.GetQuery()
	queryInfo := fmt.Sprintf("query=%v, startEpoch=%v, endEpoch=%v", query, startEpoch, endEpoch)
	result, err := sendSplunkQuery(qm.url, query, startEpoch, endEpoch)
	if err != nil {
		log.Fatalf("queryManager.runQuery: failed to run %v; err=%v", queryInfo, err)
	}

	err = validator.MatchesResult(result)
	if err != nil {
		log.Fatalf("queryManager.runQuery: incorrect results for %v; err=%v", queryInfo, err)
	}

	log.Infof("queryManager.runQuery: successfully ran %v", queryInfo)
	qm.numRunningQueries.Add(-1)
}

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

const delayForFlush = 60 * time.Second
const defaultMaxRunnableQueries = 100 // Kind of arbitrary.

type QueryTemplate struct {
	validator        queryValidator
	timeRangeSeconds uint64
	maxInProgress    int
	numInProgress    atomic.Int32
}

type validatorWithCounter struct {
	validator queryValidator
	counter   *atomic.Int32
}

type queryManager struct {
	templates         []*QueryTemplate
	setupOnce         sync.Once
	inProgressQueries []*validatorWithCounter
	runnableQueries   []queryValidator

	// If both locks are needed, acquire inProgressLock first.
	inProgressLock sync.Mutex
	runnableLock   sync.Mutex

	templateChan chan *QueryTemplate

	maxConcurrentQueries int32
	maxRunnable          int
	numRunningQueries    atomic.Int32

	lastLogEpochMs int64

	url string

	failOnError bool
	stats       queryStats
}

type queryStats struct {
	lock           sync.Mutex
	numFailedToRun int
	numBadResults  int
	numWarnings    int
	numSuccess     int

	lastFailure string
}

func (qs *queryStats) Log() {
	qs.lock.Lock()
	defer qs.lock.Unlock()

	log.Infof("QueryStats: %d failed to run, %d gave bad results, %d gave warnings, %d succeeded",
		qs.numFailedToRun, qs.numBadResults, qs.numWarnings, qs.numSuccess)

	if qs.lastFailure != "" {
		log.Infof("QueryStats: Last failure: %s", qs.lastFailure)
	}
}

func NewQueryTemplate(validator queryValidator, timeRangeSeconds uint64, maxInProgress int) *QueryTemplate {
	return &QueryTemplate{
		validator:        validator,
		timeRangeSeconds: timeRangeSeconds,
		maxInProgress:    maxInProgress,
	}
}

func NewQueryManager(templates []*QueryTemplate, maxConcurrentQueries int32, url string, failOnError bool) *queryManager {
	manager := &queryManager{
		templates:            templates,
		inProgressQueries:    make([]*validatorWithCounter, 0),
		runnableQueries:      make([]queryValidator, 0),
		templateChan:         make(chan *QueryTemplate),
		maxConcurrentQueries: maxConcurrentQueries,
		maxRunnable:          defaultMaxRunnableQueries,
		url:                  url,
		failOnError:          failOnError,
	}

	manager.spawnTemplateAdders()
	go manager.logStatsOnInterval(1 * time.Minute)

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
				if template.numInProgress.Load() < int32(template.maxInProgress) {
					// Note: there is a race condition here (by the time we add
					// to the atomic, the condition may be false); however,
					// it's not a critical issue. This counter is used to make
					// sure we don't overwhelm the system, and if we go over by
					// 1 it shouldn't cause an issue.
					template.numInProgress.Add(1)
					qm.templateChan <- template
				}
			}
		}(template)
	}
}

func (qm *queryManager) logStatsOnInterval(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		qm.stats.Log()

		qm.inProgressLock.Lock()
		qm.runnableLock.Lock()
		log.Infof("QueryManager: %d running, %d runnable, %d in progress",
			qm.numRunningQueries.Load(), len(qm.runnableQueries), len(qm.inProgressQueries))
		qm.runnableLock.Unlock()
		qm.inProgressLock.Unlock()
	}
}

func (qm *queryManager) HandleIngestedLogs(logs []map[string]interface{}, allTs []uint64, didRetry bool) {
	qm.inProgressLock.Lock()
	defer qm.inProgressLock.Unlock()

	qm.setupOnce.Do(func() { qm.addInitialQueries(logs) })
	qm.addInProgessQueries()
	qm.sendToValidators(logs, allTs, didRetry)

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

			// We don't need to increment the numInProgress counter here; it's
			// incremented when the template is sent on the channel.
			qm.inProgressQueries = append(qm.inProgressQueries, &validatorWithCounter{
				validator: validator,
				counter:   &template.numInProgress,
			})
		default:
			return
		}
	}
}

func (qm *queryManager) sendToValidators(logs []map[string]interface{}, allTs []uint64, didRetry bool) {
	// Just forward to the in progress queries. We don't need to send the logs
	// to the runnable queries because they don't get marked as runnable until
	// we've reached an epoch where the time filtering means they won't accept
	// any more logs.
	for _, query := range qm.inProgressQueries {
		for i, log := range logs {
			query.validator.HandleLog(log, allTs[i], didRetry)
		}
	}
}

func (qm *queryManager) moveToRunnable(epoch uint64) {
	// Iterate backwards so we can remove elements from the slice.
	for i := len(qm.inProgressQueries) - 1; i >= 0; i-- {
		validator := qm.inProgressQueries[i].validator
		_, _, endEpoch := validator.GetQuery()
		if endEpoch < epoch {
			if qm.runnableSlotsAreFull() {
				return
			}

			// Move it to runnable, since no future logs will affect the results.
			qm.inProgressQueries[i].counter.Add(-1)
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

func (qm *queryManager) runnableSlotsAreFull() bool {
	qm.runnableLock.Lock()
	defer qm.runnableLock.Unlock()

	return len(qm.runnableQueries) >= qm.maxRunnable
}

func (qm *queryManager) addInitialQueries(logs []map[string]interface{}) {
	firstEpoch, ok := qm.getFirstEpoch(logs)
	if !ok {
		log.Warnf("queryManager.addInitialQueries: no logs found to determine first epoch")
		return
	}

	for _, template := range qm.templates {
		if template.maxInProgress <= 0 {
			log.Warnf("queryManager.addInitialQueries: maxInProgress is 0 for template %v; skipping",
				template.validator.Info())
			continue
		}

		seconds := template.timeRangeSeconds / uint64(template.maxInProgress)
		seconds = max(seconds, 1)

		for i := 0; i < template.maxInProgress; i++ {
			validator := template.validator.Copy()

			startEpochMs := firstEpoch
			endEpochMs := startEpochMs + uint64((i+1)*int(seconds)*1000)
			if validator.AllowsAllStartTimes() {
				startEpochMs = endEpochMs - uint64(template.timeRangeSeconds*1000)
			}
			validator.SetTimeRange(startEpochMs, endEpochMs)

			template.numInProgress.Add(1)
			qm.inProgressQueries = append(qm.inProgressQueries, &validatorWithCounter{
				validator: validator,
				counter:   &template.numInProgress,
			})
		}
	}
}

func (qm *queryManager) getFirstEpoch(logs []map[string]interface{}) (uint64, bool) {
	if len(logs) == 0 {
		return 0, false
	}

	firstLog := logs[0]
	timestamp, ok := firstLog[timestampCol]
	if !ok {
		return 0, false
	}

	return utils.AsUint64(timestamp)
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
	queryFailed := false
	queryInfo := validator.Info()
	result, err := sendSplunkQuery(qm.url, query, startEpoch, endEpoch)
	if err != nil {
		queryFailed = true
		qm.stats.lock.Lock()
		qm.stats.numFailedToRun++
		qm.stats.lastFailure = fmt.Sprintf("failed to run %v; err=%v", queryInfo, err)
		qm.stats.lock.Unlock()

		qm.logErrorf("queryManager.runQuery: failed to run %v; err=%v", queryInfo, err)
	}

	err = validator.MatchesResult(result)
	if err != nil {
		queryFailed = true
		logger := qm.logErrorf

		qm.stats.lock.Lock()
		if validator.ServerMightHaveDuplicates() {
			logger = log.Warnf
			qm.stats.numWarnings++
		} else {
			qm.stats.numBadResults++
		}
		qm.stats.lastFailure = fmt.Sprintf("incorrect results for %v; err=%v", queryInfo, err)
		qm.stats.lock.Unlock()

		logger("queryManager.runQuery: incorrect results for %v; err=%v", queryInfo, err)
	}

	if !queryFailed {
		qm.stats.lock.Lock()
		qm.stats.numSuccess++
		qm.stats.lock.Unlock()
	}

	qm.numRunningQueries.Add(-1)

}

func (qm *queryManager) logErrorf(format string, args ...interface{}) {
	if qm.failOnError {
		qm.stats.Log() // One more time, before exiting.
		log.Fatalf(format, args...)
	} else {
		log.Errorf(format, args...)
	}
}

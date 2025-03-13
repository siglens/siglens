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
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"verifier/pkg/utils"

	log "github.com/sirupsen/logrus"
)

const timestampCol = "timestamp"

type queryValidator interface {
	Copy() queryValidator
	HandleLog(map[string]interface{}) error
	GetQuery() (string, uint64, uint64) // Query, start epoch, end epoch.
	SetTimeRange(startEpoch uint64, endEpoch uint64)
	MatchesResult(jsonResult []byte) error
	PastEndTime(timestamp uint64) bool
}

type basicValidator struct {
	startEpoch uint64
	endEpoch   uint64
	query      string
}

func (b *basicValidator) HandleLog(log map[string]interface{}) error {
	return fmt.Errorf("basicValidator.HandleLog: not implemented")
}

func (b *basicValidator) GetQuery() (string, uint64, uint64) {
	return b.query, b.startEpoch, b.endEpoch
}

func (b *basicValidator) SetTimeRange(startEpoch uint64, endEpoch uint64) {
	b.startEpoch = startEpoch
	b.endEpoch = endEpoch
}

func (b *basicValidator) MatchesResult(result []byte) error {
	return fmt.Errorf("basicValidator.MatchesResult: not implemented")
}

func (b *basicValidator) PastEndTime(timestamp uint64) bool {
	return timestamp > b.endEpoch
}

type filterQueryValidator struct {
	basicValidator
	key             string
	value           string
	head            int
	reversedResults []map[string]interface{}
	lock            sync.Mutex
}

func NewFilterQueryValidator(key string, value string, head int, startEpoch uint64,
	endEpoch uint64) (queryValidator, error) {

	if head < 1 || head > 99 {
		// The 99 limit is to simplify the expected results. If siglens returns
		// 100+ records, it will say "gte 100" records returned, but below that
		// it will say "eq N" records returned. So by limiting to 99, we can
		// always expect "eq N" records returned.
		return nil, fmt.Errorf("NewFilterQueryValidator: head must be between 1 and 99 inclusive")
	}

	return &filterQueryValidator{
		basicValidator: basicValidator{
			startEpoch: startEpoch,
			endEpoch:   endEpoch,
			query:      fmt.Sprintf("%v=%v | head %v", key, value, head),
		},
		key:             key,
		value:           value,
		head:            head,
		reversedResults: make([]map[string]interface{}, 0),
	}, nil
}

func (f *filterQueryValidator) Copy() queryValidator {
	return &filterQueryValidator{
		basicValidator: basicValidator{
			startEpoch: f.startEpoch,
			endEpoch:   f.endEpoch,
			query:      f.query,
		},
		key:             f.key,
		value:           f.value,
		head:            f.head,
		reversedResults: make([]map[string]interface{}, 0),
	}
}

// Note: this assumes successive calls to this are for logs with increasing timestamps.
func (f *filterQueryValidator) HandleLog(log map[string]interface{}) error {
	if !withinTimeRange(log, f.startEpoch, f.endEpoch) {
		return nil
	}

	value, ok := log[f.key]
	if !ok || value != fmt.Sprintf("%v", f.value) {
		return nil
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	f.reversedResults = append(f.reversedResults, log)

	if len(f.reversedResults) > f.head {
		f.reversedResults = f.reversedResults[1:]
	}

	return nil
}

type logsResponse struct {
	Hits       hits     `json:"hits"`
	AllColumns []string `json:"allColumns"`
}

type hits struct {
	TotalMatched totalMatched             `json:"totalMatched"`
	Records      []map[string]interface{} `json:"records"`
}

type totalMatched struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

func (f *filterQueryValidator) MatchesResult(result []byte) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	response := logsResponse{}
	if err := json.Unmarshal(result, &response); err != nil {
		return fmt.Errorf("FQV.MatchesResult: cannot unmarshal %s; err=%v", result, err)
	}

	if response.Hits.TotalMatched.Value != len(f.reversedResults) {
		return fmt.Errorf("FQV.MatchesResult: expected %d logs, got %d",
			len(f.reversedResults), response.Hits.TotalMatched.Value)
	}

	if response.Hits.TotalMatched.Relation != "eq" {
		return fmt.Errorf("FQV.MatchesResult: expected relation to be eq, got %s",
			response.Hits.TotalMatched.Relation)
	}

	if len(response.Hits.Records) != len(f.reversedResults) {
		return fmt.Errorf("FQV.MatchesResult: expected %d actual records, got %d",
			len(f.reversedResults), len(response.Hits.Records))
	}

	// Parsing json treats all numbers as float64, so we need to convert the logs.
	for i := range f.reversedResults {
		f.reversedResults[i] = copyLogWithFloats(f.reversedResults[i])
	}

	// Compare the logs.
	expectedLogs := f.reversedResults
	slices.Reverse(expectedLogs)
	for i, record := range response.Hits.Records {
		if !utils.EqualMaps(record, expectedLogs[i]) {
			return fmt.Errorf("FQV.MatchesResult: expected %+v, got %+v for iter %v",
				expectedLogs[i], record, i)
		}
	}

	// Compare the columns.
	expectedColumnsSet := make(map[string]struct{})
	for _, log := range expectedLogs {
		for col := range log {
			expectedColumnsSet[col] = struct{}{}
		}
	}

	actualColumnsSet := make(map[string]struct{})
	for _, col := range response.AllColumns {
		actualColumnsSet[col] = struct{}{}
	}

	if !utils.EqualMaps(expectedColumnsSet, actualColumnsSet) {
		return fmt.Errorf("FQV.MatchesResult: expected columns %+v, got %+v", expectedColumnsSet, actualColumnsSet)
	}

	log.Infof("FQV.MatchesResult: successfully matched %d logs", len(f.reversedResults))

	return nil
}

func withinTimeRange(record map[string]interface{}, startEpoch uint64, endEpoch uint64) bool {
	timestamp, ok := record[timestampCol]
	if !ok {
		log.Errorf("withinTimeRange: missing timestamp column")
		return false
	}

	switch timestamp := timestamp.(type) {
	case uint64:
		return timestamp >= startEpoch && timestamp <= endEpoch
	}

	log.Errorf("withinTimeRange: invalid timestamp type %T", timestamp)

	return false
}

func copyLogWithFloats(log map[string]interface{}) map[string]interface{} {
	newLog := make(map[string]interface{})
	for k, v := range log {
		switch val := v.(type) {
		case uint:
			newLog[k] = float64(val)
		case int:
			newLog[k] = float64(val)
		case uint8:
			newLog[k] = float64(val)
		case int8:
			newLog[k] = float64(val)
		case uint16:
			newLog[k] = float64(val)
		case int16:
			newLog[k] = float64(val)
		case uint32:
			newLog[k] = float64(val)
		case int32:
			newLog[k] = float64(val)
		case uint64:
			newLog[k] = float64(val)
		case int64:
			newLog[k] = float64(val)
		case float32:
			newLog[k] = float64(val)
		default:
			newLog[k] = v
		}
	}

	return newLog
}

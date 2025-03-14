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
	"time"
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
	Info() string
}

type basicValidator struct {
	startEpoch uint64
	endEpoch   uint64
	query      string
}

func (b *basicValidator) GetQuery() (string, uint64, uint64) {
	return b.query, b.startEpoch, b.endEpoch
}

func (b *basicValidator) SetTimeRange(startEpoch uint64, endEpoch uint64) {
	b.startEpoch = startEpoch
	b.endEpoch = endEpoch
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

func (f *filterQueryValidator) Info() string {
	duration := time.Duration(f.endEpoch-f.startEpoch) * time.Millisecond
	numResults := min(len(f.reversedResults), f.head)

	return fmt.Sprintf("query=%v, timeSpan=%v (%v-%v), got %v results",
		f.query, duration, f.startEpoch, f.endEpoch, numResults)
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
		lastKeptLog := f.reversedResults[len(f.reversedResults)-f.head]
		var lastKeptTimestamp uint64
		if timestamp, ok := lastKeptLog[timestampCol]; !ok {
			return fmt.Errorf("FQV.HandleLog: missing timestamp column")
		} else if lastKeptTimestamp, ok = utils.AsUint64(timestamp); !ok {
			return fmt.Errorf("FQV.HandleLog: invalid timestamp type %T", timestamp)
		}

		numToDelete := 0
		for i := range f.reversedResults[:len(f.reversedResults)-f.head] {
			var thisTimestamp uint64
			if timestamp, ok := f.reversedResults[i][timestampCol]; !ok {
				return fmt.Errorf("FQV.HandleLog: missing timestamp column")
			} else if thisTimestamp, ok = utils.AsUint64(timestamp); !ok {
				return fmt.Errorf("FQV.HandleLog: invalid timestamp type %T", timestamp)
			} else if thisTimestamp < lastKeptTimestamp {
				numToDelete++
			}
		}

		f.reversedResults = f.reversedResults[numToDelete:]
	}

	return nil
}

type logsResponse struct {
	Hits       hits     `json:"hits"`
	AllColumns []string `json:"allColumns"`

	// Used for aggregation queries.
	MeasureFunctions []string        `json:"measureFunctions,omitempty"`
	Measure          []measureResult `json:"measure,omitempty"`
	BucketCount      int             `json:"bucketCount,omitempty"`
}

type hits struct {
	TotalMatched totalMatched             `json:"totalMatched"`
	Records      []map[string]interface{} `json:"records"`
}

type totalMatched struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

type measureResult struct {
	GroupByValues []string               `json:"GroupByValues"`
	Value         map[string]interface{} `json:"MeasureVal"`
}

func (f *filterQueryValidator) MatchesResult(result []byte) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	response := logsResponse{}
	if err := json.Unmarshal(result, &response); err != nil {
		return fmt.Errorf("FQV.MatchesResult: cannot unmarshal %s; err=%v", result, err)
	}

	numExpectedLogs := min(len(f.reversedResults), f.head)
	if response.Hits.TotalMatched.Value != numExpectedLogs {
		return fmt.Errorf("FQV.MatchesResult: expected %d logs, got %d",
			numExpectedLogs, response.Hits.TotalMatched.Value)
	}

	if response.Hits.TotalMatched.Relation != "eq" {
		return fmt.Errorf("FQV.MatchesResult: expected relation to be eq, got %s",
			response.Hits.TotalMatched.Relation)
	}

	if len(response.Hits.Records) != numExpectedLogs {
		return fmt.Errorf("FQV.MatchesResult: expected %d actual records, got %d",
			numExpectedLogs, len(response.Hits.Records))
	}

	// Parsing json treats all numbers as float64, so we need to convert the logs.
	for i := range f.reversedResults {
		f.reversedResults[i] = copyLogWithFloats(f.reversedResults[i])
	}

	// Compare the logs.
	slices.Reverse(f.reversedResults)       // Reverse it to match the order of the response.
	defer slices.Reverse(f.reversedResults) // Revert to the original order, so subsequent calls work.
	expectedLogs := f.reversedResults

	err := logsMatch(expectedLogs, response.Hits.Records)
	if err != nil {
		return err
	}

	return nil
}

// Returns no error if the logs match the expected logs, and they're in the
// same order. It also returns no error if the logs are in a different order,
// but it's a valid sorting order; since sorting is on the timestamp, this
// happens when multiple logs have the same timestamp.
func logsMatch(expectedLogs []map[string]interface{}, actualLogs []map[string]interface{}) error {
	expectedGroups, err := groupBySortColumn(expectedLogs, timestampCol)
	if err != nil {
		return fmt.Errorf("logsMatch: failed to group expected logs; err=%v", err)
	}

	actualGroups, err := groupBySortColumn(actualLogs, timestampCol)
	if err != nil {
		return fmt.Errorf("logsMatch: failed to group actual logs; err=%v", err)
	}

	if len(expectedGroups) != len(actualGroups) {
		return fmt.Errorf("logsMatch: expected %d unique timestamps, got %d",
			len(expectedGroups), len(actualGroups))
	}

	if len(expectedGroups) == 0 {
		return nil
	}

	for i := range expectedGroups[:len(expectedGroups)-1] {
		if !utils.IsPermutation(expectedGroups[i], actualGroups[i], utils.EqualMaps) {
			return fmt.Errorf("logsMatch: expected logs in group %v: %+v, got %+v",
				i, expectedGroups[i], actualGroups[i])
		}
	}

	// For the last group, there can be some ambiguity (e.g., the last 3 logs
	// all have the same timestamp, but 4 logs with that timestamp match the
	// query, so any 3 of those 4 logs are valid).
	i := len(expectedGroups) - 1
	if !utils.SliceContainsItems(expectedGroups[i], actualGroups[i], utils.EqualMaps) {
		return fmt.Errorf("logsMatch: expected logs in final group: %+v, got %+v",
			expectedGroups[i], actualGroups[i])
	}

	return nil
}

// This assumes the logs are already sorted by the sort column.
func groupBySortColumn(logs []map[string]interface{}, sortColumn string) ([][]map[string]interface{}, error) {
	if len(logs) == 0 {
		return nil, nil
	}

	groups := make([][]map[string]interface{}, 0)
	groups = append(groups, make([]map[string]interface{}, 0))

	curValue, ok := logs[0][sortColumn]
	if !ok {
		return nil, fmt.Errorf("groupBySortColumn: missing sort column %v", sortColumn)
	}

	for _, log := range logs {
		value, ok := log[sortColumn]
		if !ok {
			return nil, fmt.Errorf("groupBySortColumn: missing sort column %v", sortColumn)
		}

		if value != curValue {
			groups = append(groups, make([]map[string]interface{}, 0))
			curValue = value
		}

		groups[len(groups)-1] = append(groups[len(groups)-1], log)
	}

	return groups, nil
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

type countQueryValidator struct {
	basicValidator
	key        string
	value      string
	numMatches int
	lock       sync.Mutex
}

func NewCountQueryValidator(key string, value string, startEpoch uint64,
	endEpoch uint64) (queryValidator, error) {

	return &countQueryValidator{
		basicValidator: basicValidator{
			startEpoch: startEpoch,
			endEpoch:   endEpoch,
			query:      fmt.Sprintf("%v=%v | stats count", key, value),
		},
		key:        key,
		value:      value,
		numMatches: 0,
	}, nil
}

func (c *countQueryValidator) Copy() queryValidator {
	return &countQueryValidator{
		basicValidator: basicValidator{
			startEpoch: c.startEpoch,
			endEpoch:   c.endEpoch,
			query:      c.query,
		},
		key:        c.key,
		value:      c.value,
		numMatches: c.numMatches,
	}
}

func (c *countQueryValidator) Info() string {
	duration := time.Duration(c.endEpoch-c.startEpoch) * time.Millisecond

	return fmt.Sprintf("query=%v, timeSpan=%v (%v-%v), got %v results",
		c.query, duration, c.startEpoch, c.endEpoch, c.numMatches)
}

func (c *countQueryValidator) HandleLog(log map[string]interface{}) error {
	if !withinTimeRange(log, c.startEpoch, c.endEpoch) {
		return nil
	}

	value, ok := log[c.key]
	if !ok || value != fmt.Sprintf("%v", c.value) {
		return nil
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	c.numMatches++

	return nil
}

func (c *countQueryValidator) MatchesResult(result []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	response := logsResponse{}
	if err := json.Unmarshal(result, &response); err != nil {
		return fmt.Errorf("CQV.MatchesResult: cannot unmarshal %s; err=%v", result, err)
	}

	if response.Hits.TotalMatched.Value != c.numMatches {
		return fmt.Errorf("FQV.MatchesResult: expected %d logs, got %d",
			c.numMatches, response.Hits.TotalMatched.Value)
	}

	if response.Hits.TotalMatched.Relation != "eq" {
		return fmt.Errorf("FQV.MatchesResult: expected relation to be eq, got %s",
			response.Hits.TotalMatched.Relation)
	}

	return nil
}

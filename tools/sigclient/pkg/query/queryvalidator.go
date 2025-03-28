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
	"regexp"
	"sort"
	"strings"
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
	WithAllowAllStartTimes() queryValidator
	AllowsAllStartTimes() bool
}

type filter interface {
	Matches(map[string]interface{}) bool
	String() string
	Copy() filter
}

type kvFilter struct {
	key   string
	value stringOrRegex
}

func Filter(key string, value string) (filter, error) {
	// Don't allow matching literal asterisks.
	if strings.Contains(value, "\\*") {
		return nil, fmt.Errorf("Filter: matching literal asterisks is not implemented")
	}

	finalValue := stringOrRegex{isRegex: false, rawString: value}
	if strings.Contains(finalValue.rawString, "*") {
		s := strings.ReplaceAll(finalValue.rawString, "*", ".*")
		s = fmt.Sprintf("^%v$", s)
		regex, err := regexp.Compile(s)
		if err != nil {
			return nil, fmt.Errorf("Filter: invalid regex %v; err=%v", finalValue.rawString, err)
		}

		finalValue.isRegex = true
		finalValue.regex = *regex
	}
	return &kvFilter{
		key:   key,
		value: finalValue,
	}, nil
}

func (kv *kvFilter) Matches(log map[string]interface{}) bool {
	value, ok := log[kv.key]
	if !ok {
		return false
	}

	return kv.value.Matches(fmt.Sprintf("%v", value))
}

func (kv kvFilter) String() string {
	return fmt.Sprintf(`%v="%v"`, kv.key, kv.value)
}

func (kv *kvFilter) Copy() filter {
	return &kvFilter{
		key:   kv.key,
		value: kv.value,
	}
}

type matchAllFilter struct{}

func MatchAll() filter {
	return &matchAllFilter{}
}

func (m *matchAllFilter) Matches(log map[string]interface{}) bool {
	return true
}

func (m matchAllFilter) String() string {
	return "*"
}

func (m *matchAllFilter) Copy() filter {
	return &matchAllFilter{}
}

type dynamicFilter struct {
	filter filter
}

func DynamicFilter() filter {
	return &dynamicFilter{}
}

func (df *dynamicFilter) Matches(log map[string]interface{}) bool {
	if df.filter == nil {
		df.setFrom(log)
	}

	return df.filter.Matches(log)
}

func (df *dynamicFilter) setFrom(log map[string]interface{}) {
	// Choose a random key=value pair from the log to filter on. Currently,
	// the validator only supports string values. Map iterations in Go are
	// nondeterministic, so we should get a variety of keys over time.
	for key, value := range log {
		switch v := value.(type) {
		case string:
			df.filter = &kvFilter{
				key:   key,
				value: stringOrRegex{isRegex: false, rawString: v},
			}

			return
		}
	}

	df.filter = MatchAll()
}

func (df *dynamicFilter) String() string {
	if df.filter == nil {
		return "unset"
	}

	return df.filter.String()
}

func (df *dynamicFilter) Copy() filter {
	if df.filter != nil {
		return df.filter.Copy()
	}

	return &dynamicFilter{}
}

type basicValidator struct {
	startEpoch uint64
	endEpoch   uint64

	// If true, the start time may be before the test was started. So
	// validation should be less strict because the system may have preexisting
	// data that this validator doesn't know about.
	allowAllStartTimes bool
}

func (b *basicValidator) SetTimeRange(startEpoch uint64, endEpoch uint64) {
	b.startEpoch = startEpoch
	b.endEpoch = endEpoch
}

func (b *basicValidator) PastEndTime(timestamp uint64) bool {
	return timestamp > b.endEpoch
}

func (b *basicValidator) AllowsAllStartTimes() bool {
	return b.allowAllStartTimes
}

type filterQueryValidator struct {
	basicValidator
	filter  filter
	sortCol string
	head    int
	results []map[string]interface{} // Sorted descending by sortCol.
	lock    sync.Mutex
}

type stringOrRegex struct {
	isRegex   bool
	rawString string
	regex     regexp.Regexp
}

func (s stringOrRegex) String() string {
	return s.rawString
}

func (s *stringOrRegex) Matches(value string) bool {
	if s.isRegex {
		return s.regex.MatchString(value)
	}

	return value == s.rawString
}

func NewFilterQueryValidator(filter filter, numericSortCol string, head int,
	startEpoch uint64, endEpoch uint64) (queryValidator, error) {

	if head < 1 || head > 99 {
		// The 99 limit is to simplify the expected results. If siglens returns
		// 100+ records, it will say "gte 100" records returned, but below that
		// it will say "eq N" records returned. So by limiting to 99, we can
		// always expect "eq N" records returned.
		return nil, fmt.Errorf("NewFilterQueryValidator: head must be between 1 and 99 inclusive")
	}

	if numericSortCol == "" {
		numericSortCol = timestampCol
	}

	return &filterQueryValidator{
		basicValidator: basicValidator{
			startEpoch: startEpoch,
			endEpoch:   endEpoch,
		},
		filter:  filter,
		sortCol: numericSortCol,
		head:    head,
		results: make([]map[string]interface{}, 0),
	}, nil
}

func (f *filterQueryValidator) GetQuery() (string, uint64, uint64) {
	var query string
	if f.sortCol == timestampCol {
		query = fmt.Sprintf(`%v | head %v`, f.filter, f.head)
	} else {
		// Only sorting by numeric columns is supported for now.
		// Sort so the highest values are first.
		query = fmt.Sprintf(`%v | sort %v -num(%v)`, f.filter, f.head, f.sortCol)
	}

	return query, f.startEpoch, f.endEpoch
}

func (f *filterQueryValidator) Copy() queryValidator {
	return &filterQueryValidator{
		basicValidator: basicValidator{
			startEpoch: f.startEpoch,
			endEpoch:   f.endEpoch,
		},
		filter:  f.filter.Copy(),
		sortCol: f.sortCol,
		head:    f.head,
		results: make([]map[string]interface{}, 0),
	}
}

func (f *filterQueryValidator) Info() string {
	duration := time.Duration(f.endEpoch-f.startEpoch) * time.Millisecond
	numResults := min(len(f.results), f.head)
	query, startEpoch, endEpoch := f.GetQuery()

	validation := "strict"
	if f.allowAllStartTimes {
		validation = "minimal"
	}

	return fmt.Sprintf("query=%v, timeSpan=%v (%v-%v), validation=%v, got %v matches",
		query, duration, startEpoch, endEpoch, validation, numResults)
}

// Note: this assumes successive calls to this are for logs with increasing timestamps.
func (f *filterQueryValidator) HandleLog(log map[string]interface{}) error {
	if !withinTimeRange(log, f.startEpoch, f.endEpoch) {
		return nil
	}

	if !f.filter.Matches(log) {
		return nil
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	f.results = append(f.results, log)
	sort.Slice(f.results, func(i, j int) bool {
		iSortVal, ok := f.results[i][f.sortCol]
		if !ok {
			return false
		}

		jSortVal, ok := f.results[j][f.sortCol]
		if !ok {
			return true
		}

		iFloat, ok := utils.AsFloat64(iSortVal)
		if !ok {
			return false
		}

		jFloat, ok := utils.AsFloat64(jSortVal)
		if !ok {
			return true
		}

		return iFloat > jFloat
	})

	if len(f.results) > f.head {
		lastKeptLog := f.results[f.head-1]
		var lastKeptVal float64
		var lastOk bool
		if sortVal, ok := lastKeptLog[f.sortCol]; !ok {
			lastOk = false
		} else if lastKeptVal, ok = utils.AsFloat64(sortVal); !ok {
			return fmt.Errorf("FQV.HandleLog: invalid type in sort column %v: %T", f.sortCol, sortVal)
		} else {
			lastOk = true
		}

		numToDelete := 0
		for i := f.head; i < len(f.results); i++ {
			var thisSortVal float64
			var thisOk bool
			if sortVal, ok := f.results[i][f.sortCol]; !ok {
				thisOk = false
			} else if thisSortVal, ok = utils.AsFloat64(sortVal); !ok {
				return fmt.Errorf("FQV.HandleLog: invalid type in sort column %v: %T", f.sortCol, sortVal)
			} else {
				thisOk = true
			}

			if lastOk != thisOk || (lastOk && thisOk && thisSortVal != lastKeptVal) {
				numToDelete++
			}
		}

		f.results = f.results[:len(f.results)-numToDelete]
	}

	return nil
}

func (f *filterQueryValidator) WithAllowAllStartTimes() queryValidator {
	f.allowAllStartTimes = true
	return f
}

type logsResponse struct {
	Hits       hits     `json:"hits"`
	AllColumns []string `json:"allColumns"`

	// Used for aggregation queries.
	MeasureFunctions []string        `json:"measureFunctions,omitempty"`
	Measure          []measureResult `json:"measure,omitempty"`
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

	if f.allowAllStartTimes {
		// Skip the rest of the validation, since we're not sure what the
		// expected result is.
		return nil
	}

	numExpectedLogs := min(len(f.results), f.head)
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
	for i := range f.results {
		f.results[i] = copyLogWithFloats(f.results[i])
	}

	// Compare the logs.
	err := logsMatch(f.results, response.Hits.Records, f.sortCol)
	if err != nil {
		return err
	}

	return nil
}

// Returns no error if the logs match the expected logs, and they're in the
// same order. It also returns no error if the logs are in a different order,
// but it's a valid sorting order; this happens when multiple logs have the
// same value in the column being sorted on.
func logsMatch(expectedLogs []map[string]interface{}, actualLogs []map[string]interface{},
	sortCol string) error {

	expectedGroups, err := groupBySortColumn(expectedLogs, sortCol)
	if err != nil {
		return fmt.Errorf("logsMatch: failed to group expected logs; err=%v", err)
	}

	actualGroups, err := groupBySortColumn(actualLogs, sortCol)
	if err != nil {
		return fmt.Errorf("logsMatch: failed to group actual logs; err=%v", err)
	}

	if len(expectedGroups) != len(actualGroups) {
		return fmt.Errorf("logsMatch: expected %d unique sort values, got %d",
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

	curValue, curOk := logs[0][sortColumn]

	for _, log := range logs {
		value, ok := log[sortColumn]

		if ok != curOk || (ok && curOk && value != curValue) {
			groups = append(groups, make([]map[string]interface{}, 0))
			curValue = value
			curOk = ok
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
	filter     filter
	numMatches int
	lock       sync.Mutex
}

func NewCountQueryValidator(filter filter, startEpoch uint64,
	endEpoch uint64) (queryValidator, error) {

	return &countQueryValidator{
		basicValidator: basicValidator{
			startEpoch: startEpoch,
			endEpoch:   endEpoch,
		},
		filter:     filter,
		numMatches: 0,
	}, nil
}

func (c *countQueryValidator) GetQuery() (string, uint64, uint64) {
	query := fmt.Sprintf("%v | stats count", c.filter)
	return query, c.startEpoch, c.endEpoch
}

func (c *countQueryValidator) Copy() queryValidator {
	return &countQueryValidator{
		basicValidator: basicValidator{
			startEpoch: c.startEpoch,
			endEpoch:   c.endEpoch,
		},
		filter:     c.filter.Copy(),
		numMatches: c.numMatches,
	}
}

func (c *countQueryValidator) Info() string {
	duration := time.Duration(c.endEpoch-c.startEpoch) * time.Millisecond
	query, startEpoch, endEpoch := c.GetQuery()

	validation := "strict"
	if c.allowAllStartTimes {
		validation = "minimal"
	}

	return fmt.Sprintf("query=%v, timeSpan=%v (%v-%v), validation=%v, got %v matches",
		query, duration, startEpoch, endEpoch, validation, c.numMatches)
}

func (c *countQueryValidator) WithAllowAllStartTimes() queryValidator {
	c.allowAllStartTimes = true
	return c
}

func (c *countQueryValidator) HandleLog(log map[string]interface{}) error {
	if !withinTimeRange(log, c.startEpoch, c.endEpoch) {
		return nil
	}

	if !c.filter.Matches(log) {
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

	if c.allowAllStartTimes {
		// Skip the rest of the validation, since we're not sure what the
		// expected result is.
		return nil
	}

	if response.Hits.TotalMatched.Value != c.numMatches {
		return fmt.Errorf("CQV.MatchesResult: expected %d logs, got %d",
			c.numMatches, response.Hits.TotalMatched.Value)
	}

	if response.Hits.TotalMatched.Relation != "eq" {
		return fmt.Errorf("CQV.MatchesResult: expected relation to be eq, got %s",
			response.Hits.TotalMatched.Relation)
	}

	if len(response.AllColumns) != 1 || response.AllColumns[0] != "count(*)" {
		return fmt.Errorf("CQV.MatchesResult: expected allColumns to be [count(*)], got %v",
			response.AllColumns)
	}

	if len(response.MeasureFunctions) != 1 || response.MeasureFunctions[0] != "count(*)" {
		return fmt.Errorf("CQV.MatchesResult: expected measureFunctions to be [count(*)], got %v",
			response.MeasureFunctions)
	}

	if len(response.Measure) != 1 {
		return fmt.Errorf("CQV.MatchesResult: expected 1 measure, got %d", len(response.Measure))
	}

	measure := response.Measure[0]
	if len(measure.GroupByValues) != 1 || measure.GroupByValues[0] != "*" {
		return fmt.Errorf("CQV.MatchesResult: expected groupByValues to be [*], got %v",
			measure.GroupByValues)
	}

	if len(measure.Value) != 1 {
		return fmt.Errorf("CQV.MatchesResult: expected 1 value, got %d", len(measure.Value))
	}

	if count, ok := measure.Value["count(*)"]; !ok {
		return fmt.Errorf("CQV.MatchesResult: expected measure[0] to have key count(*), got %v",
			measure.Value)
	} else if countUint, ok := utils.AsUint64(count); !ok {
		return fmt.Errorf("CQV.MatchesResult: invalid count type %T in measure[0] value %v",
			count, measure.Value)
	} else if countUint != uint64(c.numMatches) {
		return fmt.Errorf("CQV.MatchesResult: expected measure[0] count to be %d, got %d",
			c.numMatches, countUint)
	}

	return nil
}

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
	"verifier/pkg/utils"

	log "github.com/sirupsen/logrus"
)

const timestampCol = "timestamp"

type queryValidator interface {
	HandleLog(map[string]interface{}) error
	GetQuery() (string, uint64, uint64) // Query, start epoch, end epoch.
	MatchesResult([]byte) error
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

func (b *basicValidator) MatchesResult(result []byte) error {
	return fmt.Errorf("basicValidator.MatchesResult: not implemented")
}

type filterQueryValidator struct {
	basicValidator
	key             string
	value           string
	head            int
	reversedResults []map[string]interface{}
}

func NewFilterQueryValidator(key string, value string, head int, startEpoch uint64,
	endEpoch uint64) queryValidator {

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
	}
}

func (f *filterQueryValidator) HandleLog(log map[string]interface{}) error {
	if !withinTimeRange(log, f.startEpoch, f.endEpoch) {
		return nil
	}

	value, ok := log[f.key]
	if !ok || value != fmt.Sprintf("%v", f.value) {
		return nil
	}

	f.reversedResults = append(f.reversedResults, log)

	if len(f.reversedResults) > f.head {
		f.reversedResults = f.reversedResults[1:]
	}

	return nil
}

type logsResponse struct {
	TotalMatched totalMatched             `json:"totalMatched"`
	Records      []map[string]interface{} `json:"records"`
	AllColumns   []string                 `json:"allColumns"`
}

type totalMatched struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

func (f *filterQueryValidator) MatchesResult(result []byte) error {
	response := logsResponse{}
	if err := json.Unmarshal(result, &response); err != nil {
		return fmt.Errorf("FQV.MatchesResult: cannot unmarshal %s; err=%v", result, err)
	}

	if response.TotalMatched.Value != len(f.reversedResults) {
		return fmt.Errorf("FQV.MatchesResult: expected %d logs, got %d", len(f.reversedResults), response.TotalMatched.Value)
	}

	if response.TotalMatched.Relation != "eq" {
		return fmt.Errorf("FQV.MatchesResult: expected relation to be eq, got %s", response.TotalMatched.Relation)
	}

	// Compare the logs.
	expectedLogs := f.reversedResults
	slices.Reverse(expectedLogs)
	for i, log := range response.Records {
		if !utils.EqualMaps(log, expectedLogs[i]) {
			return fmt.Errorf("FQV.MatchesResult: expected %+v, got %+v", expectedLogs[i], log)
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

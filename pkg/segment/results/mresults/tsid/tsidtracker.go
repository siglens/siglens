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

package tsidtracker

import (
	"fmt"
	"sort"

	"github.com/valyala/bytebufferpool"
)

var TAG_VALUE_DELIMITER_BYTE = []byte(",")

var TAG_VALUE_DELIMITER_STR = (",")

type AllMatchedTSIDsInfo struct {
	MetricName     string
	TagKeyTagValue map[string]interface{}
}

/*
Holder struct to track all matched TSIDs
*/
type AllMatchedTSIDs struct {
	allTSIDs    map[uint64]*bytebufferpool.ByteBuffer // raw tsids that are currently being tracked
	first       bool
	tsidInfoMap map[uint64]*AllMatchedTSIDsInfo
}

/*
This function should initialize a TSID tracker
*/
func InitTSIDTracker(numTagFilters int) (*AllMatchedTSIDs, error) {

	return &AllMatchedTSIDs{
		allTSIDs:    make(map[uint64]*bytebufferpool.ByteBuffer, 0),
		first:       true,
		tsidInfoMap: make(map[uint64]*AllMatchedTSIDsInfo, 0),
	}, nil
}

func (tr *AllMatchedTSIDs) AddTSID(tsid uint64, groupIdStr string, tagKey string, addToBuf bool) error {
	buff, ok := tr.allTSIDs[tsid]
	if !ok {
		buff = bytebufferpool.Get()
		_, err := buff.WriteString(fmt.Sprintf("%v{", groupIdStr))
		if err != nil {
			return err
		}
		tr.allTSIDs[tsid] = buff
	} else {
		if addToBuf {
			_, err := buff.Write(TAG_VALUE_DELIMITER_BYTE)
			if err != nil {
				return err
			}
			_, err = buff.WriteString(fmt.Sprintf("%+v:%+v", tagKey, groupIdStr))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// If first time, add all tsids to map
// Else, intersect with existing tsids
func (tr *AllMatchedTSIDs) BulkAdd(rawTagValueToTSIDs map[string]map[uint64]struct{}, metricName string, tagKey string) error {
	if tr.first {
		for tagValue, tsids := range rawTagValueToTSIDs {
			for id := range tsids {
				buff := bytebufferpool.Get()
				_, err := buff.WriteString(fmt.Sprintf("%v{", metricName))
				if err != nil {
					return err
				}

				_, err = buff.WriteString(fmt.Sprintf("%+v:%+v", tagKey, tagValue))
				if err != nil {
					return err
				}

				_, err = buff.Write(TAG_VALUE_DELIMITER_BYTE)
				if err != nil {
					return err
				}

				tr.allTSIDs[id] = buff
			}
		}
	} else {
		valid := 0
		for ts, tsidInfo := range tr.allTSIDs {
			shouldKeep := false
			for tagValue, tsids := range rawTagValueToTSIDs {
				if _, ok := tsids[ts]; ok {
					shouldKeep = true
					valid++

					_, err := tsidInfo.WriteString(fmt.Sprintf("%+v:%+v", tagKey, tagValue))
					if err != nil {
						return err
					}

					_, err = tsidInfo.Write(TAG_VALUE_DELIMITER_BYTE)
					if err != nil {
						return err
					}

					break
				}
			}

			if !shouldKeep {
				delete(tr.allTSIDs, ts)
			}
		}
	}

	return nil
}

// If first time, add all tsids to map
// Else, intersect with existing tsids
func (tr *AllMatchedTSIDs) BulkAddTagsOnly(rawTagValueToTSIDs map[string]map[uint64]struct{}, metricName string, tagKey string) error {
	if tr.first {
		for tagValue, tsids := range rawTagValueToTSIDs {
			for id := range tsids {
				tsIDinfo := AllMatchedTSIDsInfo{
					MetricName:     metricName,
					TagKeyTagValue: make(map[string]interface{}),
				}
				tsIDinfo.TagKeyTagValue[tagKey] = tagValue
				tr.tsidInfoMap[id] = &tsIDinfo
			}
		}
	} else {
		valid := 0
		for ts, tsidInfo := range tr.tsidInfoMap {
			shouldKeep := false
			for tagValue, tsids := range rawTagValueToTSIDs {
				if _, ok := tsids[ts]; ok {
					shouldKeep = true
					valid++

					// Write the tagKey and tagValue to the existing tsidInfo
					tsidInfo.TagKeyTagValue[tagKey] = tagValue

					break
				}
			}

			if !shouldKeep {
				delete(tr.tsidInfoMap, ts)
			}
		}
	}

	return nil
}
func (tr *AllMatchedTSIDs) BulkAddStarTagsOnly(rawTagValueToTSIDs map[string]map[uint64]struct{}, initMetricName string, tagKey string, numValueFiltersNonZero bool) error {
	for tagValue, tsids := range rawTagValueToTSIDs {
		for id := range tsids {
			tsidInfo, ok := tr.tsidInfoMap[id]
			if !ok {
				if numValueFiltersNonZero {
					continue
				}

				tsidInfo = &AllMatchedTSIDsInfo{
					MetricName:     initMetricName,
					TagKeyTagValue: make(map[string]interface{}),
				}
				tr.tsidInfoMap[id] = tsidInfo
			}

			tsidInfo.TagKeyTagValue[tagKey] = tagValue
		}
	}

	return nil
}

// For all incoming tsids, always add tsid and groupid to stored tsids
func (tr *AllMatchedTSIDs) BulkAddStar(rawTagValueToTSIDs map[string]map[uint64]struct{}, initMetricName string, tagKey string, numValueFiltersNonZero bool) error {
	var err error
	for tagValue, tsids := range rawTagValueToTSIDs {
		for id := range tsids {
			buf, ok := tr.allTSIDs[id]
			if !ok {

				if numValueFiltersNonZero {
					continue
				}

				buf = bytebufferpool.Get()
				_, err = buf.WriteString(initMetricName)
				if err != nil {
					return err
				}
				tr.allTSIDs[id] = buf
			}
			_, err = buf.WriteString(fmt.Sprintf("%+v:%+v", tagKey, tagValue))
			if err != nil {
				return err
			}

			_, err = buf.Write(TAG_VALUE_DELIMITER_BYTE)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// needs to know if first update or not
func (tr *AllMatchedTSIDs) FinishBlock() error {
	tr.first = false
	return nil
}

/*
TSO access is highly optimized for TSIDs to be accessed in increasing order

This reorders the matched TSIDS in increasing order
*/
func (tr *AllMatchedTSIDs) FinishAllMatches() {
	keys := make([]uint64, 0, len(tr.allTSIDs))
	for k := range tr.allTSIDs {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	retVal := make(map[uint64]*bytebufferpool.ByteBuffer, len(tr.allTSIDs))
	for _, k := range keys {
		retVal[k] = tr.allTSIDs[k]
	}
	tr.allTSIDs = retVal
}

func (tr *AllMatchedTSIDs) GetNumMatchedTSIDs() int {
	return len(tr.allTSIDs)
}

// returns a map of tsid to groupid
func (tr *AllMatchedTSIDs) GetAllTSIDs() map[uint64]*bytebufferpool.ByteBuffer {
	return tr.allTSIDs
}

func (tr *AllMatchedTSIDs) GetTSIDInfoMap() map[uint64]*AllMatchedTSIDsInfo {
	return tr.tsidInfoMap
}

func (tsidInfo *AllMatchedTSIDs) MergeTSIDs(other *AllMatchedTSIDs) {
	for k, v := range other.allTSIDs {
		tsidInfo.allTSIDs[k] = v
	}
	for k, v := range other.tsidInfoMap {
		tsidInfo.tsidInfoMap[k] = v
	}
}

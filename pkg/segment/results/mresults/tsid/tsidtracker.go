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
	"sort"

	"github.com/valyala/bytebufferpool"
)

var TAG_VALUE_DELIMITER_BYTE = []byte("`")

var TAG_VALUE_DELIMITER_STR = ("`")

/*
Holder struct to track all matched TSIDs
*/
type AllMatchedTSIDs struct {
	allTSIDs map[uint64]*bytebufferpool.ByteBuffer // raw tsids that are currently being tracked
	first    bool
}

/*
This function should initialize a TSID tracker
*/
func InitTSIDTracker(numTagFilters int) (*AllMatchedTSIDs, error) {

	return &AllMatchedTSIDs{
		allTSIDs: make(map[uint64]*bytebufferpool.ByteBuffer, 0),
		first:    true,
	}, nil
}

// If first time, add all tsids to map
// Else, intersect with existing tsids
func (tr *AllMatchedTSIDs) BulkAdd(rawTagValueToTSIDs map[string]map[uint64]struct{}) error {
	if tr.first {
		for tagValue, tsids := range rawTagValueToTSIDs {
			for id := range tsids {
				buff := bytebufferpool.Get()
				_, err := buff.WriteString(tagValue)
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

					_, err := tsidInfo.WriteString(tagValue)
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

// For all incoming tsids, always add tsid and groupid to stored tsids
func (tr *AllMatchedTSIDs) BulkAddStar(rawTagValueToTSIDs map[string]map[uint64]struct{}) error {
	var err error
	for tagValue, tsids := range rawTagValueToTSIDs {
		for id := range tsids {
			buf, ok := tr.allTSIDs[id]
			if !ok {
				buf = bytebufferpool.Get()
				tr.allTSIDs[id] = buf
			}
			_, err = buf.WriteString(tagValue)
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

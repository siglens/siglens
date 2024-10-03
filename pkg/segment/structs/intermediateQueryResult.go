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

package structs

import "github.com/siglens/siglens/pkg/segment/utils"

// Like RecordResultContainer, but with less bloat.
type smallRRC struct {
	Timestamp     uint64
	EncodedSegKey uint64
	BlockNum      uint16
	RecordNum     uint16
}

type IQR struct {
	rrcs             []*smallRRC
	segKeyToEncoding map[string]uint64
	encodingToSegKey map[uint64]string
	encodingToVTable map[uint64]string
}

// All the RRCs must belong to the same semgent.
func (iqr *IQR) AddRRCs(rrcs []*utils.RecordResultContainer, segKey string) {
	if len(rrcs) == 0 {
		return
	}

	encodedSegKey, isNew := iqr.getEncodedSegKey(segKey)
	if isNew {
		iqr.segKeyToEncoding[segKey] = encodedSegKey
		iqr.encodingToSegKey[encodedSegKey] = segKey

		// Since all RRCs have the same segKey, they have the same vTable.
		iqr.encodingToVTable[encodedSegKey] = rrcs[0].VirtualTableName
	}

	for _, rrc := range rrcs {
		iqr.rrcs = append(iqr.rrcs, &smallRRC{
			Timestamp:     rrc.TimeStamp,
			EncodedSegKey: encodedSegKey,
			BlockNum:      rrc.BlockNum,
			RecordNum:     rrc.RecordNum,
		})
	}
}

func (iqr *IQR) getEncodedSegKey(segKey string) (uint64, bool) {
	if encoding, ok := iqr.segKeyToEncoding[segKey]; ok {
		return encoding, false
	}

	return uint64(len(iqr.segKeyToEncoding)), true
}

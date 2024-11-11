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

package processor

import (
	"io"

	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/utils"
)

type scrollProcessor struct {
	scrollFrom uint64
	qid        uint64
}

func (p *scrollProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if iqr == nil {
		return nil, io.EOF
	}
	if p.scrollFrom == 0 {
		return iqr, nil
	}

	numRecordsToDiscard := iqr.NumberOfRecords()
	if p.scrollFrom < uint64(iqr.NumberOfRecords()) {
		numRecordsToDiscard = int(p.scrollFrom)
		p.scrollFrom = 0
	} else {
		p.scrollFrom -= uint64(iqr.NumberOfRecords())
	}
	err := iqr.Discard(numRecordsToDiscard)
	if err != nil {
		return nil, utils.TeeErrorf("scrollProcessor.Process: failed to discard records %v, err: %v", numRecordsToDiscard, err)
	}
	err = query.IncRecordsSent(p.qid, uint64(numRecordsToDiscard))
	if err != nil {
		return nil, utils.TeeErrorf("scrollProcessor.Process: failed to increment progress, err: %v", err)
	}

	return iqr, nil
}

func (p *scrollProcessor) Rewind() {
	// Do nothing
}

func (p *scrollProcessor) Cleanup() {
	// Do nothing
}

func (p *scrollProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return nil, false
}

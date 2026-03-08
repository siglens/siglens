// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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

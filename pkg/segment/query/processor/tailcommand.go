// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package processor

import (
	"fmt"
	"io"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
)

type tailProcessor struct {
	options  *structs.TailExpr
	finalIqr *iqr.IQR
	eof      bool
}

func (p *tailProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if iqr != nil {
		if p.finalIqr == nil || iqr.NumberOfRecords() >= int(p.options.TailRows) {
			p.finalIqr = iqr
			if iqr.NumberOfRecords() > int(p.options.TailRows) {
				extraRows := iqr.NumberOfRecords() - int(p.options.TailRows)
				err := iqr.Discard(extraRows)
				if err != nil {
					return nil, fmt.Errorf("tailProcessor.limitRows: failed to discard extra %v rows: %v", extraRows, err)
				}
			}
		} else {
			recordsToKeep := int(p.options.TailRows) - iqr.NumberOfRecords()
			recordsToDiscard := p.finalIqr.NumberOfRecords() - recordsToKeep
			if recordsToDiscard > 0 {
				err := p.finalIqr.Discard(recordsToDiscard)
				if err != nil {
					return nil, fmt.Errorf("tailProcessor.Process: failed to discard %v rows: %v", recordsToDiscard, err)
				}
			}
			err := p.finalIqr.Append(iqr)
			if err != nil {
				return nil, fmt.Errorf("tailProcessor.Process: failed to append records: %v", err)
			}
		}
		return nil, nil
	}

	if p.eof {
		return nil, io.EOF
	}

	p.eof = true

	if p.finalIqr == nil {
		return nil, io.EOF
	}

	err := p.finalIqr.ReverseRecords()
	if err != nil {
		return nil, fmt.Errorf("tailProcessor.Process: failed to reverse records: %v", err)
	}

	return p.finalIqr, io.EOF
}

func (p *tailProcessor) Rewind() {
	// nothing to do
}

func (p *tailProcessor) Cleanup() {
	// nothing to do
}

func (p *tailProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	if p.eof {
		return p.finalIqr, true
	}
	return nil, false
}

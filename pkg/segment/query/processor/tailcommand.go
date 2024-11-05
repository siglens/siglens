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
		if p.finalIqr == nil {
			p.finalIqr = iqr
		} else {
			err := p.finalIqr.Append(iqr)
			if err != nil {
				return nil, fmt.Errorf("tailProcessor.Process: failed to append iqr: %v", err)
			}
		}
		if p.finalIqr.NumberOfRecords() > int(p.options.TailRows) {
			extraRows := p.finalIqr.NumberOfRecords() - int(p.options.TailRows)
			err := p.finalIqr.Discard(extraRows)
			if err != nil {
				return nil, fmt.Errorf("tailProcessor.Process: failed to discard extra %v rows: %v", extraRows, err)
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

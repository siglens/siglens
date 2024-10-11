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

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	log "github.com/sirupsen/logrus"
)

type headProcessor struct {
	options        *structs.HeadExpr
	numRecordsSent uint64
}

func (p *headProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if iqr == nil {
		return nil, nil
	}

	limit := p.options.MaxRows
	numToKeep := limit - p.numRecordsSent
	err := iqr.DiscardAfter(numToKeep)
	if err != nil {
		log.Errorf("headProcessor: failed to discard after %v records: %v", numToKeep, err)
		return nil, err
	}

	p.numRecordsSent += uint64(iqr.NumberOfRecords())

	if p.numRecordsSent >= limit {
		return iqr, io.EOF
	} else {
		return iqr, nil
	}
}

func (p *headProcessor) Rewind() {
	panic("not implemented")
}

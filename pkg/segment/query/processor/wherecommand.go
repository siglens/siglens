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
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type whereProcessor struct {
	options *structs.BoolExpr
}

func (p *whereProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if iqr == nil {
		// There's no more input. Since "where" doesn't store any unprocessed
		// data, just return EOF.
		return nil, io.EOF
	}

	requiredFields := p.options.GetFields()
	valuesOfRequiredFields := make([][]sutils.CValueEnclosure, 0, len(requiredFields))
	for _, field := range requiredFields {
		values, err := iqr.ReadColumn(field)
		if err != nil {
			return nil, utils.TeeErrorf("where.Process: cannot get field values; field: %s; err: %v", field, err)
		}
		if values == nil {
			// The field is not present, discard all the rows
			// TODO: Fix evaluation for non existing fields
			err := iqr.Discard(iqr.NumberOfRecords())
			if err != nil {
				return nil, utils.TeeErrorf("where.Process: Error while discarding rows due to col %v not found, err: %v", field, err)
			}
			return iqr, nil
		}

		valuesOfRequiredFields = append(valuesOfRequiredFields, values)
	}

	rowsToDiscard := make([]int, 0, iqr.NumberOfRecords())
	singleRow := make(map[string]sutils.CValueEnclosure, len(requiredFields))
	for row := 0; row < iqr.NumberOfRecords(); row++ {
		for col, field := range requiredFields {
			singleRow[field] = valuesOfRequiredFields[col][row]
		}

		shouldKeep, err := p.options.Evaluate(singleRow)
		if err != nil {
			return nil, utils.TeeErrorf("where.Process: cannot evaluate expression; err=%v", err)
		}

		if !shouldKeep {
			rowsToDiscard = append(rowsToDiscard, row)
		}
	}

	err := iqr.DiscardRows(rowsToDiscard)
	if err != nil {
		log.Errorf("where.Process: cannot discard rows; err=%v", err)
		return nil, err
	}

	return iqr, nil
}

func (p *whereProcessor) Rewind() {
	// Nothing to do.
}

func (p *whereProcessor) Cleanup() {
	// Nothing to do.
}

func (p *whereProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return nil, false
}

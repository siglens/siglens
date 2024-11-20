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
	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type headProcessor struct {
	options        *structs.HeadExpr
	numRecordsSent uint64
}

func (p *headProcessor) processHeadExpr(iqr *iqr.IQR) (*iqr.IQR, error) {
	if iqr == nil || p.options.Done {
		return nil, io.EOF
	}

	requiredFields := p.options.BoolExpr.GetFields()
	records, err := iqr.ReadColumnsWithBackfill(requiredFields)
	if err != nil {
		return nil, toputils.TeeErrorf("headProcessor.processHeadExpr: failed to read columns, requiredFields: %v, err: %v", requiredFields, err)
	}

	row := 0
	for row < iqr.NumberOfRecords() && !p.options.Done && p.numRecordsSent < p.options.MaxRows {
		fieldToValue := make(map[string]utils.CValueEnclosure, len(requiredFields))
		for _, field := range requiredFields {
			fieldToValue[field] = records[field][row]
		}

		// Will result in true, false, or backfill;
		conditionCValEnc, err := p.options.BoolExpr.EvaluateWithNull(fieldToValue)
		if err != nil {
			nullFields, errGetNullFields := p.options.BoolExpr.GetNullFields(fieldToValue)
			if errGetNullFields != nil {
				return nil, toputils.TeeErrorf("headProcessor.processHeadExpr: error while getting null fields, err: %v", errGetNullFields)
			}

			if len(nullFields) > 0 {
				err := conditionCValEnc.ConvertValue(nil)
				if err != nil {
					return nil, fmt.Errorf("headProcessor.processHeadExpr: failed to convert value to nil: %v", err)
				}
			}
		}

		if conditionCValEnc.Dtype == utils.SS_DT_BACKFILL {
			if p.options.Null {
				// include the records since null is allowed
			} else if p.options.Keeplast {
				// if keeplast is true, we need to include this record
				p.options.Done = true
			} else {
				// if null is not allowed and keeplast is false, we are done processing
				p.options.Done = true
				break
			}
		} else if conditionCValEnc.Dtype == utils.SS_DT_BOOL {
			conditionPassed := conditionCValEnc.CVal.(bool)
			if !conditionPassed {
				// false condition so adding last record if keeplast
				if p.options.Keeplast {
					p.options.Done = true
				} else {
					p.options.Done = true
					break
				}
			}
		} else {
			return nil, fmt.Errorf("headProcessor.processHeadExpr: unexpected dtype: %v", conditionCValEnc.Dtype)
		}

		row++
		p.numRecordsSent++
	}

	if p.numRecordsSent == p.options.MaxRows {
		p.options.Done = true
	}
	err = iqr.DiscardAfter(uint64(row))
	if err != nil {
		return nil, toputils.TeeErrorf("headProcessor.processHeadExpr: failed to discard after %v records, totalRecords: %v, err: %v", row, iqr.NumberOfRecords(), err)
	}

	if p.options.Done {
		return iqr, io.EOF
	} else {
		return iqr, nil
	}
}

func (p *headProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if iqr == nil {
		return nil, io.EOF
	}

	if p.options.BoolExpr != nil {
		return p.processHeadExpr(iqr)
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
	p.options.Done = false
	p.numRecordsSent = 0
}

func (p *headProcessor) Cleanup() {
	// Nothing to do.
}

func (p *headProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return nil, false
}

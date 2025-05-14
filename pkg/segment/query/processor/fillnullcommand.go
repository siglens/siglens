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

type fillnullProcessor struct {
	options      *structs.FillNullExpr
	knownColumns map[string]struct{}
	secondPass   bool
}

func performFillNullForTheFields(iqr *iqr.IQR, fields map[string]struct{}, cTypeFillValue sutils.CValueEnclosure) {

	for field := range fields {
		values, err := iqr.ReadColumn(field)
		if err != nil {
			log.Errorf("performFillNullForTheFields: cannot read column %v; err=%v", field, err)
			continue
		}

		if values == nil {
			values = utils.ResizeSliceWithDefault(values, iqr.NumberOfRecords(), cTypeFillValue)
		} else {

			for i := range values {
				if values[i].IsNull() {
					values[i].CVal = cTypeFillValue.CVal
					values[i].Dtype = cTypeFillValue.Dtype
				}
			}
		}

		err = iqr.AppendKnownValues(map[string][]sutils.CValueEnclosure{field: values})
		if err != nil {
			log.Errorf("performFillNullForTheFields: failed to append known values; err=%v", err)
		}
	}
}

func (p *fillnullProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if iqr == nil {
		return nil, io.EOF
	}

	cTypeFillValue := sutils.CValueEnclosure{}
	err := cTypeFillValue.ConvertValue(p.options.Value)
	if err != nil {
		return nil, utils.TeeErrorf("performFillNullForTheFields: cannot convert fill value; err=%v", err)
	}

	if len(p.options.FieldList) > 0 {
		fieldListMap := make(map[string]struct{}, len(p.options.FieldList))
		utils.AddSliceToSet(fieldListMap, p.options.FieldList)

		performFillNullForTheFields(iqr, fieldListMap, cTypeFillValue)
		return iqr, nil
	}

	/* If no fields are specified, fill null for all columns. */

	if p.secondPass {
		// This means that the firstPass is done, and we have all the columns for all the possible records.
		// So, we can fill null for all the columns.
		performFillNullForTheFields(iqr, p.knownColumns, cTypeFillValue)
		return iqr, nil
	}

	// If we are in first Pass: Then Fetch all columns from the IQR and store in knownColumns.
	columns, err := iqr.GetColumns()
	if err != nil {
		return nil, utils.TeeErrorf("fillnull.Process: cannot get columns; err=%v", err)
	}

	if p.knownColumns == nil {
		p.knownColumns = columns
	} else {
		utils.AddMapKeysToSet(p.knownColumns, columns)
	}

	return iqr, nil
}

// In the two-pass version of fillnull, Rewind() should remember all the
// columns it saw in the first pass.
func (p *fillnullProcessor) Rewind() {
	p.secondPass = true
}

func (p *fillnullProcessor) Cleanup() {
	p.knownColumns = nil
	p.secondPass = false
}

func (p *fillnullProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return nil, false
}

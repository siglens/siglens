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
	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type fillnullProcessor struct {
	options *structs.FillNullExpr
}

func getTheFinalFillValue(fillValue string, qid uint64) (utils.CValueEnclosure, error) {
	fillValueDtype, err := utils.CreateDtypeEnclosure(fillValue, qid)
	if err != nil {
		return utils.CValueEnclosure{}, toputils.TeeErrorf("qid=%v, performFillNullForTheFields: cannot create dtype for the fill Value; err=%v", qid, err)
	}

	var finalFillValue interface{}

	if fillValueDtype.IsBool() {
		finalFillValue = fillValueDtype.BoolVal
	} else if fillValueDtype.IsInt() {
		finalFillValue = fillValueDtype.SignedVal
	} else if fillValueDtype.IsFloat() {
		finalFillValue = fillValueDtype.FloatVal
	} else {
		finalFillValue = fillValue
	}

	return utils.CValueEnclosure{CVal: finalFillValue, Dtype: fillValueDtype.Dtype}, nil
}

func performFillNullForTheFields(iqr *iqr.IQR, fields []string, cTypeFillValue utils.CValueEnclosure) {
	fillNullForAllRecords := make(map[string]struct{})

	for _, field := range fields {
		values, err := iqr.ReadColumn(field)
		if err != nil {
			fillNullForAllRecords[field] = struct{}{}
			continue
		}

		for i, value := range values {
			if value.IsNull() {
				value.CVal = cTypeFillValue.CVal
				value.Dtype = cTypeFillValue.Dtype
				values[i] = value
			}
		}

		err = iqr.AppendKnownValues(map[string][]utils.CValueEnclosure{field: values})
		if err != nil {
			log.Errorf("performFillNullForTheFields: failed to append known values; err=%v", err)
		}
	}

	for field := range fillNullForAllRecords {
		err := iqr.AddColumnWithDefaultValue(field, cTypeFillValue)
		if err != nil {
			log.Errorf("performFillNullForTheFields: failed to add column with default value; err=%v", err)
		}
	}
}

func (p *fillnullProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if iqr == nil {
		return nil, io.EOF
	}

	cTypeFillValue, err := getTheFinalFillValue(p.options.Value, iqr.GetQID())
	if err != nil {
		return nil, err
	}

	if len(p.options.FieldList) > 0 {
		performFillNullForTheFields(iqr, p.options.FieldList, cTypeFillValue)
	}

	return iqr, nil
}

// In the two-pass version of fillnull, Rewind() should remember all the
// columns it saw in the first pass.
func (p *fillnullProcessor) Rewind() {
	panic("not implemented")
}

func (p *fillnullProcessor) Cleanup() {
	panic("not implemented")
}

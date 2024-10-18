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

func performFillNullForTheFields(iqr *iqr.IQR, fields []string, cTypeFillValue utils.CValueEnclosure) {

	for _, field := range fields {
		values, err := iqr.ReadColumn(field)
		if err != nil {
			values = toputils.ResizeSliceWithDefault(values, iqr.NumberOfRecords(), cTypeFillValue)
		} else {

			for i := range values {
				if values[i].IsNull() {
					values[i].CVal = cTypeFillValue.CVal
					values[i].Dtype = cTypeFillValue.Dtype
				}
			}
		}

		err = iqr.AppendKnownValues(map[string][]utils.CValueEnclosure{field: values})
		if err != nil {
			log.Errorf("performFillNullForTheFields: failed to append known values; err=%v", err)
		}
	}
}

func (p *fillnullProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if iqr == nil {
		return nil, io.EOF
	}

	cTypeFillValue := utils.CValueEnclosure{}
	err := cTypeFillValue.ConvertValue(p.options.Value)
	if err != nil {
		return nil, toputils.TeeErrorf("performFillNullForTheFields: cannot convert fill value; err=%v", err)
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

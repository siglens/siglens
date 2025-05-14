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
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type mvexpandProcessor struct {
	options *structs.MultiValueColLetRequest
}

type orderedItem[T any] struct {
	index int
	value T
}

func (p *mvexpandProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if iqr == nil {
		return nil, io.EOF
	}

	if p.options.Command != "mvexpand" {
		return nil, fmt.Errorf("mvexpand.Process: unexpected command: %s", p.options.Command)
	}

	fieldToValues, err := iqr.ReadAllColumns()
	if err != nil {
		log.Errorf("mvexpand.Process: failed to read all columns; err=%v", err)
		return nil, err
	}

	values, ok := fieldToValues[p.options.ColName]
	if !ok {
		// This column doesn't exist.
		return iqr, nil
	}

	orderedItems := make([]orderedItem[interface{}], 0, len(values))
	limit, hasLimit := p.options.Limit.Get()
	for i, value := range values {
		switch value.Dtype {
		case sutils.SS_DT_STRING_SLICE:
			for j, v := range value.CVal.([]string) {
				if hasLimit && j >= int(limit) {
					break
				}

				orderedItems = append(orderedItems, orderedItem[interface{}]{index: i, value: v})
			}
		default:
			return nil, utils.TeeErrorf("mvexpand.Process: unexpected dtype: %v", value.Dtype)
		}
	}

	// TODO: As a memory optimization, if we have to make a lot of new rows, we
	// may want to make a new IQR with only some of them, and keep the extras
	// in our internal state for the next call to Process.
	newFieldToValues := make(map[string][]sutils.CValueEnclosure, len(fieldToValues))
	for fieldName := range fieldToValues {
		columnValues := make([]sutils.CValueEnclosure, 0, len(orderedItems))

		if fieldName == p.options.ColName {
			for _, item := range orderedItems {
				value := sutils.CValueEnclosure{
					Dtype: sutils.SS_DT_STRING,
					CVal:  item.value,
				}

				columnValues = append(columnValues, value)
			}
		} else {
			for _, item := range orderedItems {
				columnValues = append(columnValues, fieldToValues[fieldName][item.index])
			}
		}

		newFieldToValues[fieldName] = columnValues
	}

	newIQR := iqr.BlankCopy()
	err = newIQR.AppendKnownValues(newFieldToValues)
	if err != nil {
		log.Errorf("mvexpand.Process: failed to set new values; err=%v", err)
		return nil, err
	}

	return newIQR, nil
}

func (p *mvexpandProcessor) Rewind() {
	// Nothing to do.
}

func (p *mvexpandProcessor) Cleanup() {
	// Nothing to do.
}

func (p *mvexpandProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return nil, false
}

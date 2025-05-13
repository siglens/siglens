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
)

type evalProcessor struct {
	options *structs.EvalExpr
}

func (p *evalProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if iqr == nil {
		return nil, io.EOF
	}
	if p.options == nil {
		return iqr, fmt.Errorf("evalProcessor.Process: options is nil")
	}
	if p.options.ValueExpr == nil {
		return iqr, utils.TeeErrorf("evalProcessor.Process: ValueExpr is nil")
	}
	fieldsInExpr := p.options.ValueExpr.GetFields()

	if len(fieldsInExpr) == 1 && fieldsInExpr[0] == "*" {
		allCnames, err := iqr.GetColumns()
		if err != nil {
			return nil, fmt.Errorf("evalProcessor.Process: failed to get all column names, err: %v", err)
		}
		fieldsInExpr = []string{}
		for cname := range allCnames {
			fieldsInExpr = append(fieldsInExpr, cname)
		}
	}

	numRecords := iqr.NumberOfRecords()

	records := make(map[string][]sutils.CValueEnclosure)
	for _, field := range fieldsInExpr {
		record, err := iqr.ReadColumn(field)
		if err != nil {
			return nil, fmt.Errorf("evalProcessor.Process: failed to read column %v, err: %v", field, err)
		}
		if len(record) != numRecords {
			return nil, fmt.Errorf("evalProcessor.Process: column %v has %v records, expected: %v", field, len(record), numRecords)
		}
		records[field] = record
	}

	knownValues := make(map[string][]sutils.CValueEnclosure)
	knownValuesCvals := make([]sutils.CValueEnclosure, numRecords)

	fieldToValue := make(map[string]sutils.CValueEnclosure)
	for i := 0; i < numRecords; i++ {
		for field, record := range records {
			fieldToValue[field] = record[i]
		}

		value, err := p.options.ValueExpr.EvaluateValueExpr(fieldToValue)
		if err != nil {
			return nil, fmt.Errorf("evalProcessor.Process: failed to evaluate ValueExpr on raw record, err: %v", err)
		}

		err = knownValuesCvals[i].ConvertValue(value)
		if err != nil {
			return nil, fmt.Errorf("evalProcessor.Process: failed to convert value: %v, err: %v", value, err)
		}
	}

	knownValues[p.options.FieldName] = knownValuesCvals

	err := iqr.AppendKnownValues(knownValues)
	if err != nil {
		return nil, fmt.Errorf("evalProcessor.Process: failed to append known values, err: %v", err)
	}

	return iqr, nil
}

func (p *evalProcessor) Rewind() {
	// Nothing to do
}

func (p *evalProcessor) Cleanup() {
	// Nothing to do
}

func (p *evalProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return nil, false
}

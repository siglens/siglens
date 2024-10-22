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
)

type evalProcessor struct {
	options *structs.EvalExpr
}

func evaluateValueExpr(valueExpr *structs.ValueExpr, fieldToValue map[string]utils.CValueEnclosure) (interface{}, error) {
	switch valueExpr.ValueExprMode {
	case structs.VEMConditionExpr:
		value, err := valueExpr.ConditionExpr.EvaluateCondition(fieldToValue)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate condition expr, err=%v", err)
		}
		return value, nil
	case structs.VEMStringExpr:
		value, err := valueExpr.EvaluateValueExprAsString(fieldToValue)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate string expr, err=%v", err)
		}
		return value, nil
	case structs.VEMNumericExpr:
		value, err := valueExpr.EvaluateToFloat(fieldToValue)
		if err != nil {
			// It failed to evaluate to a float, it could possibly that the field given is a string
			valueStr, err := valueExpr.EvaluateToString(fieldToValue)
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate numeric expr to Numeric Expr and string Expr, err=%v", err)
			}

			return valueStr, err
		}
		return value, nil
	case structs.VEMBooleanExpr:
		value, err := valueExpr.EvaluateToString(fieldToValue)
		if err != nil {
			return nil, fmt.Errorf(" failed to evaluate boolean expr, err=%v", err)
		}
		return value, nil
	case structs.VEMMultiValueExpr:
		mvSlice, err := valueExpr.EvaluateToMultiValue(fieldToValue)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate multi value expr, err=%v", err)
		}
		return mvSlice, nil
	default:
		return nil, fmt.Errorf("unknown value expr mode %v", valueExpr.ValueExprMode)
	}
}

func (p *evalProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if iqr == nil {
		return nil, io.EOF
	}
	if p.options == nil {
		return iqr, nil
	}
	if p.options.ValueExpr == nil {
		return iqr, fmt.Errorf("value expr is nil")
	}
	fieldsInExpr := p.options.ValueExpr.GetFields()
	allCnames, err := iqr.GetAllColumnNames()
	if err != nil {
		return nil, fmt.Errorf("failed to get all column names, err=%v", err)
	}
	if len(fieldsInExpr) == 1 && fieldsInExpr[0] == "*" {
		fieldsInExpr = []string{}
		for cname := range allCnames {
			fieldsInExpr = append(fieldsInExpr, cname)
		}
	}

	records := make(map[string][]utils.CValueEnclosure)
	for _, field := range fieldsInExpr {
		record, err := iqr.ReadColumn(field)
		if err != nil {
			return nil, fmt.Errorf("failed to read column, err=%v", err)
		}
		records[field] = record
	}

	knownValues := make(map[string][]utils.CValueEnclosure)

	for i := 0; i < iqr.NumberOfRecords(); i++ {
		fieldToValue := make(map[string]utils.CValueEnclosure)
		for field, record := range records {
			fieldToValue[field] = record[i]
		}

		value, err := evaluateValueExpr(p.options.ValueExpr, fieldToValue)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate ValueExpr on raw record, err=%v", err)
		}
		CValEnc := utils.CValueEnclosure{}
		err = CValEnc.ConvertValue(value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert value, err=%v", err)
		}
		knownValues[p.options.FieldName] = append(knownValues[p.options.FieldName], CValEnc)
	}

	err = iqr.AppendKnownValues(knownValues)
	if err != nil {
		return nil, fmt.Errorf("failed to append known values, err=%v", err)
	}

	return iqr, nil
}

func (p *evalProcessor) Rewind() {
	// Nothing to do
}

func (p *evalProcessor) Cleanup() {
	// Nothing to do
}

// Copyright (c) 2021-2025 SigScalr, Inc.
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
	"encoding/json"
	"io"
	"strings"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
)

type tojsonProcessor struct {
	options *structs.ToJsonExpr
}

// stores the result as a string. If another command needs the json, it needs to unmarshal the string first
func (p *tojsonProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if iqr == nil {
		return nil, io.EOF
	}

	columnToDtype := make(map[string]structs.ToJsonDtypes)
	if len(p.options.FieldsDtypes) != 0 {
		allColumns, err := iqr.GetColumns()
		if err != nil {
			return nil, utils.TeeErrorf("tojson.Process: cannot get all column names; err: %v", err)
		}
		for cname := range allColumns {
			for _, option := range p.options.FieldsDtypes {
				compiledRgx := option.Regex.GetCompiledRegex()
				if compiledRgx.Match([]byte(cname)) {
					if option.Dtype == structs.TJ_PostProcess {
						columnToDtype[cname] = p.options.DefaultType.Dtype
					} else {
						columnToDtype[cname] = option.Dtype
					}
				}
			}
		}
	}

	var err error
	var values map[string][]sutils.CValueEnclosure
	if p.options.AllFields {
		values, err = iqr.ReadAllColumns()
		if err != nil {
			return nil, utils.TeeErrorf("tojson.Process: cannot get values for all the columns; err: %v", err)
		}
	} else {
		values = make(map[string][]sutils.CValueEnclosure)
		for cname := range columnToDtype {
			value, err := iqr.ReadColumn(cname)
			if err != nil {
				return nil, utils.TeeErrorf("tojson.Process: cannot get values for the column: %v", cname)
			}
			values[cname] = value
		}
	}
	var anyKey string
	for anyKey = range values {
		break
	}
	res := make([]map[string]any, len(values[anyKey]))
	for i := range res {
		res[i] = make(map[string]any, len(values))
	}
	for cname, vals := range values {
		dtype, ok := columnToDtype[cname]
		if !ok {
			dtype = p.options.DefaultType.Dtype
		}
		for i, val := range vals {
			strVal, err := val.GetValueAsString()
			if err != nil {
				continue
			}
			convertedVal, skip := ConvertToValidToJsonDtype(strVal, val.Dtype, dtype, cname, p.options.FillNull, p.options.DefaultType.Dtype)
			if skip {
				continue
			}
			res[i][cname] = convertedVal
		}
	}

	rowJsons := make([]sutils.CValueEnclosure, len(res))
	for i, row := range res {
		cVal := sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_STRING,
		}
		jsonBytes, err := json.Marshal(row)
		if err != nil {
			cVal.CVal = "{}"
			rowJsons[i] = cVal
			continue
		}
		cVal.CVal = string(jsonBytes)
		rowJsons[i] = cVal
	}
	jsonField := map[string][]sutils.CValueEnclosure{
		strings.ReplaceAll(p.options.OutputField, "\"", ""): rowJsons,
	}
	err = iqr.AppendKnownValues(jsonField)
	if err != nil {
		return nil, utils.TeeErrorf("tojson.Process: cannot add values to the iqr: err: %v", err)
	}
	return iqr, nil
}

func ConvertToValidToJsonDtype(val string, inDtype sutils.SS_DTYPE, dtype structs.ToJsonDtypes, cname string, fillNull bool, defaultDtype structs.ToJsonDtypes) (any, bool) {
	returnNull := func(fillNull bool) (any, bool) {
		if fillNull {
			return nil, false
		} else {
			return nil, true
		}
	}
	switch dtype {
	case structs.TJ_None:
		num, err := utils.FastParseFloat([]byte(val))
		if err != nil {
			return val, false
		} else {
			return num, false
		}
	case structs.TJ_Auto:
		switch val {
		case "true":
			return true, false
		case "false":
			return false, false
		case "null":
			return nil, false
		case "":
			return nil, false
		default:
			num, err := utils.FastParseFloat([]byte(val))
			if err != nil {
				var v any
				err := json.Unmarshal([]byte(val), &v)
				if err == nil {
					return v, false
				} else {
					return val, false
				}
			} else {
				return num, false
			}
		}
	case structs.TJ_Bool:
		switch strings.ToLower(val) {
		case "true", "t", "yes":
			return true, false
		case "false", "f", "no", "0":
			return false, false
		default:
			return returnNull(fillNull)
		}
	case structs.TJ_Json:
		num, err := utils.FastParseFloat([]byte(val))
		if err != nil {
			var v any
			err := json.Unmarshal([]byte(val), &v)
			if err != nil {
				if inDtype == sutils.SS_DT_STRING {
					return val, false
				} else {
					return returnNull(fillNull)
				}
			} else {
				return v, false
			}
		} else {
			return num, false
		}
	case structs.TJ_Num:
		num, err := utils.FastParseFloat([]byte(val))
		if err != nil {
			return returnNull(fillNull)
		} else {
			return num, false
		}
	case structs.TJ_Str:
		return val, false
	default:
		return returnNull(fillNull)
	}
}

func (p *tojsonProcessor) Rewind() {
	// Nothing to do
}

func (p *tojsonProcessor) Cleanup() {
	// Nothing to do
}

func (p *tojsonProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return nil, false
}

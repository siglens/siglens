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
	"regexp"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type rexProcessor struct {
	options       *structs.RexExpr
	compiledRegex *regexp.Regexp
}

func (p *rexProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if iqr == nil {
		return nil, io.EOF
	}

	if p.compiledRegex == nil {
		compiledRegex, err := regexp.Compile(p.options.Pattern)
		if err != nil {
			log.Errorf("rex.Process: cannot compile regex; pattern=%s; err=%v",
				p.options.Pattern, err)
			return nil, err
		}

		if compiledRegex == nil {
			return nil, toputils.TeeErrorf("rex.Process: compiled regex is nil for %v",
				p.options.Pattern)
		}

		p.compiledRegex = compiledRegex
	}

	values, err := iqr.ReadColumn(p.options.FieldName)
	if err != nil {
		log.Errorf("rex.Process: cannot get field values; field=%s; err=%v",
			p.options.FieldName, err)
		return nil, err
	}

	if len(values) == 0 {
		return iqr, nil
	}

	newColValues := make(map[string][]segutils.CValueEnclosure, len(p.options.RexColNames))
	for _, rexColName := range p.options.RexColNames {
		newColValues[rexColName] = toputils.ResizeSliceWithDefault(newColValues[rexColName], len(values), segutils.CValueEnclosure{
			Dtype: segutils.SS_DT_BACKFILL,
			CVal:  nil,
		})
	}

	for i, value := range values {
		valueStr, err := value.GetString()
		if err != nil {
			log.Errorf("rex.Process: cannot convert value %v to string; err=%v",
				value, err)
			return nil, err
		}

		rexResultMap, err := structs.MatchAndExtractGroups(valueStr, p.compiledRegex)
		if err != nil {
			// If there are no matches we will skip this row
			continue
		}

		for rexColName, rexValue := range rexResultMap {
			newColValues[rexColName][i].Dtype = segutils.SS_DT_STRING
			newColValues[rexColName][i].CVal = rexValue
		}
	}

	err = iqr.AppendKnownValues(newColValues)
	if err != nil {
		log.Errorf("rex.Process: cannot add new columns; err=%v", err)
		return nil, err
	}

	return iqr, nil
}

func (p *rexProcessor) Rewind() {
	// Nothing to do.
}

func (p *rexProcessor) Cleanup() {
	// Nothing to do.
}

func (p *rexProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return nil, false
}

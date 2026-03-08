// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package processor

import (
	"io"
	"regexp"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
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
			return nil, utils.TeeErrorf("rex.Process: compiled regex is nil for %v",
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

	newColValues := make(map[string][]sutils.CValueEnclosure, len(p.options.RexColNames))
	for _, rexColName := range p.options.RexColNames {
		newColValues[rexColName] = utils.ResizeSliceWithDefault(newColValues[rexColName], len(values), sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_BACKFILL,
			CVal:  nil,
		})
	}

	for idx, value := range values {
		valueStr, err := value.GetValueAsString()
		if err != nil {
			log.Errorf("rex.Process: cannot convert value %v to string; err=%v",
				value, err)
			return nil, err
		}

		err = structs.MatchAndPopulateNamedGroups(valueStr, p.compiledRegex, newColValues,
			idx, len(values))
		if err != nil {
			// If there are no matches we will skip this row
			continue
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

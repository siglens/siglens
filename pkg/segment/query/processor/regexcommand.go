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
	"regexp"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
)

type regexProcessor struct {
	options       *structs.RegexExpr
	compiledRegex *regexp.Regexp
}

func (p *regexProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if iqr == nil {
		return nil, io.EOF
	}

	var keepMatch bool

	switch p.options.Op {
	case "=":
		keepMatch = true
	case "!=":
		keepMatch = false
	default:
		return nil, utils.TeeErrorf("qid=%v, regex.Process: unknown operator; op=%s", iqr.GetQID(), p.options.Op)
	}

	if p.compiledRegex == nil {
		p.compiledRegex = p.options.GobRegexp.GetCompiledRegex()
		if p.compiledRegex == nil {
			return nil, utils.TeeErrorf("qid=%v, regex.Process: Gob compiled regex is nil", iqr.GetQID())
		}
	}

	if p.options.Field == "*" {
		return p.processRegexOnAllColumns(iqr, keepMatch)
	} else {
		return p.processRegexOnSingleColumn(iqr, keepMatch)
	}
}

func (p *regexProcessor) Rewind() {
	// Nothing to do here.
}

func (p *regexProcessor) Cleanup() {
	// Nothing to do here.
}

func (p *regexProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return nil, false
}

func (p *regexProcessor) performRegexMatch(value sutils.CValueEnclosure) bool {
	stringVal, err := value.GetString()
	if err != nil {
		stringVal = fmt.Sprintf("%v", value.CVal)
	}

	return p.compiledRegex.MatchString(stringVal)
}

func (p *regexProcessor) processRegexOnAllColumns(iqr *iqr.IQR, keepMatch bool) (*iqr.IQR, error) {
	numberOfRecords := iqr.NumberOfRecords()

	rowsToDiscard := make([]int, 0, numberOfRecords)

	valuesMap, err := iqr.ReadAllColumns()
	if err != nil {
		return iqr, utils.TeeErrorf("qid=%v, regex.Process.processRegexOnAllColumns: cannot read all columns; err=%v", iqr.GetQID(), err)
	}

	for i := 0; i < numberOfRecords; i++ {
		matched := false
		for _, values := range valuesMap {
			matched = p.performRegexMatch(values[i])
			if matched {
				break
			}
		}

		if matched != keepMatch {
			rowsToDiscard = append(rowsToDiscard, i)
		}
	}

	err = iqr.DiscardRows(rowsToDiscard)
	if err != nil {
		return nil, utils.TeeErrorf("qid=%v, regex.Process.processRegexOnAllColumns: cannot discard rows; err=%v", iqr.GetQID(), err)
	}

	return iqr, nil

}

func (p *regexProcessor) processRegexOnSingleColumn(iqr *iqr.IQR, keepMatch bool) (*iqr.IQR, error) {
	numberOfRecords := iqr.NumberOfRecords()

	values, err := iqr.ReadColumn(p.options.Field)
	if err != nil {
		return iqr, utils.TeeErrorf("qid=%v, regex.Process.processRegexOnSingleColumn: cannot get field values; field=%s; err=%v", iqr.GetQID(), p.options.Field, err)
	}

	rowsToDiscard := make([]int, 0, numberOfRecords)

	for i := range values {

		matched := p.performRegexMatch(values[i])

		if matched != keepMatch {
			rowsToDiscard = append(rowsToDiscard, i)
		}
	}

	err = iqr.DiscardRows(rowsToDiscard)
	if err != nil {
		return nil, utils.TeeErrorf("qid=%v, regex.Process.processRegexOnSingleColumn: cannot discard rows; err=%v", iqr.GetQID(), err)
	}

	return iqr, nil
}

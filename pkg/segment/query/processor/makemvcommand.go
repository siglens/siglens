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
	"strings"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type makemvProcessor struct {
	options       *structs.MultiValueColLetRequest
	compiledRegex *regexp.Regexp
}

func (p *makemvProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if iqr == nil {
		return nil, io.EOF
	}

	if p.options.Command != "makemv" {
		return nil, fmt.Errorf("makemv.Process: unexpected command: %s", p.options.Command)
	}

	if p.options.IsRegex && p.compiledRegex == nil {
		regex, err := regexp.Compile(p.options.DelimiterString)
		if err != nil {
			log.Errorf("makemv.Process: failed to compile regex %v; err=%v",
				p.options.DelimiterString, err)
			return nil, err
		}

		p.compiledRegex = regex
	}

	values, err := iqr.ReadColumn(p.options.ColName)
	if err != nil {
		log.Errorf("makemv.Process: failed to read column %v; err=%v", p.options.ColName, err)
		return nil, err
	}

	for i := range values {
		err := p.processOneValue(&values[i])
		if err != nil {
			log.Errorf("makemv.Process: failed to perform makemv; err=%v", err)
			return nil, err
		}
	}

	return iqr, nil
}

func (p *makemvProcessor) processOneValue(value *sutils.CValueEnclosure) error {
	if value == nil {
		return utils.TeeErrorf("makemv.processOneValue: value is nil")
	}

	strVal, err := value.GetString()
	if err != nil {
		if utils.IsNilValueError(err) {
			cErr := value.ConvertValue(nil)
			if cErr != nil {
				return fmt.Errorf("makemv.processOneValue: failed to convert nil value; err=%v", cErr)
			}
			return nil
		}
		return fmt.Errorf("makemv.processOneValue: failed to get string value; err=%v", err)
	}

	var values []string
	if p.options.IsRegex {
		matches := p.compiledRegex.FindAllStringSubmatch(strVal, -1)
		for _, match := range matches {
			if len(match) > 1 {
				values = append(values, match[1])
			}
		}
	} else {
		values = strings.Split(strVal, p.options.DelimiterString)
	}

	if !p.options.AllowEmpty {
		values = utils.SelectFromSlice(values, func(s string) bool {
			return s != ""
		})
	}

	if p.options.Setsv {
		value.Dtype = sutils.SS_DT_STRING
		value.CVal = strings.Join(values, " ")
	} else {
		value.Dtype = sutils.SS_DT_STRING_SLICE
		value.CVal = values
	}

	return nil
}

func (p *makemvProcessor) Rewind() {
	// Nothing to do.
}

func (p *makemvProcessor) Cleanup() {
	// Nothing to do.
}

func (p *makemvProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return nil, false
}

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
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type sortProcessor struct {
	options      *structs.SortExpr
	resultsSoFar *iqr.IQR
	err          error
}

func (p *sortProcessor) Process(inputIQR *iqr.IQR) (*iqr.IQR, error) {
	if inputIQR == nil {
		// There's no more input, so we can send the results.
		return p.resultsSoFar, io.EOF
	}

	err := inputIQR.Sort(p.less)
	if err != nil {
		log.Errorf("sort.Process: cannot sort IQR; err=%v", err)
		return nil, err
	}

	if p.resultsSoFar == nil {
		err = inputIQR.DiscardAfter(p.options.Limit)
		if err != nil {
			log.Errorf("sort.Process: cannot discard after limit; err=%v", err)
			return nil, err
		}

		p.resultsSoFar = inputIQR
		return nil, nil
	}

	// Merge the two IQRs.
	result, firstExhausted, err := iqr.MergeIQRs([]*iqr.IQR{p.resultsSoFar, inputIQR}, p.less)
	if err != nil {
		log.Errorf("sort.Process: cannot merge IQRs; err=%v", err)
		return nil, err
	}
	if p.err != nil {
		log.Errorf("sort.Process: error comparing records; err=%v", p.err)
		return nil, p.err
	}

	var leftover *iqr.IQR
	switch firstExhausted {
	case 0:
		leftover = inputIQR
	case 1:
		leftover = p.resultsSoFar
	default:
		return nil, utils.TeeErrorf("sort.Process: unexpected value for firstExhausted: %v",
			firstExhausted)
	}

	err = result.Append(leftover)
	if err != nil {
		log.Errorf("sort.Process: cannot append leftover IQR; err=%v", err)
		return nil, err
	}

	p.resultsSoFar = result
	err = p.resultsSoFar.DiscardAfter(p.options.Limit)
	if err != nil {
		log.Errorf("sort.Process: cannot discard after limit; err=%v", err)
		return nil, err
	}

	return nil, nil
}

// This function cannot return an error, so it stores any error in the
// processor to avoid flooding the logs with the same error.
func (p *sortProcessor) less(a, b *iqr.Record) bool {
	// Compare the values of the first sort element.
	// If they are equal, compare the values of the second sort element, and so on.
	for i, element := range p.options.SortEles {
		valA, err := a.ReadColumn(element.Field)
		if err != nil {
			p.err = fmt.Errorf("cannot read column %v from record A; err=%v", element.Field, err)
			return false
		}
		valB, err := b.ReadColumn(element.Field)
		if err != nil {
			p.err = fmt.Errorf("cannot read column %v from record B; err=%v", element.Field, err)
			return false
		}

		// From https://docs.splunk.com/Documentation/Splunk/9.1.1/SearchReference/Sort#Sort_field_options
		switch p.options.SortEles[i].Op {
		case "", "auto":
			// Try as number first, then as string.
			// TODO: try as IP before generic string?

			valANum, err := valA.GetFloatValue()
			if err == nil {
				valBNum, err := valB.GetFloatValue()
				if err == nil {
					if valANum == valBNum {
						continue
					}

					if element.SortByAsc {
						return valANum < valBNum
					} else {
						return valANum > valBNum
					}
				}
			}

			valAStr, err := valA.GetString()
			if err != nil {
				p.err = fmt.Errorf("cannot convert value of column %v from record A to string; err=%v",
					element.Field, err)
				return false
			}

			valBStr, err := valB.GetString()
			if err != nil {
				p.err = fmt.Errorf("cannot convert value of column %v from record B to string; err=%v",
					element.Field, err)
				return false
			}

			if valAStr == valBStr {
				continue
			}

			if element.SortByAsc {
				return valAStr < valBStr
			} else {
				return valAStr > valBStr
			}
		case "str":
			valAStr, err := valA.GetString()
			if err != nil {
				p.err = fmt.Errorf("cannot convert value of column %v from record A to string; err=%v",
					element.Field, err)
				return false
			}

			valBStr, err := valB.GetString()
			if err != nil {
				p.err = fmt.Errorf("cannot convert value of column %v from record B to string; err=%v",
					element.Field, err)
				return false
			}

			if valAStr == valBStr {
				continue
			}

			if element.SortByAsc {
				return valAStr < valBStr
			} else {
				return valAStr > valBStr
			}
		case "num":
			valANum, err := valA.GetFloatValue()
			if err != nil {
				p.err = fmt.Errorf("cannot convert value of column %v from record A to number; err=%v",
					element.Field, err)
				return false
			}

			valBNum, err := valB.GetFloatValue()
			if err != nil {
				p.err = fmt.Errorf("cannot convert value of column %v from record B to number; err=%v",
					element.Field, err)
				return false
			}

			if valANum == valBNum {
				continue
			}

			if element.SortByAsc {
				return valANum < valBNum
			} else {
				return valANum > valBNum
			}
		case "ip": // TODO
			p.err = fmt.Errorf("IP comparison not implemented")
		default:
			p.err = fmt.Errorf("invalid sort operation: %v", p.options.SortEles[i].Op)
			log.Errorf("sortProcessor.less: invalid operation %v", p.options.SortEles[i].Op)
			return false
		}
	}

	return false
}

func (p *sortProcessor) Rewind() {
	// Nothing to do.
}

func (p *sortProcessor) Cleanup() {
	// Nothing to do.
}

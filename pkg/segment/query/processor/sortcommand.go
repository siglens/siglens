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
	"strconv"

	dtypeutils "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type sortProcessor struct {
	options        *structs.SortExpr
	resultsSoFar   *iqr.IQR
	err            error
	hasFinalResult bool
}

func (p *sortProcessor) Process(inputIQR *iqr.IQR) (*iqr.IQR, error) {
	if inputIQR == nil {
		// There's no more input, so we can send the results.
		p.hasFinalResult = true
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

type dTypeRank uint8

const (
	RANK_NUMERIC dTypeRank = iota + 1
	RANK_STRING
	RANK_OTHER
)

type compare uint8

const (
	EQUAL compare = iota + 1
	LESS
	GREATER
)

func compareFloat(a, b float64) compare {
	if dtypeutils.AlmostEquals(a, b) {
		return EQUAL
	}

	if a < b {
		return LESS
	}

	return GREATER
}

func compareString(a, b string) compare {
	if a == b {
		return EQUAL
	}

	if a < b {
		return LESS
	}

	return GREATER
}

func getRank(CValEnc *segutils.CValueEnclosure, op string) dTypeRank {
	switch CValEnc.Dtype {
	case segutils.SS_DT_BACKFILL, segutils.SS_INVALID:
		return RANK_OTHER
	case segutils.SS_DT_SIGNED_NUM, segutils.SS_DT_UNSIGNED_NUM, segutils.SS_DT_FLOAT:
		switch op {
		case "num", "auto", "":
			return RANK_NUMERIC
		case "str":
			return RANK_STRING
		default:
			return RANK_NUMERIC
		}
	case segutils.SS_DT_STRING:
		switch op {
		case "num", "auto", "":
			strVal := CValEnc.CVal.(string)
			if utils.MightBeFloat(strVal) {
				_, err := strconv.ParseFloat(strVal, 64)
				if err == nil {
					// If floatValue is possible then it is considered as a number
					return RANK_NUMERIC
				}
			}
			return RANK_STRING
		case "str":
			return RANK_STRING
		default:
			return RANK_STRING
		}
	default:
		_, err := CValEnc.GetValueAsString()
		if err == nil {
			return RANK_STRING
		}
		return RANK_OTHER
	}
}

// Returns A comparison B
func compareValues(valueA, valueB *segutils.CValueEnclosure, asc bool, op string) compare {
	var result compare
	rankA := getRank(valueA, op)
	rankB := getRank(valueB, op)

	// OTHER rank records always get sorted to the end
	if rankA == RANK_OTHER && rankB == RANK_OTHER {
		return EQUAL
	}

	if rankA == RANK_OTHER && rankB != RANK_OTHER {
		return GREATER
	}

	if rankA != RANK_OTHER && rankB == RANK_OTHER {
		return LESS
	}

	if rankA < rankB {
		result = compare(LESS)
	} else if rankA > rankB {
		result = compare(GREATER)
	} else {
		switch rankA {
		case RANK_NUMERIC:
			floatValA, isFloat := valueA.GetFloatValueIfPossible()
			if !isFloat {
				return GREATER
			}
			floatValB, isFloat := valueB.GetFloatValueIfPossible()
			if !isFloat {
				return LESS
			}
			result = compareFloat(floatValA, floatValB)
		case RANK_STRING:
			strValA, err := valueA.GetValueAsString()
			if err != nil {
				return GREATER
			}
			strValB, err := valueB.GetValueAsString()
			if err != nil {
				return LESS
			}
			result = compareString(strValA, strValB)
		default:
			result = compare(LESS)
		}
	}

	if !asc {
		if result == LESS {
			result = GREATER
		} else if result == GREATER {
			result = LESS
		}
	}

	return result
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
		case "", "auto", "str", "num":
			// Try as number first, then as string.
			// TODO: try as IP before generic string?
			comparison := compareValues(valA, valB, element.SortByAsc, p.options.SortEles[i].Op)
			if comparison == EQUAL {
				continue
			}

			return comparison == LESS
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

func (p *sortProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	if p.hasFinalResult {
		return p.resultsSoFar, true
	}

	return nil, false
}

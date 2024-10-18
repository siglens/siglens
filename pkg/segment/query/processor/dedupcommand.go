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

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type dedupProcessor struct {
	options         *structs.DedupExpr
	prevCombination []*segutils.CValueEnclosure
}

func (p *dedupProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if len(p.options.DedupSortEles) > 0 {
		return nil, toputils.TeeErrorf("dedup.Process: sorting is not yet implemented")
	}

	if len(p.options.FieldList) == 0 {
		return nil, toputils.TeeErrorf("dedup.Process: no field specified")
	}

	if iqr == nil {
		return nil, io.EOF
	}

	if iqr.NumberOfRecords() == 0 {
		return iqr, nil
	}

	fieldToValues := make(map[string][]segutils.CValueEnclosure)
	for _, field := range p.options.FieldList {
		values, err := iqr.ReadColumn(field)
		if err != nil {
			if p.options.DedupOptions.KeepEmpty {
				return iqr, nil
			} else {
				// Drop all rows.
				return nil, nil
			}
		}

		fieldToValues[field] = values
	}

	rowsToDiscard := make([]int, 0)

	if p.options.DedupOptions.Consecutive {
		numRecords := len(fieldToValues[p.options.FieldList[0]])
		curCombination := make([]*segutils.CValueEnclosure, len(p.options.FieldList))

		startIndex := 0
		if p.prevCombination == nil {
			startIndex = 1
			p.prevCombination = make([]*segutils.CValueEnclosure, len(p.options.FieldList))
			for i, field := range p.options.FieldList {
				p.prevCombination[i] = &fieldToValues[field][0]
			}
		}

	RecordLoop:
		for i := startIndex; i < numRecords; i++ {
			for k, field := range p.options.FieldList {
				curCombination[k] = &fieldToValues[field][i]

				if fieldToValues[field][i].Dtype == segutils.SS_DT_BACKFILL {
					if !p.options.DedupOptions.KeepEmpty {
						rowsToDiscard = append(rowsToDiscard, i)
					}

					copy(p.prevCombination, curCombination)
					continue RecordLoop
				}
			}

			if shouldDiscardConsecutive(p.prevCombination, curCombination) {
				rowsToDiscard = append(rowsToDiscard, i)
			}

			copy(p.prevCombination, curCombination)
		}
	}

	// Discard the records.
	if p.options.DedupOptions.KeepEvents {
		// Clear the fields instead of discarding the rows.
		for _, values := range fieldToValues {
			for _, rowIndex := range rowsToDiscard {
				values[rowIndex].Dtype = segutils.SS_DT_BACKFILL
				values[rowIndex].CVal = nil
			}
		}
	} else {
		err := iqr.DiscardRows(rowsToDiscard)
		if err != nil {
			log.Errorf("dedup.Process: failed to discard rows: %v", err)
			return nil, err
		}
	}

	return iqr, nil
}

func (p *dedupProcessor) Rewind() {
	p.prevCombination = nil
}

func (p *dedupProcessor) Cleanup() {
	p.prevCombination = nil
}

func shouldDiscardConsecutive(prevCombination, curCombination []*segutils.CValueEnclosure) bool {
	if len(prevCombination) == 0 {
		// The curCombination is the first record.
		return false
	}

	if len(prevCombination) != len(curCombination) {
		log.Errorf("shouldDiscardConsecutive: combinations have different lengths (%v and %v)",
			len(prevCombination), len(curCombination))
		return false
	}

	for i := 0; i < len(prevCombination); i++ {
		if *prevCombination[i] != *curCombination[i] {
			return false
		}
	}

	return true
}

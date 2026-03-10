// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package processor

import (
	"io"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type dedupProcessor struct {
	options           *structs.DedupExpr
	combinationHashes map[uint64]int
}

func (p *dedupProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if len(p.options.DedupSortEles) > 0 {
		return nil, utils.TeeErrorf("dedup.Process: sorting is not yet implemented")
	}

	if len(p.options.FieldList) == 0 {
		return nil, utils.TeeErrorf("dedup.Process: no field specified")
	}

	if iqr == nil {
		return nil, io.EOF
	}

	if iqr.NumberOfRecords() == 0 {
		return iqr, nil
	}

	if p.combinationHashes == nil {
		p.combinationHashes = make(map[uint64]int)
	}

	fieldToValues := make(map[string][]sutils.CValueEnclosure)
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

	numRecords := len(fieldToValues[p.options.FieldList[0]])
	rowsToDiscard := make([]int, 0)

RecordLoop:
	for i := 0; i < numRecords; i++ {
		hash := uint64(0)
		for _, field := range p.options.FieldList {
			hash ^= fieldToValues[field][i].Hash()

			if fieldToValues[field][i].Dtype == sutils.SS_DT_BACKFILL ||
				fieldToValues[field][i].Dtype == sutils.SS_INVALID {
				if !p.options.DedupOptions.KeepEmpty {
					rowsToDiscard = append(rowsToDiscard, i)
				}

				continue RecordLoop
			}
		}

		if value, ok := p.combinationHashes[hash]; ok {
			if value >= int(p.options.Limit) {
				rowsToDiscard = append(rowsToDiscard, i)
			}
			p.combinationHashes[hash]++
		} else {
			p.combinationHashes[hash] = 1
		}

		if p.options.DedupOptions.Consecutive {
			// Keep only this hash.
			for other := range p.combinationHashes {
				if other != hash {
					delete(p.combinationHashes, other)
				}
			}
		}
	}

	// Discard the records.
	if p.options.DedupOptions.KeepEvents {
		// Clear the fields instead of discarding the rows.
		for _, values := range fieldToValues {
			for _, rowIndex := range rowsToDiscard {
				values[rowIndex].Dtype = sutils.SS_DT_BACKFILL
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
	p.combinationHashes = nil
}

func (p *dedupProcessor) Cleanup() {
	p.combinationHashes = nil
}

func (p *dedupProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return nil, false
}

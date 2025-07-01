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
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/utils"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	"github.com/valyala/fasthttp"
)

type EventcountProcessor struct {
	options *structs.EventCountExpr
	qid     uint64
	limit   uint64
	eof     bool
	ctx     *fasthttp.RequestCtx // Context for request-specific operations
}

// Change the method signature to match the interface
func (p *EventcountProcessor) Process(inpIqr *iqr.IQR) (*iqr.IQR, error) {
	if inpIqr != nil {
		return nil, fmt.Errorf("eventcountProcessor.Process: IQR is non-nil")
	}

	if p.eof {
		return nil, io.EOF
	}

	if p.options == nil {
		return nil, fmt.Errorf("eventcountProcessor.Process: EventCount options is nil")
	}

	newIQR := iqr.NewIQR(p.qid)

	var expandedIndices []string
	if p.options.ListVix {
		allTables, err := vtable.GetVirtualTableNames(0)
		if err != nil {
			return nil, fmt.Errorf("eventcountProcessor.Process: Error listing virtual tables: %v", err)
		}
		for tableName := range allTables {
			expandedIndices = append(expandedIndices, tableName)
		}
	} else if len(p.options.Indices) > 0 {
		indexSet := make(map[string]bool)
		for _, indexPattern := range p.options.Indices {
			// Use the struct field p.ctx
			expanded := vtable.ExpandAndReturnIndexNames(indexPattern, int64(0), false, p.ctx)
			for _, idx := range expanded {
				indexSet[idx] = true
			}
		}
		for idx := range indexSet {
			expandedIndices = append(expandedIndices, idx)
		}
	}

	// Prepare result columns
	indexColumn := make([]sutils.CValueEnclosure, 0, len(expandedIndices))
	countColumn := make([]sutils.CValueEnclosure, 0, len(expandedIndices))
	sizeColumn := make([]sutils.CValueEnclosure, 0, len(expandedIndices))

	// Get global segment metadata and vtable counts once
	allSegMetas := writer.ReadGlobalSegmetas()
	allCounts := writer.GetVTableCountsForAll(0, allSegMetas)
	orgID := utils.NewOptionWithValue(int64(0))

	var totalCount uint64
	var totalSize uint64

	// Get counts for each index
	for _, index := range expandedIndices {
		var count uint64
		var size uint64

		// Get rotated counts from existing segments
		if vCounts, ok := allCounts[index]; ok {
			count += vCounts.RecordCount
		}

		// Add unrotated counts
		unrotatedByteCount, unrotatedRecCount, _, err := writer.GetUnrotatedVTableCounts(index, orgID)
		if err == nil {
			count += uint64(unrotatedRecCount)
			size += unrotatedByteCount
		}

		// Get size stats
		stats, err2 := writer.GetIndexSizeStats(index, orgID)
		if err2 == nil && stats != nil {
			// Calculate size as the sum of CMI and CSG sizes plus unrotated bytes
			size += stats.TotalCmiSize + stats.TotalCsgSize
		}

		// Add to result columns
		indexColumn = append(indexColumn, sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_STRING,
			CVal:  index,
		})

		countColumn = append(countColumn, sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_UNSIGNED_NUM,
			CVal:  count,
		})

		if p.options.ReportSize {
			sizeColumn = append(sizeColumn, sutils.CValueEnclosure{
				Dtype: sutils.SS_DT_UNSIGNED_NUM,
				CVal:  size,
			})
		}

		totalCount += count
		totalSize += size
	}

	// Create the result map
	results := map[string][]sutils.CValueEnclosure{
		"index": indexColumn,
		"count": countColumn,
	}

	if p.options.ReportSize {
		results["size"] = sizeColumn
	}

	// If summarize is true, add a summary row with totals
	if p.options.Summarize && len(countColumn) > 0 {
		results["index"] = append(results["index"], sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_STRING,
			CVal:  "TOTAL",
		})

		results["count"] = append(results["count"], sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_UNSIGNED_NUM,
			CVal:  totalCount,
		})

		if p.options.ReportSize {
			results["size"] = append(results["size"], sutils.CValueEnclosure{
				Dtype: sutils.SS_DT_UNSIGNED_NUM,
				CVal:  totalSize,
			})
		}
	}

	// Append the results to the IQR
	err := newIQR.AppendKnownValues(results)
	if err != nil {
		return nil, fmt.Errorf("eventcountProcessor.Process: Error appending known values: %v", err)
	}

	// Mark as done since this is a one-time operation
	p.eof = true

	return newIQR, nil
}

func (p *EventcountProcessor) Rewind() {
	p.eof = false
}

func (p *EventcountProcessor) Cleanup() {
	// Nothing to cleanup
}

func (p *EventcountProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return nil, false
}

func (p *EventcountProcessor) IsEOF() bool {
	return p.eof
}

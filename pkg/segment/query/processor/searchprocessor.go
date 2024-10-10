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
	"sort"

	"github.com/siglens/siglens/pkg/segment/query"
	_ "github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type block struct {
	structs.BlockSummary
	structs.BlockMetadataHolder
}

type searchProcessor struct {
	queryInfo *query.QueryInformation
	sortMode  sortMode
}

func (sp *searchProcessor) Rewind() {
	panic("not implemented")
}

func (sp *searchProcessor) Fetch() (*iqr.IQR, error) {
	switch sp.queryInfo.GetQueryType() {
	case structs.SegmentStatsCmd:
		panic("not implemented") // TODO
	case structs.GroupByCmd:
		panic("not implemented") // TODO
	case structs.RRCCmd:
		return sp.fetchRRCs()
	default:
		return nil, toputils.TeeErrorf("searchProcessor.Fetch: invalid query type: %v",
			sp.queryInfo.GetQueryType())
	}
}

func (sp *searchProcessor) fetchRRCs() (*iqr.IQR, error) {
	blocks, err := sp.getBlocks()
	if err != nil {
		log.Errorf("searchProcessor.fetchRRCs: failed to get blocks: %v", err)
		return nil, err
	}

	sp.sort(blocks)
	endTime := sp.getNextEndTime()
	nextBlocks := sp.getBlocksForTimeRange(endTime)
	for _, block := range nextBlocks {
		rrcs, err := sp.readRRCs(block)
		if err != nil {
			log.Errorf("searchProcessor.fetchRRCs: failed to read RRCs: %v", err)
			return nil, err
		}

		sp.mergeRRCs(rrcs)
		sp.removeBlocks(blocks, nextBlocks)
	}

	validRRCs := sp.getValidRRCs()
	iqr := iqr.NewIQR(sp.queryInfo.GetQid())

	// Maybe convert small RRCs to normal RRCs first?
	err = iqr.AppendRRCs(validRRCs, nil) // TODO: figure out how to merge.

	return iqr, nil
}

func (sp *searchProcessor) getBlocks() ([]*block, error) {
	panic("not implemented")
}

type sortMode int

const (
	invalidSortMode sortMode = iota
	recentFirst
	recentLast
	anyOrder
)

func (sp *searchProcessor) sort(blocks []*block) {
	switch sp.sortMode {
	case anyOrder:
		return
	case recentFirst:
		sort.Slice(blocks, func(i, j int) bool {
			return blocks[i].HighTs > blocks[j].HighTs
		})
	case recentLast:
		sort.Slice(blocks, func(i, j int) bool {
			return blocks[i].HighTs < blocks[j].HighTs
		})
	default:
		log.Errorf("searchProcessor.sort: invalid sort mode: %v", sp.sortMode)
	}
}

func (sp *searchProcessor) getNextEndTime() uint64 {
	panic("not implemented")
}

func (sp *searchProcessor) getBlocksForTimeRange(endTime uint64) []*block {
	panic("not implemented")
}

func (sp *searchProcessor) readRRCs(block *block) ([]*segutils.RecordResultContainer, error) {
	panic("not implemented")
}

func (sp *searchProcessor) mergeRRCs(rrcs []*segutils.RecordResultContainer) {
	panic("not implemented")
}

func (sp *searchProcessor) removeBlocks(blocks []*block, nextBlocks []*block) {
	panic("not implemented")
}

func (sp *searchProcessor) getValidRRCs() []*segutils.RecordResultContainer {
	panic("not implemented")
}

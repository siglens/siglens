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
	"math"
	"sort"
	"time"

	"github.com/siglens/siglens/pkg/segment/query"
	_ "github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/results/segresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type block struct {
	*structs.BlockSummary
	*structs.BlockMetadataHolder
	parentSSR *structs.SegmentSearchRequest
	filename  string
}

type searcher struct {
	queryInfo    *query.QueryInformation
	querySummary *summary.QuerySummary
	sortMode     sortMode

	gotBlocks             bool
	remainingBlocksSorted []*block // Sorted by time as specified by sortMode.
}

func (s *searcher) Rewind() {
	panic("not implemented")
}

func (s *searcher) Fetch() (*iqr.IQR, error) {
	if !s.gotBlocks {
		blocks, err := s.getBlocks()
		if err != nil {
			log.Errorf("searchProcessor.Fetch: failed to get blocks: %v", err)
			return nil, err
		}

		sortBlocks(blocks, s.sortMode)
		s.remainingBlocksSorted = blocks
		s.gotBlocks = true
	}

	switch s.queryInfo.GetQueryType() {
	case structs.SegmentStatsCmd:
		panic("not implemented") // TODO
	case structs.GroupByCmd:
		panic("not implemented") // TODO
	case structs.RRCCmd:
		return s.fetchRRCs()
	default:
		return nil, toputils.TeeErrorf("searchProcessor.Fetch: invalid query type: %v",
			s.queryInfo.GetQueryType())
	}
}

func (s *searcher) fetchRRCs() (*iqr.IQR, error) {
	endTime := getNextEndTime(s.remainingBlocksSorted, s.sortMode)
	nextBlocks, err := getBlocksForTimeRange(s.remainingBlocksSorted, s.sortMode, endTime)
	if err != nil {
		log.Errorf("searchProcessor.fetchRRCs: failed to get blocks for time range: %v", err)
		return nil, err
	}

	for _, nextBlock := range nextBlocks {
		rrcs, err := s.readRRCs([]*block{nextBlock})
		if err != nil {
			log.Errorf("searchProcessor.fetchRRCs: failed to read RRCs: %v", err)
			return nil, err
		}

		s.mergeRRCs(rrcs)
		s.removeBlocks(s.remainingBlocksSorted, nextBlocks)
	}

	validRRCs := s.getValidRRCs()
	iqr := iqr.NewIQR(s.queryInfo.GetQid())

	// Maybe convert small RRCs to normal RRCs first?
	err = iqr.AppendRRCs(validRRCs, nil) // TODO: figure out how to merge.

	return iqr, nil
}

func (s *searcher) getBlocks() ([]*block, error) {
	startTime := time.Now() // TODO: maybe this should be a field on searcher.
	qsrs, err := query.GetSortedQSRs(s.queryInfo, startTime, s.querySummary)
	if err != nil {
		log.Errorf("searchProcessor.getBlocks: failed to get sorted QSRs: %v", err)
		return nil, err
	}

	allBlocks := make([]*block, 0)
	for _, qsr := range qsrs {
		fileToSSR, err := query.GetSSRsFromQSR(qsr, s.querySummary)
		if err != nil {
			log.Errorf("searchProcessor.getBlocks: failed to get SSRs from QSR %+v; err=%v", qsr, err)
			return nil, err
		}

		for file, ssr := range fileToSSR {
			blocks := makeBlocksFromSSR(file, ssr)
			allBlocks = append(allBlocks, blocks...)
		}
	}

	return allBlocks, nil
}

func makeBlocksFromSSR(filename string, ssr *structs.SegmentSearchRequest) []*block {
	blocks := make([]*block, 0, len(ssr.AllBlocksToSearch))

	for blockNum, blockMeta := range ssr.AllBlocksToSearch {
		blocks = append(blocks, &block{
			BlockSummary:        ssr.SearchMetadata.BlockSummaries[blockNum],
			BlockMetadataHolder: blockMeta,
			parentSSR:           ssr,
			filename:            filename,
		})
	}

	return blocks
}

type sortMode int

const (
	invalidSortMode sortMode = iota
	recentFirst
	recentLast
	anyOrder
)

func sortBlocks(blocks []*block, mode sortMode) {
	switch mode {
	case anyOrder:
		return
	case recentFirst:
		sort.Slice(blocks, func(i, j int) bool {
			return blocks[i].HighTs > blocks[j].HighTs
		})
	case recentLast:
		sort.Slice(blocks, func(i, j int) bool {
			return blocks[i].LowTs < blocks[j].LowTs
		})
	default:
		log.Errorf("searchProcessor.sort: invalid sort mode: %v", mode)
	}
}

func getNextEndTime(sortedBlocks []*block, mode sortMode) uint64 {
	if len(sortedBlocks) == 0 {
		return 0
	}

	// TODO: we may want to optimize this; e.g., minimize the number of blocks
	// that will be in this time range, or try to get a certain number of
	// blocks.
	switch mode {
	case recentFirst:
		return sortedBlocks[0].LowTs
	case recentLast:
		return sortedBlocks[0].HighTs
	default:
		log.Errorf("searchProcessor.getNextEndTime: invalid sort mode: %v", mode)
		return 0
	}
}

func getBlocksForTimeRange(blocks []*block, mode sortMode, endTime uint64) ([]*block, error) {
	if len(blocks) == 0 {
		return nil, nil
	}

	selectedBlocks := make([]*block, 0)

	switch mode {
	case recentFirst:
		for _, block := range blocks {
			if block.HighTs >= endTime {
				selectedBlocks = append(selectedBlocks, block)
			}
		}
	case recentLast:
		for _, block := range blocks {
			if block.LowTs <= endTime {
				selectedBlocks = append(selectedBlocks, block)
			}
		}
	case anyOrder:
		selectedBlocks = blocks[0:]
	default:
		return nil, toputils.TeeErrorf("getBlocksForTimeRange: invalid sort mode: %v", mode)
	}

	return selectedBlocks, nil
}

func (s *searcher) readRRCs(blocks []*block) ([]*segutils.RecordResultContainer, error) {
	allSegRequests, err := getSSRs(blocks)
	if err != nil {
		log.Errorf("searchProcessor.readRRCs: failed to get SSRs: %v", err)
		return nil, err
	}

	parallelismPerFile := s.queryInfo.GetParallelismPerFile()
	searchNode := s.queryInfo.GetSearchNode()
	timeRange := s.queryInfo.GetQueryRange()
	sizeLimit := uint64(math.MaxUint64)
	aggs := s.queryInfo.GetAggregators()
	qid := s.queryInfo.GetQid()
	qs := s.querySummary

	queryType := s.queryInfo.GetQueryType()
	searchResults, err := segresults.InitSearchResults(sizeLimit, aggs, queryType, qid)
	if err != nil {
		log.Errorf("searchProcessor.readRRCs: failed to initialize search results: %v", err)
		return nil, err
	}

	err = query.ApplyFilterOperatorInternal(searchResults, allSegRequests,
		parallelismPerFile, searchNode, timeRange, sizeLimit, aggs, qid, qs)
	if err != nil {
		log.Errorf("searchProcessor.readRRCs: failed to apply filter operator: %v", err)
		return nil, err
	}

	return searchResults.GetResults(), nil
}

// All of the blocks should be for the same SSR.
func getSSRs(blocks []*block) (map[string]*structs.SegmentSearchRequest, error) {
	if len(blocks) == 0 {
		return nil, nil
	}

	// Each block should have come from the same SSR.
	firstSSR := blocks[0].parentSSR
	for _, block := range blocks {
		if block.parentSSR != firstSSR {
			return nil, toputils.TeeErrorf("searchProcessor.getSSRs: blocks are from different SSRs")
		}
	}

	fileToSSR := make(map[string]*structs.SegmentSearchRequest)
	for _, block := range blocks {
		ssr, ok := fileToSSR[block.filename]
		if !ok {
			ssrCopy := *firstSSR
			ssrCopy.AllBlocksToSearch = make(map[uint16]*structs.BlockMetadataHolder)
			fileToSSR[block.filename] = &ssrCopy
			ssr = &ssrCopy
		}

		ssr.AllBlocksToSearch[block.BlkNum] = block.BlockMetadataHolder
	}

	return fileToSSR, nil
}

func (s *searcher) mergeRRCs(rrcs []*segutils.RecordResultContainer) {
	panic("not implemented")
}

func (s *searcher) removeBlocks(blocks []*block, nextBlocks []*block) {
	panic("not implemented")
}

func (s *searcher) getValidRRCs() []*segutils.RecordResultContainer {
	panic("not implemented")
}

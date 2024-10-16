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
	parentSSR   *structs.SegmentSearchRequest
	segkeyFname string
}

type searcher struct {
	qid          uint64
	queryInfo    *query.QueryInformation
	querySummary *summary.QuerySummary
	sortMode     sortMode
	startTime    time.Time

	gotBlocks             bool
	remainingBlocksSorted []*block // Sorted by time as specified by sortMode.

	unsentRRCs  []*segutils.RecordResultContainer
	segEncToKey *toputils.TwoWayMap[uint16, string]
}

func NewSearcher(queryInfo *query.QueryInformation, querySummary *summary.QuerySummary,
	sortMode sortMode, startTime time.Time) (*searcher, error) {

	if queryInfo == nil {
		return nil, toputils.TeeErrorf("searchProcessor.NewSearcher: queryInfo is nil")
	}
	if querySummary == nil {
		return nil, toputils.TeeErrorf("searchProcessor.NewSearcher: querySummary is nil")
	}

	qid := queryInfo.GetQid()

	return &searcher{
		qid:                   qid,
		queryInfo:             queryInfo,
		querySummary:          querySummary,
		sortMode:              sortMode,
		startTime:             startTime,
		remainingBlocksSorted: make([]*block, 0),
		unsentRRCs:            make([]*segutils.RecordResultContainer, 0),
		segEncToKey:           toputils.NewTwoWayMap[uint16, string](),
	}, nil
}

func (s *searcher) Rewind() {
	s.gotBlocks = false
	s.remainingBlocksSorted = make([]*block, 0)
	s.unsentRRCs = make([]*segutils.RecordResultContainer, 0)
	s.segEncToKey = toputils.NewTwoWayMap[uint16, string]()
}

func (s *searcher) Fetch() (*iqr.IQR, error) {
	if !s.gotBlocks {
		blocks, err := s.getBlocks()
		if err != nil {
			log.Errorf("qid=%v, searchProcessor.Fetch: failed to get blocks: %v", s.qid, err)
			return nil, err
		}

		err = sortBlocks(blocks, s.sortMode)
		if err != nil {
			log.Errorf("qid=%v, searchProcessor.Fetch: failed to sort blocks: %v", s.qid, err)
			return nil, err
		}

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
		return nil, toputils.TeeErrorf("qid=%v, searchProcessor.Fetch: invalid query type: %v",
			s.qid, s.queryInfo.GetQueryType())
	}
}

func (s *searcher) fetchRRCs() (*iqr.IQR, error) {
	if s.gotBlocks && len(s.remainingBlocksSorted) == 0 && len(s.unsentRRCs) == 0 {
		return nil, io.EOF
	}

	endTime, err := getNextEndTime(s.remainingBlocksSorted, s.sortMode)
	if err != nil {
		log.Errorf("qid=%v, searchProcessor.fetchRRCs: failed to get next end time: %v", s.qid, err)
		return nil, err
	}

	nextBlocks, err := getBlocksForTimeRange(s.remainingBlocksSorted, s.sortMode, endTime)
	if err != nil {
		log.Errorf("qid=%v, searchProcessor.fetchRRCs: failed to get blocks for time range: %v", s.qid, err)
		return nil, err
	}

	// Remove the blocks we're going to process. Since the blocks are sorted,
	// we always take blocks from the front of the list.
	s.remainingBlocksSorted = s.remainingBlocksSorted[len(nextBlocks):]

	allRRCsSlices := make([][]*segutils.RecordResultContainer, len(nextBlocks)+1)
	for i, nextBlock := range nextBlocks {
		segkey := nextBlock.parentSSR.SegmentKey
		rrcs, segEncToKey, err := s.readSortedRRCs([]*block{nextBlock}, segkey)
		if err != nil {
			log.Errorf("qid=%v, searchProcessor.fetchRRCs: failed to read RRCs: %v", s.qid, err)
			return nil, err
		}

		if s.segEncToKey.Conflicts(segEncToKey) {
			return nil, toputils.TeeErrorf("qid=%v, searchProcessor.fetchRRCs: conflicting segEncToKey (%v and %v)",
				s.qid, s.segEncToKey, segEncToKey)
		}

		// There's no conflicts, so we can safely merge the two maps.
		for k, v := range segEncToKey {
			s.segEncToKey.Set(k, v)
		}

		allRRCsSlices[i] = rrcs
	}

	// Merge all these RRCs with any leftover RRCs from previous fetches.
	allRRCsSlices[len(nextBlocks)] = s.unsentRRCs
	sortingFunc, err := getSortingFunc(s.sortMode)
	if err != nil {
		log.Errorf("qid=%v, searchProcessor.fetchRRCs: failed to get sorting function: %v", s.qid, err)
		return nil, err
	}

	s.unsentRRCs = toputils.MergeSortedSlices(sortingFunc, allRRCsSlices...)

	validRRCs, err := getValidRRCs(s.unsentRRCs, endTime, s.sortMode)
	if err != nil {
		log.Errorf("qid=%v, searchProcessor.fetchRRCs: failed to get valid RRCs: %v", s.qid, err)
		return nil, err
	}

	// TODO: maybe look into optimizations for unsentRRCs so we can discard
	// the memory at the beginning (which will never be used again).
	s.unsentRRCs = s.unsentRRCs[len(validRRCs):]

	iqr := iqr.NewIQR(s.queryInfo.GetQid())
	err = iqr.AppendRRCs(validRRCs, s.segEncToKey.GetNormalMap())
	if err != nil {
		log.Errorf("qid=%v, searchProcessor.fetchRRCs: failed to append RRCs: %v", s.qid, err)
		return nil, err
	}

	return iqr, nil
}

func getSortingFunc(sortMode sortMode) (func(a, b *segutils.RecordResultContainer) bool, error) {
	switch sortMode {
	case recentFirst:
		return func(a, b *segutils.RecordResultContainer) bool {
			return a.TimeStamp > b.TimeStamp
		}, nil
	case recentLast:
		return func(a, b *segutils.RecordResultContainer) bool {
			return a.TimeStamp < b.TimeStamp
		}, nil
	case anyOrder:
		return func(a, b *segutils.RecordResultContainer) bool {
			return true
		}, nil
	default:
		return nil, toputils.TeeErrorf("getSortingFunc: invalid sort mode: %v", sortMode)
	}
}

func (s *searcher) getBlocks() ([]*block, error) {
	qsrs, err := query.GetSortedQSRs(s.queryInfo, s.startTime, s.querySummary)
	if err != nil {
		log.Errorf("qid=%v, searchProcessor.getBlocks: failed to get sorted QSRs: %v", s.qid, err)
		return nil, err
	}

	allBlocks := make([]*block, 0)
	for _, qsr := range qsrs {
		fileToSSR, err := query.GetSSRsFromQSR(qsr, s.querySummary)
		if err != nil {
			log.Errorf("qid=%v, searchProcessor.getBlocks: failed to get SSRs from QSR %+v; err=%v", s.qid, qsr, err)
			return nil, err
		}

		for file, ssr := range fileToSSR {
			blocks := makeBlocksFromSSR(file, ssr)
			allBlocks = append(allBlocks, blocks...)
		}
	}

	return allBlocks, nil
}

func makeBlocksFromSSR(segkeyFname string, ssr *structs.SegmentSearchRequest) []*block {
	blocks := make([]*block, 0, len(ssr.AllBlocksToSearch))

	for blockNum, blockMeta := range ssr.AllBlocksToSearch {
		blocks = append(blocks, &block{
			BlockSummary:        ssr.SearchMetadata.BlockSummaries[blockNum],
			BlockMetadataHolder: blockMeta,
			parentSSR:           ssr,
			segkeyFname:         segkeyFname,
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

func sortBlocks(blocks []*block, mode sortMode) error {
	switch mode {
	case anyOrder:
		return nil
	case recentFirst:
		sort.Slice(blocks, func(i, j int) bool {
			return blocks[i].HighTs > blocks[j].HighTs
		})
	case recentLast:
		sort.Slice(blocks, func(i, j int) bool {
			return blocks[i].LowTs < blocks[j].LowTs
		})
	default:
		return toputils.TeeErrorf("sortBlocks: invalid sort mode: %v", mode)
	}

	return nil
}

func getNextEndTime(sortedBlocks []*block, mode sortMode) (uint64, error) {
	if len(sortedBlocks) == 0 {
		return 0, nil
	}

	// TODO: we may want to optimize this; e.g., minimize the number of blocks
	// that will be in this time range, or try to get a certain number of
	// blocks.
	switch mode {
	case recentFirst:
		return sortedBlocks[0].LowTs, nil
	case recentLast:
		return sortedBlocks[0].HighTs, nil
	default:
		return 0, toputils.TeeErrorf("getNextEndTime: invalid sort mode: %v", mode)
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

func (s *searcher) readSortedRRCs(blocks []*block, segkey string) ([]*segutils.RecordResultContainer, map[uint16]string, error) {
	allSegRequests, err := getSSRs(blocks)
	if err != nil {
		log.Errorf("qid=%v, searchProcessor.readSortedRRCs: failed to get SSRs: %v", s.qid, err)
		return nil, nil, err
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
		log.Errorf("qid=%v, searchProcessor.readSortedRRCs: failed to initialize search results: %v", s.qid, err)
		return nil, nil, err
	}

	encoding, ok := s.segEncToKey.GetReverse(segkey)
	if !ok {
		encoding = uint16(s.segEncToKey.Len())
		s.segEncToKey.Set(encoding, segkey)
	}
	searchResults.NextSegKeyEnc = encoding

	err = query.ApplyFilterOperatorInternal(searchResults, allSegRequests,
		parallelismPerFile, searchNode, timeRange, sizeLimit, aggs, qid, qs)
	if err != nil {
		log.Errorf("qid=%v, searchProcessor.readSortedRRCs: failed to apply filter operator: %v", s.qid, err)
		return nil, nil, err
	}

	// TODO: verify the results or sorted, or sort them here.
	return searchResults.GetResults(), searchResults.SegEncToKey, nil
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
			return nil, toputils.TeeErrorf("getSSRs: blocks are from different SSRs")
		}
	}

	fileToSSR := make(map[string]*structs.SegmentSearchRequest)
	for _, block := range blocks {
		ssr, ok := fileToSSR[block.segkeyFname]
		if !ok {
			ssrCopy := *firstSSR
			ssrCopy.AllBlocksToSearch = make(map[uint16]*structs.BlockMetadataHolder)
			fileToSSR[block.segkeyFname] = &ssrCopy
			ssr = &ssrCopy
		}

		ssr.AllBlocksToSearch[block.BlkNum] = block.BlockMetadataHolder
	}

	return fileToSSR, nil
}

func getValidRRCs(sortedRRCs []*segutils.RecordResultContainer, lastTimestamp uint64,
	mode sortMode) ([]*segutils.RecordResultContainer, error) {

	switch mode {
	case recentFirst:
		i := sort.Search(len(sortedRRCs), func(k int) bool {
			return sortedRRCs[k].TimeStamp < lastTimestamp
		})
		return sortedRRCs[:i], nil
	case recentLast:
		i := sort.Search(len(sortedRRCs), func(k int) bool {
			return sortedRRCs[k].TimeStamp > lastTimestamp
		})
		return sortedRRCs[:i], nil
	case anyOrder:
		return sortedRRCs, nil
	default:
		return nil, toputils.TeeErrorf("getValidRRCs: invalid sort mode: %v", mode)
	}
}

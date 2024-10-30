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
	"runtime"
	"sort"
	"time"

	"github.com/siglens/siglens/pkg/segment/metadata"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	"github.com/siglens/siglens/pkg/segment/query"
	_ "github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/query/pqs"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/results/segresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type block struct {
	*structs.BlockSummary
	*structs.BlockMetadataHolder
	parentQSR *query.QuerySegmentRequest

	// For PQS blocks, these must be set.
	parentPQMR toputils.Option[*pqmr.SegmentPQMRResults]

	// For raw search blocks, these must be set.
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
	qsrs                  []*query.QuerySegmentRequest

	unsentRRCs  []*segutils.RecordResultContainer
	segEncToKey *toputils.TwoWayMap[uint16, string]
}

func NewSearcher(queryInfo *query.QueryInformation, querySummary *summary.QuerySummary,
	sortMode sortMode, startTime time.Time) (*searcher, error) {

	if queryInfo == nil {
		return nil, toputils.TeeErrorf("searcher.NewSearcher: queryInfo is nil")
	}
	if querySummary == nil {
		return nil, toputils.TeeErrorf("searcher.NewSearcher: querySummary is nil")
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

func getNumRecords(blocks []*block) uint64 {
	var totalRecords uint64
	for _, block := range blocks {
		totalRecords += uint64(block.RecCount)
	}
	return totalRecords
}

func (s *searcher) Fetch() (*iqr.IQR, error) {
	switch s.queryInfo.GetQueryType() {
	case structs.SegmentStatsCmd, structs.GroupByCmd:
		return s.fetchStatsResults()
	case structs.RRCCmd:
		if !s.gotBlocks {
			blocks, err := s.getBlocks()
			if err != nil {
				log.Errorf("qid=%v, searcher.Fetch: failed to get blocks: %v", s.qid, err)
				return nil, err
			}

			err = sortBlocks(blocks, s.sortMode)
			if err != nil {
				log.Errorf("qid=%v, searcher.Fetch: failed to sort blocks: %v", s.qid, err)
				return nil, err
			}

			s.remainingBlocksSorted = blocks
			s.gotBlocks = true
			query.InitProgressForRRCCmd(uint64(len(blocks)), s.qid)
		}

		return s.fetchRRCs()
	default:
		return nil, toputils.TeeErrorf("qid=%v, searcher.Fetch: invalid query type: %v",
			s.qid, s.queryInfo.GetQueryType())
	}
}

func (s *searcher) fetchRRCs() (*iqr.IQR, error) {
	if s.gotBlocks && len(s.remainingBlocksSorted) == 0 && len(s.unsentRRCs) == 0 {
		err := query.SetRawSearchFinished(s.qid)
		if err != nil {
			log.Errorf("qid=%v, searcher.fetchRRCs: failed to set raw search finished: %v", s.qid, err)
			return nil, err
		}

		return nil, io.EOF
	}

	desiredMaxBlocks := runtime.GOMAXPROCS(0) // TODO: tune this
	nextBlocks, endTime, err := getNextBlocks(s.remainingBlocksSorted, desiredMaxBlocks, s.sortMode)
	if err != nil {
		log.Errorf("qid=%v, searcher.fetchRRCs: failed to get next end time: %v", s.qid, err)
		return nil, err
	}

	// Remove the blocks we're going to process. Since the blocks are sorted,
	// we always take blocks from the front of the list.
	s.remainingBlocksSorted = s.remainingBlocksSorted[len(nextBlocks):]

	allRRCsSlices := make([][]*segutils.RecordResultContainer, 0, len(nextBlocks)+1)

	// Prepare to call BatchProcess.
	getBatchKey := func(block *block) string {
		return block.parentQSR.GetSegKey()
	}
	batchKeyLess := toputils.NewUnsetOption[func(string, string) bool]()
	// The return value is not needed, so use struct{} as a placeholder.
	batchOperation := func(blocks []*block) ([]*struct{}, error) {
		if len(blocks) == 0 {
			// This should never happen.
			return nil, nil
		}

		segkey := blocks[0].parentQSR.GetSegKey()
		rrcs, segEncToKey, err := s.readSortedRRCs(blocks, segkey)
		if err != nil {
			log.Errorf("qid=%v, searcher.fetchRRCs: failed to read RRCs: %v", s.qid, err)
			return nil, err
		}

		if s.segEncToKey.Conflicts(segEncToKey) {
			return nil, toputils.TeeErrorf("qid=%v, searcher.fetchRRCs: conflicting segEncToKey (%v and %v)",
				s.qid, s.segEncToKey, segEncToKey)
		}

		// There's no conflicts, so we can safely merge the two maps.
		for k, v := range segEncToKey {
			s.segEncToKey.Set(k, v)
		}

		allRRCsSlices = append(allRRCsSlices, rrcs)
		return nil, nil
	}

	_, err = toputils.BatchProcess(nextBlocks, getBatchKey, batchKeyLess, batchOperation)
	if err != nil {
		log.Errorf("qid=%v, searcher.fetchRRCs: failed to batch process blocks: %v", s.qid, err)
		return nil, err
	}

	// Merge all these RRCs with any leftover RRCs from previous fetches.
	allRRCsSlices = append(allRRCsSlices, s.unsentRRCs)
	sortingFunc, err := getSortingFunc(s.sortMode)
	if err != nil {
		log.Errorf("qid=%v, searcher.fetchRRCs: failed to get sorting function: %v", s.qid, err)
		return nil, err
	}

	s.unsentRRCs = toputils.MergeSortedSlices(sortingFunc, allRRCsSlices...)

	validRRCs, err := getValidRRCs(s.unsentRRCs, endTime, s.sortMode)
	if err != nil {
		log.Errorf("qid=%v, searcher.fetchRRCs: failed to get valid RRCs: %v", s.qid, err)
		return nil, err
	}

	// TODO: maybe look into optimizations for unsentRRCs so we can discard
	// the memory at the beginning (which will never be used again).
	s.unsentRRCs = s.unsentRRCs[len(validRRCs):]

	iqr := iqr.NewIQR(s.queryInfo.GetQid())
	err = iqr.AppendRRCs(validRRCs, s.segEncToKey.GetMapForReading())
	if err != nil {
		log.Errorf("qid=%v, searcher.fetchRRCs: failed to append RRCs: %v", s.qid, err)
		return nil, err
	}

	err = query.IncProgressForRRCCmd(getNumRecords(nextBlocks), uint64(len(nextBlocks)), s.qid)
	if err != nil {
		return nil, toputils.TeeErrorf("qid=%v, searcher.fetchRRCs: failed to increment progress: %v", s.qid, err)
	}

	return iqr, nil
}

func (s *searcher) fetchStatsResults() (*iqr.IQR, error) {
	sizeLimit := uint64(0)
	aggs := s.queryInfo.GetAggregators()
	qid := s.queryInfo.GetQid()
	qType := s.queryInfo.GetQueryType()
	orgId := s.queryInfo.GetOrgId()

	queryType := s.queryInfo.GetQueryType()
	searchResults, err := segresults.InitSearchResults(sizeLimit, aggs, queryType, qid)
	if err != nil {
		log.Errorf("qid=%v, searcher.fetchGroupByResults: failed to initialize search results: %v", s.qid, err)
		return nil, err
	}

	var nodeResult *structs.NodeResult

	if qType == structs.SegmentStatsCmd {
		nodeResult = query.GetNodeResultsForSegmentStatsCmd(s.queryInfo, s.startTime, searchResults, nil, s.querySummary, orgId)
	} else if qType == structs.GroupByCmd {
		nodeResult, err = s.fetchGroupByResults(searchResults, aggs)
		if err != nil {
			return nil, toputils.TeeErrorf("qid=%v, searcher.fetchStatsResults: failed to get group by results: %v", s.qid, err)
		}
	} else {
		return nil, toputils.TeeErrorf("qid=%v, searcher.fetchStatsResults: invalid query type: %v", qid, qType)
	}

	// post getting of stats results
	iqr := iqr.NewIQR(s.queryInfo.GetQid())

	err = iqr.CreateStatsResults(nodeResult.MeasureResults, nodeResult.MeasureFunctions, nodeResult.GroupByCols, nodeResult.BucketCount)
	if err != nil {
		return nil, toputils.TeeErrorf("qid=%v, searcher.fetchStatsResults: failed to create stats results: %v", qid, err)
	}

	s.qsrs = s.qsrs[0:] // Clear the QSRs so we don't process them again.

	return iqr, io.EOF
}

func (s *searcher) fetchGroupByResults(searchResults *segresults.SearchResults, aggs *structs.QueryAggregators) (*structs.NodeResult, error) {
	if s.qsrs == nil {
		err := s.initializeQSRs()
		if err != nil {
			return nil, toputils.TeeErrorf("qid=%v, searcher.fetchSegmentStatsResults: failed to get and set QSRs: %v", s.qid, err)
		}
	}

	if len(s.qsrs) == 0 {
		return nil, io.EOF
	}

	bucketLimit := int(utils.QUERY_MAX_BUCKETS)
	if aggs.BucketLimit != 0 && aggs.BucketLimit < bucketLimit {
		bucketLimit = aggs.BucketLimit
	}
	aggs.BucketLimit = bucketLimit

	nodeResult := query.GetNodeResultsFromQSRS(s.qsrs, s.queryInfo, s.startTime, searchResults, s.querySummary)

	bucketHolderArr, measureFuncs, aggGroupByCols, _, bucketCount := searchResults.GetGroupyByBuckets(int(utils.QUERY_MAX_BUCKETS))
	nodeResult.MeasureResults = bucketHolderArr
	nodeResult.MeasureFunctions = measureFuncs
	nodeResult.GroupByCols = aggGroupByCols
	nodeResult.BucketCount = bucketCount

	return nodeResult, nil
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

func (s *searcher) initializeQSRs() error {
	qsrs, err := query.GetSortedQSRs(s.queryInfo, s.startTime, s.querySummary)
	if err != nil {
		log.Errorf("qid=%v, searcher.initializeQSRs: failed to get sorted QSRs: %v", s.qid, err)
		return err
	}

	s.qsrs = qsrs
	return nil
}

func (s *searcher) getBlocks() ([]*block, error) {
	qsrs, err := query.GetSortedQSRs(s.queryInfo, s.startTime, s.querySummary)
	if err != nil {
		log.Errorf("qid=%v, searcher.getBlocks: failed to get sorted QSRs: %v", s.qid, err)
		return nil, err
	}

	pqmrs := make([]toputils.Option[*pqmr.SegmentPQMRResults], len(qsrs))

	for i, qsr := range qsrs {
		// The query may require filtering out records after search, so we
		// shouldn't limit the searcher.
		qsr.SetSizeLimit(uint64(math.MaxUint64))

		if qsr.GetSegType() != structs.PQS {
			continue
		}

		spqmr, err := pqs.GetAllPersistentQueryResults(qsr.GetSegKey(), qsr.QueryInformation.GetPqid())
		if err != nil {
			log.Errorf("qid=%d, searcher.getBlocks: Cannot get persistent query results; searching all blocks; err=%v",
				s.qid, err)
			qsr.SetSegType(structs.RAW_SEARCH)
			qsr.SetBlockTracker(structs.InitEntireFileBlockTracker())
		} else {
			qsr.SetBlockTracker(structs.InitExclusionBlockTracker(spqmr))
			pqmrs[i].Set(spqmr)
		}
	}

	allBlocks := make([]*block, 0)
	for i, qsr := range qsrs {
		pqmrBlockNumbers := make(map[uint16]struct{})
		if pqmr, ok := pqmrs[i].Get(); ok {
			blockToMetadata, blockSummaries, err := metadata.GetSearchInfoAndSummaryForPQS(qsr.GetSegKey(), pqmr)
			if err != nil {
				log.Errorf("qid=%v, searcher.getBlocks: failed to get search info and summary for PQS: %v",
					s.qid, err)
				return nil, err
			}

			blocks := makeBlocksFromPQMR(blockToMetadata, blockSummaries, qsr, pqmr)
			allBlocks = append(allBlocks, blocks...)

			for _, block := range blocks {
				pqmrBlockNumbers[block.BlkNum] = struct{}{}
			}
		}

		fileToSSR, err := query.GetSSRsFromQSR(qsr, s.querySummary)
		if err != nil {
			log.Errorf("qid=%v, searcher.getBlocks: failed to get SSRs from QSR %+v; err=%v", s.qid, qsr, err)
			return nil, err
		}

		for file, ssr := range fileToSSR {
			blocks := makeBlocksFromSSR(qsr, file, ssr)

			for _, block := range blocks {
				if _, ok := pqmrBlockNumbers[block.BlkNum]; !ok {
					allBlocks = append(allBlocks, block)
				}
			}
		}
	}

	return allBlocks, nil
}

func makeBlocksFromPQMR(blockToMetadata map[uint16]*structs.BlockMetadataHolder,
	blockSummaries []*structs.BlockSummary, qsr *query.QuerySegmentRequest,
	pqmr *pqmr.SegmentPQMRResults) []*block {

	blocks := make([]*block, 0, len(blockToMetadata))
	for blkNum, blockMeta := range blockToMetadata {
		blocks = append(blocks, &block{
			BlockSummary:        blockSummaries[blkNum],
			BlockMetadataHolder: blockMeta,
			parentQSR:           qsr,
			parentPQMR:          toputils.NewOptionWithValue(pqmr),
		})
	}

	return blocks
}

func makeBlocksFromSSR(qsr *query.QuerySegmentRequest, segkeyFname string,
	ssr *structs.SegmentSearchRequest) []*block {

	blocks := make([]*block, 0, len(ssr.AllBlocksToSearch))

	for blockNum, blockMeta := range ssr.AllBlocksToSearch {
		blocks = append(blocks, &block{
			BlockSummary:        ssr.SearchMetadata.BlockSummaries[blockNum],
			BlockMetadataHolder: blockMeta,
			parentQSR:           qsr,
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

// Returns the next blocks to search and the end time; every record before the
// end time will be in one of the returned blocks; if a record has the exact
// same timestamp as the end time, it may or may not be in the returned blocks.
//
// When possible (adhering to the above guarantee), no more than maxBlocks will
// be returned. When the number of returned blocks does not exceed maxBlocks,
// it will be as close to maxBlocks as possible.
func getNextBlocks(sortedBlocks []*block, maxBlocks int, mode sortMode) ([]*block, uint64, error) {
	if len(sortedBlocks) == 0 {
		return nil, 0, nil
	}

	if mode == anyOrder {
		if len(sortedBlocks) > maxBlocks {
			return sortedBlocks[:maxBlocks], 0, nil
		}

		return sortedBlocks, 0, nil
	}

	var startTimeOf func(block *block) uint64
	var endTimeOf func(block *block) uint64
	switch mode {
	case recentFirst:
		startTimeOf = func(block *block) uint64 {
			return block.HighTs
		}
		endTimeOf = func(block *block) uint64 {
			return block.LowTs
		}
	case recentLast:
		startTimeOf = func(block *block) uint64 {
			return block.LowTs
		}
		endTimeOf = func(block *block) uint64 {
			return block.HighTs
		}
	default:
		return nil, 0, toputils.TeeErrorf("getNextBlocks: invalid sort mode: %v", mode)
	}

	numBlocks := 0
	for i := 0; i < len(sortedBlocks); i++ {
		if startTimeOf(sortedBlocks[i]) == startTimeOf(sortedBlocks[0]) {
			numBlocks++
		} else {
			break
		}
	}

	for {
		nextPossibleNumBlocks := numBlocks + 1
		for i := numBlocks; i < len(sortedBlocks); i++ {
			if startTimeOf(sortedBlocks[i]) == startTimeOf(sortedBlocks[numBlocks]) {
				nextPossibleNumBlocks = i + 1
			} else {
				break
			}
		}

		if nextPossibleNumBlocks > maxBlocks {
			break
		}

		numBlocks = nextPossibleNumBlocks

		if numBlocks == len(sortedBlocks) {
			break
		}
	}

	if numBlocks >= len(sortedBlocks) {
		overallEndTime := endTimeOf(sortedBlocks[0])
		for i := 0; i < len(sortedBlocks); i++ {
			endTime := endTimeOf(sortedBlocks[i])

			switch mode {
			case recentFirst:
				if endTime < overallEndTime {
					overallEndTime = endTime
				}
			case recentLast:
				if endTime > overallEndTime {
					overallEndTime = endTime
				}
			default:
				return nil, 0, toputils.TeeErrorf("getNextBlocks: invalid sort mode: %v", mode)
			}
		}

		return sortedBlocks, overallEndTime, nil
	}

	return sortedBlocks[:numBlocks], startTimeOf(sortedBlocks[numBlocks]), nil
}

// All of the blocks must be for the same segment.
func (s *searcher) readSortedRRCs(blocks []*block, segkey string) ([]*segutils.RecordResultContainer, map[uint16]string, error) {
	if len(blocks) == 0 {
		return nil, nil, nil
	}

	sizeLimit := uint64(math.MaxUint64)
	aggs := s.queryInfo.GetAggregators()
	aggs.Sort = nil // We'll sort later, so don't do extra sorting work.
	queryType := s.queryInfo.GetQueryType()
	searchResults, err := segresults.InitSearchResults(sizeLimit, aggs, queryType, s.qid)
	if err != nil {
		log.Errorf("qid=%v, searcher.readSortedRRCs: failed to initialize search results: %v", s.qid, err)
		return nil, nil, err
	}

	encoding, ok := s.segEncToKey.GetReverse(segkey)
	if !ok {
		encoding = uint16(s.segEncToKey.Len())
		s.segEncToKey.Set(encoding, segkey)
	}
	searchResults.NextSegKeyEnc = encoding

	pqmrBlocks := make([]*block, 0, len(blocks))
	rawSearchBlocks := make([]*block, 0, len(blocks))
	for _, block := range blocks {
		if _, ok := block.parentPQMR.Get(); ok {
			pqmrBlocks = append(pqmrBlocks, block)
		} else {
			rawSearchBlocks = append(rawSearchBlocks, block)
		}
	}

	err = s.addRRCsFromPQMR(searchResults, pqmrBlocks)
	if err != nil {
		log.Errorf("qid=%v, searcher.readSortedRRCs: failed to get RRCs from PQMR: %v", s.qid, err)
		return nil, nil, err
	}

	err = s.addRRCsFromRawSearch(searchResults, rawSearchBlocks)
	if err != nil {
		log.Errorf("qid=%v, searcher.readSortedRRCs: failed to get RRCs from search: %v", s.qid, err)
		return nil, nil, err
	}

	rrcs := searchResults.GetResults()
	err = sortRRCs(rrcs, s.sortMode)
	if err != nil {
		log.Errorf("qid=%v, searcher.readSortedRRCs: failed to sort RRCs: %v", s.qid, err)
		return nil, nil, err
	}

	return rrcs, searchResults.SegEncToKey, nil
}

func sortRRCs(rrcs []*segutils.RecordResultContainer, mode sortMode) error {
	switch mode {
	case recentFirst:
		sort.Slice(rrcs, func(i, j int) bool {
			return rrcs[i].TimeStamp > rrcs[j].TimeStamp
		})
	case recentLast:
		sort.Slice(rrcs, func(i, j int) bool {
			return rrcs[i].TimeStamp < rrcs[j].TimeStamp
		})
	case anyOrder:
		// Do nothing.
	default:
		return toputils.TeeErrorf("sortRRCs: invalid sort mode: %v", mode)
	}

	return nil
}

func (s *searcher) addRRCsFromPQMR(searchResults *segresults.SearchResults, blocks []*block) error {
	if len(blocks) == 0 {
		return nil
	}

	metas := make(map[uint16]*structs.BlockMetadataHolder)
	summaries := make([]*structs.BlockSummary, 0)
	for _, block := range blocks {
		metas[block.BlkNum] = block.BlockMetadataHolder
		if block.BlkNum >= uint16(len(summaries)) {
			summaries = toputils.ResizeSlice(summaries, int(block.BlkNum+1))
		}
		summaries[block.BlkNum] = block.BlockSummary
	}
	for i := range summaries {
		if summaries[i] == nil {
			summaries[i] = &structs.BlockSummary{}
		}
	}

	pqmr, err := getPQMR(blocks)
	if err != nil {
		log.Errorf("qid=%v, searcher.addRRCsFromPQMR: failed to get PQMR: %v", s.qid, err)
		return err
	}

	err = query.ApplySinglePQSRawSearch(blocks[0].parentQSR, searchResults, pqmr,
		metas, summaries, s.querySummary)
	if err != nil {
		log.Errorf("qid=%v, searcher.addRRCsFromPQMR: failed to apply PQS: %v", s.qid, err)
		return err
	}

	return nil
}

func (s *searcher) addRRCsFromRawSearch(searchResults *segresults.SearchResults, blocks []*block) error {
	if len(blocks) == 0 {
		return nil
	}

	allSegRequests, err := getSSRs(blocks)
	if err != nil {
		log.Errorf("qid=%v, searcher.addRRCsFromRawSearch: failed to get SSRs: %v", s.qid, err)
		return err
	}

	parallelismPerFile := s.queryInfo.GetParallelismPerFile()
	searchNode := s.queryInfo.GetSearchNode()
	timeRange := s.queryInfo.GetQueryRange()
	sizeLimit := uint64(math.MaxUint64)
	aggs := s.queryInfo.GetAggregators()

	err = query.ApplyFilterOperatorInternal(searchResults, allSegRequests,
		parallelismPerFile, searchNode, timeRange, sizeLimit, aggs, s.qid, s.querySummary)
	if err != nil {
		log.Errorf("qid=%v, searcher.addRRCsFromRawSearch: failed to apply filter operator: %v", s.qid, err)
		return err
	}

	return nil
}

// All of the blocks should be for the same PQMR.
func getPQMR(blocks []*block) (*pqmr.SegmentPQMRResults, error) {
	if len(blocks) == 0 {
		return nil, nil
	}

	// Each block should have come from the same PQMR.
	firstPQMR, ok := blocks[0].parentPQMR.Get()
	if !ok {
		return nil, toputils.TeeErrorf("getPQMR: first block has no PQMR")
	}

	for _, block := range blocks {
		pqmr, ok := block.parentPQMR.Get()
		if !ok {
			return nil, toputils.TeeErrorf("getPQMR: block has no PQMR")
		} else if pqmr != firstPQMR {
			return nil, toputils.TeeErrorf("getPQMR: blocks are from different PQMRs")
		}
	}

	finalPQMR := pqmr.InitSegmentPQMResults()
	for _, block := range blocks {
		blockResults, ok := firstPQMR.GetBlockResults(block.BlkNum)
		if !ok {
			return nil, toputils.TeeErrorf("getPQMR: block %v not found", block.BlkNum)
		}

		finalPQMR.SetBlockResults(block.BlkNum, blockResults)
	}

	return finalPQMR, nil
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

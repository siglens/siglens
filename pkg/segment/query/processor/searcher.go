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
	"container/list"
	"fmt"
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
	"github.com/siglens/siglens/pkg/segment/results/blockresults"
	"github.com/siglens/siglens/pkg/segment/results/segresults"
	"github.com/siglens/siglens/pkg/segment/search"
	"github.com/siglens/siglens/pkg/segment/sortindex"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type block struct {
	*structs.BlockSummary
	parentQSR *query.QuerySegmentRequest

	// For PQS blocks, these must be set.
	parentPQMR toputils.Option[*pqmr.SegmentPQMRResults]

	// For raw search blocks, these must be set.
	parentSSR   *structs.SegmentSearchRequest
	segkeyFname string
	BlkNum      uint16
}

type sortIndexState struct {
	forceNormalSearch  bool // If true, don't use sort index.
	numRecordsPerBatch int
	didFirstFetch      bool
	segKeyToInfo       map[string]*segInfo
	exhaustedSegKey    toputils.Option[string]
	numRecordsSent     uint64
	didEarlyExit       bool
}

type segInfo struct {
	qsr        *query.QuerySegmentRequest
	savedIQR   *iqr.IQR
	checkpoint *sortindex.Checkpoint
	pqmr       *pqmr.SegmentPQMRResults
}

type subsearch struct {
	subsearchers []*Searcher
	merger       *DataProcessor
}

type Searcher struct {
	*subsearch

	sortIndexState

	qid          uint64
	queryInfo    *query.QueryInformation
	querySummary *summary.QuerySummary
	sortMode     sortMode
	sortExpr     *structs.SortExpr // Overrides sortMode if not nil. TODO: remove sortMode.
	startTime    time.Time

	gotBlocks             bool
	remainingBlocksSorted []*block // Sorted by time as specified by sortMode.
	qsrs                  []*query.QuerySegmentRequest
	cutOffTimestampInMs   uint64
	unprocessedQSRs       *list.List
	processedBlocks       map[string]map[uint16]struct{}
	gotAllSegments        bool

	unsentRRCs           []*segutils.RecordResultContainer
	segEncToKey          *toputils.TwoWayMap[uint32, string]
	segEncToKeyBaseValue uint32

	setAsIqrStatsResults bool
}

func NewSearcher(queryInfo *query.QueryInformation, querySummary *summary.QuerySummary,
	sortMode sortMode, sortExpr *structs.SortExpr, startTime time.Time) (*Searcher, error) {

	return newSearcherHelper(queryInfo, querySummary, sortMode, sortExpr, startTime, true)
}

func newSearcherHelper(queryInfo *query.QueryInformation, querySummary *summary.QuerySummary,
	sortMode sortMode, sortExpr *structs.SortExpr, startTime time.Time, checkforSubsearch bool) (*Searcher, error) {

	if queryInfo == nil {
		return nil, toputils.TeeErrorf("searcher.NewSearcher: queryInfo is nil")
	}
	if querySummary == nil {
		return nil, toputils.TeeErrorf("searcher.NewSearcher: querySummary is nil")
	}

	qid := queryInfo.GetQid()

	searcher := &Searcher{
		qid:                   qid,
		queryInfo:             queryInfo,
		querySummary:          querySummary,
		sortMode:              sortMode,
		sortExpr:              sortExpr,
		startTime:             startTime,
		remainingBlocksSorted: make([]*block, 0),
		unsentRRCs:            make([]*segutils.RecordResultContainer, 0),
		segEncToKey:           toputils.NewTwoWayMap[uint32, string](),
		segEncToKeyBaseValue:  queryInfo.GetSegEncToKeyBaseValue(),
	}

	if checkforSubsearch {
		var err error
		searcher.subsearch, err = getSubsearchIfNeeded(searcher)
		if err != nil {
			log.Errorf("qid=%v, searcher.NewSearcher: failed to get subsearcher: %v", qid, err)
			return nil, err
		}
	}

	return searcher, nil
}

func getSubsearchIfNeeded(searcher *Searcher) (*subsearch, error) {
	if searcher == nil || searcher.sortExpr == nil || len(searcher.sortExpr.SortEles) == 0 {
		return nil, nil
	}

	qsrs, err := query.GetSortedQSRs(searcher.queryInfo, searcher.startTime, searcher.querySummary)
	if err != nil {
		log.Errorf("qid=%v, getSubsearchIfNeeded: failed to get sorted QSRs: %v", searcher.qid, err)
		return nil, err
	}

	cname := searcher.sortExpr.SortEles[0].Field
	sortMode, err := sortindex.ModeFromString(searcher.sortExpr.SortEles[0].Op)
	if err != nil {
		log.Errorf("qid=%v, getSubsearchIfNeeded: unknown sort mode %v; err=%v",
			searcher.qid, searcher.sortExpr.SortEles[0].Op, err)
		return nil, err
	}

	sortIndexQSRs := make([]*query.QuerySegmentRequest, 0, len(qsrs))
	otherQSRs := make([]*query.QuerySegmentRequest, 0, len(qsrs))

	for _, qsr := range qsrs {
		if sortindex.Exists(qsr.GetSegKey(), cname, sortMode) {
			sortIndexQSRs = append(sortIndexQSRs, qsr)
		} else {
			otherQSRs = append(otherQSRs, qsr)
		}
	}

	// Make two subsearchers. One handles segments that have the required sort
	// index files; the other handles the segments lacking them.
	subsearchers := make([]*Searcher, 2)
	for i := 0; i < 2; i++ {
		subsearchers[i], err = newSearcherHelper(searcher.queryInfo, searcher.querySummary,
			searcher.sortMode, searcher.sortExpr, searcher.startTime, false)
		if err != nil {
			log.Errorf("qid=%v, getSubsearchIfNeeded: failed to create subsearcher: %v", searcher.qid, err)
			return nil, err
		}
	}
	subsearchers[0].qsrs = sortIndexQSRs
	subsearchers[0].initUnprocessedQSRs()
	subsearchers[1].qsrs = otherQSRs
	subsearchers[1].initUnprocessedQSRs()
	subsearchers[1].sortIndexState.forceNormalSearch = true
	subsearchers[1].segEncToKeyBaseValue += uint32(len(sortIndexQSRs))

	query.InitProgressForRRCCmd(uint64(metadata.GetTotalBlocksInSegments(getAllSegKeysInQSRS(qsrs))), searcher.qid)

	sortExpr := searcher.sortExpr.ShallowCopy()
	sortExpr.Limit = math.MaxInt64
	merger := NewSortDP(sortExpr) // TODO: use a mergeDP, since each stream is already sorted.
	for _, searcher := range subsearchers {
		merger.streams = append(merger.streams, NewCachedStream(searcher))
	}
	merger.SetMergeSettingsBasedOnStream(merger)

	return &subsearch{
		subsearchers: subsearchers,
		merger:       merger,
	}, nil
}

func (s *Searcher) SetAsIqrStatsResults() {
	s.setAsIqrStatsResults = true
}

func (s *Searcher) Rewind() {
	s.gotBlocks = false
	s.qsrs = nil
	s.processedBlocks = nil
	s.unprocessedQSRs = nil
	s.gotAllSegments = false
	s.remainingBlocksSorted = make([]*block, 0)
	s.unsentRRCs = make([]*segutils.RecordResultContainer, 0)
	s.segEncToKey = toputils.NewTwoWayMap[uint32, string]()
}

func (s *Searcher) Cleanup() {
	// Nothing to do.
}

func (s Searcher) String() string {
	return "<base searcher>"
}

func getNumRecords(blocks []*block) uint64 {
	var totalRecords uint64
	for _, block := range blocks {
		totalRecords += uint64(block.RecCount)
	}
	return totalRecords
}

func getAllSegKeysInQSRS(qsrs []*query.QuerySegmentRequest) []string {
	segKeys := make([]string, 0, len(qsrs))
	for _, qsr := range qsrs {
		segKeys = append(segKeys, qsr.GetSegKey())
	}
	return segKeys
}

func (s *Searcher) initUnprocessedQSRs() {
	if s.qsrs == nil {
		return
	}
	s.unprocessedQSRs = list.New()
	s.processedBlocks = make(map[string]map[uint16]struct{})

	for _, qsr := range s.qsrs {
		s.unprocessedQSRs.PushBack(qsr)
	}
}

func (s *Searcher) Fetch() (*iqr.IQR, error) {
	if s.subsearch != nil {
		return s.subsearch.merger.Fetch()
	}

	switch s.queryInfo.GetQueryType() {
	case structs.SegmentStatsCmd, structs.GroupByCmd:
		return s.fetchStatsResults()
	case structs.RRCCmd:
		// Get blocks for the segment batch to process
		if s.sortExpr != nil && !s.sortIndexState.forceNormalSearch {
			return s.fetchColumnSortedRRCs()
		}
		// initialize QSRs if they don't exist
		if s.qsrs == nil {
			err := s.initializeQSRs()
			if err != nil {
				return nil, toputils.TeeErrorf("qid=%v, searcher.Fetch: failed to get and set QSRs: %v", s.qid, err)
			}
			s.initUnprocessedQSRs()
			query.InitProgressForRRCCmd(uint64(metadata.GetTotalBlocksInSegments(getAllSegKeysInQSRS(s.qsrs))), s.qid)
		}
		if !s.gotBlocks {
			blocks, err := s.getBlocks()
			if err != nil {
				log.Errorf("qid=%v, searcher.Fetch: failed to get blocks: %v", s.qid, err)
				return nil, err
			}

			blocks = append(blocks, s.remainingBlocksSorted...)
			err = sortBlocks(blocks, s.sortMode)
			if err != nil {
				log.Errorf("qid=%v, searcher.Fetch: failed to sort blocks: %v", s.qid, err)
				return nil, err
			}

			s.remainingBlocksSorted = blocks
			s.gotBlocks = true
		}

		return s.fetchRRCs()
	default:
		return nil, toputils.TeeErrorf("qid=%v, searcher.Fetch: invalid query type: %v",
			s.qid, s.queryInfo.GetQueryType())
	}
}

func (s *Searcher) getPQMRsFromQSRs(qsrs []*query.QuerySegmentRequest) []toputils.Option[*pqmr.SegmentPQMRResults] {
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

	return pqmrs
}

func (s *Searcher) fetchColumnSortedRRCs() (*iqr.IQR, error) {
	qsrs := s.qsrs
	if len(qsrs) == 0 {
		return nil, io.EOF
	}

	if s.numRecordsPerBatch == 0 {
		if s.sortExpr == nil {
			return nil, toputils.TeeErrorf("qid=%v, searcher.fetchColumnSortedRRCs: sortExpr is nil", s.qid)
		}

		// This is chosen somewhat arbitrarily. We may want to tune this.
		s.numRecordsPerBatch = max(100, int(s.sortExpr.Limit)/len(qsrs))
	}

	return s.fetchSortedRRCsFromQSRs()
}

func (s *Searcher) fetchSortedRRCsFromQSRs() (*iqr.IQR, error) {
	if s.sortIndexState.didEarlyExit {
		return nil, io.EOF
	}

	result := iqr.NewIQR(s.qid)
	sorter := &sortProcessor{
		options: s.sortExpr,
	}

	qsrs := s.qsrs
	pqmrs := s.getPQMRsFromQSRs(qsrs)

	if !s.sortIndexState.didFirstFetch {
		s.sortIndexState.didFirstFetch = true
		s.sortIndexState.segKeyToInfo = make(map[string]*segInfo)

		for i, qsr := range qsrs {
			pqmr, _ := pqmrs[i].Get()
			segInfo := &segInfo{
				qsr:  qsr,
				pqmr: pqmr,
			}
			s.sortIndexState.segKeyToInfo[qsr.GetSegKey()] = segInfo

			nextIQR, err := s.fetchSortedRRCsForQSR(qsr, pqmr)
			if err != nil && err != io.EOF {
				log.Errorf("qid=%v, searcher.fetchColumnSortedRRCs: failed to fetch sorted RRCs: %v", s.qid, err)
				return nil, err
			}

			segInfo.savedIQR = nextIQR
		}
	} else {
		if segkey, ok := s.sortIndexState.exhaustedSegKey.Get(); ok {
			segInfo, ok := s.sortIndexState.segKeyToInfo[segkey]
			if !ok {
				return nil, toputils.TeeErrorf("qid=%v, searcher.fetchColumnSortedRRCs: segkey not found: %v",
					s.qid, segkey)
			}

			nextIQR, err := s.fetchSortedRRCsForQSR(segInfo.qsr, segInfo.pqmr)
			if err != nil && err != io.EOF {
				log.Errorf("qid=%v, searcher.fetchColumnSortedRRCs: failed to fetch sorted RRCs: %v", s.qid, err)
				return nil, err
			}

			segInfo.savedIQR = nextIQR
		}

		segkeys := make([]string, 0, len(s.sortIndexState.segKeyToInfo))
		iqrs := make([]*iqr.IQR, 0, len(s.sortIndexState.segKeyToInfo))
		for segkey, segInfo := range s.sortIndexState.segKeyToInfo {
			segkeys = append(segkeys, segkey)
			iqrs = append(iqrs, segInfo.savedIQR)
		}

		if len(iqrs) == 0 {
			return result, io.EOF
		}

		var firstExhaustedIndex int
		var err error
		result, firstExhaustedIndex, err = iqr.MergeIQRs(iqrs, sorter.lessDirectRead)
		if err != nil {
			log.Errorf("qid=%v, searcher.fetchColumnSortedRRCs: failed to merge IQRs: %v", s.qid, err)
			return nil, err
		}

		if firstExhaustedIndex < 0 || firstExhaustedIndex >= len(iqrs) {
			return nil, toputils.TeeErrorf("qid=%v, searcher.fetchColumnSortedRRCs: unexpected firstExhaustedIndex: %v",
				s.qid, firstExhaustedIndex)
		}

		segkey := segkeys[firstExhaustedIndex]
		s.sortIndexState.exhaustedSegKey.Set(segkey)
		if segInfo, ok := s.sortIndexState.segKeyToInfo[segkey]; !ok {
			return nil, toputils.TeeErrorf("qid=%v, searcher.fetchColumnSortedRRCs: segkey not found: %v",
				s.qid, segkey)
		} else {
			if sortindex.IsEOF(segInfo.checkpoint) {
				s.sortIndexState.exhaustedSegKey.Clear()
				delete(s.sortIndexState.segKeyToInfo, segkey)
			}
		}
	}

	numRecords := uint64(result.NumberOfRecords())
	numRemainingRecords := s.sortExpr.Limit - s.sortIndexState.numRecordsSent

	numRecordsToSend := numRecords
	if numRecordsToSend > numRemainingRecords {
		// If we reach the limit, we can stop immediately if we're only
		// searching on one column. But if we're searching on multiple columns,
		// we need to finish reading the records with the value we're on.
		requiresFullLine := (len(s.sortExpr.SortEles) > 1)
		if !requiresFullLine {
			numRecordsToSend = numRemainingRecords
			s.sortIndexState.didEarlyExit = true
		}
	}

	err := result.DiscardAfter(numRecordsToSend)
	if err != nil {
		return nil, toputils.TeeErrorf("qid=%v, searcher.fetchColumnSortedRRCs: failed to discard after %v; err=%v",
			s.qid, numRecordsToSend, err)
	}

	s.sortIndexState.numRecordsSent += uint64(result.NumberOfRecords())

	if len(s.sortIndexState.segKeyToInfo) == 0 {
		return result, io.EOF
	}

	return result, nil
}

func (s *Searcher) fetchSortedRRCsForQSR(qsr *query.QuerySegmentRequest, pqmr *pqmr.SegmentPQMRResults) (*iqr.IQR, error) {
	if s.segKeyToInfo == nil {
		return nil, toputils.TeeErrorf("qid=%v, searcher.fetchSortedRRCsForQSR: segKeyToInfo is nil", s.qid)
	}
	segInfo, ok := s.segKeyToInfo[qsr.GetSegKey()]
	if !ok {
		return nil, toputils.TeeErrorf("qid=%v, searcher.fetchSortedRRCsForQSR: segKey not found: %v", s.qid, qsr.GetSegKey())
	}

	cname := s.sortExpr.SortEles[0].Field
	reverse := !s.sortExpr.SortEles[0].SortByAsc
	checkpoint := segInfo.checkpoint
	var sortMode sortindex.SortMode
	switch s.sortExpr.SortEles[0].Op {
	case "", "auto":
		sortMode = sortindex.SortAsAuto
	case "num":
		sortMode = sortindex.SortAsNumeric
	case "str":
		sortMode = sortindex.SortAsString
	default:
		return nil, toputils.TeeErrorf("qid=%v, searcher.fetchSortedRRCsForQSR: invalid sort mode: %v",
			s.qid, s.sortExpr.SortEles[0].Op)
	}

	// For a multicolumn sort, we only read the first column from the sort
	// index, but once we read enough records, we have to continue reading the
	// rest of the records with that value; otherwise we can get incorrect
	// results when sorting those last few records on the subsequent sort
	// columns.
	multiColSort := (len(s.sortExpr.SortEles) > 1)
	readFullLine := multiColSort
	lines, checkpoint, err := sortindex.ReadSortIndex(qsr.GetSegKey(), cname, sortMode,
		reverse, s.numRecordsPerBatch, readFullLine, checkpoint)
	if err != nil {
		log.Errorf("qid=%v, searcher.fetchSortedRRCsForQSR: failed to read sort index: err=%v", s.qid, err)
		return nil, err
	}

	segInfo.checkpoint = checkpoint

	if len(lines) == 0 {
		// TODO: raw search this segment if we got no results because it has
		// the cname but no sort index.
		return nil, io.EOF
	}

	blockToRecNums := make(map[uint16][]uint16)
	processedRecordsInBatch := 0

	for _, line := range lines {
		for _, block := range line.Blocks {
			if _, ok := blockToRecNums[block.BlockNum]; !ok {
				blockToRecNums[block.BlockNum] = make([]uint16, 0)
			}
			blockToRecNums[block.BlockNum] = append(blockToRecNums[block.BlockNum], block.RecNums...)
			processedRecordsInBatch += len(block.RecNums)
		}
	}

	defer func() {
		if processedRecordsInBatch > 0 {
			if err := query.IncProgressForRRCCmd(uint64(processedRecordsInBatch), 1, s.qid); err != nil {
				log.Errorf("qid=%v, searcher.fetchSortedRRCsForQSR: failed to update progress: %v", s.qid, err)
			}
		}
	}()

	if s.queryInfo.GetSearchNodeType() == structs.MatchAllQuery {
		queryRange := s.queryInfo.GetTimeRange()
		segmentRange := qsr.GetTimeRange()

		if queryRange.Encloses(segmentRange) {
			iqr, err := s.handleSortIndexMatchAll(qsr, lines)
			if err != nil {
				return nil, fmt.Errorf("fetchSortedRRCsForQSR: failed to handle match all: err=%v", err)
			}

			if sortindex.IsEOF(checkpoint) {
				return iqr, io.EOF
			}

			return iqr, nil
		}
	}

	return s.handleSortIndexWithFilter(qsr, lines, pqmr, blockToRecNums)
}

func (s *Searcher) handleSortIndexMatchAll(qsr *query.QuerySegmentRequest, lines []sortindex.Line) (*iqr.IQR, error) {
	segKeyEncoding := s.getSegKeyEncoding(qsr.GetSegKey())

	rrcs, values := sortindex.AsRRCs(lines, segKeyEncoding)
	iqr := iqr.NewIQR(s.queryInfo.GetQid())
	err := iqr.AppendRRCs(rrcs, s.segEncToKey.GetMapForReading())
	if err != nil {
		log.Errorf("qid=%v, searcher.handleSortIndexMatchAll: failed to append RRCs: %v", s.qid, err)
		return nil, err
	}

	cname := s.sortExpr.SortEles[0].Field
	err = iqr.AppendKnownValues(map[string][]segutils.CValueEnclosure{cname: values})
	if err != nil {
		log.Errorf("qid=%v, searcher.handleSortIndexMatchAll: failed to append known values: %v", s.qid, err)
		return nil, err
	}

	return iqr, nil
}

// TODO: read PQS if enabled.
func (s *Searcher) handleSortIndexWithFilter(qsr *query.QuerySegmentRequest, lines []sortindex.Line, pqmr *pqmr.SegmentPQMRResults, blockToValidRecNums map[uint16][]uint16) (*iqr.IQR, error) {

	sizeLimit := uint64(math.MaxUint64)
	aggs := s.queryInfo.GetAggregators()
	aggs.Sort = nil // We'll sort later, so don't do extra sorting work.
	queryType := s.queryInfo.GetQueryType()
	searchResults, err := segresults.InitSearchResults(sizeLimit, aggs, queryType, s.qid)
	if err != nil {
		log.Errorf("qid=%v, handleSortIndexWithFilter: failed to initialize search results: %v", s.qid, err)
		return nil, err
	}

	segkey := qsr.GetSegKey()
	encoding, ok := s.segEncToKey.GetReverse(segkey)
	if !ok {
		encoding = s.getNextSegEncTokey()
		s.segEncToKey.Set(encoding, segkey)
	}
	searchResults.NextSegKeyEnc = encoding

	canUsePQMR := false
	var allBlocksToSearch map[uint16]struct{}
	var blockSummaries []*structs.BlockSummary
	if pqmr != nil {
		allBlocksToSearch, blockSummaries, err = metadata.GetSearchInfoAndSummaryForPQS(qsr.GetSegKey(), pqmr)
		if err != nil {
			log.Errorf("qid=%v, fetchSortedRRCsForQSR: failed to get search info and summary for PQS: %v",
				s.qid, err)
			return nil, err
		}
		canUsePQMR = true
		for blkNum := range blockToValidRecNums {
			if _, ok := allBlocksToSearch[blkNum]; !ok {
				canUsePQMR = false
				break
			}
		}
	}

	if canUsePQMR {
		err = s.applyPQSForSortedIndex(qsr, searchResults, pqmr, allBlocksToSearch,
			blockSummaries, sizeLimit, aggs, blockToValidRecNums)
		if err != nil {
			return nil, fmt.Errorf("fetchSortedRRCsForQSR: failed to apply PQS, err: %v", err)
		}
	} else {
		err = s.applyRawSearchForSortedIndex(qsr, searchResults, sizeLimit, aggs, blockToValidRecNums)
		if err != nil {
			return nil, fmt.Errorf("fetchSortedRRCsForQSR: failed to apply raw search, err: %v", err)
		}
	}

	rrcs := searchResults.GetResults()

	iqr := iqr.NewIQR(s.queryInfo.GetQid())
	err = iqr.AppendRRCs(rrcs, s.segEncToKey.GetMapForReading())
	if err != nil {
		return nil, err
	}

	return iqr, nil
}

func (s *Searcher) applyPQSForSortedIndex(qsr *query.QuerySegmentRequest, searchResults *segresults.SearchResults,
	pqmrResults *pqmr.SegmentPQMRResults, allBlocksToSearch map[uint16]struct{},
	blkSummaries []*structs.BlockSummary,
	sizeLimit uint64, aggs *structs.QueryAggregators, blockToValidRecNums map[uint16][]uint16) error {

	if len(allBlocksToSearch) == 0 {
		log.Infof("qid=%d, applyPQSForSortedIndex: segKey %+v has 0 blocks in segment PQMR results", s.qid, qsr.GetSegKey())
		return nil
	}

	// Remove blockNums that are not required to be processed.
	allBlocksToSearchToSend := make(map[uint16]struct{})
	validBlkNums := []uint16{}
	for blockNum := range allBlocksToSearch {
		if _, ok := blockToValidRecNums[blockNum]; ok {
			allBlocksToSearchToSend[blockNum] = struct{}{}
			validBlkNums = append(validBlkNums, blockNum)
		}
	}
	pqmrToSend := pqmrResults.GetCopyOfBlockResults(validBlkNums)

	req := &structs.SegmentSearchRequest{
		SegmentKey:          qsr.GetSegKey(),
		VirtualTableName:    qsr.GetTableName(),
		AllPossibleColumns:  s.queryInfo.GetColsToSearch(),
		AllBlocksToSearch:   allBlocksToSearchToSend,
		BlockToValidRecNums: blockToValidRecNums,
		SearchMetadata: &structs.SearchMetadataHolder{
			BlockSummaries:    blkSummaries,
			SearchTotalMemory: uint64(len(blkSummaries) * 24),
		},
		ConsistentCValLenMap: qsr.ConsistentCValLenMap,
	}
	nodeRes, err := query.GetOrCreateQuerySearchNodeResult(s.qid)
	if err != nil {
		return fmt.Errorf("qid=%d, applyPQSForSortedIndex: failed to get or create query search node result! Error: %v", s.qid, err)
	}
	search.RawSearchPQMResults(req, s.queryInfo.GetParallelismPerFile(), s.queryInfo.GetTimeRange(), aggs, sizeLimit, pqmrToSend, searchResults, s.qid, s.querySummary, nodeRes)

	if req.HasMatchedRrc {
		qsr.HasMatchedRrc = true
	}

	return nil
}

func (s *Searcher) applyRawSearchForSortedIndex(qsr *query.QuerySegmentRequest, searchResults *segresults.SearchResults,
	sizeLimit uint64, aggs *structs.QueryAggregators, blockToValidRecNums map[uint16][]uint16) error {

	allSSRs, err := query.GetSSRsFromQSR(qsr, s.querySummary)
	if err != nil {
		return fmt.Errorf("fetchSortedRRCsForQSR: failed to get SSRs from QSR: err=%v", err)
	}

	for segkeyFname := range allSSRs {
		allSSRs[segkeyFname].BlockToValidRecNums = blockToValidRecNums
	}

	parallelismPerFile := s.queryInfo.GetParallelismPerFile()
	searchNode := s.queryInfo.GetSearchNode()
	timeRange := s.queryInfo.GetTimeRange()

	err = query.ApplyFilterOperatorInternal(searchResults, allSSRs,
		parallelismPerFile, searchNode, timeRange, sizeLimit, aggs, s.qid, s.querySummary)
	if err != nil {
		return fmt.Errorf("qid=%v, searcher.addRRCsFromRawSearch: failed to apply filter operator: %v", s.qid, err)
	}

	return nil
}

func (s *Searcher) fetchRRCs() (*iqr.IQR, error) {

	if len(s.remainingBlocksSorted) == 0 && len(s.unsentRRCs) == 0 && s.gotAllSegments {
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

	switch s.sortMode {
	case recentFirst:
		endTime = max(endTime, s.cutOffTimestampInMs)
	case recentLast:
		endTime = min(endTime, s.cutOffTimestampInMs)
	case anyOrder:
		// Do nothing.
	}

	// Remove the blocks we're going to process. Since the blocks are sorted,
	// we always take blocks from the front of the list.
	s.remainingBlocksSorted = s.remainingBlocksSorted[len(nextBlocks):]

	if len(s.remainingBlocksSorted) == 0 || endTime == s.cutOffTimestampInMs {
		// We've processed all the blocks that we safely can, so we need to
		// fetch more.
		s.gotBlocks = false
	}

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

	_, err = toputils.BatchProcess(nextBlocks, getBatchKey, batchKeyLess, batchOperation, 1)
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

func (s *Searcher) fetchStatsResults() (*iqr.IQR, error) {
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
	err = query.AssociateSearchResult(qid, searchResults)
	if err != nil {
		return nil, toputils.TeeErrorf("qid=%v, searcher.fetchStatsResults: failed to associate search results: %v", s.qid, err)
	}

	var nodeResult *structs.NodeResult
	var groupByBuckets *blockresults.GroupByBuckets
	var timeBuckets *blockresults.TimeBuckets

	if qType == structs.SegmentStatsCmd {
		nodeResult = query.GetNodeResultsForSegmentStatsCmd(s.queryInfo, s.startTime, searchResults, nil, s.querySummary, orgId, s.setAsIqrStatsResults)
	} else if qType == structs.GroupByCmd {
		nodeResult, err = s.fetchGroupByResults(searchResults, aggs)
		if err != nil {
			if err == io.EOF {
				nodeResult = &structs.NodeResult{}
			} else {
				return nil, toputils.TeeErrorf("qid=%v, searcher.fetchStatsResults: failed to get group by results: %v", s.qid, err)
			}
		}
		if s.setAsIqrStatsResults {
			var isGroupByBuckets, isTimeBuckets bool
			isGroupByBucketNil, isTimeBucketsNil := true, true
			if nodeResult.GroupByBuckets != nil {
				isGroupByBucketNil = false
				groupByBuckets, isGroupByBuckets = nodeResult.GroupByBuckets.(*blockresults.GroupByBuckets)
			}
			if nodeResult.TimeBuckets != nil {
				isTimeBucketsNil = false
				timeBuckets, isTimeBuckets = nodeResult.TimeBuckets.(*blockresults.TimeBuckets)
			}

			if (!isGroupByBucketNil && !isGroupByBuckets) || (!isTimeBucketsNil && !isTimeBuckets) {
				return nil, toputils.TeeErrorf("qid=%v, searcher.fetchStatsResults: Expected GroupByBuckets and TimeBuckets, got %T and %T",
					qid, nodeResult.GroupByBuckets, nodeResult.TimeBuckets)
			}
		}
	} else {
		return nil, toputils.TeeErrorf("qid=%v, searcher.fetchStatsResults: invalid query type: %v", qid, qType)
	}

	// post getting of stats results
	iqr := iqr.NewIQR(s.queryInfo.GetQid())

	if s.setAsIqrStatsResults {
		err := iqr.SetIqrStatsResults(qType, nodeResult.SegStatsMap, groupByBuckets, timeBuckets, aggs)
		if err != nil {
			return nil, toputils.TeeErrorf("qid=%v, searcher.fetchStatsResults: failed to set IQR stats results: %v", qid, err)
		}
	} else {
		if aggs.StatisticExpr != nil {
			iqr.SetStatsAggregationResult(nodeResult.Histogram)
		}

		err = iqr.CreateStatsResults(nodeResult.MeasureResults, nodeResult.MeasureFunctions, nodeResult.GroupByCols, nodeResult.BucketCount)
		if err != nil {
			return nil, toputils.TeeErrorf("qid=%v, searcher.fetchStatsResults: failed to create stats results: %v", qid, err)
		}
	}

	s.qsrs = s.qsrs[0:] // Clear the QSRs so we don't process them again.

	return iqr, io.EOF
}

func (s *Searcher) fetchGroupByResults(searchResults *segresults.SearchResults, aggs *structs.QueryAggregators) (*structs.NodeResult, error) {
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

	nodeResult := query.GetNodeResultsFromQSRS(s.qsrs, s.queryInfo, s.startTime, searchResults, s.querySummary, s.setAsIqrStatsResults)
	if s.setAsIqrStatsResults {
		return nodeResult, nil
	}

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

func (s *Searcher) initializeQSRs() error {
	qsrs, err := query.GetSortedQSRs(s.queryInfo, s.startTime, s.querySummary)
	if err != nil {
		log.Errorf("qid=%v, searcher.initializeQSRs: failed to get sorted QSRs: %v", s.qid, err)
		return err
	}

	switch s.sortMode {
	case anyOrder:
		return nil
	case recentFirst:
		sort.Slice(qsrs, func(i, j int) bool {
			return qsrs[i].GetEndEpochMs() > qsrs[j].GetEndEpochMs()
		})
	case recentLast:
		sort.Slice(qsrs, func(i, j int) bool {
			return qsrs[i].GetStartEpochMs() < qsrs[j].GetStartEpochMs()
		})
	default:
		return fmt.Errorf("initializeQSRs: invalid sort mode: %v", s.sortMode)
	}

	s.qsrs = qsrs
	return nil
}

func (s *Searcher) shouldProcessBlock(block *block) bool {
	switch s.sortMode {
	case recentFirst:
		return block.HighTs >= s.cutOffTimestampInMs
	case recentLast:
		return block.LowTs <= s.cutOffTimestampInMs
	default:
		return true
	}
}

func (s *Searcher) getFilteredBlocks(blocks []*block) []*block {
	filteredBlocks := make([]*block, 0)

	for _, block := range blocks {
		if s.processedBlocks[block.parentQSR.GetSegKey()] == nil {
			s.processedBlocks[block.parentQSR.GetSegKey()] = make(map[uint16]struct{})
		}
		_, processed := s.processedBlocks[block.parentQSR.GetSegKey()][block.BlkNum]
		if processed {
			continue
		}

		if s.shouldProcessBlock(block) {
			filteredBlocks = append(filteredBlocks, block)
			s.processedBlocks[block.parentQSR.GetSegKey()][block.BlkNum] = struct{}{}
		}
	}

	return filteredBlocks
}

func (s *Searcher) shouldProcessQSR(qsr *query.QuerySegmentRequest) bool {
	switch s.sortMode {
	case recentFirst:
		return qsr.GetEndEpochMs() >= s.cutOffTimestampInMs
	case recentLast:
		return qsr.GetStartEpochMs() <= s.cutOffTimestampInMs
	default:
		return true
	}
}

func (s *Searcher) willProcessQSRCompletely(qsr *query.QuerySegmentRequest) bool {
	switch s.sortMode {
	case recentFirst:
		return qsr.GetStartEpochMs() >= s.cutOffTimestampInMs
	case recentLast:
		return qsr.GetEndEpochMs() <= s.cutOffTimestampInMs
	default:
		return true
	}
}

func (s *Searcher) getQSRSToProcess() ([]*query.QuerySegmentRequest, error) {
	qsrs := make([]*query.QuerySegmentRequest, 0)

	if s.unprocessedQSRs.Len() == 0 {
		s.gotAllSegments = true
		return nil, nil
	}

	segForCutOff, isQSR := s.unprocessedQSRs.Front().Value.(*query.QuerySegmentRequest)
	if !isQSR {
		return nil, fmt.Errorf("qid=%v, getQSRSToProcess: invalid type: %T", s.qid, s.unprocessedQSRs.Front().Value)
	}

	switch s.sortMode {
	case recentFirst, anyOrder:
		s.cutOffTimestampInMs = segForCutOff.GetStartEpochMs()
	case recentLast:
		s.cutOffTimestampInMs = segForCutOff.GetEndEpochMs()
	default:
		return nil, fmt.Errorf("qid=%v, getQSRSToProcess: invalid sort mode: %v", s.qid, s.sortMode)
	}

	for e := s.unprocessedQSRs.Front(); e != nil; {
		next := e.Next()
		qsr, isQSR := e.Value.(*query.QuerySegmentRequest)
		if !isQSR {
			return nil, fmt.Errorf("qid=%v, getQSRSToProcess: invalid type: %T", s.qid, e.Value)
		}
		if s.shouldProcessQSR(qsr) {
			qsrs = append(qsrs, qsr)
		}
		if s.willProcessQSRCompletely(qsr) {
			s.unprocessedQSRs.Remove(e)
		}
		e = next
	}

	return qsrs, nil
}

func (s *Searcher) getBlocks() ([]*block, error) {

	allBlocksInBatch := make([]*block, 0)

	qsrs, err := s.getQSRSToProcess()
	if err != nil {
		return nil, fmt.Errorf("qid=%v, searcher.getBlocks: failed to get QSRs to process: %v", s.qid, err)
	}

	pqmrs := s.getPQMRsFromQSRs(qsrs)

	for i, qsr := range qsrs {
		pqmrBlockNumbers := make(map[uint16]struct{})
		if pqmr, ok := pqmrs[i].Get(); ok {
			allBlocksToSearch, blockSummaries, err := metadata.GetSearchInfoAndSummaryForPQS(qsr.GetSegKey(), pqmr)
			if err != nil {
				log.Errorf("qid=%v, searcher.getBlocks: failed to get search info and summary for PQS: %v",
					s.qid, err)
				return nil, err
			}

			blocks := makeBlocksFromPQMR(allBlocksToSearch, blockSummaries, qsr, pqmr)
			allBlocksInBatch = append(allBlocksInBatch, blocks...)

			for _, block := range blocks {
				pqmrBlockNumbers[block.BlkNum] = struct{}{}
			}

			totalBlocksInSegment := metadata.GetNumBlocksInSegment(qsr.GetSegKey())
			if len(pqmrBlockNumbers) == int(totalBlocksInSegment) {
				// All blocks in the segment are covered by the PQMR, so we can skip the raw search.
				continue
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
					allBlocksInBatch = append(allBlocksInBatch, block)
				}
			}
		}
	}

	return s.getFilteredBlocks(allBlocksInBatch), nil
}

func (s *Searcher) getNextSegEncTokey() uint32 {
	return s.segEncToKeyBaseValue + uint32(s.segEncToKey.Len())
}

func (s *Searcher) getSegKeyEncoding(segKey string) uint32 {
	encoding, ok := s.segEncToKey.GetReverse(segKey)
	if ok {
		return encoding
	}

	encoding = s.getNextSegEncTokey()
	s.segEncToKey.Set(encoding, segKey)

	return encoding
}

func makeBlocksFromPQMR(allBlocksToSearch map[uint16]struct{},
	blockSummaries []*structs.BlockSummary, qsr *query.QuerySegmentRequest,
	pqmr *pqmr.SegmentPQMRResults) []*block {

	blocks := make([]*block, 0, len(allBlocksToSearch))
	for blkNum := range allBlocksToSearch {
		blocks = append(blocks, &block{
			BlockSummary: blockSummaries[blkNum],
			BlkNum:       blkNum,
			parentQSR:    qsr,
			parentPQMR:   toputils.NewOptionWithValue(pqmr),
		})
	}

	return blocks
}

func makeBlocksFromSSR(qsr *query.QuerySegmentRequest, segkeyFname string,
	ssr *structs.SegmentSearchRequest) []*block {

	blocks := make([]*block, 0, len(ssr.AllBlocksToSearch))

	for blockNum := range ssr.AllBlocksToSearch {
		blocks = append(blocks, &block{
			BlockSummary: ssr.SearchMetadata.BlockSummaries[blockNum],
			BlkNum:       blockNum,
			parentQSR:    qsr,
			parentSSR:    ssr,
			segkeyFname:  segkeyFname,
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
				overallEndTime = min(overallEndTime, endTime)
			case recentLast:
				overallEndTime = max(overallEndTime, endTime)
			default:
				return nil, 0, toputils.TeeErrorf("getNextBlocks: invalid sort mode: %v", mode)
			}
		}

		return sortedBlocks, overallEndTime, nil
	}

	return sortedBlocks[:numBlocks], startTimeOf(sortedBlocks[numBlocks]), nil
}

// All of the blocks must be for the same segment.
func (s *Searcher) readSortedRRCs(blocks []*block, segkey string) ([]*segutils.RecordResultContainer, map[uint32]string, error) {
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
		encoding = s.getNextSegEncTokey()
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

	err = s.addRRCsFromRawSearch(searchResults, rawSearchBlocks, nil)
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

func (s *Searcher) addRRCsFromPQMR(searchResults *segresults.SearchResults, blocks []*block) error {
	if len(blocks) == 0 {
		return nil
	}

	allBlocksToSearch := make(map[uint16]struct{})
	summaries := make([]*structs.BlockSummary, 0)
	for _, block := range blocks {
		allBlocksToSearch[block.BlkNum] = struct{}{}
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
		allBlocksToSearch, summaries, s.querySummary)
	if err != nil {
		log.Errorf("qid=%v, searcher.addRRCsFromPQMR: failed to apply PQS: %v", s.qid, err)
		return err
	}

	return nil
}

func (s *Searcher) addRRCsFromRawSearch(searchResults *segresults.SearchResults, blocks []*block,
	blockToValidRecNums map[uint16][]uint16) error {
	if len(blocks) == 0 {
		return nil
	}

	allSegRequests, err := getSSRs(blocks, blockToValidRecNums)
	if err != nil {
		log.Errorf("qid=%v, searcher.addRRCsFromRawSearch: failed to get SSRs: %v", s.qid, err)
		return err
	}

	parallelismPerFile := s.queryInfo.GetParallelismPerFile()
	searchNode := s.queryInfo.GetSearchNode()
	timeRange := s.queryInfo.GetTimeRange()
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
func getSSRs(blocks []*block, blockToValidRecNums map[uint16][]uint16) (map[string]*structs.SegmentSearchRequest, error) {

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
			ssrCopy.AllBlocksToSearch = make(map[uint16]struct{})
			fileToSSR[block.segkeyFname] = &ssrCopy
			ssr = &ssrCopy
		}

		ssr.AllBlocksToSearch[block.BlkNum] = struct{}{}
		ssr.BlockToValidRecNums = blockToValidRecNums
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

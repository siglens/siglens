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
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type processor interface {
	Cleanup()
	Process(*iqr.IQR) (*iqr.IQR, error)
	Rewind()
	GetFinalResultIfExists() (*iqr.IQR, bool)
}

type mergeSettings struct {
	less  func(*iqr.Record, *iqr.Record) bool
	limit utils.Option[uint64]

	numReturned uint64
}

type DataProcessor struct {
	streams       []*CachedStream
	mergeSettings mergeSettings
	processor     processor

	// The next two flags are not quite opposites.
	// Let p be a permutation function.
	// Let f be a command (e.g., head, sort)
	// Let rows be some rows of data.
	// !inputOrderMatters means: for all (p, rows), f(p(rows)) == p(f(rows)).
	// ignoresInputOrder means: for all (p, rows), f(p(rows)) == f(rows).
	inputOrderMatters bool
	ignoresInputOrder bool

	isPermutingCmd    bool // This command may change the order of input.
	isBottleneckCmd   bool // This command must see all input before yielding any output.
	isTransformingCmd bool // This command transforms the input into a different format (e.g., stats).
	isTwoPassCmd      bool // A subset of bottleneck commands.
	finishedFirstPass bool // Only used for two-pass commands.

	// If true, it means it's possible to merge results from multiple instances
	// of this type of command into once instance.
	isMergeableBottleneck bool

	processorLock   *sync.Mutex
	isCleanupCalled bool

	name string // For debugging

	streamDataChan chan streamResponse
}

type streamResponse struct {
	streamId int
	iqr      *iqr.IQR
}

func (dp *DataProcessor) DoesInputOrderMatter() bool {
	return dp.inputOrderMatters
}

func (dp *DataProcessor) IgnoresInputOrder() bool {
	return dp.ignoresInputOrder
}

func (dp *DataProcessor) IsPermutingCmd() bool {
	return dp.isPermutingCmd
}

func (dp *DataProcessor) IsBottleneckCmd() bool {
	return dp.isBottleneckCmd
}

func (dp *DataProcessor) IsMergeableBottleneckCmd() bool {
	return dp.isMergeableBottleneck
}

func (dp *DataProcessor) IsTwoPassCmd() bool {
	return dp.isTwoPassCmd
}

func (dp *DataProcessor) IsTransformingCmd() bool {
	return dp.isTransformingCmd
}

func (dp *DataProcessor) SetStreams(streams []*CachedStream) {
	if streams == nil {
		streams = make([]*CachedStream, 0)
	}
	dp.streams = streams
}

// SetLessFunc sets the less function to be used for sorting the input records.
// The default less function sorts by timestamp.
func (dp *DataProcessor) setDefaultMergeSettings() {
	dp.mergeSettings.less = sortByTimestampLess
	dp.mergeSettings.limit.Clear()
}

func (dp *DataProcessor) SetMergeSettingsBasedOnStream(stream Streamer) {
	if stream == nil {
		dp.setDefaultMergeSettings()
		return
	}

	if streamDP, ok := stream.(*DataProcessor); ok {
		switch streamDP.processor.(type) {
		case *sortProcessor:
			sorter := streamDP.processor.(*sortProcessor)
			dp.mergeSettings.less = sorter.lessDirectRead
			dp.mergeSettings.limit.Set(sorter.GetLimit())
		default:
			dp.setDefaultMergeSettings()
		}

		return
	}

	dp.setDefaultMergeSettings()
}

func (dp *DataProcessor) CleanupInputStreams() {
	streams := dp.streams
	for _, CachedStream := range streams {
		go CachedStream.Cleanup()
	}
}

func (dp DataProcessor) String() string {
	inputs := make([]string, 0, len(dp.streams))
	for _, stream := range dp.streams {
		inputs = append(inputs, stream.String())
	}

	name := dp.name
	if name == "" {
		name = "<unknown>"
	}
	return fmt.Sprintf("<%s> with inputs %v", name, inputs)
}

func (dp *DataProcessor) Cleanup() {
	dp.isCleanupCalled = true

	dp.processorLock.Lock()
	dp.processor.Cleanup()
	dp.processorLock.Unlock()
}

// Rewind sets up this DataProcessor to read the input streams from the
// beginning; however, it doesn't fully reset it to its initial state. For
// example, a two-pass command that finishes its first pass should remember
// whatever state information it got from the first pass.
func (dp *DataProcessor) Rewind() {
	dp.mergeSettings.numReturned = 0
	for _, stream := range dp.streams {
		stream.Rewind()
	}

	dp.processor.Rewind()
}

func (dp *DataProcessor) Fetch() (*iqr.IQR, error) {
	var output *iqr.IQR
	var resultExists bool

	for {

		if dp.isCleanupCalled {
			return nil, io.EOF
		}

		gotEOF := false

		// Check if the processor has a final result.
		output, resultExists = dp.processor.GetFinalResultIfExists()
		if resultExists {
			gotEOF = true
		} else {
			// if the processor doesn't have a final result, fetch it from the input streams.
			input, err := dp.getStreamInput()
			if err != nil && err != io.EOF {
				log.Errorf("DP.Fetch: failed to fetch input: %v", err)
				return nil, utils.WrapErrorf(err, "DP.Fetch: failed to fetch input: %v", err)
			}

			dp.processorLock.Lock()
			if dp.isCleanupCalled {
				dp.processorLock.Unlock()
				return nil, io.EOF
			}
			output, err = dp.processor.Process(input)
			dp.processorLock.Unlock()

			if err == io.EOF {
				gotEOF = true
			} else if err != nil {
				log.Errorf("DP.Fetch: failed to process input: %v", err)
				return nil, utils.WrapErrorf(err, "DP.Fetch: failed to process input: %v", err)
			}
		}

		if gotEOF {
			if dp.isTwoPassCmd && !dp.finishedFirstPass {
				dp.finishedFirstPass = true
				dp.Rewind()
				continue
			}

			return output, io.EOF
		} else if output != nil {
			if !dp.isBottleneckCmd || (dp.isTwoPassCmd && dp.finishedFirstPass) {
				return output, nil
			}
		}
	}
}

func (dp *DataProcessor) IsDataGenerator() bool {
	switch dp.processor.(type) {
	case *gentimesProcessor:
		return true
	case *inputlookupProcessor:
		// TODO: why does IsFirstCommand matter here? Maybe this function
		// should be renamed; maybe we can merge this with GeneratesData().
		return dp.processor.(*inputlookupProcessor).options.IsFirstCommand
	default:
		return false
	}
}

func (dp *DataProcessor) GeneratesData() bool {
	switch dp.processor.(type) {
	case *gentimesProcessor, *inputlookupProcessor:
		return true
	default:
		return false
	}
}

func (dp *DataProcessor) SetLimitForDataGenerator(limit uint64) {
	switch dp.processor.(type) {
	case *gentimesProcessor:
		dp.processor.(*gentimesProcessor).limit = limit
	case *inputlookupProcessor:
		dp.processor.(*inputlookupProcessor).limit = limit
	default:
		return
	}
}

func (dp *DataProcessor) IsEOFForDataGenerator() bool {
	switch dp.processor.(type) {
	case *gentimesProcessor:
		return dp.processor.(*gentimesProcessor).IsEOF()
	case *inputlookupProcessor:
		return dp.processor.(*inputlookupProcessor).IsEOF()
	default:
		return false
	}
}

func (dp *DataProcessor) CheckAndSetQidForDataGenerator(qid uint64) {
	switch dp.processor.(type) {
	case *gentimesProcessor:
		dp.processor.(*gentimesProcessor).qid = qid
	case *inputlookupProcessor:
		dp.processor.(*inputlookupProcessor).qid = qid
	default:
	}
}

func (dp *DataProcessor) SetStatsAsIqrStatsResults() error {
	switch dp.processor.(type) {
	case *statsProcessor:
		dp.processor.(*statsProcessor).SetAsIqrStatsResults()
	case *topProcessor:
		dp.processor.(*topProcessor).SetAsIqrStatsResults()
	case *rareProcessor:
		dp.processor.(*rareProcessor).SetAsIqrStatsResults()
	case *timechartProcessor:
		dp.processor.(*timechartProcessor).SetAsIqrStatsResults()
	case *passThroughProcessor:
		streams := dp.streams
		if len(streams) == 0 {
			return fmt.Errorf("dp.SetStatsAsIqrStatsResults: no streams")
		}

		if len(streams) > 1 {
			return fmt.Errorf("dp.SetStatsAsIqrStatsResults: multiple streams")
		}

		stream := streams[0]
		searcher, ok := stream.stream.(*Searcher)
		if !ok {
			return fmt.Errorf("dp.SetStatsAsIqrStatsResults: stream is not a searcher")
		}

		searcher.SetAsIqrStatsResults()
	default:
		return fmt.Errorf("dp.SetStatsAsIqrStatsResults: processor is not a stats type processor. processor type: %T", dp.processor)
	}

	return nil
}

func (dp *DataProcessor) getStreamInput() (*iqr.IQR, error) {
	switch len(dp.streams) {
	case 0:
		if dp.IsDataGenerator() {
			return nil, io.EOF
		}
		return nil, errors.New("no streams")
	case 1:
		return dp.streams[0].Fetch()
	default:
		// TODO: remove this outer if block but keep the inner.
		if hooks.GlobalHooks.GetDistributedStreamsHook == nil {
			if dp.IgnoresInputOrder() && dp.IsBottleneckCmd() {
				// Since it ignores input order, it doesn't matter which stream we
				// get data from, and we don't need to merge multiple streams.
				//
				// Since it's a bottleneck, we need to eventually get all data, so
				// we don't need to check dp.mergeSettings.limit
				return dp.fetchFromAnyStream()
			}
		}

		iqrs, streamIndices, err := dp.fetchFromAllStreamsWithData()
		if err != nil {
			return nil, utils.WrapErrorf(err, "DP.getStreamInput: failed to fetch from all streams: %v", err)
		}

		if len(iqrs) == 0 {
			return nil, io.EOF
		}

		iqr, exhaustedIQRIndex, err := iqr.MergeIQRs(iqrs, dp.mergeSettings.less)
		if err != nil && err != io.EOF {
			return nil, utils.WrapErrorf(err, "DP.getStreamInput: failed to merge IQRs: %v", err)
		}

		totalLimit, ok := dp.mergeSettings.limit.Get()
		if ok {
			thisLimit := totalLimit - dp.mergeSettings.numReturned
			if thisLimit == 0 {
				return nil, io.EOF
			}

			err := iqr.DiscardAfter(thisLimit)
			if err != nil {
				return nil, utils.WrapErrorf(err, "DP.getStreamInput: failed to discard after limit: %v", err)
			}
		}
		dp.mergeSettings.numReturned += uint64(iqr.NumberOfRecords())

		if exhaustedIQRIndex == -1 {
			return iqr, err
		}

		for i, iqr := range iqrs {
			if i == exhaustedIQRIndex {
				dp.streams[streamIndices[i]].SetUnusedDataFromLastFetch(nil)
			} else {
				// The merging function already discarded whatever records were
				// used from this IQR, so the IQR is in a state that only has
				// unused records.
				dp.streams[streamIndices[i]].SetUnusedDataFromLastFetch(iqr)
			}
		}

		if err == io.EOF {
			return iqr, io.EOF
		}

		return iqr, nil
	}
}

func (dp *DataProcessor) fetchFromAnyStream() (*iqr.IQR, error) {
	if dp.streamDataChan == nil {
		dp.streamDataChan = make(chan streamResponse, len(dp.streams))
	}

	// Check if there's a non-nil response still in the channel from last time,
	// but avoid blocking.
loop:
	for {
		select {
		case message := <-dp.streamDataChan:
			if message.iqr != nil {
				return message.iqr, nil
			}
		default:
			// The channel is empty.
			break loop
		}
	}

	// We'll fetch from several streams and want to detect when all streams
	// have no more data. However, we'll return once any stream returns data;
	// so to check if all streams have no more data, we have to track which
	// streams responded rather than simply how many responded because the same
	// stream could respond multiple times (e.g., in the previous call to this
	// function, we returned before stream A sent data on the channel, now we
	// fetch again from stream A and it has time to complete both the previous
	// and current fetches, so it puts two messages on the channel).
	numFetchedFrom := 0
	responders := make(map[int]struct{}, len(dp.streams))
	var finalErr error
	waitGroup := sync.WaitGroup{}

	for i := range dp.streams {
		if dp.streams[i].IsExhausted() {
			continue
		}

		numFetchedFrom++

		waitGroup.Add(1)
		go func(i int) {
			defer waitGroup.Done()

			iqr, err := dp.streams[i].Fetch()
			if err != nil && err != io.EOF {
				finalErr = fmt.Errorf("DP.fetchFromAnyStream: failed to fetch from stream %d: %v", i, err)
				dp.streamDataChan <- streamResponse{streamId: i, iqr: nil}
				return
			}

			if iqr == nil && err != io.EOF {
				finalErr = fmt.Errorf("DP.fetchFromAnyStream: stream %d returned nil IQR without EOF", i)
				dp.streamDataChan <- streamResponse{streamId: i, iqr: nil}
				return
			}

			dp.streamDataChan <- streamResponse{streamId: i, iqr: iqr}
		}(i)
	}
	waitGroup.Wait()

	if numFetchedFrom == 0 {
		return nil, io.EOF
	}

	for {
		message := <-dp.streamDataChan
		if finalErr != nil {
			log.Error(finalErr)
			return nil, finalErr
		}

		if message.iqr != nil {
			return message.iqr, nil
		}

		responders[message.streamId] = struct{}{}
		if len(responders) == numFetchedFrom {
			// All streams have responded, and none of them returned an IQR.
			return nil, io.EOF
		}
	}
}

func (dp *DataProcessor) fetchFromAllStreamsWithData() ([]*iqr.IQR, []int, error) {
	iqrs := make([]*iqr.IQR, 0, len(dp.streams))
	streamIndices := make([]int, 0, len(dp.streams))
	lock := sync.Mutex{}
	waitGroup := sync.WaitGroup{}
	var finalErr error

	for i, stream := range dp.streams {
		if stream.IsExhausted() {
			continue
		}

		waitGroup.Add(1)
		go func(i int, stream *CachedStream) {
			defer waitGroup.Done()
			iqr, err := stream.Fetch()
			if err != nil && err != io.EOF {
				finalErr = utils.WrapErrorf(err, "DP.fetchFromAllStreamsWithData: failed to fetch from stream %d: %v", i, err)
				return
			}

			if iqr == nil {
				if err != io.EOF {
					finalErr = utils.WrapErrorf(err, "DP.fetchFromAllStreamsWithData: stream %d returned nil IQR without EOF", i)
				}

				return
			}

			lock.Lock()
			iqrs = append(iqrs, iqr)
			streamIndices = append(streamIndices, i)
			lock.Unlock()
		}(i, stream)
	}
	waitGroup.Wait()

	return iqrs, streamIndices, finalErr
}

func sortByTimestampLess(r1, r2 *iqr.Record) bool {
	if r1 == nil {
		return false
	} else if r2 == nil {
		return true
	}

	timestampKey := config.GetTimeStampKey()

	r1TimestampCVal, err := r1.ReadColumn(timestampKey)
	if err != nil {
		return false
	}

	r2TimestampCVal, err := r2.ReadColumn(timestampKey)
	if err != nil {
		return true
	}

	r1Timestamp, err := r1TimestampCVal.GetUIntValue()
	if err != nil {
		return false
	}

	r2Timestamp, err := r2TimestampCVal.GetUIntValue()
	if err != nil {
		return true
	}

	return r1Timestamp > r2Timestamp
}

func NewBinDP(options *structs.BinCmdOptions) *DataProcessor {
	hasSpan := options.BinSpanOptions != nil
	return &DataProcessor{
		name:                  "bin",
		streams:               make([]*CachedStream, 0),
		processor:             &binProcessor{options: options},
		inputOrderMatters:     false,
		ignoresInputOrder:     false,
		isPermutingCmd:        false,
		isBottleneckCmd:       !hasSpan,
		isTwoPassCmd:          !hasSpan,
		isMergeableBottleneck: false, // TODO: implement merging, then set to true.
		processorLock:         &sync.Mutex{},
	}
}

func NewDedupDP(options *structs.DedupExpr) *DataProcessor {
	hasSort := len(options.DedupSortEles) > 0
	return &DataProcessor{
		name:                  "dedup",
		streams:               make([]*CachedStream, 0),
		processor:             &dedupProcessor{options: options},
		inputOrderMatters:     true,
		ignoresInputOrder:     false,
		isPermutingCmd:        false,
		isBottleneckCmd:       hasSort,
		isTwoPassCmd:          false,
		isMergeableBottleneck: false, // TODO: implement merging, then set to true.
		processorLock:         &sync.Mutex{},
	}
}

func NewEvalDP(options *structs.EvalExpr) *DataProcessor {
	return &DataProcessor{
		name:              "eval",
		streams:           make([]*CachedStream, 0),
		processor:         &evalProcessor{options: options},
		inputOrderMatters: false,
		ignoresInputOrder: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
		processorLock:     &sync.Mutex{},
	}
}

func NewFieldsDP(options *structs.ColumnsRequest) *DataProcessor {
	return &DataProcessor{
		name:              "fields",
		streams:           make([]*CachedStream, 0),
		processor:         &fieldsProcessor{options: options},
		inputOrderMatters: false,
		ignoresInputOrder: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
		processorLock:     &sync.Mutex{},
	}
}

func NewRenameDP(options *structs.RenameExp) *DataProcessor {
	return &DataProcessor{
		name:              "rename",
		streams:           make([]*CachedStream, 0),
		processor:         &renameProcessor{options: options},
		inputOrderMatters: false,
		ignoresInputOrder: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
		processorLock:     &sync.Mutex{},
	}
}

func NewFillnullDP(options *structs.FillNullExpr) *DataProcessor {
	isFieldListSet := len(options.FieldList) > 0
	return &DataProcessor{
		name:              "fillnull",
		streams:           make([]*CachedStream, 0),
		processor:         &fillnullProcessor{options: options},
		inputOrderMatters: false,
		ignoresInputOrder: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   !isFieldListSet,
		isTwoPassCmd:      !isFieldListSet,
		processorLock:     &sync.Mutex{},
	}
}

func NewGentimesDP(options *structs.GenTimes) *DataProcessor {
	return &DataProcessor{
		name:    "gentimes",
		streams: make([]*CachedStream, 0),
		processor: &gentimesProcessor{
			options:       options,
			currStartTime: options.StartTime,
		},
		inputOrderMatters: false,
		ignoresInputOrder: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
		processorLock:     &sync.Mutex{},
	}
}

func NewInputLookupDP(options *structs.InputLookup) *DataProcessor {
	return &DataProcessor{
		name:    "inputlookup",
		streams: make([]*CachedStream, 0),
		processor: &inputlookupProcessor{
			options: options,
			start:   options.Start,
		},
		inputOrderMatters: false,
		ignoresInputOrder: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
		processorLock:     &sync.Mutex{},
	}
}

func NewHeadDP(options *structs.HeadExpr) *DataProcessor {
	return &DataProcessor{
		name:              "head",
		streams:           make([]*CachedStream, 0),
		processor:         &headProcessor{options: options},
		inputOrderMatters: true,
		ignoresInputOrder: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
		processorLock:     &sync.Mutex{},
	}
}

func NewTailDP(options *structs.TailExpr) *DataProcessor {
	return &DataProcessor{
		name:                  "tail",
		streams:               make([]*CachedStream, 0),
		processor:             &tailProcessor{options: options},
		inputOrderMatters:     true,
		ignoresInputOrder:     false,
		isPermutingCmd:        true,
		isBottleneckCmd:       true, // TODO: depends on the previous DPs in the chain.
		isTwoPassCmd:          false,
		isMergeableBottleneck: false,
		processorLock:         &sync.Mutex{},
	}
}

func NewMakemvDP(options *structs.MultiValueColLetRequest) *DataProcessor {
	return &DataProcessor{
		name:              "makemv",
		streams:           make([]*CachedStream, 0),
		processor:         &makemvProcessor{options: options},
		inputOrderMatters: false,
		ignoresInputOrder: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
		processorLock:     &sync.Mutex{},
	}
}

func NewMVExpandDP(options *structs.MultiValueColLetRequest) *DataProcessor {
	return &DataProcessor{
		name:              "mvexpand",
		streams:           make([]*CachedStream, 0),
		processor:         &mvexpandProcessor{options: options},
		inputOrderMatters: false,
		ignoresInputOrder: false,
		isPermutingCmd:    true,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
		processorLock:     &sync.Mutex{},
	}
}

func NewRegexDP(options *structs.RegexExpr) *DataProcessor {
	return &DataProcessor{
		name:              "regex",
		streams:           make([]*CachedStream, 0),
		processor:         &regexProcessor{options: options},
		inputOrderMatters: false,
		ignoresInputOrder: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
		processorLock:     &sync.Mutex{},
	}
}

func NewRexDP(options *structs.RexExpr) *DataProcessor {
	return &DataProcessor{
		name:              "rex",
		streams:           make([]*CachedStream, 0),
		processor:         &rexProcessor{options: options},
		inputOrderMatters: false,
		ignoresInputOrder: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
		processorLock:     &sync.Mutex{},
	}
}

func NewWhereDP(options *structs.BoolExpr) *DataProcessor {
	return &DataProcessor{
		name:              "where",
		streams:           make([]*CachedStream, 0),
		processor:         &whereProcessor{options: options},
		inputOrderMatters: false,
		ignoresInputOrder: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
		processorLock:     &sync.Mutex{},
	}
}

func NewStreamstatsDP(options *structs.StreamStatsOptions) *DataProcessor {
	return &DataProcessor{
		name:              "streamstats",
		streams:           make([]*CachedStream, 0),
		processor:         &streamstatsProcessor{options: options},
		inputOrderMatters: true,
		ignoresInputOrder: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
		processorLock:     &sync.Mutex{},
	}
}

func NewTimechartDP(options *timechartOptions) *DataProcessor {
	return &DataProcessor{
		name:                  "timechart",
		streams:               make([]*CachedStream, 0),
		processor:             NewTimechartProcessor(options),
		inputOrderMatters:     false,
		ignoresInputOrder:     true,
		isPermutingCmd:        false,
		isBottleneckCmd:       true,
		isTransformingCmd:     true,
		isTwoPassCmd:          false,
		isMergeableBottleneck: false, // TODO: implement merging, then set to true.
		processorLock:         &sync.Mutex{},
	}
}

func NewStatsDP(options *structs.StatsExpr) *DataProcessor {
	return &DataProcessor{
		name:                  "stats",
		streams:               make([]*CachedStream, 0),
		processor:             NewStatsProcessor(options),
		inputOrderMatters:     false,
		ignoresInputOrder:     true,
		isPermutingCmd:        false,
		isBottleneckCmd:       true,
		isTransformingCmd:     true,
		isTwoPassCmd:          false,
		isMergeableBottleneck: true,
		processorLock:         &sync.Mutex{},
	}
}

func NewStatisticExprDP(options *structs.QueryAggregators) *DataProcessor {
	statsExpr := &structs.StatsExpr{GroupByRequest: options.GroupByRequest}
	options.StatsExpr = statsExpr

	if options.HasTopExpr() {
		return NewTopDP(options)
	} else if options.HasRareExpr() {
		return NewRareDP(options)
	} else {
		return nil
	}
}

func NewTopDP(options *structs.QueryAggregators) *DataProcessor {
	return &DataProcessor{
		name:                  "top",
		streams:               make([]*CachedStream, 0),
		processor:             NewTopProcessor(options),
		inputOrderMatters:     false,
		ignoresInputOrder:     true,
		isPermutingCmd:        true,
		isBottleneckCmd:       true,
		isTransformingCmd:     true,
		isTwoPassCmd:          false,
		isMergeableBottleneck: false,
		processorLock:         &sync.Mutex{},
	}
}

func NewRareDP(options *structs.QueryAggregators) *DataProcessor {
	return &DataProcessor{
		name:                  "rare",
		streams:               make([]*CachedStream, 0),
		processor:             NewRareProcessor(options),
		inputOrderMatters:     false,
		ignoresInputOrder:     true,
		isPermutingCmd:        true,
		isBottleneckCmd:       true,
		isTransformingCmd:     true,
		isTwoPassCmd:          false,
		isMergeableBottleneck: false,
		processorLock:         &sync.Mutex{},
	}
}

func NewTransactionDP(options *structs.TransactionArguments) *DataProcessor {
	return &DataProcessor{
		name:              "transaction",
		streams:           make([]*CachedStream, 0),
		processor:         &transactionProcessor{options: options},
		inputOrderMatters: true,
		ignoresInputOrder: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
		processorLock:     &sync.Mutex{},
	}
}

func NewSortDP(options *structs.SortExpr) *DataProcessor {
	return &DataProcessor{
		name:                  "sort",
		streams:               make([]*CachedStream, 0),
		processor:             &sortProcessor{options: options},
		inputOrderMatters:     false,
		ignoresInputOrder:     true,
		isPermutingCmd:        true,
		isBottleneckCmd:       true,
		isTwoPassCmd:          false,
		isMergeableBottleneck: false, // TODO: implement merging, then set to true.
		processorLock:         &sync.Mutex{},
	}
}

func NewScrollerDP(scrollFrom uint64, qid uint64) *DataProcessor {
	return &DataProcessor{
		name:              "scroller",
		streams:           make([]*CachedStream, 0),
		processor:         &scrollProcessor{scrollFrom: scrollFrom, qid: qid},
		inputOrderMatters: true,
		ignoresInputOrder: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
		processorLock:     &sync.Mutex{},
	}
}

type passThroughProcessor struct{}

func (ptp *passThroughProcessor) Process(input *iqr.IQR) (*iqr.IQR, error) {
	if input == nil {
		return nil, io.EOF
	}

	return input, nil
}

func (ptp *passThroughProcessor) Rewind()  {}
func (ptp *passThroughProcessor) Cleanup() {}
func (ptp *passThroughProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return nil, false
}

func NewSearcherDP(searcher Streamer, queryType structs.QueryType) *DataProcessor {
	isTransformingCmd := queryType.IsSegmentStatsCmd() || queryType.IsGroupByCmd()
	return &DataProcessor{
		name:              "searcher",
		streams:           []*CachedStream{NewCachedStream(searcher)},
		processor:         &passThroughProcessor{},
		processorLock:     &sync.Mutex{},
		isTransformingCmd: isTransformingCmd,
	}
}

func NewPassThroughDPWithStreams(cachedStreams []*CachedStream) *DataProcessor {
	return &DataProcessor{
		name:              "passthrough",
		streams:           cachedStreams,
		processor:         &passThroughProcessor{},
		inputOrderMatters: false,
		ignoresInputOrder: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
		processorLock:     &sync.Mutex{},
	}
}

func NewMergeBottleneckDP() *DataProcessor {
	return &DataProcessor{
		name:                  "merge-bottleneck",
		streams:               make([]*CachedStream, 0),
		processor:             &mergeProcessor{},
		inputOrderMatters:     false,
		ignoresInputOrder:     true,
		isPermutingCmd:        false,
		isBottleneckCmd:       true,
		isTransformingCmd:     false,
		isTwoPassCmd:          false,
		isMergeableBottleneck: true,
		processorLock:         &sync.Mutex{},
	}
}

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
	"time"

	"github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/aggregations"
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/results/segresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type timechartOptions struct {
	timeBucket     *structs.TimeBucket
	timeChartExpr  *structs.TimechartExpr
	groupByRequest *structs.GroupByRequest
}

type errorData struct {
	readColumns     map[string]error // columnName -> error. Tracks errors while reading the column through iqr.Record.ReadColumn
	getStringErrors map[string]error // columnName -> error. Tracks errors while converting CValue to string
}

type timechartProcessor struct {
	options             *timechartOptions
	spanError           error
	qid                 uint64
	searchResults       *segresults.SearchResults
	bucketKeyWorkingBuf []byte
	timeRangeBuckets    []uint64
	errorData           *errorData
	hasFinalResult      bool
}

func NewTimechartProcessor(aggs *structs.QueryAggregators, timeRange *dtypeutils.TimeRange, qid uint64) *timechartProcessor {
	if aggs.TimeHistogram == nil || aggs.TimeHistogram.Timechart == nil {
		return &timechartProcessor{options: nil, qid: qid}
	}

	if aggs.GroupByRequest != nil {
		aggs.GroupByRequest.BucketCount = int(utils.QUERY_MAX_BUCKETS)
	}

	processor := &timechartProcessor{qid: qid}
	aggs.TimeHistogram.StartTime = timeRange.StartEpochMs
	aggs.TimeHistogram.EndTime = timeRange.EndEpochMs

	if aggs.TimeHistogram.Timechart.BinOptions != nil &&
		aggs.TimeHistogram.Timechart.BinOptions.SpanOptions != nil &&
		aggs.TimeHistogram.Timechart.BinOptions.SpanOptions.DefaultSettings {
		spanOptions, err := structs.GetDefaultTimechartSpanOptions(timeRange.StartEpochMs, timeRange.EndEpochMs, qid)
		if err != nil {
			processor.spanError = err
			return processor
		}
		aggs.TimeHistogram.Timechart.BinOptions.SpanOptions = spanOptions
		aggs.TimeHistogram.IntervalMillis = aggregations.GetIntervalInMillis(spanOptions.SpanLength.Num, spanOptions.SpanLength.TimeScalr)
	}

	processor.timeRangeBuckets = aggregations.GenerateTimeRangeBuckets(aggs.TimeHistogram)
	processor.errorData = &errorData{
		readColumns:     make(map[string]error),
		getStringErrors: make(map[string]error),
	}

	processor.options = &timechartOptions{
		timeBucket:     aggs.TimeHistogram,
		timeChartExpr:  aggs.TimeHistogram.Timechart,
		groupByRequest: aggs.GroupByRequest,
	}

	return processor
}

func (p *timechartProcessor) Process(inputIQR *iqr.IQR) (*iqr.IQR, error) {
	if p.spanError != nil {
		return nil, p.spanError
	}

	if p.options == nil {
		return nil, toputils.TeeErrorf("timechartProcessor.Process: Timechart options is nil")
	}

	if inputIQR == nil {
		return p.extractTimechartResults()
	}

	numOfRecords := inputIQR.NumberOfRecords()
	qid := inputIQR.GetQID()

	if p.bucketKeyWorkingBuf == nil {
		p.bucketKeyWorkingBuf = make([]byte, len(p.options.groupByRequest.GroupByColumns)*utils.MAX_RECORD_SIZE)
	}

	if p.searchResults == nil {
		p.options.groupByRequest.BucketCount = int(utils.QUERY_MAX_BUCKETS)
		aggs := &structs.QueryAggregators{GroupByRequest: p.options.groupByRequest, TimeHistogram: p.options.timeBucket}
		searchResults, err := segresults.InitSearchResults(uint64(numOfRecords), aggs, structs.GroupByCmd, qid)
		if err != nil {
			return nil, toputils.TeeErrorf("qid=%v, timechartProcessor.Process: cannot initialize search results; err=%v", qid, err)
		}
		p.searchResults = searchResults
	}

	blkResults := p.searchResults.BlockResults

	byField := p.options.timeChartExpr.ByField
	hasLimitOption := p.options.timeChartExpr.LimitExpr != nil
	isTsCol := byField == config.GetTimeStampKey()
	groupByColValCount := make(map[string]int, 0)

	measureInfo, internalMops := blkResults.GetConvertedMeasureInfo()
	measureResults := make([]utils.CValueEnclosure, len(internalMops))

	for i := 0; i < numOfRecords; i++ {
		bucketKeyBufIdx := 0
		var groupByColVal string

		record := inputIQR.GetRecord(i)
		ts, err := record.GetTimestamp()
		if err != nil {
			p.errorData.readColumns["timestamp"] = err
			ts = uint64(time.Now().UnixMilli())
		}
		timePoint := aggregations.FindTimeRangeBucket(p.timeRangeBuckets, ts, p.options.timeBucket.IntervalMillis)

		copy(p.bucketKeyWorkingBuf[bucketKeyBufIdx:], utils.VALTYPE_ENC_UINT64[:])
		bucketKeyBufIdx += 1
		toputils.Uint64ToBytesLittleEndianInplace(timePoint, p.bucketKeyWorkingBuf[bucketKeyBufIdx:])
		bucketKeyBufIdx += 8

		if byField != "" && !isTsCol {
			value, err := record.ReadColumn(byField)
			if err != nil {
				p.errorData.readColumns[byField] = err
			} else {
				strVal, err := value.GetString()
				if err != nil {
					p.errorData.getStringErrors[byField] = err
					strVal = fmt.Sprintf("%v", value)
				}
				groupByColVal = strVal
			}

			if hasLimitOption {
				count, exists := groupByColValCount[groupByColVal]
				if exists {
					groupByColValCount[groupByColVal] = count + 1
				} else {
					groupByColValCount[groupByColVal] = 1
				}
			}
		}

		for cname, indices := range measureInfo {
			cValue, err := record.ReadColumn(cname)
			if err != nil {
				p.errorData.readColumns[cname] = err
				cValue = &utils.CValueEnclosure{CVal: nil, Dtype: utils.SS_DT_BACKFILL}
			}

			for _, idx := range indices {
				measureResults[idx] = *cValue
			}
		}
		blkResults.AddMeasureResultsToKey(p.bucketKeyWorkingBuf[:bucketKeyBufIdx], measureResults, groupByColVal, true, qid)
	}

	if len(byField) > 0 {
		if len(blkResults.GroupByAggregation.GroupByColValCnt) > 0 {
			aggregations.MergeMap(blkResults.GroupByAggregation.GroupByColValCnt, groupByColValCount)
		} else {
			blkResults.GroupByAggregation.GroupByColValCnt = groupByColValCount
		}
	}

	p.logErrorsAndWarnings(qid)

	return nil, nil
}

func (p *timechartProcessor) Rewind() {
	// Nothing to do
}

func (p *timechartProcessor) Cleanup() {
	p.bucketKeyWorkingBuf = nil
	p.timeRangeBuckets = nil
	p.errorData = nil
	p.searchResults = nil
}

func (p *timechartProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	if p.hasFinalResult {
		iqr, err := p.extractTimechartResults()
		if err != nil && err != io.EOF {
			return nil, false
		}
		return iqr, true
	}

	return nil, false
}

func (p *timechartProcessor) extractTimechartResults() (*iqr.IQR, error) {
	if p.searchResults == nil {
		return nil, io.EOF
	}

	iqr := iqr.NewIQR(p.qid)

	// load and convert the bucket results
	_ = p.searchResults.GetBucketResults()

	bucketHolderArr, measureFuncs, aggGroupByCols, _, bucketCount := p.searchResults.GetGroupyByBuckets(int(utils.QUERY_MAX_BUCKETS))

	err := iqr.CreateStatsResults(bucketHolderArr, measureFuncs, aggGroupByCols, bucketCount)
	if err != nil {
		return nil, toputils.TeeErrorf("qid=%v, timechartProcessor.extractTimechartResults: cannot create timechart results; err=%v", iqr.GetQID(), err)
	}

	p.hasFinalResult = true
	return iqr, io.EOF
}

func (p *timechartProcessor) logErrorsAndWarnings(qid uint64) {
	if len(p.errorData.readColumns) > 0 {
		log.Warnf("qid=%v, timechartProcessor.logErrorsAndWarnings: failed to read columns: %v", qid, p.errorData.readColumns)
	}

	if len(p.errorData.getStringErrors) > 0 {
		log.Errorf("qid=%v, timechartProcessor.logErrorsAndWarnings: failed to get string from CValue: %v", qid, p.errorData.getStringErrors)
	}

	allErrorsLen := len(p.searchResults.AllErrors)
	if allErrorsLen > 0 {
		size := allErrorsLen
		if allErrorsLen > utils.MAX_SIMILAR_ERRORS_TO_LOG {
			size = utils.MAX_SIMILAR_ERRORS_TO_LOG
		}
		log.Errorf("qid=%v, timechartProcessor.logErrorsAndWarnings: search results errors: %v", qid, p.searchResults.AllErrors[:size])
	}
}

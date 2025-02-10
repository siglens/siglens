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

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/results/segresults"
	"github.com/siglens/siglens/pkg/segment/search"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer/stats"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	bbp "github.com/valyala/bytebufferpool"
)

type ErrorData struct {
	readColumns           map[string]error    // columnName -> error. Tracks errors while reading the column through iqr.Record.ReadColumn
	cValueGetStringErr    map[string]error    // columnName -> error. Tracks errors while converting CValue to string
	notSupportedStatsType map[string]struct{} // columnName -> struct{}. Tracks unsupported stats types
}

type statsProcessor struct {
	options              *structs.StatsExpr
	bucketKeyWorkingBuf  []byte
	byteBuffer           *bbp.ByteBuffer
	searchResults        *segresults.SearchResults
	statsResults         *segresults.StatsResults
	qid                  uint64
	processorType        structs.QueryType
	errorData            *ErrorData
	hasFinalResult       bool
	setAsIqrStatsResults bool
}

func NewStatsProcessor(options *structs.StatsExpr) *statsProcessor {
	processor := &statsProcessor{
		options: options,
	}

	if options != nil {
		if options.GroupByRequest != nil {
			processor.processorType = structs.GroupByCmd
		} else if options.MeasureOperations != nil {
			processor.processorType = structs.SegmentStatsCmd
		}
	}

	return processor
}

func (p *statsProcessor) SetAsIqrStatsResults() {
	p.setAsIqrStatsResults = true
}

func (p *statsProcessor) Process(inputIQR *iqr.IQR) (*iqr.IQR, error) {
	// Initialize error data
	if p.errorData == nil {
		p.errorData = &ErrorData{
			readColumns:           make(map[string]error),
			cValueGetStringErr:    make(map[string]error),
			notSupportedStatsType: make(map[string]struct{}),
		}
	}

	// If inputIQR is nil, we are done with the input
	if inputIQR == nil {
		return p.extractFinalStatsResults()
	} else {
		p.qid = inputIQR.GetQID()
	}

	switch p.processorType {
	case structs.GroupByCmd:
		return p.processGroupByRequest(inputIQR)
	case structs.SegmentStatsCmd:
		return p.processMeasureOperations(inputIQR)
	default:
		return nil, toputils.TeeErrorf("qid=%v, statsProcessor.Process: no group by or measure operations specified", inputIQR.GetQID())
	}
}

func (p *statsProcessor) Rewind() {
	// nothing to do
}

func (p *statsProcessor) Cleanup() {
	p.logErrorsAndWarnings(p.qid)

	p.bucketKeyWorkingBuf = nil
	if p.byteBuffer != nil {
		bbp.Put(p.byteBuffer)
		p.byteBuffer = nil
	}

	p.searchResults = nil
	p.errorData = nil
}

func (p *statsProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	if p.hasFinalResult {
		iqr, err := p.extractFinalStatsResults()
		if err != nil && err != io.EOF {
			return nil, false
		}

		return iqr, true
	}

	return nil, false
}

func (p *statsProcessor) extractFinalStatsResults() (*iqr.IQR, error) {
	// If searchResults is nil, it means there is no data to process
	if p.searchResults == nil {
		return nil, io.EOF
	}

	iqr := iqr.NewIQR(p.qid)

	switch p.processorType {
	case structs.GroupByCmd:
		return p.extractGroupByResults(iqr)
	case structs.SegmentStatsCmd:
		return p.extractSegmentStatsResults(iqr)
	default:
		return nil, toputils.TeeErrorf("qid=%v, statsProcessor.extractFinalStatsResults: invalid processor type", p.qid)
	}
}

func (p *statsProcessor) processGroupByRequest(inputIQR *iqr.IQR) (*iqr.IQR, error) {
	numOfRecords := inputIQR.NumberOfRecords()
	qid := inputIQR.GetQID()

	if len(p.bucketKeyWorkingBuf) == 0 {
		p.bucketKeyWorkingBuf = make([]byte, len(p.options.GroupByRequest.GroupByColumns)*utils.MAX_RECORD_SIZE)
	}

	if p.searchResults == nil {
		p.options.GroupByRequest.BucketCount = int(utils.QUERY_MAX_BUCKETS)
		p.options.GroupByRequest.IsBucketKeySeparatedByDelim = true
		aggs := &structs.QueryAggregators{GroupByRequest: p.options.GroupByRequest}
		searchResults, err := segresults.InitSearchResults(uint64(numOfRecords), aggs, structs.GroupByCmd, qid)
		if err != nil {
			return nil, toputils.TeeErrorf("qid=%v, statsProcessor.processGroupByRequest: cannot initialize search results; err=%v", qid, err)
		}
		p.searchResults = searchResults
	}

	blkResults := p.searchResults.BlockResults

	measureInfo, internalMops := blkResults.GetConvertedMeasureInfo()
	measureResults := make([]utils.CValueEnclosure, len(internalMops))

	for i := 0; i < numOfRecords; i++ {
		record := inputIQR.GetRecord(i)

		// Bucket Key index
		bucketKeyBufIdx := 0

		for _, cname := range p.options.GroupByRequest.GroupByColumns {
			cValue, err := record.ReadColumn(cname)
			if err != nil {
				p.errorData.readColumns[cname] = err
				cValue = &utils.CValueEnclosure{CVal: nil, Dtype: utils.SS_DT_BACKFILL}
			}
			p.bucketKeyWorkingBuf, bucketKeyBufIdx = cValue.WriteToBytesWithType(p.bucketKeyWorkingBuf, bucketKeyBufIdx)
		}

		for cname, indices := range measureInfo {
			cValue, err := record.ReadColumn(cname)
			if err != nil {
				p.errorData.readColumns[cname] = err
				cValue = &utils.CValueEnclosure{CVal: utils.VALTYPE_ENC_BACKFILL, Dtype: utils.SS_DT_BACKFILL}
			}

			for _, idx := range indices {
				measureResults[idx] = *cValue
			}
		}
		blkResults.AddMeasureResultsToKey(p.bucketKeyWorkingBuf[:bucketKeyBufIdx], measureResults, "", false, qid)
	}

	return nil, nil
}

func (p *statsProcessor) extractGroupByResults(iqr *iqr.IQR) (*iqr.IQR, error) {
	// load and convert the bucket results

	if p.setAsIqrStatsResults {
		aggs := p.searchResults.GetAggs()
		groupByBuckets, TimeBuckets := p.searchResults.BlockResults.GroupByAggregation, p.searchResults.BlockResults.TimeAggregation
		err := iqr.SetIqrStatsResults(structs.GroupByCmd, nil, groupByBuckets, TimeBuckets, aggs)
		if err != nil {
			return nil, toputils.TeeErrorf("qid=%v, statsProcessor.extractGroupByResults: cannot set iqr stats results; err=%v", iqr.GetQID(), err)
		}
	} else {
		err := iqr.CreateGroupByStatsResults(p.searchResults)
		if err != nil {
			return nil, toputils.TeeErrorf("qid=%v, statsProcessor.extractGroupByResults: cannot create group by stats results; err=%v", iqr.GetQID(), err)
		}
	}

	p.hasFinalResult = true
	return iqr, io.EOF
}

func (p *statsProcessor) processMeasureOperations(inputIQR *iqr.IQR) (*iqr.IQR, error) {
	numOfRecords := uint64(inputIQR.NumberOfRecords())
	qid := inputIQR.GetQID()

	if p.searchResults == nil {
		searchResults, err := segresults.InitSearchResults(numOfRecords, &structs.QueryAggregators{MeasureOperations: p.options.MeasureOperations}, structs.SegmentStatsCmd, inputIQR.GetQID())
		if err != nil {
			return nil, toputils.TeeErrorf("qid=%v, statsProcessor.processMeasureOperations: cannot initialize search results; err=%v", qid, err)
		}
		p.searchResults = searchResults
		p.searchResults.InitSegmentStatsResults(p.options.MeasureOperations)
		p.statsResults = segresults.InitStatsResults()
	}

	if p.byteBuffer == nil {
		p.byteBuffer = bbp.Get()
	}

	segStatsMap := make(map[string]*structs.SegStats)

	measureColsMap, aggColUsage, valuesUsage, listUsage := search.GetSegStatsMeasureCols(p.options.MeasureOperations)
	timestampKey := config.GetTimeStampKey()
	if _, ok := aggColUsage[timestampKey]; !ok {
		delete(measureColsMap, timestampKey)
	}

	for colName := range measureColsMap {
		if colName == "*" {
			stats.AddSegStatsCount(segStatsMap, colName, numOfRecords)
			continue
		}

		values, err := inputIQR.ReadColumn(colName)
		if err != nil {
			p.errorData.readColumns[colName] = err
			continue
		}

		var GetnumVals []float64

		for _, item := range values {
			if item.IsNumeric() {
				switch {
				case item.IsFloat():
					GetnumVals = append(GetnumVals, item.CVal.(float64))
				default:
					intValue, err := item.GetIntValue()
					if err != nil {
						log.Errorf("qid=%v, statsProcessor.processMeasureOperations: error in getting integer value: %v", qid, err)
						continue
					}
					GetnumVals = append(GetnumVals, float64(intValue))
				}
			}
		}

		for _, measureOp := range p.options.MeasureOperations {
			switch measureOp.MeasureFunc {
			case utils.ExactPerc:
				if measureOp.MeasureCol == colName {
					result := stats.ExactPercentileCalculation(GetnumVals, 99.0)
					stats.AddSegStatsNums(segStatsMap, colName, utils.SS_FLOAT64, 0, 0, result,
						fmt.Sprintf("%v", result), p.byteBuffer, aggColUsage, valuesUsage[colName], listUsage[colName])
				}
			case utils.Perc:
				if measureOp.MeasureCol == colName {
					result := stats.PercentileCalculation(GetnumVals, 66.6)
					stats.AddSegStatsNums(segStatsMap, colName, utils.SS_FLOAT64, 0, 0, result,
						fmt.Sprintf("%v", result), p.byteBuffer, aggColUsage, valuesUsage[colName], listUsage[colName])
				}
			case utils.UpperPerc:
				if measureOp.MeasureCol == colName {
					result := stats.UpperPercentileCalculation(GetnumVals, 6.6)
					stats.AddSegStatsNums(segStatsMap, colName, utils.SS_FLOAT64, 0, 0, result,
						fmt.Sprintf("%v", result), p.byteBuffer, aggColUsage, valuesUsage[colName], listUsage[colName])
				}
			default:
			}
		}

		for i := range values {
			hasValuesFunc := valuesUsage[colName]
			hasListFunc := listUsage[colName]

			if values[i].IsString() {
				stats.AddSegStatsStr(segStatsMap, colName, values[i].CVal.(string), p.byteBuffer, aggColUsage, hasValuesFunc, hasListFunc)
			} else if values[i].IsNumeric() {
				stringVal, err := values[i].GetString()
				if err != nil {
					p.errorData.cValueGetStringErr[colName] = err
					stringVal = fmt.Sprintf("%v", values[i].CVal)
				}

				if values[i].IsFloat() {
					stats.AddSegStatsNums(segStatsMap, colName, utils.SS_FLOAT64, 0, 0, values[i].CVal.(float64),
						stringVal, p.byteBuffer, aggColUsage, hasValuesFunc, hasListFunc)
				} else {
					intVal, err := values[i].GetIntValue()
					if err != nil {
						// This should never happen
						log.Errorf("qid=%v, statsProcessor.processMeasureOperations: cannot get int value; err=%v", qid, err)
						intVal = 0
					}

					stats.AddSegStatsNums(segStatsMap, colName, utils.SS_INT64, intVal, 0, 0, stringVal, p.byteBuffer, aggColUsage, hasValuesFunc, hasListFunc)
				}
			} else {
				p.errorData.notSupportedStatsType[colName] = struct{}{}
				continue
			}
		}
	}

	p.statsResults.MergeSegStats(segStatsMap)

	return nil, nil
}

func (p *statsProcessor) extractSegmentStatsResults(iqr *iqr.IQR) (*iqr.IQR, error) {

	if p.statsResults == nil {
		return nil, io.EOF
	}

	segStatsMap := p.statsResults.GetSegStats()

	if p.setAsIqrStatsResults {
		aggs := p.searchResults.GetAggs()
		err := iqr.SetIqrStatsResults(structs.SegmentStatsCmd, segStatsMap, nil, nil, aggs)
		if err != nil {
			return nil, toputils.TeeErrorf("qid=%v, statsProcessor.extractSegmentStatsResults: cannot set iqr stats results; err=%v", iqr.GetQID(), err)
		}
	} else {
		err := iqr.CreateSegmentStatsResults(p.searchResults, segStatsMap, p.options.MeasureOperations)
		if err != nil {
			return nil, toputils.TeeErrorf("qid=%v, statsProcessor.extractSegmentStatsResults: cannot create segment stats results; err=%v", iqr.GetQID(), err)
		}
	}

	p.hasFinalResult = true
	return iqr, io.EOF
}

func (p *statsProcessor) logErrorsAndWarnings(qid uint64) {
	if p.errorData == nil {
		return
	}

	if len(p.errorData.readColumns) > 0 {
		log.Warnf("qid=%v, statsProcessor.logErrorsAndWarnings: failed to read columns: %v", qid, p.errorData.readColumns)
	}

	if len(p.errorData.cValueGetStringErr) > 0 {
		log.Errorf("qid=%v, statsProcessor.logErrorsAndWarnings: failed to get string from CValue: %v", qid, p.errorData.cValueGetStringErr)
	}

	if len(p.errorData.notSupportedStatsType) > 0 {
		log.Errorf("qid=%v, statsProcessor.logErrorsAndWarnings: not supported stats type: %v", qid, p.errorData.notSupportedStatsType)
	}

	if p.searchResults != nil {
		allErrorsLen := len(p.searchResults.AllErrors)
		if allErrorsLen > 0 {
			size := allErrorsLen
			if allErrorsLen > utils.MAX_SIMILAR_ERRORS_TO_LOG {
				size = utils.MAX_SIMILAR_ERRORS_TO_LOG
			}
			log.Errorf("qid=%v, statsProcessor.logErrorsAndWarnings: search results errors: %v", qid, p.searchResults.AllErrors[:size])
		}
	}
}

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
	options             *structs.StatsExpr
	bucketKeyWorkingBuf []byte
	byteBuffer          *bbp.ByteBuffer
	searchResults       *segresults.SearchResults
	qid                 uint64
	errorData           *ErrorData
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

	if p.options.GroupByRequest != nil {
		return p.processGroupByRequest(inputIQR)
	} else if p.options.MeasureOperations != nil {
		return p.processMeasureOperations(inputIQR)
	} else {
		return nil, toputils.TeeErrorf("qid=%v, statsProcessor.Process: no group by or measure operations specified", inputIQR.GetQID())
	}
}

func (p *statsProcessor) Rewind() {
	// nothing to do
}

func (p *statsProcessor) Cleanup() {
	p.bucketKeyWorkingBuf = nil
	if p.byteBuffer != nil {
		bbp.Put(p.byteBuffer)
		p.byteBuffer = nil
	}

	p.searchResults = nil
	p.errorData = nil
}

func (p *statsProcessor) processGroupByRequest(inputIQR *iqr.IQR) (*iqr.IQR, error) {
	if inputIQR == nil {
		if p.searchResults != nil {
			inputIQR = iqr.NewIQR(p.qid)
			return p.extractGroupByResults(inputIQR)
		}
		return nil, io.EOF
	}

	numOfRecords := inputIQR.NumberOfRecords()
	qid := inputIQR.GetQID()

	if p.bucketKeyWorkingBuf == nil {
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

		// Bucket Key
		bucketKeyBufIdx := 0

		for idx, cname := range p.options.GroupByRequest.GroupByColumns {
			if idx > 0 {
				copy(p.bucketKeyWorkingBuf[bucketKeyBufIdx:], utils.BYTE_TILDE)
				bucketKeyBufIdx += utils.BYTE_TILDE_LEN
			}

			cValue, err := record.ReadColumn(cname)
			if err != nil {
				p.errorData.readColumns[cname] = err
				copy(p.bucketKeyWorkingBuf[bucketKeyBufIdx:], utils.VALTYPE_ENC_BACKFILL)
				bucketKeyBufIdx += 1
			} else {
				bytesVal := cValue.AsBytes()
				copy(p.bucketKeyWorkingBuf[bucketKeyBufIdx:], bytesVal)
				bucketKeyBufIdx += len(bytesVal)
			}
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

	p.logErrorsAndWarnings(qid)

	return nil, nil
}

func (p *statsProcessor) extractGroupByResults(iqr *iqr.IQR) (*iqr.IQR, error) {
	if p.searchResults == nil {
		return iqr, io.EOF
	}

	// load and convert the bucket results
	_ = p.searchResults.GetBucketResults()

	bucketHolderArr, measureFuncs, aggGroupByCols, _, bucketCount := p.searchResults.GetGroupyByBuckets(int(utils.QUERY_MAX_BUCKETS))

	err := iqr.AppendStatsResults(bucketHolderArr, measureFuncs, aggGroupByCols, bucketCount)
	if err != nil {
		return nil, toputils.TeeErrorf("qid=%v, statsProcessor.extractGroupByResults: cannot append stats results; err=%v", iqr.GetQID(), err)
	}

	return iqr, io.EOF
}

func (p *statsProcessor) processMeasureOperations(inputIQR *iqr.IQR) (*iqr.IQR, error) {
	if inputIQR == nil {
		if p.searchResults != nil {
			inputIQR = iqr.NewIQR(p.qid)
			return p.extractSegmentStatsResults(inputIQR)
		}
		return nil, io.EOF
	}

	numOfRecords := uint64(inputIQR.NumberOfRecords())
	qid := inputIQR.GetQID()

	if p.searchResults == nil {
		searchResults, err := segresults.InitSearchResults(numOfRecords, &structs.QueryAggregators{MeasureOperations: p.options.MeasureOperations}, structs.SegmentStatsCmd, inputIQR.GetQID())
		if err != nil {
			return nil, toputils.TeeErrorf("qid=%v, statsProcessor.processMeasureOperations: cannot initialize search results; err=%v", qid, err)
		}
		p.searchResults = searchResults
		p.searchResults.InitSegmentStatsResults(p.options.MeasureOperations)
	}

	if p.byteBuffer == nil {
		p.byteBuffer = bbp.Get()
	}

	segStatsMap := make(map[string]*structs.SegStats)

	measureColsMap, aggColUsage, valuesUsage, listUsage := search.GetSegStatsMeasureCols(p.options.MeasureOperations)
	delete(measureColsMap, config.GetTimeStampKey())

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

	err := p.searchResults.UpdateSegmentStats(segStatsMap, p.options.MeasureOperations)
	if err != nil {
		log.Errorf("qid=%v, statsProcessor.processMeasureOperations: cannot update segment stats; err=%v", qid, err)
	}

	return nil, nil
}

func (p *statsProcessor) extractSegmentStatsResults(iqr *iqr.IQR) (*iqr.IQR, error) {
	if p.searchResults == nil {
		return iqr, io.EOF
	}

	aggMeasureRes, aggMeasureFunctions, groupByCols, _, bucketCount := p.searchResults.GetSegmentStatsResults(0, false)

	err := iqr.AppendStatsResults(aggMeasureRes, aggMeasureFunctions, groupByCols, bucketCount)
	if err != nil {
		return nil, toputils.TeeErrorf("qid=%v, statsProcessor.extractSegmentStatsResults: cannot append stats results; err=%v", iqr.GetQID(), err)
	}

	return iqr, io.EOF
}

func (p *statsProcessor) logErrorsAndWarnings(qid uint64) {
	if len(p.errorData.readColumns) > 0 {
		log.Warnf("qid=%v, statsProcessor.logErrorsAndWarnings: failed to read columns: %v", qid, p.errorData.readColumns)
	}

	if len(p.errorData.cValueGetStringErr) > 0 {
		log.Errorf("qid=%v, statsProcessor.logErrorsAndWarnings: failed to get string from CValue: %v", qid, p.errorData.cValueGetStringErr)
	}

	if len(p.errorData.notSupportedStatsType) > 0 {
		log.Errorf("qid=%v, statsProcessor.logErrorsAndWarnings: not supported stats type: %v", qid, p.errorData.notSupportedStatsType)
	}

	allErrorsLen := len(p.searchResults.AllErrors)
	if allErrorsLen > 0 {
		size := allErrorsLen
		if allErrorsLen > utils.MAX_SIMILAR_ERRORS_TO_LOG {
			size = utils.MAX_SIMILAR_ERRORS_TO_LOG
		}
		log.Errorf("qid=%v, statsProcessor.logErrorsAndWarnings: search results errors: %v", qid, p.searchResults.AllErrors[:size])
	}
}

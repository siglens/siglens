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
	readColumns           map[string]struct{}
	convertToCValueErr    map[string]interface{}
	cValueGetStringErr    map[string]interface{}
	notSupportedStatsType map[string]struct{}
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
			readColumns:           make(map[string]struct{}),
			convertToCValueErr:    make(map[string]interface{}),
			cValueGetStringErr:    make(map[string]interface{}),
			notSupportedStatsType: make(map[string]struct{}),
		}
	}

	if p.options.GroupByRequest != nil {
		return p.processGroupByRequest(inputIQR)
	} else if p.options.MeasureOperations != nil {
		return p.processMeasureOperations(inputIQR)
	} else {
		return nil, toputils.TeeErrorf("stats.Process: no group by or measure operations specified")
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
			return p.ExtractGroupByResults(inputIQR)
		}
		return nil, io.EOF
	}

	numOfRecords := inputIQR.NumberOfRecords()

	if p.bucketKeyWorkingBuf == nil {
		p.bucketKeyWorkingBuf = make([]byte, len(p.options.GroupByRequest.GroupByColumns)*utils.MAX_RECORD_SIZE)
	}

	if p.searchResults == nil {
		p.options.GroupByRequest.BucketCount = int(utils.QUERY_MAX_BUCKETS)
		p.options.GroupByRequest.IsBucketKeySeparatedByDelim = true
		searchResults, err := segresults.InitSearchResults(uint64(numOfRecords), &structs.QueryAggregators{GroupByRequest: p.options.GroupByRequest}, structs.GroupByCmd, inputIQR.GetQID())
		if err != nil {
			return nil, toputils.TeeErrorf("stats.Process: cannot initialize search results; err=%v", err)
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
				p.errorData.readColumns[cname] = struct{}{}
				copy(p.bucketKeyWorkingBuf[bucketKeyBufIdx:], utils.VALTYPE_ENC_BACKFILL)
				bucketKeyBufIdx += 1
			} else {
				bytesVal := cValue.ConvertToBytesValue()
				copy(p.bucketKeyWorkingBuf[bucketKeyBufIdx:], bytesVal)
				bucketKeyBufIdx += len(bytesVal)
			}
		}

		for cname, indices := range measureInfo {
			cValue, err := record.ReadColumn(cname)
			if err != nil {
				p.errorData.readColumns[cname] = struct{}{}
				cValue = &utils.CValueEnclosure{CVal: utils.VALTYPE_ENC_BACKFILL, Dtype: utils.SS_DT_BACKFILL}
			}

			for _, idx := range indices {
				measureResults[idx] = *cValue
			}
		}
		blkResults.AddMeasureResultsToKey(p.bucketKeyWorkingBuf[:bucketKeyBufIdx], measureResults, "", false, inputIQR.GetQID())
	}

	return nil, nil
}

func (p *statsProcessor) ExtractGroupByResults(iqr *iqr.IQR) (*iqr.IQR, error) {
	if p.searchResults == nil {
		return iqr, nil
	}

	// load and convert the bucket results
	_ = p.searchResults.GetBucketResults()

	bucketHolderArr, retMFuns, aggGroupByCols, _, _ := p.searchResults.GetGroupyByBuckets(int(utils.QUERY_MAX_BUCKETS))

	knownValues := make(map[string][]utils.CValueEnclosure)

	for _, aggGroupByCol := range aggGroupByCols {
		knownValues[aggGroupByCol] = make([]utils.CValueEnclosure, len(bucketHolderArr))
	}
	for _, retMFun := range retMFuns {
		knownValues[retMFun] = make([]utils.CValueEnclosure, len(bucketHolderArr))
	}

	for i, bucketHolder := range bucketHolderArr {
		for idx, aggGroupByCol := range aggGroupByCols {
			colValue := bucketHolder.GroupByValues[idx]
			err := knownValues[aggGroupByCol][i].ConvertValue(colValue)
			if err != nil {
				p.errorData.convertToCValueErr[fmt.Sprintf("%v_%v", i, aggGroupByCol)] = colValue
			}
		}

		for _, retMFun := range retMFuns {
			value := bucketHolder.MeasureVal[retMFun]
			err := knownValues[retMFun][i].ConvertValue(value)
			if err != nil {
				p.errorData.convertToCValueErr[fmt.Sprintf("%v_%v", i, retMFun)] = value
			}
		}
	}

	err := iqr.AppendKnownValues(knownValues)
	if err != nil {
		return nil, toputils.TeeErrorf("stats.Process: cannot append known values; err=%v", err)
	}

	p.logErrors()

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

	if p.searchResults == nil {
		searchResults, err := segresults.InitSearchResults(numOfRecords, &structs.QueryAggregators{MeasureOperations: p.options.MeasureOperations}, structs.SegmentStatsCmd, inputIQR.GetQID())
		if err != nil {
			return nil, toputils.TeeErrorf("stats.Process.processMeasureOperations: cannot initialize search results; err=%v", err)
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
			p.errorData.readColumns[colName] = struct{}{}
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
					stats.AddSegStatsNums(segStatsMap, colName, utils.SS_FLOAT64, 0, 0, values[i].CVal.(float64), stringVal, p.byteBuffer, aggColUsage, hasValuesFunc, hasListFunc)
				} else {
					intVal, err := values[i].GetIntValue()
					if err != nil {
						// This should never happen
						log.Errorf("stats.Process: cannot get int value; err=%v", err)
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
		log.Errorf("stats.Process: cannot update segment stats; err=%v", err)
		p.searchResults.AddError(err)
	}

	return nil, nil
}

func (p *statsProcessor) extractSegmentStatsResults(iqr *iqr.IQR) (*iqr.IQR, error) {
	if p.searchResults == nil {
		return iqr, nil
	}

	aggMeasureRes, aggMeasureFunctions, _, _, _ := p.searchResults.GetSegmentStatsResults(0)

	knownValues := make(map[string][]utils.CValueEnclosure)

	for _, measureFunction := range aggMeasureFunctions {
		knownValues[measureFunction] = make([]utils.CValueEnclosure, len(aggMeasureRes))
	}

	for i, measureRes := range aggMeasureRes {
		for _, measureFunction := range aggMeasureFunctions {
			value := measureRes.MeasureVal[measureFunction]
			err := knownValues[measureFunction][i].ConvertValue(value)
			if err != nil {
				p.errorData.convertToCValueErr[fmt.Sprintf("%v_%v", i, measureFunction)] = value
			}
		}
	}

	err := iqr.AppendKnownValues(knownValues)
	if err != nil {
		return nil, toputils.TeeErrorf("stats.Process.extractSegmentStatsResults: cannot append known values; err=%v", err)
	}

	p.logErrors()

	return iqr, io.EOF
}

func (p *statsProcessor) logErrors() {
	if len(p.errorData.readColumns) > 0 {
		log.Errorf("stats.Process: failed to read columns: %v", p.errorData.readColumns)
	}

	if len(p.errorData.convertToCValueErr) > 0 {
		log.Errorf("stats.Process: failed to convert to CValue: %v", p.errorData.convertToCValueErr)
	}

	if len(p.errorData.cValueGetStringErr) > 0 {
		log.Errorf("stats.Process: failed to get string from CValue: %v", p.errorData.cValueGetStringErr)
	}

	if len(p.errorData.notSupportedStatsType) > 0 {
		log.Errorf("stats.Process: not supported stats type: %v", p.errorData.notSupportedStatsType)
	}

	if len(p.searchResults.AllErrors) > 0 {
		log.Errorf("stats.Process: search results errors: %v", p.searchResults.AllErrors)
	}
}

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

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/results/segresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type ErrorData struct {
	// columnName -> error. Tracks errors while reading the column through iqr.Record.ReadColumn()
	// This is logged as a warning. As the column may not exist in one segment but exist in another.
	readColumns map[string]error
}

type statsProcessor struct {
	options             *structs.StatsExpr
	bucketKeyWorkingBuf []byte
	errorData           *ErrorData
	searchResults       *segresults.SearchResults
	qid                 uint64
}

func (p *statsProcessor) Process(inputIQR *iqr.IQR) (*iqr.IQR, error) {
	// Initialize error data
	if p.errorData == nil {
		p.errorData = &ErrorData{
			readColumns: make(map[string]error),
		}
	}

	if p.options.GroupByRequest != nil {
		return p.processGroupByRequest(inputIQR)
	} else if p.options.MeasureOperations != nil {
		// TODO: Implement measure operations
		return nil, toputils.TeeErrorf("stats.Process: measure operations not implemented")
	} else {
		return nil, toputils.TeeErrorf("qid=%v, stats.Process: no group by or measure operations specified", inputIQR.GetQID())
	}
}

func (p *statsProcessor) Rewind() {
	// nothing to do
}

func (p *statsProcessor) Cleanup() {
	if p.searchResults != nil {
		p.searchResults = nil
	}
	p.bucketKeyWorkingBuf = nil
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
			return nil, toputils.TeeErrorf("qid=%v, stats.Process.processGroupByRequest: cannot initialize search results; err=%v", qid, err)
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
		return iqr, nil
	}

	// load and convert the bucket results
	_ = p.searchResults.GetBucketResults()

	bucketHolderArr, measureFuncs, aggGroupByCols, _, bucketCount := p.searchResults.GetGroupyByBuckets(int(utils.QUERY_MAX_BUCKETS))

	err := iqr.AppendStatsResults(bucketHolderArr, measureFuncs, aggGroupByCols, bucketCount)
	if err != nil {
		return nil, toputils.TeeErrorf("qid=%v, stats.Process.extractGroupByResults: cannot append stats results; err=%v", iqr.GetQID(), err)
	}

	return iqr, io.EOF
}

func (p *statsProcessor) logErrorsAndWarnings(qid uint64) {
	if len(p.errorData.readColumns) > 0 {
		log.Warnf("qid=%v, stats.Process: failed to read columns: %v", qid, p.errorData.readColumns)
	}
}

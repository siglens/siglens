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

package metrics

import (
	"bytes"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/results/mresults"
	tsidtracker "github.com/siglens/siglens/pkg/segment/results/mresults/tsid"
	"github.com/siglens/siglens/pkg/segment/structs"
	log "github.com/sirupsen/logrus"
)

func SearchUnrotatedMetricsBlock(mQuery *structs.MetricsQuery, segTsidInfo *tsidtracker.AllMatchedTSIDs, searchReq *structs.MetricsSearchRequest, res *mresults.MetricsResult,
	bytesBuffer *bytes.Buffer, timeRange *dtu.MetricsTimeRange, qid uint64, querySummary *summary.QuerySummary) bool {

	searchInRotated := false

	if searchReq.QueryType != structs.UNROTATED_METRICS_SEARCH {
		log.Errorf("qid=%d, SearchUnrotatedMetricsBlock: invalid query type %v", qid, searchReq.QueryType)
		return searchInRotated
	}

	mSegment, err := getMetricSegmentFromMid(searchReq.Mid, mQuery.OrgId)
	if err != nil {
		log.Errorf("qid=%d, SearchUnrotatedMetricsBlock: failed to get metric segment for mid=%s, err=%v", qid, searchReq.Mid, err)
		return searchInRotated
	}

	mSegment.rwLock.RLock()
	defer mSegment.rwLock.RUnlock()

	// The length of the unrotated block is 1
	for blkNum, shouldSearch := range searchReq.UnrotatedBlkToSearch {
		if blkNum != mSegment.mBlock.mBlockSummary.Blknum {
			return shouldSearch
		}
	}

	if !timeRange.CheckRangeOverLap(mSegment.mBlock.mBlockSummary.LowTs, mSegment.mBlock.mBlockSummary.HighTs) {
		return searchInRotated
	}

	localRes := mresults.InitMetricResults(mQuery, qid)

	for tsid, tsGroupId := range segTsidInfo.GetAllTSIDs() {
		found, tsitr, err := mSegment.mBlock.getUnrotatedBlockTimeSeriesIterator(tsid, bytesBuffer)
		if err != nil {
			log.Errorf("qid=%d, SearchUnrotatedMetricsBlock: failed to get time series iterator for tsid=%d, err=%v", qid, tsid, err)
			continue
		}

		if !found {
			log.Debugf("qid=%d, SearchUnrotatedMetricsBlock: tsid=%d not found in segment", qid, tsid)
			continue
		}

		// Search the time series for the datapoints
		series := mresults.InitSeriesHolder(mQuery, tsGroupId)
		for tsitr.Next() {
			ts, dp := tsitr.At()
			if !timeRange.CheckInRange(ts) {
				continue
			}
			series.AddEntry(ts, dp)
		}
		err = tsitr.Err()
		if err != nil {
			log.Errorf("qid=%v, SearchUnrotatedMetricsBlock: iterator failed %v", qid, err)
			res.AddError(err)
		}
		if series.GetIdx() > 0 {
			localRes.AddSeries(series, tsid, tsGroupId)
		}

		// reset the buffer
		bytesBuffer.Reset()
	}

	err = res.Merge(localRes)
	if err != nil {
		res.AddError(err)
		log.Errorf("qid=%v, SearchUnrotatedMetricsBlock: Failed to merge local results to global results!", qid)
	}

	return searchInRotated
}

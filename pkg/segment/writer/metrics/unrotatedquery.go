// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

//

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
	bytesBuffer *bytes.Buffer, timeRange *dtu.MetricsTimeRange, qid uint64, querySummary *summary.QuerySummary) {

	if searchReq.QueryType != structs.UNROTATED_METRICS_SEARCH {
		log.Errorf("qid=%d, SearchUnrotatedMetricsBlock: invalid query type %v", qid, searchReq.QueryType)
		return
	}

	mSegment, err := getUnrotatedMetricSegment(searchReq.Mid, mQuery.OrgId)
	if err != nil {
		log.Errorf("qid=%d, SearchUnrotatedMetricsBlock: failed to get metric segment for mid=%s, err=%v", qid, searchReq.Mid, err)
		return
	}

	mSegment.rwLock.RLock()
	defer mSegment.rwLock.RUnlock()

	_, ok := searchReq.UnrotatedBlkToSearch[mSegment.mBlock.mBlockSummary.Blknum]
	if !ok {
		// Since the current unrotated block is not in the search request unrotated block list,
		// it is assumed that the block is rotated and the search should be done in the rotated block.
		// So, check the search request unrotated block list and add the block to rotated block list,
		// if the block should be searched.
		for blkNum, shouldSearch := range searchReq.UnrotatedBlkToSearch {
			if shouldSearch {
				searchReq.BlocksToSearch[blkNum] = true
			}
		}
	}

	if !timeRange.CheckRangeOverLap(mSegment.mBlock.mBlockSummary.LowTs, mSegment.mBlock.mBlockSummary.HighTs) {
		return
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
}

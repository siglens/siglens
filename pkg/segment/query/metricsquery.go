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

package query

import (
	"time"

	"github.com/cespare/xxhash"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/query/metadata"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/reader/metrics/tagstree"
	"github.com/siglens/siglens/pkg/segment/results/mresults"
	"github.com/siglens/siglens/pkg/segment/search"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer/metrics"
	log "github.com/sirupsen/logrus"
)

func ApplyMetricsQuery(mQuery *structs.MetricsQuery, timeRange *dtu.MetricsTimeRange, qid uint64, querySummary *summary.QuerySummary) *mresults.MetricsResult {

	// init metrics results structs
	mRes := mresults.InitMetricResults(mQuery, qid)

	// get all metrics segments that pass the initial time + metric name filter
	mSegments, err := metadata.GetMetricsSegmentRequests(mQuery.MetricName, timeRange, querySummary, mQuery.OrgId)
	if err != nil {
		log.Errorf("ApplyMetricsQuery: failed to get rotated metric segments: %v", err)
		return &mresults.MetricsResult{
			ErrList: []error{err},
		}
	}

	unrotatedMSegments, err := metrics.GetUnrotatedMetricsSegmentRequests(mQuery.MetricName, timeRange, querySummary, mQuery.OrgId)
	if err != nil {
		log.Errorf("ApplyMetricsQuery: failed to get unrotated metric segments: %v", err)
		return &mresults.MetricsResult{
			ErrList: []error{err},
		}
	}

	mSegments = mergeRotatedAndUnrotatedRequests(unrotatedMSegments, mSegments)
	allTagKeys := make(map[string]bool)

	for _, allMSearchReqs := range mSegments {
		for _, mSeg := range allMSearchReqs {
			for tk := range mSeg.AllTagKeys {
				allTagKeys[tk] = true
			}
		}
	}

	if mQuery.SelectAllSeries {
		for _, v := range mQuery.TagsFilters {
			delete(allTagKeys, v.TagKey)
		}
		for tkey, present := range allTagKeys {
			if present {
				mQuery.TagsFilters = append(mQuery.TagsFilters, &structs.TagsFilter{
					TagKey:          tkey,
					RawTagValue:     tagstree.STAR,
					HashTagValue:    xxhash.Sum64String(tagstree.STAR),
					LogicalOperator: utils.And,
					TagOperator:     utils.Equal,
				})
			}
		}
	}
	mQuery.ReorderTagFilters()

	// iterate through all metrics segments, applying search as needed
	applyMetricsOperatorOnSegments(mQuery, mSegments, mRes, timeRange, qid, querySummary)
	parallelism := int(config.GetParallelism()) * 2
	errors := mRes.DownsampleResults(mQuery.Downsampler, parallelism)
	if errors != nil {
		for _, err := range errors {
			mRes.AddError(err)
		}

		return mRes
	}

	errors = mRes.AggregateResults(parallelism)
	if errors != nil {
		for _, err := range errors {
			mRes.AddError(err)
		}

		return mRes
	}

	err = mRes.ApplyRangeFunctionsToResults(parallelism, mQuery.Aggregator.RangeFunction)
	if err != nil {
		mRes.AddError(err)
	}

	return mRes
}

func mergeRotatedAndUnrotatedRequests(unrotatedMSegments map[string][]*structs.MetricsSearchRequest, mSegments map[string][]*structs.MetricsSearchRequest) map[string][]*structs.MetricsSearchRequest {
	for k, v := range unrotatedMSegments {
		if _, ok := mSegments[k]; ok {
			mSegments[k] = append(mSegments[k], v...)
		} else {
			mSegments[k] = v
		}
	}
	return mSegments
}

func applyMetricsOperatorOnSegments(mQuery *structs.MetricsQuery, allSearchReqests map[string][]*structs.MetricsSearchRequest,
	mRes *mresults.MetricsResult, timeRange *dtu.MetricsTimeRange, qid uint64, querySummary *summary.QuerySummary) {
	// for each metrics segment, apply a single metrics segment search
	// var tsidInfo *tsidtracker.AllMatchedTSIDs

	for baseDir, allMSearchReqs := range allSearchReqests {
		attr, err := tagstree.InitAllTagsTreeReader(baseDir)
		if err != nil {
			mRes.AddError(err)
			continue
		}
		sTime := time.Now()

		tsidInfo, err := attr.FindTSIDS(mQuery)
		querySummary.UpdateTimeSearchingTagsTrees(time.Since(sTime))
		querySummary.IncrementNumTagsTreesSearched(1)
		if err != nil {
			mRes.AddError(err)
			continue
		}
		querySummary.IncrementNumTSIDsMatched(uint64(tsidInfo.GetNumMatchedTSIDs()))
		for _, mSeg := range allMSearchReqs {
			search.RawSearchMetricsSegment(mQuery, tsidInfo, mSeg, mRes, timeRange, qid, querySummary)
		}
	}
}

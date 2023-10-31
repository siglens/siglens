/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

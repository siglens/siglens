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
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	"github.com/prometheus/prometheus/promql/parser"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	rutils "github.com/siglens/siglens/pkg/readerUtils"
	segmetadata "github.com/siglens/siglens/pkg/segment/metadata"
	"github.com/siglens/siglens/pkg/segment/query/metricsevaluator"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/reader/metrics/series"
	"github.com/siglens/siglens/pkg/segment/reader/metrics/tagstree"
	"github.com/siglens/siglens/pkg/segment/results/mresults"
	tsidtracker "github.com/siglens/siglens/pkg/segment/results/mresults/tsid"
	"github.com/siglens/siglens/pkg/segment/search"
	"github.com/siglens/siglens/pkg/segment/structs"
	tutils "github.com/siglens/siglens/pkg/segment/tracing/utils"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer/metrics"
	"github.com/siglens/siglens/pkg/usageStats"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type TagKeySeriesCount struct {
	Key       string `json:"key"`
	NumSeries uint64 `json:"numSeries"`
}

func getAllRequestsWithinTimeRange(timeRange *dtu.MetricsTimeRange, myid int64, querySummary *summary.QuerySummary) (map[string][]*structs.MetricsSearchRequest, error) {
	rotatedMetricRequests, err := segmetadata.GetMetricsSegmentRequests(timeRange, querySummary, myid)
	if err != nil {
		err = fmt.Errorf("getAllRequestsWithinTimeRange: failed to get rotated metric segments for time range %+v; err=%v", timeRange, err)
		log.Errorf(err.Error())
		return nil, err
	}

	unrotatedMetricRequests, err := metrics.GetUnrotatedMetricsSegmentRequests(timeRange, querySummary, myid)
	if err != nil {
		err = fmt.Errorf("getAllRequestsWithinTimeRange: failed to get unrotated metric segments for time range %+v; err=%v", timeRange, err)
		log.Errorf(err.Error())
		return nil, err
	}

	allSearchRequests := mergeMetricSearchRequests(unrotatedMetricRequests, rotatedMetricRequests)

	return allSearchRequests, nil
}

func GetAllTagsTreesWithinTimeRange(timeRange *dtu.MetricsTimeRange, myid int64, querySummary *summary.QuerySummary) ([]*tagstree.AllTagTreeReaders, error) {
	allSearchRequests, err := getAllRequestsWithinTimeRange(timeRange, myid, querySummary)
	if err != nil {
		err = fmt.Errorf("GetAllTagsTreesWithinTimeRange: failed to get all metric requests within time range %+v; err=%v", timeRange, err)
		log.Errorf(err.Error())
		return nil, err
	}

	// Extract the tags trees from the metric requests.
	tagsTrees := make([]*tagstree.AllTagTreeReaders, 0)
	for tthBaseDir := range allSearchRequests {
		allTagsTreeReader, err := tagstree.InitAllTagsTreeReader(tthBaseDir)
		if err != nil {
			err = fmt.Errorf("GetAllTagsTreesWithinTimeRange: failed to get tags tree reader for tthBaseDir: %s; err=%v", tthBaseDir, err)
			log.Errorf(err.Error())
			return nil, err
		}

		tagsTrees = append(tagsTrees, allTagsTreeReader)
	}

	return tagsTrees, nil
}

func ApplyMetricsQuery(mQuery *structs.MetricsQuery, timeRange *dtu.MetricsTimeRange, qid uint64, querySummary *summary.QuerySummary) *mresults.MetricsResult {

	// init metrics results structs
	mRes := mresults.InitMetricResults(mQuery, qid)

	finalTimeRange := &dtu.MetricsTimeRange{
		StartEpochSec: timeRange.StartEpochSec,
		EndEpochSec:   timeRange.EndEpochSec,
	}

	// If LookBackToInclude is set, then we need to adjust the StartEpochSec
	// to include the lookback time.
	if mQuery.LookBackToInclude > 0 {
		finalTimeRange.StartEpochSec = timeRange.StartEpochSec - uint32(mQuery.LookBackToInclude)
	}

	// todo: switch this to use rotated segs only
	mSegments, err := getAllRequestsWithinTimeRange(finalTimeRange, mQuery.OrgId, querySummary)
	if err != nil {
		log.Errorf("ApplyMetricsQuery: failed to get all metric segments within time range %+v; err=%v", finalTimeRange, err)
		return &mresults.MetricsResult{
			ErrList: []error{err},
		}
	}

	allTagKeys := make(map[string]bool)

	for _, allMSearchReqs := range mSegments {
		for _, mSeg := range allMSearchReqs {
			for tk := range mSeg.AllTagKeys {
				allTagKeys[tk] = true
			}
		}
	}

	if mQuery.SelectAllSeries {
		filteredTags := make([]*structs.TagsFilter, 0, len(allTagKeys))
		for _, v := range mQuery.TagsFilters {
			delete(allTagKeys, v.TagKey)
			if v.IgnoreTag && !v.NotInitialGroup {
				continue
			}

			filteredTags = append(filteredTags, v)
		}

		mQuery.TagsFilters = filteredTags

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

	if mQuery.SubsequentAggs != nil {
		mQuery.FirstAggregator = *mQuery.SubsequentAggs.AggregatorBlock // The first Aggregation in the MQueryAggs is always a AggregatorBlock
		mQuery.SubsequentAggs = mQuery.SubsequentAggs.Next
	}

	if mQuery.TagValueSearchOnly {
		applyTagValuesSearchOnlyOnSegments(mQuery, mSegments, mRes, timeRange, qid, querySummary)
		applyTagValuesSearchOnlyOnUnrotatedSegs(mQuery, mRes, timeRange, qid, querySummary)
		return mRes
	}

	// iterate through all metrics segments, applying search as needed
	// use finalTimeRange to get the series and data points including the lookback time
	applyMetricsOperatorOnSegments(mQuery, mSegments, mRes, finalTimeRange, qid, querySummary)
	if mQuery.ExitAfterTagsSearch {
		return mRes
	}

	if mQuery.IsQueryCancelled() {
		mRes.AddError(fmt.Errorf("query cancelled"))
		return mRes
	}

	parallelism := int(config.GetParallelism()) * 2
	errors := mRes.DownsampleResults(mQuery.Downsampler, parallelism)
	if errors != nil {
		for _, err := range errors {
			mRes.AddError(err)
		}

		return mRes
	}

	if mQuery.IsQueryCancelled() {
		mRes.AddError(fmt.Errorf("query cancelled"))
		return mRes
	}

	mRes.MetricName = mQuery.MetricName

	errors = mRes.AggregateResults(parallelism, mQuery.FirstAggregator)
	if errors != nil {
		for _, err := range errors {
			mRes.AddError(err)
		}

		return mRes
	}

	ProcessMQueryAggsChain(mQuery, timeRange, mRes, qid)

	return mRes
}

func ProcessMQueryAggsChain(mQuery *structs.MetricsQuery, timeRange *dtu.MetricsTimeRange, mRes *mresults.MetricsResult, qid uint64) {
	parallelism := int(config.GetParallelism()) * 2

	for mQuery.SubsequentAggs != nil {
		if mQuery.IsQueryCancelled() {
			mRes.AddError(fmt.Errorf("query cancelled"))
			return
		}

		if mQuery.SubsequentAggs.AggBlockType == structs.FunctionBlock {
			mQuery.Function = *mQuery.SubsequentAggs.FunctionBlock
			errors := mRes.ApplyFunctionsToResults(parallelism, mQuery.Function, timeRange)
			if errors != nil {
				for _, err := range errors {
					mRes.AddError(err)
				}

				return
			}
		} else if mQuery.SubsequentAggs.AggBlockType == structs.AggregatorBlock {
			mQuery.FirstAggregator = *mQuery.SubsequentAggs.AggregatorBlock
			errors := mRes.ApplyAggregationToResults(parallelism, mQuery.FirstAggregator)
			if errors != nil {
				for _, err := range errors {
					mRes.AddError(err)
				}

				return
			}
		} else {
			log.Errorf("ApplyMetricsQuery: Invalid AggBlockType: %v", mQuery.SubsequentAggs.AggBlockType)
			mRes.AddError(fmt.Errorf("invalid AggBlockType: %v", mQuery.SubsequentAggs.AggBlockType))
			return
		}

		mQuery.SubsequentAggs = mQuery.SubsequentAggs.Next
	}
}

func mergeMetricSearchRequests(unrotatedMSegments map[string][]*structs.MetricsSearchRequest, mSegments map[string][]*structs.MetricsSearchRequest) map[string][]*structs.MetricsSearchRequest {
	for k, v := range unrotatedMSegments {
		if _, ok := mSegments[k]; ok {
			mSegments[k] = append(mSegments[k], v...)
		} else {
			mSegments[k] = v
		}
	}
	return mSegments
}

func GetAllMetricNamesOverTheTimeRange(timeRange *dtu.MetricsTimeRange, orgid int64) ([]string, error) {
	mSgementsMeta := segmetadata.GetMetricSegmentsOverTheTimeRange(timeRange, orgid)

	unrotatedMSegments, err := metrics.GetUnrotatedMetricSegmentsOverTheTimeRange(timeRange, orgid)
	if err != nil {
		log.Errorf("GetAllMetricNamesOverTheTimeRange: failed to get unrotated metric segments: %v", err)
		unrotatedMSegments = make([]*metrics.MetricsSegment, 0)
	}

	if len(mSgementsMeta) == 0 && len(unrotatedMSegments) == 0 {
		return make([]string, 0), nil
	}

	resultContainerLock := &sync.RWMutex{}
	resultContainer := make(map[string]bool)
	unrotatedResultContainer := make(map[string]bool)
	wg := &sync.WaitGroup{}
	parallelism := int(config.GetParallelism())
	parallelismCounter := 0
	var gErr error

	parallelismCounter++
	wg.Add(1)
	go func(unrotatedMSeg []*metrics.MetricsSegment) {
		defer wg.Done()
		for _, mSeg := range unrotatedMSeg {
			mSeg.LoadMetricNamesIntoMap(unrotatedResultContainer)
		}
	}(unrotatedMSegments)

	for _, mSegMeta := range mSgementsMeta {
		wg.Add(1)
		go func(msm *structs.MetricsMeta) {
			defer wg.Done()

			mNamesMap, err := series.GetAllMetricNames(msm.MSegmentDir)
			if err != nil {
				gErr = err
				return
			}

			for mName := range mNamesMap {
				resultContainerLock.RLock()
				_, ok := resultContainer[mName]
				resultContainerLock.RUnlock()
				if !ok {
					resultContainerLock.Lock()
					resultContainer[mName] = true
					resultContainerLock.Unlock()
				}
			}

		}(mSegMeta)

		if parallelismCounter%parallelism == 0 {
			wg.Wait()
		}
		parallelismCounter++
	}
	wg.Wait()

	if gErr != nil {
		return nil, gErr
	}

	for mName := range unrotatedResultContainer {
		if mName == "" {
			continue
		}
		_, ok := resultContainer[mName]
		if !ok {
			resultContainer[mName] = true
		}
	}

	result := make([]string, 0, len(resultContainer))
	for mName := range resultContainer {
		result = append(result, mName)
	}

	return result, gErr
}

func applyTagValuesSearchOnlyOnUnrotatedSegs(mQuery *structs.MetricsQuery,
	mRes *mresults.MetricsResult, timeRange *dtu.MetricsTimeRange, qid uint64,
	querySummary *summary.QuerySummary) {

	if mRes.TagValues == nil {
		mRes.TagValues = make(map[string]map[string]struct{})
	}

	err := metrics.FindTagValuesUnrotated(timeRange, mQuery, mRes.TagValues)
	if err != nil {
		log.Errorf("applyTagValuesSearchOnlyOnUnrotatedSegs: failed to find tag vals for unrotated, err: %v", err)
	}
}

func applyTagValuesSearchOnlyOnSegments(mQuery *structs.MetricsQuery, allSearchRequests map[string][]*structs.MetricsSearchRequest,
	mRes *mresults.MetricsResult, timeRange *dtu.MetricsTimeRange, qid uint64, querySummary *summary.QuerySummary) {

	mRes.TagValues = make(map[string]map[string]struct{})

	for tthBaseDir := range allSearchRequests {
		attr, err := tagstree.InitAllTagsTreeReader(tthBaseDir)
		if err != nil {
			mRes.AddError(err)
			continue
		}
		sTime := time.Now()
		err = attr.FindTagValuesOnly(mQuery, mRes.TagValues)

		querySummary.UpdateTimeSearchingTagsTrees(time.Since(sTime))
		querySummary.IncrementNumTagsTreesSearched(1)

		if err != nil {
			mRes.AddError(err)
			continue
		}
	}
}

func applyMetricsOperatorOnSegments(mQuery *structs.MetricsQuery, allSearchReqests map[string][]*structs.MetricsSearchRequest,
	mRes *mresults.MetricsResult, timeRange *dtu.MetricsTimeRange, qid uint64, querySummary *summary.QuerySummary) {
	// for each metrics segment, apply a single metrics segment search
	// var tsidInfo *tsidtracker.AllMatchedTSIDs

	var err error
	// allocate 50 KB buffer
	bytesBuffer := bytes.NewBuffer(make([]byte, 0, 50*1024))

	// The tthBaseDir is the base directory of the tags tree holder but not the segment directory.
	// The Segement base Directory can be taken from the first MetricSearchRequest.
	// tthBaseDir can span for 24 hours, and it can contain multiple mSegs, some of these mSegs
	// can be rotated and unrotated
	for tthBaseDir, allMSearchReqs := range allSearchReqests {
		if mQuery.IsQueryCancelled() {
			return
		}

		var metricNames []string
		if mQuery.IsRegexOnMetricName() {
			// Regex Search on Metric Name. We need to get all the Metric Names in this Segment.
			if len(allMSearchReqs) == 0 {
				mRes.AddError(fmt.Errorf("no metric search request found for the tags tree holder tthBaseDir: %s", tthBaseDir))
				continue
			}

			metricNames, err = getRegexMatchedMetricNames(allMSearchReqs[0], mQuery.MetricNameRegexPattern, mQuery.MetricOperator)
			if err != nil {
				log.Errorf("qid=%d, applyMetricsOperatorOnSegments: Error getting regex matched metric names. Regex Pattern: %v, Error=%v", qid, mQuery.MetricNameRegexPattern, err)
				continue
			}
		} else {
			metricNames = []string{mQuery.MetricName}
		}

		if len(metricNames) == 0 {
			continue
		}

		sTime := time.Now()

		allMatchedTsids, err := tsidtracker.InitTSIDTracker(len(mQuery.TagsFilters))
		if err != nil {
			mRes.AddError(err)
			continue
		}

		err = tagstree.SearchAndInsertTSIDs(mQuery, allMatchedTsids, metricNames, tthBaseDir,
			allMSearchReqs[0], qid)
		if err != nil {
			mRes.AddError(err)
			continue
		}

		querySummary.UpdateTimeSearchingTagsTrees(time.Since(sTime))
		querySummary.IncrementNumTagsTreesSearched(1)

		querySummary.IncrementNumTSIDsMatched(uint64(allMatchedTsids.GetNumMatchedTSIDs()))
		if mQuery.ExitAfterTagsSearch {
			mRes.AddAllSeriesTagsOnlyMap(allMatchedTsids.GetTSIDInfoMap())
			continue
		}

		for _, mSeg := range allMSearchReqs {
			if mQuery.IsQueryCancelled() {
				return
			}

			search.RawSearchMetricsSegment(mQuery, allMatchedTsids, mSeg, mRes, bytesBuffer, timeRange, qid, querySummary)
		}
	}
}

func getRegexMatchedMetricNames(mSegSearchReq *structs.MetricsSearchRequest, regexPattern string, operator utils.TagOperator) ([]string, error) {
	var mNamesMap map[string]bool
	var err error

	if len(mSegSearchReq.UnrotatedMetricNames) > 0 {
		mNamesMap = mSegSearchReq.UnrotatedMetricNames
	} else {
		mNamesMap, err = series.GetAllMetricNames(mSegSearchReq.MetricsKeyBaseDir)
		if err != nil {
			return nil, err
		}
	}

	metricNames := make([]string, 0)
	for mName := range mNamesMap {
		regexpMatched, err := regexp.MatchString(regexPattern, mName)
		if err != nil {
			return nil, err
		}

		appendToList := (regexpMatched && operator == utils.Regex) || (!regexpMatched && operator == utils.NegRegex)

		if appendToList {
			metricNames = append(metricNames, mName)
		}
	}

	return metricNames, nil
}

func GetSeriesCardinalityOverTimeRange(timeRange *dtu.MetricsTimeRange, myid int64) (uint64, error) {
	diff := uint64(timeRange.EndEpochSec - timeRange.StartEpochSec)
	if diff <= 0 {
		log.Errorf("GetSeriesCardinalityOverTimeRange: invalid time range, diff: %+v, start: %v, end: %v",
			diff, timeRange.EndEpochSec, timeRange.StartEpochSec)
		return 0, errors.New("Invalid time range")
	}

	pastXhours := max(1, diff/3600)
	asCounts, err := usageStats.GetActiveSeriesCounts(pastXhours, myid)
	if err != nil {
		log.Errorf("GetSeriesCardinalityOverTimeRange: failed to get cardinality, err: %v", err)
		return 0, err
	}

	if len(asCounts) <= 0 {
		// the older versions of usageStats would return 0 buckets
		return 0, nil
	}

	p95 := uint64(tutils.FindPercentileData(asCounts, 95))

	return p95, nil
}

func GetRotatedTagsTreesWithinTimeRange(timeRange *dtu.MetricsTimeRange, myid int64, querySummary *summary.QuerySummary) ([]*tagstree.AllTagTreeReaders, error) {

	allSearchRequests, err := segmetadata.GetMetricsSegmentRequests(timeRange, querySummary, myid)
	if err != nil {
		err = fmt.Errorf("GetRotatedTagsTreesWithinTimeRange: failed to get rotated metric segments for time range %+v; err=%v", timeRange, err)
		log.Errorf(err.Error())
		return nil, err
	}

	// Extract the tags trees from the metric requests.
	tagsTrees := make([]*tagstree.AllTagTreeReaders, 0)
	for tthBaseDir := range allSearchRequests {
		allTagsTreeReader, err := tagstree.InitAllTagsTreeReader(tthBaseDir)
		if err != nil {
			err = fmt.Errorf("GetRotatedTagsTreesWithinTimeRange: failed to get tags tree reader for tthBaseDir: %s; err=%v", tthBaseDir, err)
			log.Errorf(err.Error())
			return nil, err
		}

		tagsTrees = append(tagsTrees, allTagsTreeReader)
	}

	return tagsTrees, nil
}

func GetTagKeysWithMostSeriesRequest(timeRange *dtu.MetricsTimeRange,
	limit uint64, myid int64) ([]TagKeySeriesCount, error) {

	querySummary := summary.InitQuerySummary(summary.METRICS, rutils.GetNextQid())
	defer querySummary.LogMetricsQuerySummary(myid)
	tagsTreeReaders, err := GetRotatedTagsTreesWithinTimeRange(timeRange, myid, querySummary)
	if err != nil {
		return nil, err
	}

	tagKeys := make(map[string]struct{})
	for _, segmentTagTreeReader := range tagsTreeReaders {
		tagKeys = toputils.MergeMaps(tagKeys, segmentTagTreeReader.GetAllTagKeys())
	}

	seriesCardMap := make(map[string]*toputils.GobbableHll)
	for tagKey := range tagKeys {
		tsidCard := structs.CreateNewHll()
		seriesCardMap[tagKey] = tsidCard
		for _, segmentTagTreeReader := range tagsTreeReaders {
			err := segmentTagTreeReader.CountTSIDsForKey(tagKey, tsidCard)
			if err != nil {
				log.Warningf("ProcessGetTagKeysWithMostSeriesRequest: Failed to get tsids for key %v, continuing with rest", tagKey)
				continue
			}
		}
	}

	// do unrotated
	err = metrics.CountUnrotatedTSIDsForTagKeys(timeRange, myid, seriesCardMap)
	if err != nil {
		log.Warningf("ProcessGetTagKeysWithMostSeriesRequest: Failed to count tsids for unrotated, contiuing with rest, err: %v", err)
	}

	seriesCounts := make([]TagKeySeriesCount, 0, len(seriesCardMap))
	for tagKey, tsidCard := range seriesCardMap {
		keyAndCount := TagKeySeriesCount{
			Key:       tagKey,
			NumSeries: tsidCard.Cardinality(),
		}
		seriesCounts = append(seriesCounts, keyAndCount)
	}

	sort.Slice(seriesCounts, func(i, j int) bool {
		return seriesCounts[i].NumSeries > seriesCounts[j].NumSeries
	})

	if limit != 0 && limit < uint64(len(seriesCounts)) {
		seriesCounts = seriesCounts[:limit]
	}

	return seriesCounts, nil
}

func ExecuteInstantQuery(qid uint64, orgId int64, reader metricsevaluator.DiskReader, query string,
	evalTime uint32, querySummary *summary.QuerySummary) (*structs.MetricsPromQLInstantQueryResponse, error) {

	expr, err := parser.ParseExpr(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %v", err)
	}

	evaluator := metricsevaluator.NewEvaluator(reader, evalTime, evalTime, 1, querySummary, qid)

	allSeries, err := evaluator.EvalExpr(expr)
	if err != nil {
		return nil, fmt.Errorf("qid=%v, ExecuteInstantQuery: error evaluating query: %v", qid, err)
	}

	return asInstantQueryResponse(allSeries), nil
}

func asInstantQueryResponse(allSeries map[metricsevaluator.SeriesId]*metricsevaluator.SeriesResult) *structs.MetricsPromQLInstantQueryResponse {
	data := structs.PromQLInstantData{
		ResultType:   "vector",
		VectorResult: make([]structs.InstantVectorResult, 0, len(allSeries)),
	}
	data.Result = data.VectorResult

	for _, series := range allSeries {
		if len(series.Values) == 0 {
			continue
		}

		instantVectorResult := structs.InstantVectorResult{
			Metric: series.Labels,
			Value:  []interface{}{series.Values[0].Ts, fmt.Sprintf("%v", series.Values[0].Value)},
		}
		data.VectorResult = append(data.VectorResult, instantVectorResult)
	}

	return &structs.MetricsPromQLInstantQueryResponse{
		Status: "success",
		Data:   &data,
	}
}

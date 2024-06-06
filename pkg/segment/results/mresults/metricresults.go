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

package mresults

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	parser "github.com/prometheus/prometheus/promql/parser"
	tsidtracker "github.com/siglens/siglens/pkg/segment/results/mresults/tsid"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	"github.com/valyala/bytebufferpool"
)

type bucketState uint8

const (
	SERIES_READING bucketState = iota
	DOWNSAMPLING
	AGGREGATED
)

var steps = []uint32{1, 5, 10, 20, 60, 120, 300, 600, 1200, 3600, 7200, 14400, 28800, 57600, 115200, 230400, 460800, 921600}

const TEN_YEARS_IN_SECS = 315_360_000

/*
Represents the results for a running query

Depending on the State the stored information is different:
  - SERIES_READING: maps a tsid to the all raw read dp & downsampled times
  - DOWNSAMPLING: maps a group to all downsampled series results.  This downsampled series may have repeated timestamps from different tsids
  - AGGREGATING: maps a groupid to the resulting aggregated values
*/
type MetricsResult struct {
	MetricName string
	// maps tsid to the raw read series (with downsampled timestamp)
	AllSeries map[uint64]*Series

	// maps groupid to all raw downsampled series. This downsampled series may have repeated timestamps from different tsids
	DsResults map[string]*DownsampleSeries
	// maps groupid to a map of ts to value. This aggregates DsResults based on the aggregation function
	Results map[string]map[uint32]float64

	State bucketState

	rwLock               *sync.RWMutex
	ErrList              []error
	TagValues            map[string]map[string]struct{}
	AllSeriesTagsOnlyMap map[uint64]*tsidtracker.AllMatchedTSIDsInfo
}

/*
Inits a metricsresults holder

TODO: depending on metrics query, have different cases on how to resolve dps
*/
func InitMetricResults(mQuery *structs.MetricsQuery, qid uint64) *MetricsResult {
	return &MetricsResult{
		MetricName:           mQuery.MetricName,
		AllSeries:            make(map[uint64]*Series),
		rwLock:               &sync.RWMutex{},
		ErrList:              make([]error, 0),
		AllSeriesTagsOnlyMap: make(map[uint64]*tsidtracker.AllMatchedTSIDsInfo, 0),
	}
}

/*
Add a given series for the tsid and group information

This does not protect againt concurrency. The caller is responsible for coordination
*/
func (r *MetricsResult) AddSeries(series *Series, tsid uint64, tsGroupId *bytebufferpool.ByteBuffer) {
	currSeries, ok := r.AllSeries[tsid]
	if !ok {
		currSeries = series
		r.AllSeries[tsid] = series
		return
	}
	currSeries.Merge(series)
}

func (r *MetricsResult) AddAllSeriesTagsOnlyMap(tsidInfoMap map[uint64]*tsidtracker.AllMatchedTSIDsInfo) {
	for tsid, tsidInfo := range tsidInfoMap {
		r.AllSeriesTagsOnlyMap[tsid] = tsidInfo
	}
}

/*
Return the number of final series in the metrics result
*/
func (r *MetricsResult) GetNumSeries() uint64 {
	return uint64(len(r.Results))
}

/*
Downsample all series

Insert into r.DsResults, mapping a groupid to all RunningDownsample Series entries
This means that a single tsid will have unique timetamps, but those timestamps can exist for another tsid
*/
func (r *MetricsResult) DownsampleResults(ds structs.Downsampler, parallelism int) []error {

	// maps a group id to the running downsampled series
	allDSSeries := make(map[string]*DownsampleSeries, len(r.AllSeries))

	var idx int
	wg := &sync.WaitGroup{}
	dataLock := &sync.Mutex{}

	errorLock := &sync.Mutex{}
	errors := make([]error, 0)

	for _, series := range r.AllSeries {
		wg.Add(1)

		go func(s *Series) {
			defer wg.Done()

			dsSeries, err := s.Downsample(ds)
			if err != nil {
				errorLock.Lock()
				errors = append(errors, err)
				errorLock.Unlock()
				return
			}

			grp := s.grpID.String()

			dataLock.Lock()
			allDS, ok := allDSSeries[grp]
			if !ok {
				allDSSeries[grp] = dsSeries
			} else {
				allDS.Merge(dsSeries)
			}
			dataLock.Unlock()
		}(series)
		idx++
		if idx%parallelism == 0 {
			wg.Wait()
		}
	}
	wg.Wait()
	r.DsResults = allDSSeries
	r.State = DOWNSAMPLING
	r.AllSeries = nil

	if len(errors) > 0 {
		return errors
	}

	return nil
}

/*
Aggregate results for series sharing a groupid

Internally, this will store the final aggregated results
e.g. will store avg instead of running sum&count
*/
func (r *MetricsResult) AggregateResults(parallelism int, aggregation structs.Aggregation) []error {
	if r.State != DOWNSAMPLING {
		return []error{errors.New("results is not in downsampling state")}
	}

	r.Results = make(map[string]map[uint32]float64)
	errors := make([]error, 0)

	// For some aggregations like sum and avg, we can compute the result from a single timeseries within a vector.
	// However, for aggregations like count, topk, and bottomk, we must retrieve all the time series in the vector and can only compute the results after traversing all of these time series.
	if aggregation.IsAggregateFromAllTimeseries() {
		err := r.aggregateFromAllTimeseries(aggregation)
		if err != nil {
			errors = append(errors, err)
			return errors
		}
		return nil
	}

	lock := &sync.Mutex{}
	wg := &sync.WaitGroup{}

	errorLock := &sync.Mutex{}

	var idx int
	for grpID, runningDS := range r.DsResults {
		wg.Add(1)
		go func(grp string, ds *DownsampleSeries) {
			defer wg.Done()

			grpVal, err := ds.AggregateFromSingleTimeseries()
			if err != nil {
				errorLock.Lock()
				errors = append(errors, err)
				errorLock.Unlock()
				return
			}

			lock.Lock()
			r.Results[grp] = grpVal
			lock.Unlock()
		}(grpID, runningDS)
		idx++
		if idx%parallelism == 0 {
			wg.Wait()
		}
	}

	wg.Wait()
	r.DsResults = nil
	r.State = AGGREGATED

	if len(errors) > 0 {
		return errors
	}

	return nil
}

// extractGroupByFieldsFromSeriesId extracts the groupByFields from the seriesId
// And returns the slice of Group By Fields as key-value pairs.
func extractGroupByFieldsFromSeriesId(seriesId string, groupByFields []string) []string {
	var groupKeyValuePairs []string
	for _, field := range groupByFields {
		start := strings.Index(seriesId, field+":")
		if start == -1 {
			continue
		}
		start += len(field) + 1 // +1 to skip the ':'
		end := strings.Index(seriesId[start:], ",")
		if end == -1 {
			end = len(seriesId)
		} else {
			end += start
		}
		keyValuePair := fmt.Sprintf("%s:%s", field, seriesId[start:end])
		groupKeyValuePairs = append(groupKeyValuePairs, keyValuePair)
	}
	return groupKeyValuePairs
}

// getAggSeriesId returns the group seriesId for the aggregated series based on the given seriesId and groupByFields
// If groupByFields is empty, it returns the "metricName{" as the group seriesId
// If groupByFields is not empty, it returns the "metricName{key1:value1,key2:value2,..." as the group seriesId
// Where key1, key2, ... are the groupByFields and value1, value2, ... are the values of the groupByFields in the seriesId
// The groupByFields are extracted from the seriesId
func getAggSeriesId(metricName string, seriesId string, groupByFields []string) string {
	if len(groupByFields) == 0 {
		return metricName + "{"
	}
	groupKeyValuePairs := extractGroupByFieldsFromSeriesId(seriesId, groupByFields)
	seriesId = metricName + "{" + strings.Join(groupKeyValuePairs, ",")
	return seriesId
}

func (r *MetricsResult) ApplyAggregationToResults(parallelism int, aggregation structs.Aggregation) []error {
	if r.State != AGGREGATED {
		return []error{errors.New("results is not in aggregated state")}
	}

	results := make(map[string]map[uint32]float64, len(r.Results))
	errors := make([]error, 0)

	// For some aggregations like sum and avg, we can compute the result from a single timeseries within a vector.
	// However, for aggregations like count, topk, and bottomk, we must retrieve all the time series in the vector and can only compute the results after traversing all of these time series.
	if aggregation.IsAggregateFromAllTimeseries() {
		err := r.aggregateFromAllTimeseries(aggregation)
		if err != nil {
			errors = append(errors, err)
			return errors
		}
		return nil
	}

	lock := &sync.Mutex{}
	wg := &sync.WaitGroup{}

	errorLock := &sync.Mutex{}

	var idx int

	seriesEntriesMap := make(map[string]map[uint32][]RunningEntry, 0)

	for seriesId, timeSeries := range r.Results {
		aggSeriesId := getAggSeriesId(r.MetricName, seriesId, aggregation.GroupByFields)
		if _, ok := results[aggSeriesId]; !ok {
			results[aggSeriesId] = make(map[uint32]float64, 0)
			seriesEntriesMap[aggSeriesId] = make(map[uint32][]RunningEntry, 0)
		}
		for ts, val := range timeSeries {
			if _, ok := seriesEntriesMap[aggSeriesId][ts]; !ok {
				seriesEntriesMap[aggSeriesId][ts] = make([]RunningEntry, 0)
			}
			seriesEntriesMap[aggSeriesId][ts] = append(seriesEntriesMap[aggSeriesId][ts], RunningEntry{runningCount: 1, runningVal: val})
		}
	}

	for seriesId, timeSeries := range seriesEntriesMap {

		wg.Add(1)
		go func(grp string, ts map[uint32][]RunningEntry) {
			defer wg.Done()

			for ts, entries := range ts {
				aggVal, err := ApplyAggregationFromSingleTimeseries(entries, aggregation)
				if err != nil {
					errorLock.Lock()
					errors = append(errors, err)
					errorLock.Unlock()
					return
				}
				lock.Lock()
				results[grp][ts] = aggVal
				lock.Unlock()
			}

		}(seriesId, timeSeries)
		idx++
		if idx%parallelism == 0 {
			wg.Wait()
		}

	}

	wg.Wait()
	r.Results = results

	if len(errors) > 0 {
		return errors
	}

	return nil
}

/*
Apply function to results for series sharing a groupid.
*/
func (r *MetricsResult) ApplyFunctionsToResults(parallelism int, function structs.Function) []error {

	lock := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	errList := []error{} // Thread-safe list of errors

	// Use a temporary map to record the results modified by goroutines, thus resolving concurrency issues caused by modifying a map during iteration.
	results := make(map[string]map[uint32]float64, len(r.Results))

	var idx int
	for grpID, timeSeries := range r.Results {
		wg.Add(1)
		go func(grp string, ts map[uint32]float64, function structs.Function) {
			defer wg.Done()
			grpVal, err := ApplyFunction(ts, function)
			if err != nil {
				lock.Lock()
				errList = append(errList, err)
				lock.Unlock()
				return
			}
			lock.Lock()
			results[grp] = grpVal
			lock.Unlock()
		}(grpID, timeSeries, function)
		idx++
		if idx%parallelism == 0 {
			wg.Wait()
		}
	}

	wg.Wait()
	r.Results = results

	if len(errList) > 0 {
		return errList
	}

	r.DsResults = nil

	return nil
}

func (r *MetricsResult) AddError(err error) {
	r.rwLock.Lock()
	r.ErrList = append(r.ErrList, err)
	r.rwLock.Unlock()
}

/*
Merge series with global results

This can only merge results if both structs are in SERIES_READING state
*/
func (r *MetricsResult) Merge(localRes *MetricsResult) error {
	if r.State != SERIES_READING || localRes.State != SERIES_READING {
		return errors.New("merged results are not in serires reading state")
	}
	r.rwLock.Lock()
	defer r.rwLock.Unlock()
	r.ErrList = append(r.ErrList, localRes.ErrList...)
	for tsid, series := range localRes.AllSeries {
		currSeries, ok := r.AllSeries[tsid]
		if !ok {
			currSeries = series
			r.AllSeries[tsid] = series
			continue
		}
		currSeries.Merge(series)
	}
	return nil
}

// The groupID string should be in the format of "metricName{tk1:tv1,tk2:tv2,..."
// As per the flow, there would be no trailing "}" in the groupID string
func removeMetricNameFromGroupID(groupID string) string {
	stringVals := strings.Split(groupID, "{")
	if len(stringVals) != 2 {
		return groupID
	} else {
		return stringVals[1]
	}
}

func ExtractMetricNameFromGroupID(groupID string) string {
	stringVals := strings.Split(groupID, "{")
	if len(stringVals) != 2 {
		return groupID
	} else {
		return stringVals[0]
	}
}

func (r *MetricsResult) GetOTSDBResults(mQuery *structs.MetricsQuery) ([]*structs.MetricsQueryResponse, error) {
	if r.State != AGGREGATED {
		return nil, errors.New("results is not in aggregated state")
	}
	retVal := make([]*structs.MetricsQueryResponse, len(r.Results))

	idx := 0
	uniqueTagKeys := make(map[string]bool)
	tagKeys := make([]string, 0)
	for _, tag := range mQuery.TagsFilters {
		if _, ok := uniqueTagKeys[tag.TagKey]; !ok {
			uniqueTagKeys[tag.TagKey] = true
			tagKeys = append(tagKeys, tag.TagKey)
		}
	}

	for grpId, results := range r.Results {
		tags := make(map[string]string)
		tagValues := strings.Split(removeTrailingComma(grpId), tsidtracker.TAG_VALUE_DELIMITER_STR)
		if len(tagKeys) != len(tagValues) {
			err := errors.New("GetResults: the length of tag key and tag value pair must match")
			return nil, err
		}

		for _, val := range tagValues {
			keyValue := strings.Split(removeMetricNameFromGroupID(val), ":")
			tags[keyValue[0]] = keyValue[1]
		}
		retVal[idx] = &structs.MetricsQueryResponse{
			MetricName: mQuery.MetricName,
			Tags:       tags,
			Dps:        results,
		}
		idx++
	}
	return retVal, nil
}

func (r *MetricsResult) GetResultsPromQl(mQuery *structs.MetricsQuery, pqlQuerytype parser.ValueType) (*structs.MetricsQueryResponsePromQl, error) {
	if r.State != AGGREGATED {
		return nil, errors.New("results is not in aggregated state")
	}
	var pqldata structs.Data

	switch pqlQuerytype {
	case parser.ValueTypeVector:
		pqldata.ResultType = parser.ValueType("vector")
		for grpId, results := range r.Results {

			tagValues := strings.Split(removeTrailingComma(grpId), tsidtracker.TAG_VALUE_DELIMITER_STR)

			var result structs.Result
			var keyValue []string
			result.Metric = make(map[string]string)
			result.Metric["__name__"] = mQuery.MetricName
			for idx, val := range tagValues {
				if idx == 0 {
					keyValue = strings.Split(removeMetricNameFromGroupID(val), ":")
				} else {
					keyValue = strings.Split(val, ":")
				}
				if len(keyValue) > 1 {
					result.Metric[keyValue[0]] = keyValue[1]
				}
			}
			for k, v := range results {
				result.Value = append(result.Value, []interface{}{int64(k), fmt.Sprintf("%v", v)})
			}
			pqldata.Result = append(pqldata.Result, result)
		}
	default:
		return nil, fmt.Errorf("GetResultsPromQl: Unsupported PromQL query result type")
	}
	return &structs.MetricsQueryResponsePromQl{
		Status: "success",
		Data:   pqldata,
	}, nil
}
func (res *MetricsResult) GetMetricTagsResultSet(mQuery *structs.MetricsQuery) ([]string, []string, error) {
	if res.State != SERIES_READING {
		return nil, nil, errors.New("results is not in Series Reading state")
	}

	// The Tag Keys in the TagFilters will be unique,
	// as they will be cleaned up in the mQuery.ReorderTagFilters()
	uniqueTagKeys := make([]string, 0)
	for i, tag := range mQuery.TagsFilters {
		if _, exists := mQuery.TagIndicesToKeep[i]; exists {
			uniqueTagKeys = append(uniqueTagKeys, tag.TagKey)
		}
	}

	uniqueTagKeyValues := make(map[string]bool)
	tagKeyValueSet := make([]string, 0)

	for _, tsidInfo := range res.AllSeriesTagsOnlyMap {
		for tagKey, tagValue := range tsidInfo.TagKeyTagValue {
			tagKeyValue := fmt.Sprintf("%s:%s", tagKey, tagValue)
			if _, ok := uniqueTagKeyValues[tagKeyValue]; !ok {
				uniqueTagKeyValues[tagKeyValue] = true
				tagKeyValueSet = append(tagKeyValueSet, tagKeyValue)
			}
		}
	}

	return uniqueTagKeys, tagKeyValueSet, nil
}

func (res *MetricsResult) GetSeriesByLabel() ([]map[string]interface{}, error) {
	if res.State != SERIES_READING {
		return nil, errors.New("results is not in Series Reading state")
	}

	data := make([]map[string]interface{}, 0)

	for _, tsidInfo := range res.AllSeriesTagsOnlyMap {
		tagMap := make(map[string]interface{})
		tagMap["__name__"] = tsidInfo.MetricName

		for tagKey, tagValue := range tsidInfo.TagKeyTagValue {
			tagMap[tagKey] = tagValue
		}

		data = append(data, tagMap)
	}
	return data, nil
}

func (r *MetricsResult) GetResultsPromQlForUi(mQuery *structs.MetricsQuery, pqlQuerytype parser.ValueType, startTime, endTime uint32) (utils.MetricsStatsResponseInfo, error) {
	var httpResp utils.MetricsStatsResponseInfo
	httpResp.AggStats = make(map[string]map[string]interface{})
	if r.State != AGGREGATED {
		return utils.MetricsStatsResponseInfo{}, errors.New("results is not in aggregated state")
	}

	for grpId, results := range r.Results {
		groupId := mQuery.MetricName + "{"
		groupId += grpId
		groupId += "}"
		httpResp.AggStats[groupId] = make(map[string]interface{}, 1)
		for ts, v := range results {
			temp := time.Unix(int64(ts), 0)

			bucketInterval := temp.Format("2006-01-02T15:04")

			httpResp.AggStats[groupId][bucketInterval] = v
		}
	}
	endEpoch := time.Unix(int64(endTime), 0)
	startEpoch := time.Unix(int64(startTime), 0)
	runningTs := startEpoch
	var startDuration, endDuration int64

	switch pqlQuerytype {
	case parser.ValueTypeVector:
		for groupId := range httpResp.AggStats {
			startDuration = (startEpoch.UnixMilli() / segutils.MS_IN_MIN) * segutils.MS_IN_MIN
			endDuration = (endEpoch.UnixMilli() / segutils.MS_IN_MIN) * segutils.MS_IN_MIN
			runningTs = startEpoch

			for endDuration > startDuration+segutils.MS_IN_MIN {
				runningTs = runningTs.Add(1 * time.Minute)
				startDuration = startDuration + segutils.MS_IN_MIN
				bucketInterval := runningTs.Format("2006-01-02T15:04")
				if _, ok := httpResp.AggStats[groupId][bucketInterval]; !ok {
					httpResp.AggStats[groupId][bucketInterval] = 0
				}
			}

		}
	default:
		return httpResp, fmt.Errorf("GetResultsPromQl: Unsupported PromQL query result type")
	}

	return httpResp, nil
}

func removeTrailingComma(s string) string {
	return strings.TrimSuffix(s, ",")
}

func (r *MetricsResult) FetchPromqlMetricsForUi(mQuery *structs.MetricsQuery, pqlQuerytype parser.ValueType, startTime, endTime uint32) (utils.MetricStatsResponse, error) {
	var httpResp utils.MetricStatsResponse
	httpResp.Series = make([]string, 0)
	httpResp.Values = make([][]*float64, 0)
	httpResp.StartTime = startTime

	if r.State != AGGREGATED {
		return utils.MetricStatsResponse{}, errors.New("results is not in aggregated state")
	}

	// Calculate the interval using the start and end times
	timerangeSeconds := endTime - startTime
	calculatedInterval, err := CalculateInterval(timerangeSeconds)
	if err != nil {
		return utils.MetricStatsResponse{}, err
	}
	httpResp.IntervalSec = calculatedInterval

	// Create a map of all unique timestamps across all results.
	allTimestamps := make(map[uint32]struct{})
	for _, results := range r.Results {
		for ts := range results {
			allTimestamps[ts] = struct{}{}
		}
	}
	// Convert the map of unique timestamps into a sorted slice.
	httpResp.Timestamps = make([]uint32, 0, len(allTimestamps))
	for ts := range allTimestamps {
		httpResp.Timestamps = append(httpResp.Timestamps, ts)
	}
	sort.Slice(httpResp.Timestamps, func(i, j int) bool { return httpResp.Timestamps[i] < httpResp.Timestamps[j] })

	for grpId, results := range r.Results {
		groupId := grpId
		groupId = removeTrailingComma(groupId)
		groupId += "}"
		httpResp.Series = append(httpResp.Series, groupId)

		values := make([]*float64, len(httpResp.Timestamps))
		for i, ts := range httpResp.Timestamps {
			// Check if there is a value for the current timestamp in results.
			if v, ok := results[uint32(ts)]; ok {
				values[i] = &v
			} else {
				values[i] = nil
			}
		}

		httpResp.Values = append(httpResp.Values, values)
	}

	return httpResp, nil
}

func CalculateInterval(timerangeSeconds uint32) (uint32, error) {
	// If timerangeSeconds is greater than 10 years reject the request
	if timerangeSeconds > TEN_YEARS_IN_SECS {
		return 0, errors.New("timerangeSeconds is greater than 10 years")
	}
	for _, step := range steps {
		if timerangeSeconds/step <= 360 {
			return step, nil
		}
	}

	// If no suitable step is found, return an error
	return 0, errors.New("no suitable step found")
}

func (r *MetricsResult) aggregateFromAllTimeseries(aggregation structs.Aggregation) error {

	switch aggregation.AggregatorFunction {
	case segutils.Count:
		r.computeAggCount(aggregation)
	// Todo: Add TopK and BottomK
	default:
		return fmt.Errorf("aggregateFromAllTimeseries: Unsupported aggregation: %v", aggregation)
	}

	return nil
}

// Count only cares about the number of time series at each timestamp, so it does not need to reduce entries to calculate the values.
func (r *MetricsResult) computeAggCount(aggregation structs.Aggregation) {

	// groupByCols seriesId mapping to map[uint32]map[string]struct{}
	// We can determine the number of full unique grpIDs for each timestamp.
	// For example, count by (color,gender) (metric0)
	// ["color:red,gender:male"] = { 1: {{"color:red,gender:male,age:20"}, {"color:red,gender:male,age:5"}}, 2: ...   }
	// ["color:yellow,gender:male"] = { 1: {{"color:yellow,gender:male,age:3"}}, 2: ...   }
	seriesIdEntriesMap := make(map[string]map[uint32]map[string]struct{})

	for grpID, runningDS := range r.DsResults {
		seriesId := getAggSeriesId(r.MetricName, grpID, aggregation.GroupByFields)
		_, exists := seriesIdEntriesMap[seriesId]
		if !exists {
			seriesIdEntriesMap[seriesId] = make(map[uint32]map[string]struct{})
		}

		for i := 0; i < runningDS.idx; i++ {
			timestamp := runningDS.runningEntries[i].downsampledTime

			_, exists := seriesIdEntriesMap[seriesId][timestamp]
			if !exists {
				seriesIdEntriesMap[seriesId][timestamp] = make(map[string]struct{})
			}
			seriesIdEntriesMap[seriesId][timestamp][grpID] = struct{}{}
		}
	}

	for seriesId, entries := range seriesIdEntriesMap {
		grpVal := make(map[uint32]float64)
		for timestamp, grpIdSet := range entries {
			grpVal[timestamp] = float64(len(grpIdSet))
		}
		r.Results[seriesId] = grpVal
	}

	r.DsResults = nil
	r.State = AGGREGATED
}

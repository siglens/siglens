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
	"container/heap"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	parser "github.com/prometheus/prometheus/promql/parser"
	"github.com/siglens/siglens/pkg/common/dtypeutils"
	putils "github.com/siglens/siglens/pkg/integrations/prometheus/utils"
	tsidtracker "github.com/siglens/siglens/pkg/segment/results/mresults/tsid"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
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

	IsScalar    bool
	ScalarValue float64

	IsInstantQuery bool

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
		IsInstantQuery:       mQuery.IsInstantQuery,
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
	err := currSeries.Merge(series)
	if err != nil {
		r.AddError(err)
	}
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
		return []error{fmt.Errorf("AggregateResults: results is not in downsampling state, state: %v", r.State)}
	}

	r.Results = make(map[string]map[uint32]float64)
	errors := make([]error, 0)

	seriesEntriesMap := make(map[string]map[uint32][]RunningEntry, 0)

	for grpID, ds := range r.DsResults {
		if ds == nil {
			err := fmt.Errorf("AggregateResults: Group %v has nonexistent downsample series", grpID)
			errors = append(errors, err)
			return errors
		}

		var aggSeriesId string

		if aggregation.IsAggregateFromAllTimeseries() {
			aggSeriesId = grpID
		} else {
			aggSeriesId = getAggSeriesId(grpID, &aggregation)
		}

		if _, exists := seriesEntriesMap[aggSeriesId]; !exists {
			seriesEntriesMap[aggSeriesId] = make(map[uint32][]RunningEntry, 0)
		}

		for i := 0; i < ds.idx; i++ {
			entry := ds.runningEntries[i]
			seriesEntriesMap[aggSeriesId][entry.downsampledTime] = append(seriesEntriesMap[aggSeriesId][entry.downsampledTime], entry)
		}
	}

	// For some aggregations like sum and avg, we can compute the result from a single timeseries within a vector.
	// However, for aggregations like count, topk, and bottomk, we must retrieve all the time series in the vector and can only compute the results after traversing all of these time series.
	if aggregation.IsAggregateFromAllTimeseries() {

		err := r.aggregateFromAllTimeseries(aggregation, seriesEntriesMap)
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
	for seriesId, timeSeries := range seriesEntriesMap {
		wg.Add(1)
		go func(grp string, timeSeries map[uint32][]RunningEntry) {
			defer wg.Done()

			for ts, entries := range timeSeries {
				aggVal, err := ApplyAggregationFromSingleTimeseries(entries, aggregation)
				if err != nil {
					errorLock.Lock()
					errors = append(errors, err)
					errorLock.Unlock()
					return
				}
				lock.Lock()
				tsMap, ok := r.Results[grp]
				if !ok {
					tsMap = make(map[uint32]float64, 0)
					r.Results[grp] = tsMap
				}

				tsMap[ts] = aggVal
				lock.Unlock()
			}

		}(seriesId, timeSeries)
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

// ExtractGroupByFieldsFromSeriesId extracts the groupByFields from the seriesId
// And returns the slice of Group By Fields as key-value pairs, and the slice of values of the groupByFields
func ExtractGroupByFieldsFromSeriesId(seriesId string, groupByFields []string) ([]string, []string) {
	var groupKeyValuePairs []string
	var values []string
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
		values = append(values, seriesId[start:end])
		groupKeyValuePairs = append(groupKeyValuePairs, keyValuePair)
	}
	return groupKeyValuePairs, values
}

func GetSeriesIdWithoutFields(seriesId string, fields []string) string {
	if len(fields) == 0 {
		return seriesId
	}

	fieldsSet := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		fieldsSet[field] = struct{}{}
	}

	parts := strings.Split(seriesId, ",")
	var filteredParts []string
	var metricName string

	for i, part := range parts {
		if i == 0 {
			splitVals := strings.SplitN(part, "{", 2)
			metricName = splitVals[0]

			if len(splitVals) == 2 {
				part = splitVals[1]
			}
		}

		keyValue := strings.SplitN(part, ":", 2)
		if len(keyValue) == 2 {
			if _, exists := fieldsSet[keyValue[0]]; exists {
				continue
			}
		}

		filteredParts = append(filteredParts, part)
	}

	return metricName + "{" + strings.Join(filteredParts, ",")
}

// getAggSeriesId returns the group seriesId for the aggregated series based on the given seriesId and groupByFields
// The seriesId is in the format of "metricName{key1:value1,key2:value2,..."
// If groupByFields is empty, it returns the "metricName{" as the group seriesId
// If groupByFields is not empty, it returns the "metricName{key1:value1,key2:value2,..." as the group seriesId
// Where key1, key2, ... are the groupByFields and value1, value2, ... are the values of the groupByFields in the seriesId
// The groupByFields are extracted from the seriesId
func getAggSeriesId(seriesId string, aggregation *structs.Aggregation) string {
	if aggregation == nil {
		return seriesId
	}

	if aggregation.Without {
		return GetSeriesIdWithoutFields(seriesId, aggregation.GroupByFields)
	}

	metricName := ExtractMetricNameFromGroupID(seriesId)
	groupByFields := aggregation.GroupByFields

	if len(groupByFields) == 0 {
		return metricName + "{"
	}

	groupKeyValuePairs, _ := ExtractGroupByFieldsFromSeriesId(seriesId, groupByFields)
	seriesId = metricName + "{" + strings.Join(groupKeyValuePairs, ",")
	return seriesId
}

func (r *MetricsResult) ApplyAggregationToResults(parallelism int, aggregation structs.Aggregation) []error {
	if r.State != AGGREGATED {
		return []error{fmt.Errorf("ApplyAggregationToResults: results is not in aggregated state, state: %v", r.State)}
	}

	results := make(map[string]map[uint32]float64, len(r.Results))
	errors := make([]error, 0)

	// For some aggregations like sum and avg, we can compute the result from a single timeseries within a vector.
	// However, for aggregations like count, topk, and bottomk, we must retrieve all the time series in the vector and can only compute the results after traversing all of these time series.
	if aggregation.IsAggregateFromAllTimeseries() {
		seriesEntriesMap := make(map[string]map[uint32][]RunningEntry, 0)

		for aggSeriesId, timeSeries := range r.Results {
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

		err := r.aggregateFromAllTimeseries(aggregation, seriesEntriesMap)
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
		aggSeriesId := getAggSeriesId(seriesId, &aggregation)
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
func (r *MetricsResult) ApplyFunctionsToResults(parallelism int, function structs.Function, timeRange *dtypeutils.MetricsTimeRange) []error {

	lock := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	errList := []error{} // Thread-safe list of errors

	if function.FunctionType == structs.HistogramFunction {
		err := r.ApplyHistogramToResults(parallelism, function.HistogramFunction)
		if err != nil {
			errList = append(errList, err)
			return errList
		}

		return nil
	}

	// Use a temporary map to record the results modified by goroutines, thus resolving concurrency issues caused by modifying a map during iteration.
	results := make(map[string]map[uint32]float64, len(r.Results))

	var idx int
	for grpID, timeSeries := range r.Results {
		wg.Add(1)
		go func(grp string, ts map[uint32]float64, function structs.Function, timeRange *dtypeutils.MetricsTimeRange) {
			defer wg.Done()
			grpID, grpVal, err := ApplyFunction(grp, ts, function, timeRange)
			if err != nil {
				lock.Lock()
				errList = append(errList, err)
				lock.Unlock()
				return
			}
			lock.Lock()
			results[grpID] = grpVal
			lock.Unlock()
		}(grpID, timeSeries, function, timeRange)
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

func (r *MetricsResult) ApplyHistogramToResults(parallelism int, agg *structs.HistogramAgg) error {
	if agg == nil {
		log.Errorf("ApplyHistogramToResults: HistogramAgg is nil")
		return fmt.Errorf("nil histogram agg")
	}

	seriesIds := make([]string, 0, len(r.Results))
	for seriesId := range r.Results {
		seriesIds = append(seriesIds, seriesId)
	}

	switch agg.Function {
	case segutils.HistogramQuantile:
		return r.applyHistogramQunatile(seriesIds, agg)
	}

	return nil
}

func (r *MetricsResult) applyHistogramQunatile(seriesIds []string, agg *structs.HistogramAgg) error {
	// Get the histogram bins from the seriesIds
	histogramBinsPerSeries, err := getHistogramBins(seriesIds, r.Results)
	if err != nil {
		return err
	}

	errors := make([]error, 0)

	// Apply histogram quantile to the histogram bins
	for seriesId, tsToBins := range histogramBinsPerSeries {
		for ts, bins := range tsToBins {
			val, err := histogramQuantile(agg.Quantile, bins)
			if err != nil {
				errors = append(errors, err)
				continue
			}

			if _, ok := r.Results[seriesId]; !ok {
				r.Results[seriesId] = make(map[uint32]float64, 0)
			}

			r.Results[seriesId][ts] = val
		}
	}

	if len(errors) > 0 {
		sliceLen := min(5, len(errors))
		log.Errorf("applyHistogramQunatile: len(errors):%v, errors:%v", len(errors), errors[:sliceLen])
	}

	return nil
}

func getHistogramBins(seriesIds []string, results map[string]map[uint32]float64) (map[string]map[uint32][]histogramBin, error) {
	histogramBinsPerSeries := make(map[string]map[uint32][]histogramBin, 0)

	for _, seriesIdWithLe := range seriesIds {
		timeSeries, ok := results[seriesIdWithLe]
		if !ok {
			continue
		}

		delete(results, seriesIdWithLe)

		seriesId, leValue, hasLe, err := extractAndRemoveLeFromSeriesId(seriesIdWithLe)
		if err != nil {
			log.Errorf("getHistogramBins: Failed to extract and remove le from seriesId %v, err: %v", seriesIdWithLe, err)
			continue
		}

		if !hasLe {
			continue
		}

		tsToBins, ok := histogramBinsPerSeries[seriesId]
		if !ok {
			tsToBins = make(map[uint32][]histogramBin, 0)
			histogramBinsPerSeries[seriesId] = tsToBins
		}

		for ts, val := range timeSeries {
			bins, ok := tsToBins[ts]
			if !ok {
				bins = make([]histogramBin, 0)
			}

			bins = append(bins, histogramBin{upperBound: leValue, count: val})
			tsToBins[ts] = bins
		}
	}

	return histogramBinsPerSeries, nil
}

func extractAndRemoveLeFromSeriesId(seriesId string) (string, float64, bool, error) {
	// Regex pattern to find le="VALUE" and capture the VALUE
	re := regexp.MustCompile(`le:(\+?Inf|-?Inf|-?[0-9.]+),?`)

	matches := re.FindStringSubmatch(seriesId)
	var leValueStr string
	if len(matches) == 2 {
		leValueStr = matches[1]
	} else {
		return seriesId, 0, false, nil
	}

	var leValue float64
	var err error

	if leValueStr == "+Inf" {
		leValue = math.Inf(1)
	} else if leValueStr == "-Inf" {
		leValue = math.Inf(-1)
	} else {
		leValue, err = strconv.ParseFloat(leValueStr, 64)
		if err != nil {
			return seriesId, 0, false, fmt.Errorf("extractAndRemoveleFromSeriesId: Failed to parse le value %v, err: %v", leValueStr, err)
		}
	}

	// Remove the le="VALUE" from the seriesId
	cleanedSeriesId := re.ReplaceAllString(seriesId, "")

	cleanedSeriesId = strings.Replace(cleanedSeriesId, "{,", "{", 1)
	cleanedSeriesId = strings.TrimSuffix(cleanedSeriesId, ",")

	return cleanedSeriesId, leValue, true, nil
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
		return fmt.Errorf("Merge: merged results are not in serires reading state, state: %v", r.State)
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
		err := currSeries.Merge(series)
		if err != nil {
			r.AddError(err)
		}
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
		return nil, fmt.Errorf("GetOTSDBResults: results is not in aggregated state, state: %v", r.State)
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
		tagValues := strings.Split(RemoveTrailingComma(grpId), tsidtracker.TAG_VALUE_DELIMITER_STR)
		if len(tagKeys) != len(tagValues) {
			err := fmt.Errorf("GetResults: the length of tag key and tag value pair must match. Tag Key: %v; Tag Value: %v", tagKeys, tagValues)
			return nil, err
		}

		for _, val := range tagValues {
			keyValue := strings.Split(removeMetricNameFromGroupID(val), ":")
			if len(keyValue) != 2 {
				log.Errorf("GetResults: Invalid tag keyvalue: %v", val)
				continue
			}
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

func getPromQLSeriesFormat(seriesId string) map[string]string {
	tagValues := strings.Split(RemoveTrailingComma(seriesId), tsidtracker.TAG_VALUE_DELIMITER_STR)

	var keyValue []string
	metric := make(map[string]string)
	metric["__name__"] = ExtractMetricNameFromGroupID(seriesId)
	for idx, val := range tagValues {
		if idx == 0 {
			keyValue = strings.SplitN(removeMetricNameFromGroupID(val), ":", 2)
		} else {
			keyValue = strings.SplitN(val, ":", 2)
		}

		if len(keyValue) > 1 {
			metric[keyValue[0]] = keyValue[1]
		}
	}

	return metric
}

func (r *MetricsResult) GetResultsPromQlInstantQuery(pqlQueryType parser.ValueType, timestamp uint32) (*structs.MetricsPromQLInstantQueryResponse, error) {
	var pqlData structs.PromQLInstantData

	switch pqlQueryType {
	case parser.ValueTypeScalar:
		pqlData.ResultType = pqlQueryType
		pqlData.SliceResult = []interface{}{timestamp, fmt.Sprintf("%v", r.ScalarValue)}
	case parser.ValueTypeString:
		// TODO: Implement this
		return nil, errors.New("GetResultsPromQlInstantQuery: ValueTypeString is not supported")
	case parser.ValueTypeVector:
		pqlData.ResultType = pqlQueryType
		for seriesId, results := range r.Results {
			if len(results) == 0 {
				continue
			}

			metricSeries := getPromQLSeriesFormat(seriesId)

			result := structs.InstantVectorResult{
				Metric: metricSeries,
			}

			// Instant Query is expected to have only the latest timestamp
			latestTime := uint32(0)
			latestValue := float64(0)

			for ts, val := range results {
				if ts > latestTime {
					latestTime = ts
					latestValue = val
				}
			}

			result.Value = []interface{}{latestTime, fmt.Sprintf("%v", latestValue)}
			pqlData.VectorResult = append(pqlData.VectorResult, result)
		}
	case parser.ValueTypeMatrix:
		return nil, errors.New("ValueTypeMatrix is not supported for Instant Queries")
	default:
		return nil, fmt.Errorf("GetResultsPromQlInstantQuery: Unsupported PromQL query result type: %v", pqlQueryType)
	}

	return &structs.MetricsPromQLInstantQueryResponse{
		Status: "success",
		Data:   &pqlData,
	}, nil
}

func (r *MetricsResult) GetResultsPromQl(mQuery *structs.MetricsQuery, pqlQuerytype parser.ValueType) (*structs.MetricsPromQLRangeQueryResponse, error) {
	if r.State != AGGREGATED {
		return nil, fmt.Errorf("GetResultsPromQl: results is not in aggregated state, state: %v", r.State)
	}
	var pqldata structs.PromQLRangeData

	switch pqlQuerytype {
	case parser.ValueTypeVector, parser.ValueTypeMatrix:
		pqldata.ResultType = parser.ValueType("matrix")
		for seriesId, results := range r.Results {
			metricSeries := getPromQLSeriesFormat(seriesId)

			result := &structs.RangeVectorResult{
				Metric: metricSeries,
				Values: make([]interface{}, 0, len(results)),
			}

			for k, v := range results {
				result.Values = append(result.Values, []interface{}{int64(k), fmt.Sprintf("%v", v)})
			}

			sort.Slice(result.Values, func(i, j int) bool {
				return result.Values[i].([]interface{})[0].(int64) < result.Values[j].([]interface{})[0].(int64)
			})

			pqldata.Result = append(pqldata.Result, *result)
		}
	default:
		return nil, fmt.Errorf("GetResultsPromQl: Unsupported PromQL query result type: %v", pqlQuerytype)
	}
	return &structs.MetricsPromQLRangeQueryResponse{
		Status: "success",
		Data:   &pqldata,
	}, nil
}

func (r *MetricsResult) GetResultsPromQlForScalarType(pqlQueryType parser.ValueType, startTime, endTime uint32, step uint32) (*structs.MetricsPromQLRangeQueryResponse, error) {
	if pqlQueryType != parser.ValueTypeScalar {
		return nil, fmt.Errorf("GetResultsPromQlForScalarType: Unsupported PromQL query result type: %v", pqlQueryType)
	}

	var pqlData structs.PromQLRangeData
	pqlData.ResultType = parser.ValueType("matrix")

	scalarValue := r.ScalarValue

	evalTime := startTime

	pqlData.Result = make([]structs.RangeVectorResult, 1)

	values := make([]interface{}, 0)

	for evalTime <= endTime {
		values = append(values, []interface{}{evalTime, fmt.Sprintf("%v", scalarValue)})
		evalTime += step
	}

	pqlData.Result[0] = structs.RangeVectorResult{
		Metric: map[string]string{},
		Values: values,
	}

	return &structs.MetricsPromQLRangeQueryResponse{
		Status: "success",
		Data:   &pqlData,
	}, nil
}

func (res *MetricsResult) GetMetricTagsResultSet(mQuery *structs.MetricsQuery) ([]string, []string, error) {
	if res.State != SERIES_READING {
		return nil, nil, fmt.Errorf("GetMetricTagsResultSet: results is not in Series Reading state, state: %v", res.State)
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
		return nil, fmt.Errorf("GetSeriesByLabel: results is not in Series Reading state, state: %v", res.State)
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
		return utils.MetricsStatsResponseInfo{}, fmt.Errorf("GetResultsPromQlForUi: results is not in aggregated state, state: %v", r.State)
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
		return httpResp, fmt.Errorf("GetResultsPromQl: Unsupported PromQL query result type: %v", pqlQuerytype)
	}

	return httpResp, nil
}

func RemoveTrailingComma(s string) string {
	return strings.TrimSuffix(s, ",")
}

func (r *MetricsResult) FetchScalarMetricsForUi(finalSearchText string, pqlQuerryType parser.ValueType, startTime, endTime uint32) (utils.MetricStatsResponse, error) {
	var httpResp utils.MetricStatsResponse
	httpResp.Series = make([]string, 0)
	httpResp.Values = make([][]*float64, 0)
	httpResp.StartTime = uint32(startTime)
	// Calculate the interval using the start and end times
	timerangeSeconds := endTime - startTime
	calculatedInterval, err := CalculateInterval(timerangeSeconds)
	if err != nil {
		return utils.MetricStatsResponse{}, err
	}
	httpResp.IntervalSec = calculatedInterval

	httpResp.Series = append(httpResp.Series, finalSearchText)
	httpResp.Values = append(httpResp.Values, []*float64{sanitizeFloatValue(r.ScalarValue)})
	httpResp.Timestamps = []uint32{uint32(endTime)}

	return httpResp, nil
}

func (r *MetricsResult) FetchPromqlMetricsForUi(mQuery *structs.MetricsQuery, pqlQuerytype parser.ValueType, startTime, endTime uint32) (utils.MetricStatsResponse, error) {
	var httpResp utils.MetricStatsResponse
	httpResp.Series = make([]string, 0)
	httpResp.Values = make([][]*float64, 0)
	httpResp.StartTime = startTime

	if r.State != AGGREGATED {
		return utils.MetricStatsResponse{}, fmt.Errorf("FetchPromqlMetricsForUi: results is not in aggregated state, state: %v", r.State)
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
		groupId = RemoveTrailingComma(groupId)
		groupId += "}"
		httpResp.Series = append(httpResp.Series, groupId)

		values := make([]*float64, len(httpResp.Timestamps))
		for i, ts := range httpResp.Timestamps {
			// Check if there is a value for the current timestamp in results.
			if v, ok := results[uint32(ts)]; ok {
				values[i] = sanitizeFloatValue(v)
			} else {
				values[i] = nil
			}
		}

		httpResp.Values = append(httpResp.Values, values)
	}

	return httpResp, nil
}

func sanitizeFloatValue(val float64) *float64 {
	if math.IsInf(val, -1) || math.IsInf(val, 1) || math.IsNaN(val) {
		return nil
	}
	return &val
}

func CalculateInterval(timerangeSeconds uint32) (uint32, error) {
	// If timerangeSeconds is greater than 10 years reject the request
	if timerangeSeconds > TEN_YEARS_IN_SECS {
		return 0, fmt.Errorf("timerangeSeconds:%v is greater than 10 years", timerangeSeconds)
	}
	for _, step := range steps {
		if timerangeSeconds/step <= 360 {
			return step, nil
		}
	}

	// If no suitable step is found, return an error
	return 0, errors.New("no suitable step found")
}

func (r *MetricsResult) aggregateFromAllTimeseries(aggregation structs.Aggregation, seriesEntriesMap map[string]map[uint32][]RunningEntry) error {

	var err error
	switch aggregation.AggregatorFunction {
	case segutils.Count:
		r.computeAggCount(aggregation, seriesEntriesMap)
	case segutils.TopK:
		err = r.computeExtremesKElements(aggregation.FuncConstant, -1.0, seriesEntriesMap)
	case segutils.BottomK:
		err = r.computeExtremesKElements(aggregation.FuncConstant, 1.0, seriesEntriesMap)
	case segutils.Stdvar:
		fallthrough
	case segutils.Stddev:
		r.computeAggStdvarOrStddev(aggregation, seriesEntriesMap)
	default:
		return fmt.Errorf("aggregateFromAllTimeseries: Unsupported aggregation: %v", aggregation)
	}

	if err != nil {
		return err
	}

	return nil
}

// The larger the priority, the earlier it will be popped out. Since we use the value as the priority, for `topk`, the larger the value, the more we want it to remain in the priority queue. Therefore, its priority should be smaller.
// For bottomk, it's the opposite
func (r *MetricsResult) computeExtremesKElements(funcConstant float64, factor float64, seriesEntriesMap map[string]map[uint32][]RunningEntry) error {
	r.Results = make(map[string]map[uint32]float64)

	capacity := int(funcConstant)

	if capacity <= 0 {
		return fmt.Errorf("computeExtremesKElements: k must larger than 0")
	}

	// Use a PriorityQueue to store the top k elements for each timestamp, then separate the (timestamp, val) key-value pairs for each time series and generate the result
	timestampToHeap := make(map[uint32]*utils.PriorityQueue)

	for grpID, runningDS := range seriesEntriesMap {
		for timestamp, entries := range runningDS {
			for _, entry := range entries {
				// Initialize the priority queue if it doesn't exist for this timestamp
				pq, exists := timestampToHeap[timestamp]
				if !exists {
					newPQ := make(utils.PriorityQueue, 0)
					heap.Init(&newPQ)
					heap.Push(&newPQ, &utils.Item{
						Value:    grpID,
						Priority: (entry.runningVal * factor),
					})
					timestampToHeap[timestamp] = &newPQ
				} else {
					// Only keep the top k elements by checking the queue size
					if len(*pq) >= capacity {
						item := heap.Pop(pq).(*utils.Item)
						if item.Priority < entry.runningVal*factor {
							heap.Push(pq, item)
							continue
						}
					}
					heap.Push(pq, &utils.Item{
						Value:    grpID,
						Priority: (entry.runningVal * factor),
					})
				}
			}
		}
	}

	// After processing all entries, store the results
	for timestamp, pq := range timestampToHeap {
		for pq.Len() > 0 {
			item := heap.Pop(pq).(*utils.Item)
			_, exists := r.Results[item.Value]
			if !exists {
				r.Results[item.Value] = make(map[uint32]float64)
			}
			r.Results[item.Value][timestamp] = (item.Priority / factor) // Restore to original value
		}
	}

	r.DsResults = nil
	r.State = AGGREGATED

	return nil
}

// Count only cares about the number of time series at each timestamp, so it does not need to reduce entries to calculate the values.
func (r *MetricsResult) computeAggCount(aggregation structs.Aggregation, seriesEntriesMap map[string]map[uint32][]RunningEntry) {
	// groupByCols seriesId mapping to map[uint32]map[string]struct{}
	// We can determine the number of full unique grpIDs for each timestamp.
	// For example, count by (color,gender) (metric0)
	// ["color:red,gender:male"] = { 1: {{"color:red,gender:male,age:20"}, {"color:red,gender:male,age:5"}}, 2: ...   }
	// ["color:yellow,gender:male"] = { 1: {{"color:yellow,gender:male,age:3"}}, 2: ...   }
	seriesIdEntriesMap := make(map[string]map[uint32]map[string]struct{})
	r.Results = make(map[string]map[uint32]float64)

	if len(aggregation.GroupByFields) > 0 {
		for grpID, timeSeries := range seriesEntriesMap {
			seriesId := getAggSeriesId(grpID, &aggregation)
			if _, exists := seriesIdEntriesMap[seriesId]; !exists {
				seriesIdEntriesMap[seriesId] = make(map[uint32]map[string]struct{})
			}

			for timestamp, entries := range timeSeries {
				for i := range entries {
					// Modify grpID to include more unique attributes to ensure that each entry is uniquely identified,
					// even when there is only one tag filter. This prevents all entries from having the same grpID,
					// which would result in a count of 1 for each timestamp.
					uniqueGrpID := fmt.Sprintf("%s-%d", grpID, i)
					if _, exists := seriesIdEntriesMap[seriesId][timestamp]; !exists {
						seriesIdEntriesMap[seriesId][timestamp] = make(map[string]struct{})
					}
					seriesIdEntriesMap[seriesId][timestamp][uniqueGrpID] = struct{}{}
				}
			}
		}

		for seriesId, entries := range seriesIdEntriesMap {
			grpVal := make(map[uint32]float64)
			for timestamp, grpIdSet := range entries {
				grpVal[timestamp] = float64(len(grpIdSet))
			}
			r.Results[seriesId] = grpVal
		}
	} else {
		timestampToCount := make(map[uint32]float64)

		for _, timeSeries := range seriesEntriesMap {
			for timestamp := range timeSeries {
				timestampToCount[timestamp]++
			}
		}
		r.Results[r.MetricName+"{"] = timestampToCount
	}

	r.DsResults = nil
	r.State = AGGREGATED
}

// Retrieve all series values at each timestamp and calculate the results based on those values.
func (r *MetricsResult) computeAggStdvarOrStddev(aggregation structs.Aggregation, seriesEntriesMap map[string]map[uint32][]RunningEntry) {
	timestampToVals := make(map[uint32][]float64)
	r.Results = make(map[string]map[uint32]float64)

	// If we use group by for this vector, We need to obtain all the timeseries within a group, and then calculate the variance or standard deviation separately for each group
	if len(aggregation.GroupByFields) > 0 {
		// All the time series values under one group
		grpIDToEntryMap := make(map[string]map[uint32][]float64)

		for grpID, timeSeries := range seriesEntriesMap {
			matchingLabelValStr := putils.ExtractMatchingLabelSet(grpID, aggregation.GroupByFields, true)

			if _, exists := grpIDToEntryMap[matchingLabelValStr]; !exists {
				grpIDToEntryMap[matchingLabelValStr] = make(map[uint32][]float64)
			}

			for timestamp, entries := range timeSeries {
				for _, entry := range entries {
					grpIDToEntryMap[matchingLabelValStr][timestamp] = append(grpIDToEntryMap[matchingLabelValStr][timestamp], entry.runningVal)
				}
			}
		}

		// Compute standard variance or deviation for each group
		for grpID, entry := range grpIDToEntryMap {
			grpID = r.MetricName + "{" + grpID
			r.Results[grpID] = make(map[uint32]float64)
			for timestamp, values := range entry {
				resVal := utils.CalculateStandardVariance(values)
				if aggregation.AggregatorFunction == segutils.Stddev {
					resVal = math.Sqrt(resVal)
				}
				r.Results[grpID][timestamp] = resVal
			}
		}

	} else { // Without using group by, perform aggregation for all values at each timestamp.
		for _, timeSeries := range seriesEntriesMap {
			for timestamp, entries := range timeSeries {
				for _, entry := range entries {
					timestampToVals[timestamp] = append(timestampToVals[timestamp], entry.runningVal)
				}
			}
		}

		resultMap := make(map[uint32]float64)

		for timestamp, values := range timestampToVals {
			resVal := utils.CalculateStandardVariance(values)
			if aggregation.AggregatorFunction == segutils.Stddev {
				resVal = math.Sqrt(resVal)
			}
			resultMap[timestamp] = resVal
		}

		r.Results[r.MetricName+"{"] = resultMap
	}

	r.State = AGGREGATED
}

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
	"strings"
	"sync"
	"time"

	pql "github.com/influxdata/promql/v2"
	"github.com/influxdata/promql/v2/pkg/labels"
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

/*
Represents the results for a running query

Depending on the State the stored information is different:
  - SERIES_READING: maps a tsid to the all raw read dp & downsampled times
  - DOWNSAMPLING: maps a group to all downsampled series results.  This downsampled series may have repeated timestamps from different tsids
  - AGGREGATING: maps a groupid to the resulting aggregated values
*/
type MetricsResult struct {
	// maps tsid to the raw read series (with downsampled timestamp)
	AllSeries map[uint64]*Series

	// maps groupid to all raw downsampled series. This downsampled series may have repeated timestamps from different tsids
	DsResults map[string]*DownsampleSeries
	// maps groupid to a map of ts to value. This aggregates DsResults based on the aggregation function
	Results map[string]map[uint32]float64

	State bucketState

	rwLock  *sync.RWMutex
	ErrList []error
}

/*
Inits a metricsresults holder

TODO: depending on metrics query, have different cases on how to resolve dps
*/
func InitMetricResults(mQuery *structs.MetricsQuery, qid uint64) *MetricsResult {
	return &MetricsResult{
		AllSeries: make(map[uint64]*Series),
		rwLock:    &sync.RWMutex{},
		ErrList:   make([]error, 0),
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
func (r *MetricsResult) AggregateResults(parallelism int) []error {
	if r.State != DOWNSAMPLING {
		return []error{errors.New("results is not in downsampling state")}
	}

	r.Results = make(map[string]map[uint32]float64)
	lock := &sync.Mutex{}
	wg := &sync.WaitGroup{}

	errorLock := &sync.Mutex{}
	errors := make([]error, 0)

	var idx int
	for grpID, runningDS := range r.DsResults {
		wg.Add(1)
		go func(grp string, ds *DownsampleSeries) {
			defer wg.Done()

			grpVal, err := ds.Aggregate()
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

/*
Apply range function to results for series sharing a groupid.
*/
func (r *MetricsResult) ApplyRangeFunctionsToResults(parallelism int, function segutils.RangeFunctions) error {

	lock := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	errList := []error{} // Thread-safe list of errors

	var idx int
	for grpID, timeSeries := range r.Results {
		wg.Add(1)
		go func(grp string, ts map[uint32]float64, function segutils.RangeFunctions) {
			defer wg.Done()
			grpVal, err := ApplyRangeFunction(ts, function)
			if err != nil {
				lock.Lock()
				errList = append(errList, err)
				lock.Unlock()
				return
			}
			lock.Lock()
			r.Results[grp] = grpVal
			lock.Unlock()
		}(grpID, timeSeries, function)
		idx++
		if idx%parallelism == 0 {
			wg.Wait()
		}
	}

	wg.Wait()
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
		tagValues := strings.Split(grpId, tsidtracker.TAG_VALUE_DELIMITER_STR)
		if len(tagKeys) != len(tagValues)-1 {
			err := errors.New("GetResults: the length of tag key and tag value pair must match")
			return nil, err
		}

		for index, val := range tagValues[:len(tagValues)-1] {
			tags[tagKeys[index]] = val
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
func (r *MetricsResult) GetResultsPromQl(mQuery *structs.MetricsQuery, pqlQuerytype pql.ValueType) ([]*structs.MetricsQueryResponsePromQl, error) {
	if r.State != AGGREGATED {
		return nil, errors.New("results is not in aggregated state")
	}
	var pqldata structs.Data
	var series pql.Series
	var label structs.Label

	retVal := make([]*structs.MetricsQueryResponsePromQl, len(r.Results))
	idx := 0
	uniqueTagKeys := make(map[string]bool)
	tagKeys := make([]string, 0)
	for _, tag := range mQuery.TagsFilters {
		if _, ok := uniqueTagKeys[tag.TagKey]; !ok {
			uniqueTagKeys[tag.TagKey] = true
			tagKeys = append(tagKeys, tag.TagKey)
		}
	}
	switch pqlQuerytype {
	case pql.ValueTypeVector:
		pqldata.ResultType = pql.ValueType("vector")
		for grpId, results := range r.Results {
			tags := make(map[string]string)
			tagValues := strings.Split(grpId, tsidtracker.TAG_VALUE_DELIMITER_STR)

			if len(tagKeys) != len(tagValues)-1 {
				err := errors.New("GetResultsPromQl: the length of tag key and tag value pair must match")
				return nil, err
			}
			for index, val := range tagValues[:len(tagValues)-1] {
				tags[tagKeys[index]] = val
				label.Name = tagKeys[index]
				label.Value = val
				series.Metric = append(series.Metric, labels.Label(label))
			}
			label.Name = "__name__"
			label.Value = mQuery.MetricName
			series.Metric = append(series.Metric, labels.Label(label))
			for k, v := range results {
				var point pql.Point
				point.T = int64(k)
				point.V = v
				series.Points = append(series.Points, point)
			}
			pqldata.Result = append(pqldata.Result, series)

			retVal[idx] = &structs.MetricsQueryResponsePromQl{
				Status: "success",
				Data:   pqldata,
			}
			pqldata.Result = nil
			series = pql.Series{}
			idx++
		}
	default:
		return retVal, fmt.Errorf("GetResultsPromQl: Unsupported PromQL query result type")
	}
	return retVal, nil
}

func (r *MetricsResult) GetResultsPromQlForUi(mQuery *structs.MetricsQuery, pqlQuerytype pql.ValueType, startTime, endTime, interval uint32) (utils.MetricsStatsResponseInfo, error) {
	var httpResp utils.MetricsStatsResponseInfo
	httpResp.AggStats = make(map[string]map[string]interface{})
	if r.State != AGGREGATED {
		return utils.MetricsStatsResponseInfo{}, errors.New("results is not in aggregated state")
	}
	uniqueTagKeys := make(map[string]bool)
	tagKeys := make([]string, 0)
	for _, tag := range mQuery.TagsFilters {
		if _, ok := uniqueTagKeys[tag.TagKey]; !ok {
			uniqueTagKeys[tag.TagKey] = true
			tagKeys = append(tagKeys, tag.TagKey)
		}
	}
	for grpId, results := range r.Results {
		tagValues := strings.Split(grpId, tsidtracker.TAG_VALUE_DELIMITER_STR)
		if len(tagKeys) != len(tagValues)-1 { // Subtract 1 because grpId has a delimiter after the last value
			err := errors.New("GetResults: the length of tag key and tag value pair must match")
			return httpResp, err
		}
		groupId := mQuery.MetricName + "{"
		for index, val := range tagValues[:len(tagValues)-1] {
			groupId += fmt.Sprintf("%v=\"%v\",", tagKeys[index], val)
		}
		if last := len(groupId) - 1; last >= 0 && groupId[last] == ',' {
			groupId = groupId[:last]
		}
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
	case pql.ValueTypeVector:
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

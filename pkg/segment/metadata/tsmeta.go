// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package metadata

import (
	"sync"
	"time"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/utils"
)

const (
	TWO = 2
)

/*
Returns all tagTrees that we need to search and what MetricsSegments & MetricsBlocks pass time filtering.

Returns map[string][]*structs.MetricSearchRequest, mapping a tagsTree to all MetricSearchRequest that pass time filtering
*/
func GetMetricsSegmentRequests(tRange *dtu.MetricsTimeRange, querySummary *summary.QuerySummary, orgid utils.Option[int64]) (map[string][]*structs.MetricsSearchRequest, error) {
	sTime := time.Now()

	retUpdate := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	parallelism := int(config.GetParallelism())
	retVal := make(map[string][]*structs.MetricsSearchRequest)
	var gErr error
	org, orgPresent := orgid.Get()

	globalMetricsMetadata.updateLock.Lock()
	defer globalMetricsMetadata.updateLock.Unlock()

	for i, mSegMeta := range globalMetricsMetadata.sortedMetricsSegmentMeta {
		if !tRange.CheckRangeOverLap(mSegMeta.EarliestEpochSec, mSegMeta.LatestEpochSec) || (orgPresent && mSegMeta.OrgId != org) {
			continue
		}
		wg.Add(1)
		go func(msm *MetricsSegmentMetadata) {
			defer wg.Done()
			var forceLoaded bool
			if !msm.loadedSearchMetadata {
				err := msm.LoadSearchMetadata()
				if err != nil {
					gErr = err
					return
				}
				forceLoaded = true
			}

			retBlocks := make(map[uint16]bool)
			for _, mbsu := range msm.mBlockSummary {
				if tRange.CheckRangeOverLap(mbsu.LowTs, mbsu.HighTs) {
					retBlocks[mbsu.Blknum] = true
				}
			}

			// copy of tag keys map
			allTagKeys := make(map[string]bool)
			for tk := range msm.TagKeys {
				allTagKeys[tk] = true
			}
			if len(retBlocks) == 0 {
				return
			}
			finalReq := &structs.MetricsSearchRequest{
				MetricsKeyBaseDir:    msm.MSegmentDir,
				BlocksToSearch:       retBlocks,
				BlkWorkerParallelism: uint(TWO),
				QueryType:            structs.METRICS_SEARCH,
				AllTagKeys:           allTagKeys,
			}

			retUpdate.Lock()
			_, ok := retVal[msm.TTreeDir]
			if !ok {
				retVal[msm.TTreeDir] = make([]*structs.MetricsSearchRequest, 0)
			}
			retVal[msm.TTreeDir] = append(retVal[msm.TTreeDir], finalReq)
			retUpdate.Unlock()

			if forceLoaded {
				msm.clearSearchMetadata()
			}
		}(mSegMeta)
		if i%parallelism == 0 {
			wg.Wait()
		}
	}
	wg.Wait()
	timeElapsed := time.Since(sTime)
	querySummary.UpdateTimeGettingRotatedSearchRequests(timeElapsed)
	return retVal, gErr
}

func GetMetricSegmentsOverTheTimeRange(tRange *dtu.MetricsTimeRange, orgid utils.Option[int64]) map[string]*structs.MetricsMeta {
	globalMetricsMetadata.updateLock.Lock()
	defer globalMetricsMetadata.updateLock.Unlock()

	metricsSegMeta := make(map[string]*structs.MetricsMeta)
	org, orgPresent := orgid.Get()

	for _, mSegMeta := range globalMetricsMetadata.sortedMetricsSegmentMeta {
		if !tRange.CheckRangeOverLap(mSegMeta.EarliestEpochSec, mSegMeta.LatestEpochSec) || (orgPresent && mSegMeta.OrgId != org) {
			continue
		}
		metricsSegMeta[mSegMeta.MSegmentDir] = &mSegMeta.MetricsMeta
	}

	return metricsSegMeta
}

func GetUniqueTagKeysForRotated(tRange *dtu.MetricsTimeRange, myid int64) (map[string]struct{}, error) {
	mSegmentsMeta := GetMetricSegmentsOverTheTimeRange(tRange, utils.Some(myid))

	uniqueTagKeys := make(map[string]struct{})

	// Iterate over the metadata and extract unique tag keys
	for _, meta := range mSegmentsMeta {
		for key := range meta.TagKeys {
			uniqueTagKeys[key] = struct{}{}
		}
	}

	return uniqueTagKeys, nil
}

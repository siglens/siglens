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

package metadata

import (
	"sync"
	"time"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/structs"
)

/*
Returns all tagTrees that we need to search and what MetricsSegments & MetricsBlocks pass time filtering.

Returns map[string][]*structs.MetricSearchRequest, mapping a tagsTree to all MetricSearchRequest that pass time filtering
*/
func GetMetricsSegmentRequests(mName string, tRange *dtu.MetricsTimeRange, querySummary *summary.QuerySummary, orgid uint64) (map[string][]*structs.MetricsSearchRequest, error) {
	sTime := time.Now()

	retUpdate := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	parallelism := int(config.GetParallelism())
	retVal := make(map[string][]*structs.MetricsSearchRequest)
	var gErr error

	globalMetricsMetadata.updateLock.Lock()
	defer globalMetricsMetadata.updateLock.Unlock()

	for i, mSegMeta := range globalMetricsMetadata.sortedMetricsSegmentMeta {
		if !tRange.CheckRangeOverLap(mSegMeta.EarliestEpochSec, mSegMeta.LatestEpochSec) || mSegMeta.OrgId != orgid {
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
				MetricsKeyBaseDir: msm.MSegmentDir,
				BlocksToSearch:    retBlocks,
				Parallelism:       uint(config.GetParallelism()),
				QueryType:         structs.METRICS_SEARCH,
				AllTagKeys:        allTagKeys,
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

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

package metadata

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/reader/microreader"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
)

/*
	This file defines holder structs and rebalancing methods for metrics semgments

	Note, the memory allocation of metricssegment is idependent of metadata.go
*/

/*
Holder
*/
type allMetricsSegmentMetadata struct {

	// sortedMetricsSegmentMeta in sorted order of descending latest time, used for global memory limiting
	sortedMetricsSegmentMeta []*MetricsSegmentMetadata

	// metricsSegmentMetaMap maps a metrics base dir to its metadata
	metricsSegmentMetaMap map[string]*MetricsSegmentMetadata

	// metadata update lock
	updateLock *sync.RWMutex
}

var globalMetricsMetadata *allMetricsSegmentMetadata = &allMetricsSegmentMetadata{
	sortedMetricsSegmentMeta: make([]*MetricsSegmentMetadata, 0),
	metricsSegmentMetaMap:    make(map[string]*MetricsSegmentMetadata),
	updateLock:               &sync.RWMutex{},
}

type MetricsSegmentMetadata struct {
	structs.MetricsMeta // original read metrics meta
	MetricsSegmentSearchMetadata
}

type MetricsSegmentSearchMetadata struct {
	mBlockSummary        []*structs.MBlockSummary
	mBlockSize           uint64
	loadedSearchMetadata bool
}

func BulkAddMetricsSegment(allMMetadata []*MetricsSegmentMetadata) {
	globalMetricsMetadata.bulkAddMetricsMicroIndex(allMMetadata)
}

func InitMetricsMicroIndex(mMeta *structs.MetricsMeta) *MetricsSegmentMetadata {

	mm := &MetricsSegmentMetadata{
		MetricsMeta: *mMeta,
	}
	mm.loadedSearchMetadata = false
	mm.initMetadataSize()
	return mm
}

func (mm *MetricsSegmentMetadata) initMetadataSize() {
	searchMetadataSize := uint64(0)
	searchMetadataSize += uint64(mm.NumBlocks * structs.SIZE_OF_MBSUM) // block summaries
	mm.mBlockSize = searchMetadataSize
}

func (mm *allMetricsSegmentMetadata) bulkAddMetricsMicroIndex(allMMetadata []*MetricsSegmentMetadata) {
	mm.updateLock.Lock()
	defer mm.updateLock.Unlock()

	for _, newMMeta := range allMMetadata {
		if _, ok := mm.metricsSegmentMetaMap[newMMeta.MSegmentDir]; ok {
			continue
		}
		mm.sortedMetricsSegmentMeta = append(mm.sortedMetricsSegmentMeta, newMMeta)
		mm.metricsSegmentMetaMap[newMMeta.MSegmentDir] = newMMeta
		GlobalSegStoreSummary.IncrementTotalMetricsSegmentCount()
	}
	sort.Slice(mm.sortedMetricsSegmentMeta, func(i, j int) bool {
		return mm.sortedMetricsSegmentMeta[i].LatestEpochSec > mm.sortedMetricsSegmentMeta[j].LatestEpochSec
	})
}

func (mm *allMetricsSegmentMetadata) rebalanceMetricsSsm(ssmSizeBytes uint64) {

	sTime := time.Now()

	mm.updateLock.RLock()
	defer mm.updateLock.RUnlock()
	searchIndex := mm.getSsmMaxIndicesToLoad(ssmSizeBytes)
	evicted := mm.evictSsmPastIndices(searchIndex)

	inMemSize, inMemSearchMetaCount, newloaded := mm.loadSsmUntilIndex(searchIndex)

	log.Infof("rebalanceMetricsSsm SSM, inMem: %+v SSM, allocated: %+v MB, evicted: %v, newloaded: %v, took: %vms",
		inMemSearchMetaCount, utils.ConvertUintBytesToMB(inMemSize),
		evicted, newloaded, int(time.Since(sTime).Milliseconds()))

	GlobalSegStoreSummary.SetInMemoryMetricsSearchmetadataCount(uint64(inMemSearchMetaCount))
	GlobalSegStoreSummary.SetInMemoryMetricsSsmSizeMB(utils.ConvertUintBytesToMB(inMemSize))
}

/*
Returns the max indices that should have SSM(segmentsearchmeta) loaded
*/
func (mm *allMetricsSegmentMetadata) getSsmMaxIndicesToLoad(totalMem uint64) int {

	numBlocks := len(mm.sortedMetricsSegmentMeta)
	totalMBAllocated := uint64(0)
	maxSearchIndex := 0
	for ; maxSearchIndex < numBlocks; maxSearchIndex++ {
		searchMetadataSize := mm.sortedMetricsSegmentMeta[maxSearchIndex].mBlockSize
		if searchMetadataSize+totalMBAllocated > totalMem {
			break
		}
		totalMBAllocated += searchMetadataSize
	}

	return maxSearchIndex
}

func (mm *allMetricsSegmentMetadata) evictSsmPastIndices(searchIndex int) int {

	idxToClear := make([]int, 0)
	for i := searchIndex; i < len(mm.sortedMetricsSegmentMeta); i++ {
		if mm.sortedMetricsSegmentMeta[i].loadedSearchMetadata {
			idxToClear = append(idxToClear, i)
		}
	}

	if len(idxToClear) > 0 {
		for _, idx := range idxToClear {
			mm.sortedMetricsSegmentMeta[idx].clearSearchMetadata()
		}
	}
	return len(idxToClear)
}

// Returns total in memory size in bytes, total search metadata in memory
func (mm *allMetricsSegmentMetadata) loadSsmUntilIndex(searchMetaIdx int) (uint64, int, int) {
	totalSize := uint64(0)
	totalSearchMetaCount := int(0)

	idxToLoad := make([]int, 0)
	for i := 0; i < searchMetaIdx; i++ {
		if !mm.sortedMetricsSegmentMeta[i].loadedSearchMetadata {
			idxToLoad = append(idxToLoad, i)
		} else {
			totalSize += mm.sortedMetricsSegmentMeta[i].mBlockSize
			totalSearchMetaCount += 1
		}
	}

	if len(idxToLoad) > 0 {
		a, b := mm.loadParallel(idxToLoad)
		totalSize += a
		totalSearchMetaCount += b
	}

	return totalSize, totalSearchMetaCount, len(idxToLoad)
}

func (mm *MetricsSegmentMetadata) LoadSearchMetadata() error {
	if mm.loadedSearchMetadata {
		return nil
	}
	bSumFname := fmt.Sprintf("%s.mbsu", mm.MSegmentDir)
	blockSum, err := microreader.ReadMetricsBlockSummaries(bSumFname)
	if err != nil {
		mm.clearSearchMetadata()
		log.Errorf("LoadSearchMetadata: unable to read the metrics block summaries. Error: %v", err)
		return err
	}
	mm.loadedSearchMetadata = true
	mm.mBlockSummary = blockSum
	return nil
}

func (mm *MetricsSegmentMetadata) clearSearchMetadata() {
	mm.loadedSearchMetadata = false
	mm.mBlockSummary = nil
}

/*
Caller is responsible for acquiring the right read locks

Parameters:

	idxToLoad: indices in the sortedMetricsSegmentMeta to load metrics block summary for

Returns:

	totalSize:
	totalEntities:
*/
func (mm *allMetricsSegmentMetadata) loadParallel(idxToLoad []int) (uint64, int) {
	totalSize := uint64(0)
	totalEntities := int(0)

	wg := &sync.WaitGroup{}
	var err error
	parallelism := int(config.GetParallelism())

	for i, idx := range idxToLoad {
		wg.Add(1)
		go func(myIdx int, rbufIdx int) {
			err = mm.sortedMetricsSegmentMeta[myIdx].LoadSearchMetadata()
			if err != nil {
				log.Errorf("loadParallel: failed to load SSM at index %d. Error: %v", myIdx, err)
			}
			wg.Done()
		}(idx, i%parallelism)
		if i%parallelism == 0 {
			wg.Wait()
		}
	}
	wg.Wait()

	for _, idx := range idxToLoad {
		if mm.sortedMetricsSegmentMeta[idx].loadedSearchMetadata {
			totalSize += mm.sortedMetricsSegmentMeta[idx].mBlockSize
			totalEntities += 1
		}
	}
	return totalSize, totalEntities
}

func DeleteMetricsSegmentKey(dirPath string) error {
	err := globalMetricsMetadata.deleteMetricsSegmentKey(dirPath)
	if err != nil {
		log.Errorf("DeleteMetricsSegmentKey: err deleting the metrics segment %v. Error:%v", dirPath, err)
		return err
	}
	return nil
}

func (hm *allMetricsSegmentMetadata) deleteMetricsSegmentKey(dirPath string) error {
	hm.updateLock.Lock()
	defer hm.updateLock.Unlock()

	segment, ok := globalMetricsMetadata.metricsSegmentMetaMap[dirPath]
	if !ok {
		return fmt.Errorf("deleteSegmentKey: metrics segment directory %s not found", dirPath)
	}

	for i, m := range globalMetricsMetadata.sortedMetricsSegmentMeta {
		if m == segment {
			globalMetricsMetadata.sortedMetricsSegmentMeta = append(globalMetricsMetadata.sortedMetricsSegmentMeta[:i], globalMetricsMetadata.sortedMetricsSegmentMeta[i+1:]...)
			break
		}
	}
	delete(globalMetricsMetadata.metricsSegmentMetaMap, dirPath)

	GlobalSegStoreSummary.DecrementTotalMetricsSegmentCount()

	return nil
}

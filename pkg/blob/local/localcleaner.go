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

package local

import (
	"container/heap"
	"fmt"
	"sort"
	"time"

	"github.com/shirou/gopsutil/v3/disk"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
)

func initLocalCleaner() {
	// initialize sort
	heap.Init(&allSortedSegSetFiles)
	for segSetFile := range segSetKeys {
		segSet := segSetKeys[segSetFile]
		if segSetFile != "" {
			allSortedSegSetFiles.Push(segSet)
		}
	}

	go removeFilesForMemoryLoop()

}

func removeFilesForMemoryLoop() {
	for {
		freeMemNeeded := getMemToBeFreed()
		log.Debugf("removeFilesForMemoryLoop: Free memory needed: %vMB", utils.ConvertUintBytesToMB(freeMemNeeded))
		if freeMemNeeded > 0 {
			err := removeFilesForDiskSpace(freeMemNeeded)
			if err != nil {
				log.Errorf("removeFilesForMemoryLoop: Error making space: %v", err)
			}
		}
		time.Sleep(time.Second * 30)
	}
}

func getMemToBeFreed() uint64 {
	s, err := disk.Usage(config.GetDataPath())
	if err != nil {
		log.Errorf("getMemToBeFreed: Error getting disk usage for / err=%v", err)
		return 0
	}
	allowedVolume := (s.Total * config.GetDataDiskThresholdPercent()) / 100
	if s.Used < allowedVolume {
		return 0
	}
	return s.Used - allowedVolume
}

/*
Gets all candidates for removal based on sizeToRemove

Canidates are all present and not in use SegSetData that together sum up at least sizeToRemove
If we have more elements after summing to sizeToRemove, add them to the canidate list for access time sorting
*/
func getCanidatesForRemoval(sizeToRemove uint64) []*structs.SegSetData {

	segSetKeysLock.Lock()
	defer segSetKeysLock.Unlock()
	topNCandidates := make([]*structs.SegSetData, 0)
	inUseFiles := make([]*structs.SegSetData, 0)
	candidateSize := uint64(0)
	idx := 0
	numCandidates := allSortedSegSetFiles.Len()
	for candidateSize < sizeToRemove && idx < numCandidates {
		candidate := heap.Pop(&allSortedSegSetFiles).(*structs.SegSetData)
		if !candidate.InUse {
			candidateSize += candidate.Size
			topNCandidates = append(topNCandidates, candidate)
		} else {
			inUseFiles = append(inUseFiles, candidate)
		}
		idx++
	}
	// if we have filled up canidateSize with sizeToRemove and more elements exist in the heap,
	// add them to the candidates so they can be sorted by access time
	if idx < numCandidates {
		if finalCount := idx * 2; finalCount < numCandidates {
			for idx < finalCount && idx < numCandidates {
				candidate := heap.Pop(&allSortedSegSetFiles).(*structs.SegSetData)
				if !candidate.InUse {
					topNCandidates = append(topNCandidates, candidate)
				} else {
					inUseFiles = append(inUseFiles, candidate)
				}
				idx++
			}
		}
	}
	// re-add element to the heap
	for _, inUse := range inUseFiles {
		heap.Push(&allSortedSegSetFiles, inUse)
	}
	return topNCandidates
}

// Removes sizeToRemove bytes from disk to make space
func removeFilesForDiskSpace(sizeToRemove uint64) error {
	recreateLocalHeap()
	if allSortedSegSetFiles.Len() == 0 {
		log.Infof("removeFilesForDiskSpace: No more segset to delete. Cannot make space. sizeToRemove=%v", sizeToRemove)
		return fmt.Errorf("no more segset to delete. cannot make space")
	}
	log.Infof("removeFilesForDiskSpace: Data disk threshold reached. Removing %d bytes.", sizeToRemove)
	memFreed := uint64(0)
	topNCandidates := getCanidatesForRemoval(sizeToRemove)

	sort.SliceStable(topNCandidates, func(i, j int) bool {
		// whichever has been accessed the least, should be deleted first
		return topNCandidates[i].AccessTime < topNCandidates[j].AccessTime
	})

	removedCount := 0
	failedToDelete := 0
	log.Infof("removeFilesForDiskSpace: Found %d candidates for removal out of %d", len(topNCandidates), len(segSetKeys))
	for i := 0; i < len(topNCandidates) && memFreed < sizeToRemove; i++ {
		candidate := topNCandidates[i]
		memFreed += candidate.Size
		segSetFile := candidate.SegSetFileName
		err := DeleteLocal(segSetFile)
		if err != nil {
			log.Errorf("removeFilesForDiskSpace: Error deleting segSetFile %v Error: %v", segSetFile, err)
			failedToDelete++
			memFreed -= candidate.Size
		} else {
			log.Debugf("removeFilesForDiskSpace: deleted segSetFile %v", segSetFile)
			removedCount++
		}
	}
	log.Infof("removeFilesForDiskSpace: Successfully removed %+v MB of segSetFiles from disk across %d files. Failed to remove %d",
		utils.ConvertUintBytesToMB(memFreed), removedCount, failedToDelete)

	// recreate the heap with all elements in the map
	recreateLocalHeap()
	if memFreed == 0 {
		return fmt.Errorf("failed to delete any memory to accommodate size of %+v", sizeToRemove)
	}

	return nil
}

func recreateLocalHeap() {
	// recreate the heap with all elements in the map
	segSetKeysLock.Lock()
	defer segSetKeysLock.Unlock()
	allSortedSegSetFiles = make([]*structs.SegSetData, 0)
	heap.Init(&allSortedSegSetFiles)
	for segSetFile, segSet := range segSetKeys {
		if segSetFile == "" {
			delete(segSetKeys, segSetFile)
			continue
		}
		allSortedSegSetFiles.Push(segSet)
	}
}

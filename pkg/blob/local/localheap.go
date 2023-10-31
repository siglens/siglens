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

package local

import (
	"sync"

	"github.com/siglens/siglens/pkg/segment/structs"
)

type SortedMTimeSegSetFiles []*structs.SegSetData

var allSegSetFilesLock sync.RWMutex
var allSortedSegSetFiles SortedMTimeSegSetFiles

func (s SortedMTimeSegSetFiles) Len() int {
	allSegSetFilesLock.RLock()
	defer allSegSetFilesLock.RUnlock()
	return len(s)
}

func (s SortedMTimeSegSetFiles) Less(i, j int) bool {
	allSegSetFilesLock.RLock()
	defer allSegSetFilesLock.RUnlock()
	return s[i].AccessTime < s[j].AccessTime
}

func (s SortedMTimeSegSetFiles) Swap(i, j int) {
	allSegSetFilesLock.RLock()
	defer allSegSetFilesLock.RUnlock()
	s[i], s[j] = s[j], s[i]
}

func (s *SortedMTimeSegSetFiles) Push(x interface{}) {
	allSegSetFilesLock.Lock()
	defer allSegSetFilesLock.Unlock()
	*s = append(*s, x.(*structs.SegSetData))
}

func (s *SortedMTimeSegSetFiles) Pop() interface{} {
	allSegSetFilesLock.Lock()
	defer allSegSetFilesLock.Unlock()
	old := *s
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*s = old[0 : n-1]
	return item
}

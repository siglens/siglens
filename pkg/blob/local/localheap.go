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

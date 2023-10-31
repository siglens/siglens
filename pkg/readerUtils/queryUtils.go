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

package readerUtils

import (
	"sync"
	"sync/atomic"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/utils"
)

var globalQid uint64 = 0
var qidLock sync.Mutex

const DefaultBucketCount uint64 = 10
const DefaultMetaLookbackMins = 129600 //90 days

func GetNextQid() uint64 {
	qidLock.Lock()
	defer qidLock.Unlock() // prevents two queries from returning the same GlobalQid (small chance w/o locks)
	atomic.AddUint64(&globalQid, 1)
	return globalQid
}

func GetESDefaultQueryTimeRange() *dtu.TimeRange {
	currTime := utils.GetCurrentTimeInMs()
	lookbackInMilliSec := 60000 * uint64(DefaultMetaLookbackMins)
	return &dtu.TimeRange{
		StartEpochMs: (currTime - lookbackInMilliSec),
		EndEpochMs:   currTime,
	}
}

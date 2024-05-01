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

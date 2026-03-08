// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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

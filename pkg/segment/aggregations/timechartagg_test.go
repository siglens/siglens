// Copyright (c) 2021-2025 SigScalr, Inc.
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

package aggregations

import (
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/stretchr/testify/assert"
)

func Test_Range(t *testing.T) {
	r := &Range{
		start: 10,
		end:   20,
		step:  2,
	}

	assert.Equal(t, 10, int(FindTimeRangeBucket(r, 10)))
	assert.Equal(t, 10, int(FindTimeRangeBucket(r, 11)))
	assert.Equal(t, 12, int(FindTimeRangeBucket(r, 12)))
	assert.Equal(t, 18, int(FindTimeRangeBucket(r, 18)))
	assert.Equal(t, 18, int(FindTimeRangeBucket(r, 19)))
	assert.Equal(t, 18, int(FindTimeRangeBucket(r, 20))) // end is exclusive
}

func Test_RangeMatchesFindBucket(t *testing.T) {
	timeBucket := &structs.TimeBucket{
		StartTime:      10,
		EndTime:        20,
		IntervalMillis: 3, // Will not land exactly on the end time.
	}

	newBuckets := GenerateTimeRangeBuckets(timeBucket)
	oldBuckets := oldGenerateTimeRangeBuckets(timeBucket)

	// Check if the new buckets are the same as the old buckets
	for i := timeBucket.StartTime; i < timeBucket.EndTime; i++ {
		newBucket := FindTimeRangeBucket(newBuckets, i)
		oldBucket := oldFindTimeRangeBucket(oldBuckets, i, timeBucket.IntervalMillis)

		assert.Equal(t, oldBucket, newBucket, "failed for i=%v", i)
	}

	timeBucket = &structs.TimeBucket{
		StartTime:      10,
		EndTime:        20,
		IntervalMillis: 5, // Will land exactly on the end time.
	}

	newBuckets = GenerateTimeRangeBuckets(timeBucket)
	oldBuckets = oldGenerateTimeRangeBuckets(timeBucket)

	// Check if the new buckets are the same as the old buckets
	for i := timeBucket.StartTime; i < timeBucket.EndTime; i++ {
		newBucket := FindTimeRangeBucket(newBuckets, i)
		oldBucket := oldFindTimeRangeBucket(oldBuckets, i, timeBucket.IntervalMillis)

		assert.Equal(t, oldBucket, newBucket, "failed for i=%v", i)
	}
}

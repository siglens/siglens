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

package processor

import (
	"io"
	"testing"

	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_Head_WithLimit(t *testing.T) {
	const headLimit = 2
	dp := NewHeadDP(toputils.NewOptionWithValue[uint64](headLimit))
	stream := &mockStreamer{
		allRecords: map[string][]utils.CValueEnclosure{
			"col1": {
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "c"},
			},
		},
		qid: 0,
	}

	dp.streams = append(dp.streams, &cachedStream{stream, nil, false})
	log.Errorf("andrew dp streams: %+v", dp.streams)

	totalFetched := 0
	numFetches := 0
	for {
		log.Errorf("andrew iter %d", numFetches)
		iqr, err := dp.Fetch()
		if err != io.EOF {
			assert.NoError(t, err)
		}

		totalFetched += iqr.NumberOfRecords()
		if err == io.EOF {
			break
		}

		numFetches++
		if numFetches > headLimit {
			t.Fatalf("Number of fetches exceeded head limit")
		}
	}

	assert.Equal(t, headLimit, totalFetched)
}

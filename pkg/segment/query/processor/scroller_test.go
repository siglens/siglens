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
	"context"
	"io"
	"testing"
	"time"

	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_Scroll(t *testing.T) {
	err := initTestConfig(t)
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go query.PullQueriesToRun(ctx)
	defer cancel()

	qid := uint64(0)
	_, err = query.StartQuery(qid, true, nil, false)
	assert.NoError(t, err)
	time.Sleep(1 * time.Second)

	query.InitProgressForRRCCmd(6, qid)

	scrollFrom := uint64(3)
	dp := NewScrollerDP(scrollFrom, qid)
	stream := &mockStreamer{
		allRecords: map[string][]segutils.CValueEnclosure{
			"col1": {
				segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "a"},
				segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "b"},
				segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "c"},
				segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "d"},
				segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "e"},
				segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "f"},
			},
		},
		qid: 0,
	}

	dp.streams = append(dp.streams, &CachedStream{stream, nil, false})

	var finalIQR *iqr.IQR

	for {
		iqr, err := dp.Fetch()
		if err != io.EOF {
			assert.NoError(t, err)
		}
		if err == io.EOF {
			break
		}

		if finalIQR == nil {
			finalIQR = iqr
		} else {
			appendErr := finalIQR.Append(iqr)
			assert.NoError(t, appendErr)
		}
	}

	expectedValues := []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING, CVal: "d"},
		{Dtype: segutils.SS_DT_STRING, CVal: "e"},
		{Dtype: segutils.SS_DT_STRING, CVal: "f"},
	}

	assert.Equal(t, 3, finalIQR.NumberOfRecords())
	colValues, err := finalIQR.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(colValues))
	assert.Equal(t, expectedValues, colValues)

	query.DeleteQuery(qid)
}

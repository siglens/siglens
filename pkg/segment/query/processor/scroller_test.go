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

	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_Scroll(t *testing.T) {
	qid := uint64(0)
	_, err := query.StartQuery(qid, true, nil)
	assert.NoError(t, err)

	query.InitProgressForRRCCmd(6, qid)

	scrollFrom := uint64(3)
	dp := NewScrollerDP(scrollFrom, qid)
	stream := &mockStreamer{
		allRecords: map[string][]utils.CValueEnclosure{
			"col1": {
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "c"},
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "d"},
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "e"},
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "f"},
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

	expectedValues := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "d"},
		{Dtype: utils.SS_DT_STRING, CVal: "e"},
		{Dtype: utils.SS_DT_STRING, CVal: "f"},
	}

	assert.Equal(t, 3, finalIQR.NumberOfRecords())
	colValues, err := finalIQR.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(colValues))
	assert.Equal(t, expectedValues, colValues)

	query.DeleteQuery(qid)
}

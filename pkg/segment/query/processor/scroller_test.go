// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package processor

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
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
		allRecords: map[string][]sutils.CValueEnclosure{
			"col1": {
				sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a"},
				sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b"},
				sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "c"},
				sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "d"},
				sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "e"},
				sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "f"},
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

	expectedValues := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "d"},
		{Dtype: sutils.SS_DT_STRING, CVal: "e"},
		{Dtype: sutils.SS_DT_STRING, CVal: "f"},
	}

	assert.Equal(t, 3, finalIQR.NumberOfRecords())
	colValues, err := finalIQR.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, 3, len(colValues))
	assert.Equal(t, expectedValues, colValues)

	query.DeleteQuery(qid)
}

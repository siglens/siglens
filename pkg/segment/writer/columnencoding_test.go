// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package writer

import (
	"testing"

	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func TestEncodeDictionaryColumn(t *testing.T) {
	type args struct {
		columnValueMap map[sutils.CValueDictEnclosure][]uint16
		riValue        map[string]*RangeIndex
	}
	var recNum uint16 = 0
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			args: args{columnValueMap: map[sutils.CValueDictEnclosure][]uint16{
				{Dtype: sutils.SS_DT_STRING, CValString: "test"}: {5, 10, 11},
				{Dtype: sutils.SS_DT_STRING, CValString: "abc"}:  {3},
				{Dtype: sutils.SS_DT_BOOL, CValBool: false}:      {6, 7, 9, 20, 21},
			}},
		},
		{
			args: args{columnValueMap: map[sutils.CValueDictEnclosure][]uint16{
				{Dtype: sutils.SS_DT_STRING, CValString: "abc"}:             {1},
				{Dtype: sutils.SS_DT_FLOAT, CValFloat64: 1.34}:              {32},
				{Dtype: sutils.SS_DT_UNSIGNED_NUM, CValUInt64: uint64(134)}: {3, 10},
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encodedBytes, _ := EncodeDictionaryColumn(tt.args.columnValueMap, tt.args.riValue, recNum)
			output := DecodeDictionaryColumn(encodedBytes)
			t.Logf("%v", tt.args.columnValueMap)
			t.Logf("%v", output)
			assert.Equal(t, tt.args.columnValueMap, output)
		})
	}
}

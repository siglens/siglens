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

package writer

import (
	"testing"

	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func TestEncodeDictionaryColumn(t *testing.T) {
	type args struct {
		columnValueMap map[segutils.CValueDictEnclosure][]uint16
		riValue        map[string]*RangeIndex
	}
	var recNum uint16 = 0
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			args: args{columnValueMap: map[segutils.CValueDictEnclosure][]uint16{
				{Dtype: segutils.SS_DT_STRING, CValString: "test"}: {5, 10, 11},
				{Dtype: segutils.SS_DT_STRING, CValString: "abc"}:  {3},
				{Dtype: segutils.SS_DT_BOOL, CValBool: false}:      {6, 7, 9, 20, 21},
			}},
		},
		{
			args: args{columnValueMap: map[segutils.CValueDictEnclosure][]uint16{
				{Dtype: segutils.SS_DT_STRING, CValString: "abc"}:             {1},
				{Dtype: segutils.SS_DT_FLOAT, CValFloat64: 1.34}:              {32},
				{Dtype: segutils.SS_DT_UNSIGNED_NUM, CValUInt64: uint64(134)}: {3, 10},
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

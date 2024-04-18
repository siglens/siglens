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

// Copyright (c) 2021-2024 SigScalr, Inc.
//
// # This file is part of SigLens Observability Solution
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

package structs

import (
	"testing"

	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func Test_initIQR(t *testing.T) {
	iqr := NewIQR(0)
	err := iqr.validate()
	assert.NoError(t, err)

	iqr = &IQR{}
	err = iqr.validate()
	assert.Error(t, err)
}

func Test_AppendRRCs(t *testing.T) {
	iqr := NewIQR(0)
	segKeyInfo1 := utils.SegKeyInfo{
		SegKeyEnc: 1,
	}
	encodingToSegKey := map[uint16]string{1: "segKey1"}
	rrcs := []*utils.RecordResultContainer{
		{SegKeyInfo: segKeyInfo1, BlockNum: 1, RecordNum: 1},
		{SegKeyInfo: segKeyInfo1, BlockNum: 1, RecordNum: 2},
		{SegKeyInfo: segKeyInfo1, BlockNum: 1, RecordNum: 3},
	}

	err := iqr.AppendRRCs(rrcs, encodingToSegKey)
	assert.NoError(t, err)
	assert.Equal(t, withRRCs, iqr.mode)
	assert.Equal(t, rrcs, iqr.rrcs)
	assert.Equal(t, encodingToSegKey, iqr.encodingToSegKey)
}

func Test_SetKnownValues_OnEmptyIQR(t *testing.T) {
	iqr := NewIQR(0)

	knownValues1 := map[string][]utils.CValueEnclosure{
		"col1": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
		},
	}
	err := iqr.AppendKnownValues(knownValues1)
	assert.NoError(t, err)
	assert.Equal(t, knownValues1, iqr.knownValues)
	assert.Equal(t, withoutRRCs, iqr.mode)

	// A different column with a different number of records should fail.
	knownValues2 := map[string][]utils.CValueEnclosure{
		"col2": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
		},
	}
	err = iqr.AppendKnownValues(knownValues2)
	assert.Error(t, err)

	// A different column with the same number of records should succeed.
	knownValues3 := map[string][]utils.CValueEnclosure{
		"col2": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "x"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "y"},
		},
	}
	err = iqr.AppendKnownValues(knownValues3)
	assert.NoError(t, err)
	assert.Equal(t, toputils.MergeMaps(knownValues1, knownValues3), iqr.knownValues)
	assert.Equal(t, withoutRRCs, iqr.mode)
}

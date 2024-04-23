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

package segread

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/axiomhq/hyperloglog"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/stretchr/testify/assert"
)

func Test_sstReadWrite(t *testing.T) {

	fname := "segkey-1.sst"

	_ = os.MkdirAll(path.Dir(fname), 0755)

	myHll := hyperloglog.New16()

	for i := 0; i < 3200; i++ {
		myHll.Insert([]byte(fmt.Sprintf("mystr:%v", i)))
	}

	myNums := structs.NumericStats{
		Min: utils.NumTypeEnclosure{Ntype: utils.SS_DT_SIGNED_NUM,
			IntgrVal: 456},
		Max: utils.NumTypeEnclosure{Ntype: utils.SS_DT_FLOAT,
			FloatVal: 23.4567},
		Sum: utils.NumTypeEnclosure{Ntype: utils.SS_DT_SIGNED_NUM,
			IntgrVal: 789},
	}

	inSst := structs.SegStats{
		IsNumeric: true,
		Count:     2345,
		Hll:       myHll,
		NumStats:  &myNums,
	}

	allSst := make(map[string]*structs.SegStats)

	allSst["col-a"] = &inSst
	allSst["col-b"] = &inSst

	ss := writer.SegStore{SegmentKey: "segkey-1",
		AllSst: allSst}

	err := ss.FlushSegStats()
	assert.Nil(t, err)

	allSstMap, err := ReadSegStats("segkey-1", 123)
	assert.Nil(t, err)

	outSst, pres := allSstMap["col-b"]

	assert.True(t, pres)

	assert.Equal(t, inSst.IsNumeric, outSst.IsNumeric)

	assert.Equal(t, inSst.Count, outSst.Count)

	assert.Equal(t, inSst.NumStats, outSst.NumStats)

	assert.Equal(t, inSst.Hll.Estimate(), outSst.Hll.Estimate())

	_ = os.RemoveAll(fname)
}

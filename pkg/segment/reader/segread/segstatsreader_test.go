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

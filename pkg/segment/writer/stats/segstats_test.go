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

package stats

import (
	"fmt"
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
	bbp "github.com/valyala/bytebufferpool"
)

func Test_addSegStatsStr(t *testing.T) {

	cname := "mycol1"
	sst := make(map[string]*structs.SegStats)
	numRecs := uint64(2000)

	bb := bbp.Get()

	for i := uint64(0); i < numRecs; i++ {
		AddSegStatsStr(sst, cname, fmt.Sprintf("%v", i), bb)
	}

	assert.Equal(t, numRecs, sst[cname].Count)
}

func Test_addSegStatsNums(t *testing.T) {

	cname := "mycol1"
	sst := make(map[string]*SegStats)
	bb := bbp.Get()

	AddSegStatsNums(sst, cname, SS_UINT64, 0, uint64(2345), 0, "2345", bb)
	assert.NotEqual(t, SS_DT_FLOAT, sst[cname].NumStats.Min.Ntype)
	assert.Equal(t, int64(2345), sst[cname].NumStats.Min.IntgrVal)

	AddSegStatsNums(sst, cname, SS_FLOAT64, 0, 0, float64(345.1), "345.1", bb)
	assert.Equal(t, SS_DT_FLOAT, sst[cname].NumStats.Min.Ntype)
	assert.Equal(t, float64(345.1), sst[cname].NumStats.Min.FloatVal)

	assert.Equal(t, SS_DT_FLOAT, sst[cname].NumStats.Sum.Ntype)
	assert.Equal(t, float64(345.1+2345), sst[cname].NumStats.Sum.FloatVal)

}

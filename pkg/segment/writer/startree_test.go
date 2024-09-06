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
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
	bbp "github.com/valyala/bytebufferpool"
)

var cases = []struct {
	input string
}{
	{
		`{
					"a":"val1",
					"b":"val1",
					"c":true,
					"d":"John",
				    "e": 1,
				    "f": 2
			}`,
	},
	{
		`{
					"a":"val1",
					"b":"val1",
					"c":true,
					"d":"John",
				   "e": 1,
				   "f": 2
			}`,
	},
	{
		`{
					"a":"val1",
					"b":"val1",
					"c":true,
					"d":"John",
				   "e": 1,
				   "f": 2
			}`,
	},
	{
		`{
					"a":"val1",
					"b":"val1",
					"c":true,
					"d":"John",
				   "e": 1,
				   "f": 2
			}`,
	},
	{
		`{
					"a":"val2",
					"b":"val3",
					"c":false,
					"d":"Paul",
				   "e": 1,
				   "f": 2
			}`,
	},
	{
		`{
					"a":"val1",
					"b":"val4",
					"c":true,
					"d":"John",
				   "e": 1,
				   "f": 2
			}`,
	},
	{
		`{
					"a":"val1",
					"b":"val2",
					"c":true,
					"d":"John",
				   "e": 1,
				   "f": 2
			}`,
	},
	{
		`{
					"a":"val1",
					"b":"val1",
					"c":true,
					"d":"John",
				   "e": 1,
				   "f": 2
			}`,
	},
	{
		`{
					"a":"wow",
					"b":"val1",
					"c":true,
					"d":"John",
				   "e": 1,
				   "f": 4
			}`,
	},
	{
		`{
					"a":"val1",
					"b":"val1",
					"c":true,
					"d":"John",
				   "e": 1,
				   "f": 2
			}`,
	},
	{
		`{
					"a":"val23",
					"b":"val1",
					"c":true,
					"d":"John",
				   "f": 2
			}`,
	},
	{
		`{
					"a":"val1567",
					"b":"val1",
					"c":true,
					"d":"John",
				   "e": 1,
				   "f": 2
			}`,
	},
	{
		`{
					"a":"val1",
					"b":"val1",
					"c":true,
					"d":"John",
				   "e": 1,
				   "f": 2
			}`,
	},
	{
		`{
					"a":"",
					"b":"val1",
					"c":true,
					"d":"John",
				   "e": 1,
				   "f": 2
			}`,
	},
	{
		`{
					"a":"val1",
					"b":"val1",
					"c":true,
					"d":"John",
				   "f": 2
			}`,
	},
	{
		`{
					"a":"val1",
					"b":"val1",
					"c":true,
					"d":"John",
				   "f": 2
			}`,
	},
}

/*
func checkTree(t *testing.T, node1 *Node, node2 *Node) {
	assert.Equal(t, node1.aggValues, node2.aggValues)

	for key, child := range node1.children {
		otherChild, ok := node2.children[key]

		assert.True(t, ok)
		assert.Equal(t, child.matchedRecordsStartIndex, otherChild.matchedRecordsStartIndex)
		assert.Equal(t, child.matchedRecordsEndIndex, otherChild.matchedRecordsEndIndex)

		checkTree(t, child, otherChild)
	}
}

func check(t *testing.T, decTree StarTreeQueryMaker, groupByKeys []string, aggFunctions []*structs.MeasureAggregator,
	origTree *StarTree) {
	assert.Equal(t, groupByKeys, decTree.metadata.GroupByKeys)
	assert.Equal(t, aggFunctions, decTree.metadata.AggFunctions)

	checkTree(t, origTree.Root, decTree.tree.Root)

	assert.Equal(t, origTree.matchedRecordsIndices, decTree.tree.matchedRecordsIndices)
	}
*/

func TestStarTree(t *testing.T) {
	rangeIndex = map[string]*structs.Numbers{}

	var blockSummary structs.BlockSummary
	colWips := make(map[string]*ColWip)
	wipBlock := WipBlock{
		columnBlooms:       make(map[string]*BloomIndex),
		columnRangeIndexes: make(map[string]*RangeIndex),
		colWips:            colWips,
		columnsInBlock:     make(map[string]bool),
		blockSummary:       blockSummary,
		tomRollup:          make(map[uint64]*RolledRecs),
		tohRollup:          make(map[uint64]*RolledRecs),
		todRollup:          make(map[uint64]*RolledRecs),
		bb:                 bbp.Get(),
	}
	segstats := make(map[string]*SegStats)
	allCols := make(map[string]uint32)

	ss := NewSegStore(0)
	ss.wipBlock = wipBlock
	ss.SegmentKey = "test-segkey1"
	ss.AllSeenColumnSizes = allCols
	ss.pqTracker = initPQTracker()
	ss.AllSst = segstats
	ss.numBlocks = 0

	cnameCacheByteHashToStr := make(map[uint64]string)
	var jsParsingStackbuf [64]byte

	tsKey := config.GetTimeStampKey()
	for i, test := range cases {

		var record_json map[string]interface{}
		var json = jsoniter.ConfigCompatibleWithStandardLibrary
		decoder := json.NewDecoder(bytes.NewReader([]byte(test.input)))
		decoder.UseNumber()
		err := decoder.Decode(&record_json)
		if err != nil {
			t.Errorf("testid: %d: Failed to parse json err:%v", i+1, err)
			continue
		}
		raw, err := json.Marshal(record_json)
		assert.NoError(t, err)

		_, err = ss.EncodeColumns(raw, uint64(i), &tsKey, utils.SIGNAL_EVENTS,
			cnameCacheByteHashToStr, jsParsingStackbuf[:])
		assert.NoError(t, err)

		for _, cwip := range ss.wipBlock.colWips {
			ss.wipBlock.maxIdx = utils.MaxUint32(ss.wipBlock.maxIdx, cwip.cbufidx)
		}
		ss.wipBlock.blockSummary.RecCount += 1
	}

	groupByCols := []string{"a", "d"}
	mColNames := []string{"e", "f"}

	gcWorkBuf := make([][]string, len(groupByCols))
	for colNum := 0; colNum < len(groupByCols); colNum++ {
		gcWorkBuf[colNum] = make([]string, MaxAgileTreeNodeCountForAlloc)
	}

	builder := GetSTB().stbPtr

	for trial := 0; trial < 10; trial += 1 {
		builder.ResetSegTree(groupByCols, mColNames, gcWorkBuf)
		err := builder.ComputeStarTree(&ss.wipBlock)
		assert.NoError(t, err)
		root := builder.tree.Root

		_, err = builder.EncodeStarTree(ss.SegmentKey)
		assert.NoError(t, err)

		// first TotalMeasFns will be for col "e"
		agSumIdx := 1*(TotalMeasFns) + MeasFnSumIdx
		iv, err := root.aggValues[agSumIdx].Int64()
		assert.NoError(t, err)
		assert.Equal(t, iv,
			int64(34),
			fmt.Sprintf("expected sum of 34 for sum of column f; got %d",
				iv))

	}
	fName := fmt.Sprintf("%v.strl", ss.SegmentKey)
	_ = os.RemoveAll(fName)
	fName = fmt.Sprintf("%v.strm", ss.SegmentKey)
	_ = os.RemoveAll(fName)
}

func TestStarTreeMedium(t *testing.T) {
	rangeIndex = map[string]*structs.Numbers{}

	var largeCases []struct {
		input string
	}

	for i := 0; i < 1000; i += 1 {
		largeCases = append(largeCases, cases...)
	}

	currCases := largeCases

	var blockSummary structs.BlockSummary
	colWips := make(map[string]*ColWip)
	wipBlock := WipBlock{
		columnBlooms:       make(map[string]*BloomIndex),
		columnRangeIndexes: make(map[string]*RangeIndex),
		colWips:            colWips,
		columnsInBlock:     make(map[string]bool),
		blockSummary:       blockSummary,
		tomRollup:          make(map[uint64]*RolledRecs),
		tohRollup:          make(map[uint64]*RolledRecs),
		todRollup:          make(map[uint64]*RolledRecs),
		bb:                 bbp.Get(),
	}
	segstats := make(map[string]*SegStats)
	allCols := make(map[string]uint32)

	ss := NewSegStore(0)
	ss.wipBlock = wipBlock
	ss.SegmentKey = "test-segkey2"
	ss.AllSeenColumnSizes = allCols
	ss.pqTracker = initPQTracker()
	ss.AllSst = segstats
	ss.numBlocks = 0

	tsKey := config.GetTimeStampKey()

	cnameCacheByteHashToStr := make(map[uint64]string)
	var jsParsingStackbuf [64]byte

	for i, test := range currCases {

		var record_json map[string]interface{}
		var json = jsoniter.ConfigCompatibleWithStandardLibrary
		decoder := json.NewDecoder(bytes.NewReader([]byte(test.input)))
		decoder.UseNumber()
		err := decoder.Decode(&record_json)
		if err != nil {
			t.Errorf("testid: %d: Failed to parse json err:%v", i+1, err)
			continue
		}
		raw, err := json.Marshal(record_json)
		assert.NoError(t, err)

		_, err = ss.EncodeColumns(raw, uint64(i), &tsKey, utils.SIGNAL_EVENTS,
			cnameCacheByteHashToStr, jsParsingStackbuf[:])
		assert.NoError(t, err)

		for _, cwip := range ss.wipBlock.colWips {
			ss.wipBlock.maxIdx = utils.MaxUint32(ss.wipBlock.maxIdx, cwip.cbufidx)
		}
		ss.wipBlock.blockSummary.RecCount += 1
	}

	groupByCols := [...]string{"a", "d"}
	mColNames := []string{"e", "f"}

	gcWorkBuf := make([][]string, len(groupByCols))
	for colNum := 0; colNum < len(groupByCols); colNum++ {
		gcWorkBuf[colNum] = make([]string, MaxAgileTreeNodeCountForAlloc)
	}

	builder := GetSTB().stbPtr

	for trial := 0; trial < 10; trial += 1 {
		builder.ResetSegTree(groupByCols[:], mColNames, gcWorkBuf)
		err := builder.ComputeStarTree(&ss.wipBlock)
		assert.NoError(t, err)
		root := builder.tree.Root

		_, err = builder.EncodeStarTree(ss.SegmentKey)
		assert.NoError(t, err)

		// first TotalMeasFns will be for col "e"
		agSumIdx := 1*(TotalMeasFns) + MeasFnSumIdx
		iv, err := root.aggValues[agSumIdx].Int64()
		assert.NoError(t, err)
		assert.Equal(t, iv,
			int64(34*1000),
			fmt.Sprintf("expected sum of 340000 for sum of column f; got %d",
				iv))
	}
	fName := fmt.Sprintf("%v.strl", ss.SegmentKey)
	_ = os.RemoveAll(fName)
	fName = fmt.Sprintf("%v.strm", ss.SegmentKey)
	_ = os.RemoveAll(fName)
}

func TestStarTreeMediumEncoding(t *testing.T) {
	rangeIndex = map[string]*structs.Numbers{}

	var largeCases []struct {
		input string
	}

	for i := 0; i < 50; i += 1 {
		largeCases = append(largeCases, cases...)
	}

	currCases := largeCases

	var blockSummary structs.BlockSummary
	colWips := make(map[string]*ColWip)
	wipBlock := WipBlock{
		columnBlooms:       make(map[string]*BloomIndex),
		columnRangeIndexes: make(map[string]*RangeIndex),
		colWips:            colWips,
		columnsInBlock:     make(map[string]bool),
		blockSummary:       blockSummary,
		tomRollup:          make(map[uint64]*RolledRecs),
		tohRollup:          make(map[uint64]*RolledRecs),
		todRollup:          make(map[uint64]*RolledRecs),
		bb:                 bbp.Get(),
	}

	allCols := make(map[string]uint32)
	segstats := make(map[string]*SegStats)

	ss := NewSegStore(0)
	ss.wipBlock = wipBlock
	ss.SegmentKey = "test-segkey1"
	ss.AllSeenColumnSizes = allCols
	ss.pqTracker = initPQTracker()
	ss.AllSst = segstats
	ss.numBlocks = 0

	tsKey := config.GetTimeStampKey()

	cnameCacheByteHashToStr := make(map[uint64]string)
	var jsParsingStackbuf [64]byte

	for i, test := range currCases {

		var record_json map[string]interface{}
		var json = jsoniter.ConfigCompatibleWithStandardLibrary
		decoder := json.NewDecoder(bytes.NewReader([]byte(test.input)))
		decoder.UseNumber()
		err := decoder.Decode(&record_json)
		if err != nil {
			t.Errorf("testid: %d: Failed to parse json err:%v", i+1, err)
			continue
		}
		raw, err := json.Marshal(record_json)
		assert.NoError(t, err)

		_, err = ss.EncodeColumns(raw, uint64(i), &tsKey, utils.SIGNAL_EVENTS,
			cnameCacheByteHashToStr, jsParsingStackbuf[:])
		assert.NoError(t, err)

		for _, cwip := range ss.wipBlock.colWips {
			ss.wipBlock.maxIdx = utils.MaxUint32(ss.wipBlock.maxIdx, cwip.cbufidx)
		}
		ss.wipBlock.blockSummary.RecCount += 1
		ss.RecordCount++
	}

	groupByCols := [...]string{"a", "d"}
	mColNames := []string{"e", "f"}

	gcWorkBuf := make([][]string, len(groupByCols))
	for colNum := 0; colNum < len(groupByCols); colNum++ {
		gcWorkBuf[colNum] = make([]string, MaxAgileTreeNodeCountForAlloc)
	}

	builder := GetSTB().stbPtr

	for trial := 0; trial < 10; trial += 1 {
		builder.ResetSegTree(groupByCols[:], mColNames, gcWorkBuf)
		err := builder.ComputeStarTree(&ss.wipBlock)
		assert.NoError(t, err)
		root := builder.tree.Root

		_, err = builder.EncodeStarTree(ss.SegmentKey)
		assert.NoError(t, err)

		// first TotalMeasFns will be for col "e"
		agSumIdx := 1*(TotalMeasFns) + MeasFnSumIdx
		iv, err := root.aggValues[agSumIdx].Int64()
		assert.NoError(t, err)

		assert.Equal(t, iv,
			int64(1700),
			fmt.Sprintf("expected sum of 3400 for sum of column f; got %d",
				iv))

	}
	fName := fmt.Sprintf("%v.strl", ss.SegmentKey)
	_ = os.RemoveAll(fName)
	fName = fmt.Sprintf("%v.strm", ss.SegmentKey)
	_ = os.RemoveAll(fName)
}

func TestStarTreeMediumEncodingDecoding(t *testing.T) {
	rangeIndex = map[string]*structs.Numbers{}

	var largeCases []struct {
		input string
	}

	for i := 0; i < 50; i += 1 {
		largeCases = append(largeCases, cases...)
	}

	currCases := largeCases

	var blockSummary structs.BlockSummary
	colWips := make(map[string]*ColWip)
	wipBlock := WipBlock{
		columnBlooms:       make(map[string]*BloomIndex),
		columnRangeIndexes: make(map[string]*RangeIndex),
		colWips:            colWips,
		columnsInBlock:     make(map[string]bool),
		blockSummary:       blockSummary,
		tomRollup:          make(map[uint64]*RolledRecs),
		tohRollup:          make(map[uint64]*RolledRecs),
		todRollup:          make(map[uint64]*RolledRecs),
		bb:                 bbp.Get(),
	}
	segstats := make(map[string]*SegStats)
	allCols := make(map[string]uint32)

	ss := NewSegStore(0)
	ss.wipBlock = wipBlock
	ss.SegmentKey = "test-segkey4"
	ss.AllSeenColumnSizes = allCols
	ss.pqTracker = initPQTracker()
	ss.AllSst = segstats
	ss.numBlocks = 0

	tsKey := config.GetTimeStampKey()

	cnameCacheByteHashToStr := make(map[uint64]string)
	var jsParsingStackbuf [64]byte

	for i, test := range currCases {

		var record_json map[string]interface{}
		var json = jsoniter.ConfigCompatibleWithStandardLibrary
		decoder := json.NewDecoder(bytes.NewReader([]byte(test.input)))
		decoder.UseNumber()
		err := decoder.Decode(&record_json)
		if err != nil {
			t.Errorf("testid: %d: Failed to parse json err:%v", i+1, err)
			continue
		}
		raw, err := json.Marshal(record_json)
		assert.NoError(t, err)

		_, err = ss.EncodeColumns(raw, uint64(i), &tsKey, utils.SIGNAL_EVENTS,
			cnameCacheByteHashToStr, jsParsingStackbuf[:])
		assert.NoError(t, err)

		for _, cwip := range ss.wipBlock.colWips {
			ss.wipBlock.maxIdx = utils.MaxUint32(ss.wipBlock.maxIdx, cwip.cbufidx)
		}

		ss.wipBlock.blockSummary.RecCount += 1
	}

	groupByCols := [...]string{"a", "d"}
	mColNames := []string{"e", "f"}

	gcWorkBuf := make([][]string, len(groupByCols))
	for colNum := 0; colNum < len(groupByCols); colNum++ {
		gcWorkBuf[colNum] = make([]string, MaxAgileTreeNodeCountForAlloc)
	}

	builder := GetSTB().stbPtr

	for trial := 0; trial < 1; trial += 1 {
		builder.ResetSegTree(groupByCols[:], mColNames, gcWorkBuf)
		err := builder.ComputeStarTree(&ss.wipBlock)
		assert.NoError(t, err)
		root := builder.tree.Root

		_, err = builder.EncodeStarTree(ss.SegmentKey)
		assert.NoError(t, err)

		// first TotalMeasFns will be for col "e"
		agidx := 1*(TotalMeasFns) + MeasFnSumIdx
		iv, err := root.aggValues[agidx].Int64()
		assert.NoError(t, err)
		assert.Equal(t, int64(17*100), iv,
			fmt.Sprintf("expected 17000 for sum of column f; got %d",
				iv))

		agidx = 1*(TotalMeasFns) + MeasFnMinIdx
		iv, err = root.aggValues[agidx].Int64()
		assert.NoError(t, err)
		assert.Equal(t, int64(2), iv,
			fmt.Sprintf("expected 2 for min of column f; got %d",
				iv))

		agidx = 1*(TotalMeasFns) + MeasFnMaxIdx
		iv, err = root.aggValues[agidx].Int64()
		assert.NoError(t, err)
		assert.Equal(t, int64(4), iv,
			fmt.Sprintf("expected 4 for max of column f; got %d",
				iv))

		agidx = 1*(TotalMeasFns) + MeasFnCountIdx
		iv, err = root.aggValues[agidx].Int64()
		assert.NoError(t, err)
		assert.Equal(t, int64(800), iv,
			fmt.Sprintf("expected 800 for count of column f; got %d",
				iv))

	}
	fName := fmt.Sprintf("%v.strl", ss.SegmentKey)
	_ = os.RemoveAll(fName)
	fName = fmt.Sprintf("%v.strm", ss.SegmentKey)
	_ = os.RemoveAll(fName)
}

var cases2 = []struct {
	input2 string
}{
	{
		`{
					"brand":"toyota",
					"color":"green",
					"type":"sedan",
					"price":10,
					"perf":9,
					"rating":5
			}`,
	},
	{
		`{
					"brand":"toyota",
					"color": "yellow",
					"type": "sedan",
					"price": 8,
					"perf": 7,
					"rating": 3
			}`,
	},
	{
		`{
					"brand":"toyota",
					"color": "red",
					"type": "suv",
					"price": 9,
					"perf": 8,
					"rating": 4
			}`,
	},
	{
		`{
					"brand":"audi",
					"color": "blue",
					"type": "sedan",
					"price": 20,
					"perf": 11,
					"rating": 6
			}`,
	},
	{
		`{
					"brand":"audi",
					"color": "green",
					"type": "suv",
					"price": 30,
					"perf": 15,
					"rating": 12
			}`,
	},
	{
		`{
					"brand":"audi",
					"color": "green",
					"type": "sedan",
					"price": 16,
					"perf": 11,
					"rating": 10
			}`,
	},
	{
		`{
					"brand":"bmw",
					"color": "red",
					"type": "sedan",
					"price": 18,
					"perf": 11,
					"rating": 3
			}`,
	},
	{
		`{
					"brand":"bmw",
					"color": "green",
					"type": "sedan",
					"price": 50,
					"perf": 16,
					"rating": 11
			}`,
	},
	{
		`{
					"brand":"bmw",
					"color": "pink",
					"type": "suv",
					"price": 40,
					"perf": 3,
					"rating": 1
			}`,
	},
	{
		`{
					"brand":"bmw",
					"color": "green",
					"price": 25,
					"perf": 5,
					"rating": 2
			}`,
	},
	{
		`{
					"brand":"audi",
					"price": 20,
					"perf": 6,
					"rating": 4
			}`,
	},
	{
		`{
					"brand":"audi",
					"color":"green",
					"price": 10,
					"perf": 3,
					"rating": 2
			}`,
	},
	{
		`{
					"brand":"audi",
					"color":"blue",
					"price": 5,
					"perf": 2,
					"rating": 3
			}`,
	},
}

// Tree structure of the data:

//                                         ROOT
//                  /                       |                                    \
//                 /                        |                                      \
//              toyota                     audi (20, 6, 4)                         bmw
//           /    |    \              /           \                     /             |     \
//        green yellow red       blue (5, 2, 3)   green (10, 3, 2) green (25, 5, 2)  red   pink
//         |     |     |         |            /       \            |                   |      |
//        sedan  sedan  suv    sedan       sedan     suv         sedan                sedan   suv
//         |     |     |         |           |       |            |                    |       |
// Price:  10    8     9        20          16       30          50                    18      40
// Perf:   9     7     8        11          11       15          16                    11      3
// Rating: 5     3     4        6           10       12          11                    3       1

func getTotalLevels(node *Node) int {
	if node == nil || node.children == nil || len(node.children) == 0 {
		return 0
	}
	max := 0
	for _, child := range node.children {
		level := getTotalLevels(child)
		if level > max {
			max = level
		}
	}
	return 1 + max
}

func (builder *StarTreeBuilder) GetColValueEncodings(wip WipBlock, recGroupByValues [][]string) (map[string]map[string]uint32, error) {
	numRecs := wip.blockSummary.RecCount
	colValueToEncMap := make(map[string]map[string]uint32)
	for colNum := range builder.groupByKeys {
		colValueToEncMap[builder.groupByKeys[colNum]] = make(map[string]uint32)
	}

	// valueToEncMap will be used to store the encoded values for each group by key
	// E.g. valueToEncMap["brand"]["toyota"] = 1 indicates that "toyota" is encoded as 1 for the brand column
	// This will be used to verify the tree structure since enc values are not deterministic (because of map iteration order)
	for recNum := uint16(0); recNum < numRecs; recNum += 1 {
		for i, grpValues := range recGroupByValues[recNum] {
			colValueToEncMap[builder.groupByKeys[i]][grpValues] = builder.wipRecNumToColEnc[i][recNum]
		}
	}

	return colValueToEncMap, nil
}

func populateAggsFromTree(t *testing.T, root *Node, aggValues map[string][]*utils.Number, key string) {
	var newKey string
	if key == "" {
		newKey = fmt.Sprintf("%v", root.myKey)
	} else {
		newKey = strings.Join([]string{key, fmt.Sprintf("%v", root.myKey)}, "_")
	}
	if root.children == nil || len(root.children) == 0 {
		aggValues[newKey] = root.aggValues
		return
	}

	for _, child := range root.children {
		populateAggsFromTree(t, child, aggValues, newKey)
	}
}

func createKey(root *Node, encMap map[string]map[string]uint32, grpCols []string, grpVals []string) string {
	keys := []string{fmt.Sprintf("%v", root.myKey)}
	for i, col := range grpCols {
		keys = append(keys, fmt.Sprintf("%v", encMap[col][grpVals[i]]))
	}
	return strings.Join(keys, "_")
}

func testAggs(t *testing.T, expected []int, aggValues []*utils.Number) {
	assert.Equal(t, len(expected), len(aggValues))

	for offset := 0; offset < len(expected); offset += TotalMeasFns {
		agidx := offset + MeasFnSumIdx
		sumVal, err := aggValues[agidx].Int64()
		assert.Nil(t, err)
		assert.Equal(t, int64(expected[agidx]), sumVal)

		agidx = offset + MeasFnMinIdx
		minVal, err := aggValues[agidx].Int64()
		assert.Nil(t, err)
		assert.Equal(t, int64(expected[agidx]), minVal)

		agidx = offset + MeasFnMaxIdx
		maxVal, err := aggValues[agidx].Int64()
		assert.Nil(t, err)
		assert.Equal(t, int64(expected[agidx]), maxVal)

		agidx = offset + MeasFnCountIdx
		countVal, err := aggValues[agidx].Int64()
		assert.Nil(t, err)
		assert.Equal(t, int64(expected[agidx]), countVal)
	}
}

func TestStarTree2(t *testing.T) {

	rangeIndex = map[string]*structs.Numbers{}

	var blockSummary structs.BlockSummary
	colWips := make(map[string]*ColWip)
	wipBlock := WipBlock{
		columnBlooms:       make(map[string]*BloomIndex),
		columnRangeIndexes: make(map[string]*RangeIndex),
		colWips:            colWips,
		columnsInBlock:     make(map[string]bool),
		blockSummary:       blockSummary,
		tomRollup:          make(map[uint64]*RolledRecs),
		tohRollup:          make(map[uint64]*RolledRecs),
		todRollup:          make(map[uint64]*RolledRecs),
		bb:                 bbp.Get(),
	}
	segstats := make(map[string]*SegStats)
	allCols := make(map[string]uint32)

	ss := NewSegStore(0)
	ss.wipBlock = wipBlock
	ss.SegmentKey = "test-segkey1"
	ss.AllSeenColumnSizes = allCols
	ss.pqTracker = initPQTracker()
	ss.AllSst = segstats
	ss.numBlocks = 0

	groupByCols := []string{"brand", "color", "type"}
	mColNames := []string{"price", "perf", "rating"}

	gcWorkBuf := make([][]string, len(groupByCols))
	for colNum := 0; colNum < len(groupByCols); colNum++ {
		gcWorkBuf[colNum] = make([]string, MaxAgileTreeNodeCount)
	}

	allGrpVals := [][]string{}
	tsKey := config.GetTimeStampKey()

	cnameCacheByteHashToStr := make(map[uint64]string)
	var jsParsingStackbuf [64]byte

	for i, test := range cases2 {
		var record_json map[string]interface{}
		var json = jsoniter.ConfigCompatibleWithStandardLibrary
		decoder := json.NewDecoder(bytes.NewReader([]byte(test.input2)))
		decoder.UseNumber()
		err := decoder.Decode(&record_json)
		if err != nil {
			t.Errorf("testid: %d: Failed to parse json err:%v", i+1, err)
			continue
		}
		grpVals := []string{}
		for _, col := range groupByCols {
			value := ""
			if _, exist := record_json[col]; exist {
				value = record_json[col].(string)
			}
			grpVals = append(grpVals, value)
		}
		allGrpVals = append(allGrpVals, grpVals)
		raw, err := json.Marshal(record_json)
		assert.NoError(t, err)

		_, err = ss.EncodeColumns(raw, uint64(i), &tsKey, utils.SIGNAL_EVENTS,
			cnameCacheByteHashToStr, jsParsingStackbuf[:])
		assert.NoError(t, err)

		for _, cwip := range ss.wipBlock.colWips {
			ss.wipBlock.maxIdx = utils.MaxUint32(ss.wipBlock.maxIdx, cwip.cbufidx)
		}
		ss.wipBlock.blockSummary.RecCount += 1
	}

	builder := GetSTB().stbPtr

	// basic test
	builder.ResetSegTree(groupByCols, mColNames, gcWorkBuf)
	err := builder.ComputeStarTree(&ss.wipBlock)
	assert.NoError(t, err)
	root := builder.tree.Root

	encMap, err := builder.GetColValueEncodings(ss.wipBlock, allGrpVals)
	assert.NoError(t, err)

	aggValues := make(map[string][]*utils.Number)
	populateAggsFromTree(t, root, aggValues, "")

	// check the tree structure
	assert.Equal(t, 13, len(aggValues))
	key := createKey(root, encMap, groupByCols, []string{"toyota", "green", "sedan"})
	testAggs(t, []int{10, 10, 10, 1, 9, 9, 9, 1, 5, 5, 5, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"toyota", "yellow", "sedan"})
	testAggs(t, []int{8, 8, 8, 1, 7, 7, 7, 1, 3, 3, 3, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"toyota", "red", "suv"})
	testAggs(t, []int{9, 9, 9, 1, 8, 8, 8, 1, 4, 4, 4, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"audi", "", ""})
	testAggs(t, []int{20, 20, 20, 1, 6, 6, 6, 1, 4, 4, 4, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"audi", "blue", ""})
	testAggs(t, []int{5, 5, 5, 1, 2, 2, 2, 1, 3, 3, 3, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"audi", "blue", "sedan"})
	testAggs(t, []int{20, 20, 20, 1, 11, 11, 11, 1, 6, 6, 6, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"audi", "green", ""})
	testAggs(t, []int{10, 10, 10, 1, 3, 3, 3, 1, 2, 2, 2, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"audi", "green", "sedan"})
	testAggs(t, []int{16, 16, 16, 1, 11, 11, 11, 1, 10, 10, 10, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"audi", "green", "suv"})
	testAggs(t, []int{30, 30, 30, 1, 15, 15, 15, 1, 12, 12, 12, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"bmw", "green", ""})
	testAggs(t, []int{25, 25, 25, 1, 5, 5, 5, 1, 2, 2, 2, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"bmw", "green", "sedan"})
	testAggs(t, []int{50, 50, 50, 1, 16, 16, 16, 1, 11, 11, 11, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"bmw", "red", "sedan"})
	testAggs(t, []int{18, 18, 18, 1, 11, 11, 11, 1, 3, 3, 3, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"bmw", "pink", "suv"})
	testAggs(t, []int{40, 40, 40, 1, 3, 3, 3, 1, 1, 1, 1, 1}, aggValues[key])

	_, err = builder.EncodeStarTree(ss.SegmentKey)
	assert.NoError(t, err)

	assert.Equal(t, 3, getTotalLevels(root))
	testAggs(t, []int{5, 50, 261, 13, 2, 16, 107, 13, 1, 12, 66, 13}, root.aggValues)

	// drop an invalid column
	builder.ResetSegTree(groupByCols, mColNames, gcWorkBuf)
	err = builder.ComputeStarTree(&ss.wipBlock)
	assert.NoError(t, err)
	root = builder.tree.Root

	encMap, err = builder.GetColValueEncodings(ss.wipBlock, allGrpVals)
	assert.NoError(t, err)

	err = builder.DropColumns([]string{"invalid"})
	assert.NotNil(t, err)

	aggValues = make(map[string][]*utils.Number)
	populateAggsFromTree(t, root, aggValues, "")

	// check the tree structure
	assert.Equal(t, 13, len(aggValues))
	key = createKey(root, encMap, groupByCols, []string{"toyota", "green", "sedan"})
	testAggs(t, []int{10, 10, 10, 1, 9, 9, 9, 1, 5, 5, 5, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"toyota", "yellow", "sedan"})
	testAggs(t, []int{8, 8, 8, 1, 7, 7, 7, 1, 3, 3, 3, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"toyota", "red", "suv"})
	testAggs(t, []int{9, 9, 9, 1, 8, 8, 8, 1, 4, 4, 4, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"audi", "", ""})
	testAggs(t, []int{20, 20, 20, 1, 6, 6, 6, 1, 4, 4, 4, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"audi", "blue", ""})
	testAggs(t, []int{5, 5, 5, 1, 2, 2, 2, 1, 3, 3, 3, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"audi", "blue", "sedan"})
	testAggs(t, []int{20, 20, 20, 1, 11, 11, 11, 1, 6, 6, 6, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"audi", "green", ""})
	testAggs(t, []int{10, 10, 10, 1, 3, 3, 3, 1, 2, 2, 2, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"audi", "green", "sedan"})
	testAggs(t, []int{16, 16, 16, 1, 11, 11, 11, 1, 10, 10, 10, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"audi", "green", "suv"})
	testAggs(t, []int{30, 30, 30, 1, 15, 15, 15, 1, 12, 12, 12, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"bmw", "green", ""})
	testAggs(t, []int{25, 25, 25, 1, 5, 5, 5, 1, 2, 2, 2, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"bmw", "green", "sedan"})
	testAggs(t, []int{50, 50, 50, 1, 16, 16, 16, 1, 11, 11, 11, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"bmw", "red", "sedan"})
	testAggs(t, []int{18, 18, 18, 1, 11, 11, 11, 1, 3, 3, 3, 1}, aggValues[key])
	key = createKey(root, encMap, groupByCols, []string{"bmw", "pink", "suv"})
	testAggs(t, []int{40, 40, 40, 1, 3, 3, 3, 1, 1, 1, 1, 1}, aggValues[key])

	_, err = builder.EncodeStarTree(ss.SegmentKey)
	assert.NoError(t, err)

	assert.Equal(t, 3, getTotalLevels(root))
	testAggs(t, []int{5, 50, 261, 13, 2, 16, 107, 13, 1, 12, 66, 13}, root.aggValues)

	// Levels: 0 -> brand, 1 -> color, 2 -> type
	// remove brand
	builder.ResetSegTree(groupByCols, mColNames, gcWorkBuf)
	err = builder.ComputeStarTree(&ss.wipBlock)
	assert.NoError(t, err)
	root = builder.tree.Root
	encMap, err = builder.GetColValueEncodings(ss.wipBlock, allGrpVals)
	assert.NoError(t, err)

	err = builder.DropColumns([]string{"brand"})
	assert.NoError(t, err)

	aggValues = make(map[string][]*utils.Number, 0)
	populateAggsFromTree(t, root, aggValues, "")

	_, err = builder.EncodeStarTree(ss.SegmentKey)
	assert.NoError(t, err)

	gCols := []string{"color", "type"}

	// check the tree structure
	assert.Equal(t, 10, len(aggValues))
	key = createKey(root, encMap, gCols, []string{"green", "sedan"})
	testAggs(t, []int{10, 50, 76, 3, 9, 16, 36, 3, 5, 11, 26, 3}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"green", ""})
	testAggs(t, []int{10, 25, 35, 2, 3, 5, 8, 2, 2, 2, 4, 2}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"green", "suv"})
	testAggs(t, []int{30, 30, 30, 1, 15, 15, 15, 1, 12, 12, 12, 1}, aggValues[key])

	key = createKey(root, encMap, gCols, []string{"yellow", "sedan"})
	testAggs(t, []int{8, 8, 8, 1, 7, 7, 7, 1, 3, 3, 3, 1}, aggValues[key])

	key = createKey(root, encMap, gCols, []string{"red", "suv"})
	testAggs(t, []int{9, 9, 9, 1, 8, 8, 8, 1, 4, 4, 4, 1}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"red", "sedan"})
	testAggs(t, []int{18, 18, 18, 1, 11, 11, 11, 1, 3, 3, 3, 1}, aggValues[key])

	key = createKey(root, encMap, gCols, []string{"", ""})
	testAggs(t, []int{20, 20, 20, 1, 6, 6, 6, 1, 4, 4, 4, 1}, aggValues[key])

	key = createKey(root, encMap, gCols, []string{"blue", ""})
	testAggs(t, []int{5, 5, 5, 1, 2, 2, 2, 1, 3, 3, 3, 1}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"blue", "sedan"})
	testAggs(t, []int{20, 20, 20, 1, 11, 11, 11, 1, 6, 6, 6, 1}, aggValues[key])

	key = createKey(root, encMap, gCols, []string{"pink", "suv"})
	testAggs(t, []int{40, 40, 40, 1, 3, 3, 3, 1, 1, 1, 1, 1}, aggValues[key])

	assert.Equal(t, 2, getTotalLevels(root))
	testAggs(t, []int{5, 50, 261, 13, 2, 16, 107, 13, 1, 12, 66, 13}, root.aggValues)

	// remove color
	builder.ResetSegTree(groupByCols, mColNames, gcWorkBuf)
	err = builder.ComputeStarTree(&ss.wipBlock)
	assert.NoError(t, err)
	root = builder.tree.Root
	encMap, err = builder.GetColValueEncodings(ss.wipBlock, allGrpVals)
	assert.NoError(t, err)

	err = builder.DropColumns([]string{"color"})
	assert.NoError(t, err)

	aggValues = make(map[string][]*utils.Number)
	populateAggsFromTree(t, root, aggValues, "")

	gCols = []string{"brand", "type"}

	_, err = builder.EncodeStarTree(ss.SegmentKey)
	assert.NoError(t, err)

	// check the aggValues structure
	assert.Equal(t, 8, len(aggValues))
	key = createKey(root, encMap, gCols, []string{"toyota", "sedan"})
	testAggs(t, []int{8, 10, 18, 2, 7, 9, 16, 2, 3, 5, 8, 2}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"toyota", "suv"})
	testAggs(t, []int{9, 9, 9, 1, 8, 8, 8, 1, 4, 4, 4, 1}, aggValues[key])

	key = createKey(root, encMap, gCols, []string{"audi", ""})
	testAggs(t, []int{5, 20, 35, 3, 2, 6, 11, 3, 2, 4, 9, 3}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"audi", "sedan"})
	testAggs(t, []int{16, 20, 36, 2, 11, 11, 22, 2, 6, 10, 16, 2}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"audi", "suv"})
	testAggs(t, []int{30, 30, 30, 1, 15, 15, 15, 1, 12, 12, 12, 1}, aggValues[key])

	key = createKey(root, encMap, gCols, []string{"bmw", ""})
	testAggs(t, []int{25, 25, 25, 1, 5, 5, 5, 1, 2, 2, 2, 1}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"bmw", "sedan"})
	testAggs(t, []int{18, 50, 68, 2, 11, 16, 27, 2, 3, 11, 14, 2}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"bmw", "suv"})
	testAggs(t, []int{40, 40, 40, 1, 3, 3, 3, 1, 1, 1, 1, 1}, aggValues[key])

	assert.Equal(t, 2, getTotalLevels(root))
	testAggs(t, []int{5, 50, 261, 13, 2, 16, 107, 13, 1, 12, 66, 13}, root.aggValues)

	// remove type
	builder.ResetSegTree(groupByCols, mColNames, gcWorkBuf)
	err = builder.ComputeStarTree(&ss.wipBlock)
	assert.NoError(t, err)
	root = builder.tree.Root
	encMap, err = builder.GetColValueEncodings(ss.wipBlock, allGrpVals)
	assert.NoError(t, err)

	err = builder.DropColumns([]string{"type"})
	assert.NoError(t, err)

	aggValues = make(map[string][]*utils.Number)
	populateAggsFromTree(t, root, aggValues, "")

	_, err = builder.EncodeStarTree(ss.SegmentKey)
	assert.NoError(t, err)

	gCols = []string{"brand", "color"}
	// check the aggValues structure
	assert.Equal(t, 9, len(aggValues))
	key = createKey(root, encMap, gCols, []string{"toyota", "green"})
	testAggs(t, []int{10, 10, 10, 1, 9, 9, 9, 1, 5, 5, 5, 1}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"toyota", "yellow"})
	testAggs(t, []int{8, 8, 8, 1, 7, 7, 7, 1, 3, 3, 3, 1}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"toyota", "red"})
	testAggs(t, []int{9, 9, 9, 1, 8, 8, 8, 1, 4, 4, 4, 1}, aggValues[key])

	key = createKey(root, encMap, gCols, []string{"audi", ""})
	testAggs(t, []int{20, 20, 20, 1, 6, 6, 6, 1, 4, 4, 4, 1}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"audi", "blue"})
	testAggs(t, []int{5, 20, 25, 2, 2, 11, 13, 2, 3, 6, 9, 2}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"audi", "green"})
	testAggs(t, []int{10, 30, 56, 3, 3, 15, 29, 3, 2, 12, 24, 3}, aggValues[key])

	key = createKey(root, encMap, gCols, []string{"bmw", "green"})
	testAggs(t, []int{25, 50, 75, 2, 5, 16, 21, 2, 2, 11, 13, 2}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"bmw", "red"})
	testAggs(t, []int{18, 18, 18, 1, 11, 11, 11, 1, 3, 3, 3, 1}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"bmw", "pink"})
	testAggs(t, []int{40, 40, 40, 1, 3, 3, 3, 1, 1, 1, 1, 1}, aggValues[key])

	assert.Equal(t, 2, getTotalLevels(root))
	testAggs(t, []int{5, 50, 261, 13, 2, 16, 107, 13, 1, 12, 66, 13}, root.aggValues)

	// remove brand and color
	builder.ResetSegTree(groupByCols, mColNames, gcWorkBuf)
	err = builder.ComputeStarTree(&ss.wipBlock)
	assert.NoError(t, err)
	root = builder.tree.Root
	encMap, err = builder.GetColValueEncodings(ss.wipBlock, allGrpVals)
	assert.NoError(t, err)

	err = builder.DropColumns([]string{"brand", "color"})
	assert.NoError(t, err)

	aggValues = make(map[string][]*utils.Number)
	populateAggsFromTree(t, root, aggValues, "")

	_, err = builder.EncodeStarTree(ss.SegmentKey)
	assert.NoError(t, err)

	gCols = []string{"type"}
	// check the aggValues structure
	assert.Equal(t, 3, len(aggValues))
	key = createKey(root, encMap, gCols, []string{""})
	testAggs(t, []int{5, 25, 60, 4, 2, 6, 16, 4, 2, 4, 11, 4}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"sedan"})
	testAggs(t, []int{8, 50, 122, 6, 7, 16, 65, 6, 3, 11, 38, 6}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"suv"})
	testAggs(t, []int{9, 40, 79, 3, 3, 15, 26, 3, 1, 12, 17, 3}, aggValues[key])

	assert.Equal(t, 1, getTotalLevels(root))
	testAggs(t, []int{5, 50, 261, 13, 2, 16, 107, 13, 1, 12, 66, 13}, root.aggValues)

	// remove color and type
	builder.ResetSegTree(groupByCols, mColNames, gcWorkBuf)
	err = builder.ComputeStarTree(&ss.wipBlock)
	assert.NoError(t, err)
	root = builder.tree.Root
	encMap, err = builder.GetColValueEncodings(ss.wipBlock, allGrpVals)
	assert.NoError(t, err)

	err = builder.DropColumns([]string{"color", "type"})
	assert.NoError(t, err)

	aggValues = make(map[string][]*utils.Number)
	populateAggsFromTree(t, root, aggValues, "")

	_, err = builder.EncodeStarTree(ss.SegmentKey)
	assert.NoError(t, err)

	gCols = []string{"brand"}
	// check the aggValues structure
	assert.Equal(t, 3, len(aggValues))
	key = createKey(root, encMap, gCols, []string{"toyota"})
	testAggs(t, []int{8, 10, 27, 3, 7, 9, 24, 3, 3, 5, 12, 3}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"audi"})
	testAggs(t, []int{5, 30, 101, 6, 2, 15, 48, 6, 2, 12, 37, 6}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"bmw"})
	testAggs(t, []int{18, 50, 133, 4, 3, 16, 35, 4, 1, 11, 17, 4}, aggValues[key])

	assert.Equal(t, 1, getTotalLevels(root))
	testAggs(t, []int{5, 50, 261, 13, 2, 16, 107, 13, 1, 12, 66, 13}, root.aggValues)

	// remove brand and type
	builder.ResetSegTree(groupByCols, mColNames, gcWorkBuf)
	err = builder.ComputeStarTree(&ss.wipBlock)
	assert.NoError(t, err)
	root = builder.tree.Root
	encMap, err = builder.GetColValueEncodings(ss.wipBlock, allGrpVals)
	assert.NoError(t, err)

	err = builder.DropColumns([]string{"brand", "type"})
	assert.NoError(t, err)

	aggValues = make(map[string][]*utils.Number)
	populateAggsFromTree(t, root, aggValues, "")

	_, err = builder.EncodeStarTree(ss.SegmentKey)
	assert.NoError(t, err)

	gCols = []string{"color"}

	// check the aggValues structure
	assert.Equal(t, 6, len(aggValues))
	key = createKey(root, encMap, gCols, []string{""})
	testAggs(t, []int{20, 20, 20, 1, 6, 6, 6, 1, 4, 4, 4, 1}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"green"})
	testAggs(t, []int{10, 50, 141, 6, 3, 16, 59, 6, 2, 12, 42, 6}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"yellow"})
	testAggs(t, []int{8, 8, 8, 1, 7, 7, 7, 1, 3, 3, 3, 1}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"red"})
	testAggs(t, []int{9, 18, 27, 2, 8, 11, 19, 2, 3, 4, 7, 2}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"blue"})
	testAggs(t, []int{5, 20, 25, 2, 2, 11, 13, 2, 3, 6, 9, 2}, aggValues[key])
	key = createKey(root, encMap, gCols, []string{"pink"})
	testAggs(t, []int{40, 40, 40, 1, 3, 3, 3, 1, 1, 1, 1, 1}, aggValues[key])

	assert.Equal(t, 1, getTotalLevels(root))
	testAggs(t, []int{5, 50, 261, 13, 2, 16, 107, 13, 1, 12, 66, 13}, root.aggValues)

	fName := fmt.Sprintf("%v.strl", ss.SegmentKey)
	_ = os.RemoveAll(fName)
	fName = fmt.Sprintf("%v.strm", ss.SegmentKey)
	_ = os.RemoveAll(fName)
}

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
	"fmt"
	"math"

	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type StarTree struct {
	Root *Node
}

// its ok for this to be int, since this will be used as an index in arrays
const (
	MeasFnMinIdx int = iota // has to be always zero based
	MeasFnMaxIdx
	MeasFnSumIdx
	MeasFnCountIdx
	// Note: anytimes you add a Fn, make sure to adjust the IdxToAgFn array
	// Note: always keep this last since it is used for indexing into aggValues
	TotalMeasFns
)

var IdxToAgFn []utils.AggregateFunctions = []utils.AggregateFunctions{
	utils.Min, utils.Max,
	utils.Sum, utils.Count}

func AgFnToIdx(fn utils.AggregateFunctions) int {
	switch fn {
	case utils.Min:
		return MeasFnMinIdx
	case utils.Max:
		return MeasFnMaxIdx
	case utils.Sum:
		return MeasFnSumIdx
	case utils.Count:
		return MeasFnCountIdx
	}
	log.Errorf("AgFnToIdx: invalid fn: %v", fn)
	return MeasFnCountIdx
}

var one = utils.CValueEnclosure{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)}

type Node struct {
	myKey     uint32
	parent    *Node
	children  map[uint32]*Node
	aggValues []utils.CValueEnclosure
}

type StarTreeBuilder struct {
	groupByKeys       []string
	numGroupByCols    uint16
	mColNames         []string
	nodeCount         int
	nodePool          []Node
	tree              *StarTree
	segDictMap        []map[string]uint32 // "mac" ==> enc-2
	segDictEncRev     [][]string          // [colNum]["ios", "mac", "win" ...] , [0][enc2] --> "mac"
	segDictLastNum    []uint32            // for each ColNum maintains the lastEnc increasing seq
	wipRecNumToColEnc [][]uint32          //maintain working buffer per wipBlock
	buf               []byte
}

func (stb *StarTreeBuilder) GetNodeCount() int {
	return stb.nodeCount
}

/*
ResetSegTree

	Current assumptions:

	All groupBy columns that contain strings are dictionaryEncoded.
	Any column with len(col.deMap) != 0 is assumed to be dictionary encoded
	It is also assumed that no other values than the dic encoded strings appear in that column

	When storing all other values, their raw byte values are converted to an unsigned integer,
	and then converted to uint64 to have a consistent size

parameters:

	wipBlock: segstore's wip block
	groupByKeys: groupBy column Names
	mColNames: colnames of measure columns

returns:
*/
func (stb *StarTreeBuilder) ResetSegTree(block *WipBlock, groupByKeys []string, mColNames []string) {

	stb.groupByKeys = groupByKeys
	numGroupByCols := uint16(len(groupByKeys))
	stb.numGroupByCols = numGroupByCols
	stb.mColNames = mColNames

	stb.resetNodeData(block)

	root := stb.newNode()
	root.myKey = math.MaxUint32 // give max for root
	stb.tree = &StarTree{Root: root}

	sizeToAdd := int(numGroupByCols) - len(stb.segDictEncRev)
	if sizeToAdd <= 0 {
		stb.segDictEncRev = stb.segDictEncRev[:numGroupByCols]
		stb.segDictMap = stb.segDictMap[:numGroupByCols]
		stb.wipRecNumToColEnc = stb.wipRecNumToColEnc[:stb.numGroupByCols]
		stb.segDictLastNum = stb.segDictLastNum[:stb.numGroupByCols]
	} else {
		newArr := make([][]string, sizeToAdd)
		stb.segDictEncRev = append(stb.segDictEncRev, newArr...)
		newArr2 := make([][]uint32, sizeToAdd)
		stb.wipRecNumToColEnc = append(stb.wipRecNumToColEnc, newArr2...)
		stb.segDictMap = append(stb.segDictMap, make([]map[string]uint32, sizeToAdd)...)
		stb.segDictLastNum = append(stb.segDictLastNum, make([]uint32, sizeToAdd)...)
	}

	for colNum := uint16(0); colNum < numGroupByCols; colNum++ {
		if stb.segDictEncRev[colNum] == nil {
			// we know each col won't have more encodings than max node limit
			stb.segDictEncRev[colNum] = make([]string, MaxAgileTreeNodeCount)
		}
		if stb.segDictMap[colNum] == nil {
			stb.segDictMap[colNum] = make(map[string]uint32)
		}
		stb.segDictLastNum[colNum] = 0
		for cv := range stb.segDictMap[colNum] {
			delete(stb.segDictMap[colNum], cv)
		}
	}

	if len(stb.buf) <= 0 {
		stb.buf = make([]byte, 1_000_000) // initial start size
	}
}

func (stb *StarTreeBuilder) setColValEnc(colNum int, colVal string) uint32 {
	// todo a zero copy version of map lookups needed
	enc, ok := stb.segDictMap[colNum][colVal]
	if !ok {
		enc = stb.segDictLastNum[colNum]
		stb.segDictMap[colNum][colVal] = enc
		stb.segDictEncRev[colNum][enc] = colVal
		stb.segDictLastNum[colNum]++
	}
	return enc
}

// helper function to reset node data for builder reuse
func (stb *StarTreeBuilder) resetNodeData(wip *WipBlock) {

	for _, node := range stb.nodePool {
		node.parent = nil
		for k := range node.children {
			delete(node.children, k)
		}
		node.aggValues = nil
	}
	stb.nodeCount = 0
}

func (stb *StarTreeBuilder) newNode() *Node {

	if stb.nodeCount >= len(stb.nodePool) {
		stb.nodePool = append(stb.nodePool, Node{})
	}
	ans := stb.nodePool[stb.nodeCount]
	stb.nodeCount += 1

	if ans.children == nil {
		ans.children = make(map[uint32]*Node)
	}

	return &ans
}

func (stb *StarTreeBuilder) Aggregate(cur *Node) error {

	first := true

	lenAggValues := len(stb.mColNames) * TotalMeasFns

	if len(cur.children) != 0 {
		cur.aggValues = make([]utils.CValueEnclosure, lenAggValues)
	}

	var err error
	for _, child := range cur.children {
		err = stb.Aggregate(child)
		if err != nil {
			return err
		}

		if first {
			copy(cur.aggValues[:lenAggValues], child.aggValues[:lenAggValues])
			first = false
			continue
		}

		for mcNum := range stb.mColNames {
			midx := mcNum * TotalMeasFns
			agidx := midx + MeasFnMinIdx
			cur.aggValues[agidx], err = utils.Reduce(cur.aggValues[agidx], child.aggValues[agidx], utils.Min)
			if err != nil {
				log.Errorf("Aggregate: error in aggregating min err:%v", err)
				return err
			}
			agidx = midx + MeasFnMaxIdx
			cur.aggValues[agidx], err = utils.Reduce(cur.aggValues[agidx], child.aggValues[agidx], utils.Max)
			if err != nil {
				log.Errorf("Aggregate: error in aggregating max err:%v", err)
				return err
			}
			agidx = midx + MeasFnSumIdx
			cur.aggValues[agidx], err = utils.Reduce(cur.aggValues[agidx], child.aggValues[agidx], utils.Sum)
			if err != nil {
				log.Errorf("Aggregate: error in aggregating sum err:%v", err)
				return err
			}
			agidx = midx + MeasFnCountIdx
			cur.aggValues[agidx], err = utils.Reduce(cur.aggValues[agidx], child.aggValues[agidx], utils.Count)
			if err != nil {
				log.Errorf("Aggregate: error in aggregating count err:%v", err)
				return err
			}
		}
	}

	return nil
}

func (stb *StarTreeBuilder) insertIntoTree(node *Node, colVals []uint32, recNum uint16, idx uint) *Node {
	child, keyExists := node.children[colVals[idx]]
	if !keyExists {
		child = stb.newNode()
		child.myKey = colVals[idx]
		child.parent = node
		node.children[colVals[idx]] = child
	}

	if idx+1 != uint(len(colVals)) {
		return stb.insertIntoTree(child, colVals, recNum, idx+1)
	} else {
		return child
	}
}

func (stb *StarTreeBuilder) creatEnc(wip *WipBlock) error {

	numRecs := wip.blockSummary.RecCount

	for colNum, colName := range stb.groupByKeys {
		sizeToAdd := int(numRecs) - len(stb.wipRecNumToColEnc[colNum])
		if sizeToAdd > 0 {
			newArr := make([]uint32, sizeToAdd)
			stb.wipRecNumToColEnc[colNum] = append(stb.wipRecNumToColEnc[colNum], newArr...)
		}

		cwip := wip.colWips[colName]
		if cwip.deCount < wipCardLimit {
			for rawKey, indices := range cwip.deMap {
				enc := stb.setColValEnc(colNum, rawKey)
				for _, recNum := range indices {
					stb.wipRecNumToColEnc[colNum][recNum] = enc
				}
			}
			continue // done with this dict encoded column
		}

		// read the non-dict way
		idx := uint32(0)
		for recNum := uint16(0); recNum < numRecs; recNum++ {
			cVal, endIdx, err := getColByteSlice(cwip.cbuf[idx:], 0) // todo pass qid here
			if err != nil {
				log.Errorf("populateLeafsWithMeasVals: Could not extract val for cname: %v, idx: %v",
					colName, idx)
				return err
			}
			idx += uint32(endIdx)
			enc := stb.setColValEnc(colNum, string(cVal))
			stb.wipRecNumToColEnc[colNum][recNum] = enc
		}
		if idx < cwip.cbufidx {
			log.Errorf("creatEnc: passed thru all recNums, but idx: %v is not equal to cbufidx: %v",
				idx, cwip.cbufidx)
		}
	}
	return nil
}

func (stb *StarTreeBuilder) buildTreeStructure(wip *WipBlock) error {

	numRecs := wip.blockSummary.RecCount

	sizeToAdd := int(numRecs) - len(stb.nodePool)
	if sizeToAdd > 0 {
		newArr := make([]Node, sizeToAdd)
		stb.nodePool = append(stb.nodePool, newArr...)
	}

	curColValues := make([]uint32, stb.numGroupByCols)
	lenAggValues := len(stb.mColNames) * TotalMeasFns
	measCidx := make([]uint32, len(stb.mColNames))

	for recNum := uint16(0); recNum < numRecs; recNum += 1 {
		for colNum := range stb.groupByKeys {
			curColValues[colNum] = stb.wipRecNumToColEnc[colNum][recNum]
		}
		node := stb.insertIntoTree(stb.tree.Root, curColValues[:stb.numGroupByCols], recNum, 0)
		for mcNum, mcName := range stb.mColNames {
			cwip := wip.colWips[mcName]
			midx := mcNum * TotalMeasFns
			cVal, err := getMeasCval(cwip, recNum, measCidx, mcNum, mcName)
			if err != nil {
				log.Errorf("buildTreeStructure: Could not get measure for cname: %v, err: %v",
					mcName, err)
			}
			err = stb.addMeasures(cVal, lenAggValues, midx, node)
			if err != nil {
				log.Errorf("buildTreeStructure: Could not add measure for cname: %v", mcName)
				return err
			}
		}
	}
	return nil
}

func (stb *StarTreeBuilder) addMeasures(val utils.CValueEnclosure,
	lenAggValues int, midx int, node *Node) error {

	if node.aggValues == nil {
		node.aggValues = make([]utils.CValueEnclosure, lenAggValues)
	}

	var err error
	// always calculate all meas Fns
	agvidx := midx + MeasFnMinIdx
	node.aggValues[agvidx], err = utils.Reduce(node.aggValues[agvidx], val, utils.Min)
	if err != nil {
		log.Errorf("addMeasures: error in min err:%v", err)
		return err
	}
	agvidx = midx + MeasFnMaxIdx
	node.aggValues[agvidx], err = utils.Reduce(node.aggValues[agvidx], val, utils.Max)
	if err != nil {
		log.Errorf("addMeasures: error in max err:%v", err)
		return err
	}
	agvidx = midx + MeasFnSumIdx
	node.aggValues[agvidx], err = utils.Reduce(node.aggValues[agvidx], val, utils.Sum)
	if err != nil {
		log.Errorf("addMeasures: error in sum err:%v", err)
		return err
	}

	agvidx = midx + MeasFnCountIdx
	// for count we always use 1 instead of val
	node.aggValues[agvidx], err = utils.Reduce(node.aggValues[agvidx], one, utils.Count)
	if err != nil {
		log.Errorf("addMeasures: error in count err:%v", err)
		return err
	}
	return nil
}

/*
ComputeStarTree

	Current assumptions:

	All groupBy columns that contain strings are dictionaryEncoded.
	Any column with len(col.deMap) != 0 is assumed to be dictionary encoded
	It is also assumed that no other values than the dic encoded strings appear in that column

	When storing all other values, their raw byte values are converted to an unsigned integer,
	and then converted to uint64 to have a consistent size

parameters:

	wipBlock: segstore's wip block

returns:

	StarTree: ptr to StarTree
*/
func (stb *StarTreeBuilder) ComputeStarTree(wip *WipBlock) error {

	err := stb.creatEnc(wip)
	if err != nil {
		return err
	}

	err = stb.buildTreeStructure(wip)
	if err != nil {
		return err
	}

	//	stb.logStarTreeSummary([]*Node{stb.tree.Root}, 0)
	//stb.logStarTreeIds(tree.Root, -1)

	return nil
}

/*
func (stb *StarTreeBuilder) logStarTreeSummary(nodes []*Node, level int) {
	nextLevel := []*Node{}
	for _, n := range nodes {
		for _, child := range n.children {
			nextLevel = append(nextLevel, child)
		}
	}

	log.Infof("logStarTreeSummary: level %d has %d nodes", level, len(nodes))
	if len(nextLevel) > 0 {
		stb.logStarTreeSummary(nextLevel, level+1)
	}
}
*/

/*
func (stb *StarTreeBuilder) logStarTreeIds(node *Node, level int) {

	log.Infof("logStarTreeIds: level %d nodeId: %v, numChilds: %v", level, node.myKey, len(node.children))

	for _, child := range node.children {
		stb.logStarTreeIds(child, level+1)
	}
	}
*/

func getMeasCval(cwip *ColWip, recNum uint16, cIdx []uint32, colNum int,
	colName string) (utils.CValueEnclosure, error) {

	if cwip.deCount < wipCardLimit {
		for dword, recNumsArr := range cwip.deMap {
			if toputils.BinarySearchUint16(recNum, recNumsArr) {
				mcVal, _, err := GetCvalFromRec([]byte(dword)[0:], 0)
				if err != nil {
					log.Errorf("getMeasCval: Could not extract val for cname: %v, dword: %v",
						colName, dword)
					return utils.CValueEnclosure{}, err
				}
				return mcVal, nil
			}
		}
		return utils.CValueEnclosure{}, fmt.Errorf("could not find recNum: %v", recNum)
	}

	cVal, endIdx, err := GetCvalFromRec(cwip.cbuf[cIdx[colNum]:], 0) // todo pass qid
	if err != nil {
		log.Errorf("getMeasCval: Could not extract val for cname: %v, idx: %v",
			colName, cIdx[colNum])
		return utils.CValueEnclosure{}, err
	}
	cIdx[colNum] += uint32(endIdx)
	return cVal, nil
}

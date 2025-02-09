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
	"sync"
	"time"

	"github.com/siglens/siglens/pkg/querytracker"
	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

const MaxConcurrentAgileTrees = 5

var currentAgileTreeCount int
var atreeCounterLock sync.Mutex = sync.Mutex{}

type StarTree struct {
	Root *Node
}

type Node struct {
	myKey          uint32
	parent         *Node
	children       map[uint32]*Node
	aggValues      []*utils.Number
	commonChildren map[uint32][]*Node
}

type StarTreeBuilder struct {
	groupByKeys       []string
	numGroupByCols    uint16
	mColNames         []string
	nodeCount         int
	nodePool          []*Node
	tree              *StarTree
	segDictMap        []map[string]uint32 // "mac" ==> enc-2
	segDictEncRev     [][]string          // [colNum]["ios", "mac", "win" ...] , [0][enc2] --> "mac"
	segDictLastNum    []uint32            // for each ColNum maintains the lastEnc increasing seq
	wipRecNumToColEnc [][]uint32          //maintain working buffer per wipBlock
	buf               []byte
	// array to keep reusing for tree traversal. [level][*Node Array]
	treeTravNodePtrs [][]*Node
}

// its ok for this to be int, since this will be used as an index in arrays
const (
	MeasFnMinIdx int = iota // has to be always zero based
	MeasFnMaxIdx
	MeasFnSumIdx
	MeasFnCountIdx
	MeasFnPercIdx
	MeasFnExactPercIdx
	MeasFnUpperPercIdx
	// Note: anytimes you add a Fn, make sure to adjust the IdxToAgFn array
	// Note: always keep this last since it is used for indexing into aggValues
	TotalMeasFns
)

var STBHolderPool [MaxConcurrentAgileTrees]*STBHolder

type STBHolder struct {
	stbPtr            *StarTreeBuilder
	currentlyInUse    bool
	lastUsedTimestamp time.Time
}

func GetSTB() *STBHolder {
	atreeCounterLock.Lock()
	defer atreeCounterLock.Unlock()
	if currentAgileTreeCount >= MaxConcurrentAgileTrees {
		return nil
	}

	for i := 0; i < MaxConcurrentAgileTrees; i++ {
		if STBHolderPool[i] == nil {
			STBHolderPool[i] = &STBHolder{
				stbPtr: &StarTreeBuilder{
					// 1 extra for the root level
					treeTravNodePtrs: make([][]*Node, querytracker.MAX_NUM_GROUPBY_COLS+1)},
			}
		}

		if !STBHolderPool[i].currentlyInUse {
			STBHolderPool[i].currentlyInUse = true
			STBHolderPool[i].lastUsedTimestamp = time.Now()
			currentAgileTreeCount++

			return STBHolderPool[i]
		}
	}

	return nil
}

func (stbHolder *STBHolder) ReleaseSTB() {
	atreeCounterLock.Lock()
	defer atreeCounterLock.Unlock()

	currentAgileTreeCount--
	stbHolder.currentlyInUse = false
}

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
	case utils.Perc:
		return MeasFnPercIdx
	case utils.ExactPerc:
		return MeasFnExactPercIdx
	case utils.UpperPerc:
		return MeasFnUpperPercIdx
	default:
		log.Errorf("AgFnToIdx: invalid fn: %v", fn)
		return MeasFnCountIdx
	}
}

func (stb *StarTreeBuilder) GetGroupByKeys() []string {
	return stb.groupByKeys
}

func (stb *StarTreeBuilder) GetNodeCount() int {
	return stb.nodeCount
}

func (stb *StarTreeBuilder) GetEachColNodeCount() map[string]uint32 {
	res := make(map[string]uint32)
	for colIdx, lastNum := range stb.segDictLastNum {
		res[stb.groupByKeys[colIdx]] = lastNum
	}
	return res
}

/*
ResetSegTree

	Current assumptions:

	All groupBy columns that contain strings are dictionaryEncoded.
	It is also assumed that no other values than the dic encoded strings appear in that column

	When storing all other values, their raw byte values are converted to an unsigned integer,
	and then converted to uint64 to have a consistent size

parameters:

	groupByKeys: groupBy column Names
	mColNames: colnames of measure columns

returns:
*/
func (stb *StarTreeBuilder) ResetSegTree(groupByKeys []string,
	mColNames []string, stbDictEncWorkBuf [][]string) {

	stb.groupByKeys = groupByKeys
	numGroupByCols := uint16(len(groupByKeys))
	stb.numGroupByCols = numGroupByCols
	stb.mColNames = mColNames

	stb.resetNodeData()

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
			stb.segDictEncRev[colNum] = stbDictEncWorkBuf[colNum]
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

func (stb *StarTreeBuilder) DropSegTree(stbDictEncWorkBuf [][]string) {
	stb.ResetSegTree(stb.groupByKeys, stb.mColNames, stbDictEncWorkBuf)
}

// DropColumns assumes that caller will call ComputeStarTree after this
func (stb *StarTreeBuilder) DropColumns(colsToDrop []string) error {
	if len(colsToDrop) == 0 {
		return nil
	}

	mapColNameToIdx := make(map[string]int)
	dropIndexes := make(map[int]struct{})

	for idx, colName := range stb.groupByKeys {
		mapColNameToIdx[colName] = idx
	}

	// check if all columns to drop are present
	for _, colToDrop := range colsToDrop {
		if _, exists := mapColNameToIdx[colToDrop]; !exists {
			return fmt.Errorf("DropColumns: column to drop %v not found", colToDrop)
		}
	}

	// drop the columns
	for _, colToDrop := range colsToDrop {
		colIdx := mapColNameToIdx[colToDrop]
		err := stb.dropColumn(colToDrop)
		if err != nil {
			return fmt.Errorf("DropColumns: Error while dropping column %v, err: %v", colToDrop, err)
		}
		dropIndexes[colIdx] = struct{}{}
	}

	stb.segDictMap = toputils.RemoveElements(stb.segDictMap, dropIndexes)
	stb.segDictEncRev = toputils.RemoveElements(stb.segDictEncRev, dropIndexes)
	stb.segDictLastNum = toputils.RemoveElements(stb.segDictLastNum, dropIndexes)

	// No need to update wipRecNumToColEnc based on index,
	// since it will be repopulated based on updated groupByKeys in creatEnc via ComputeStartree
	stb.wipRecNumToColEnc = stb.wipRecNumToColEnc[:stb.numGroupByCols]

	return nil
}

// This method is not for external use, always use DropColumns
func (stb *StarTreeBuilder) dropColumn(colToDrop string) error {
	newGrpByKeys := make([]string, 0, len(stb.groupByKeys)-1)
	dropLevel := -1
	for idx, col := range stb.groupByKeys {
		if col == colToDrop {
			dropLevel = idx
			continue
		}
		newGrpByKeys = append(newGrpByKeys, col)
	}

	if dropLevel == -1 {
		return fmt.Errorf("DropColumn: column to drop %v not found", colToDrop)
	}
	err := stb.removeLevelFromTree(stb.tree.Root, 0, uint(dropLevel))
	if err != nil {
		return err
	}
	stb.numGroupByCols--
	stb.groupByKeys = newGrpByKeys

	// Rest of the cleanup is done in DropColumns for efficiency

	return nil
}

func (stb *StarTreeBuilder) setColValEnc(colNum int, colValBytes []byte) uint32 {

	// todo a zero copy version of map lookups needed
	// todo the key in these maps could be hash of the byte array and then
	// we store a reverse hash map lookup
	colVal := string(colValBytes)
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
func (stb *StarTreeBuilder) resetNodeData() {

	for _, node := range stb.nodePool {
		node.parent = nil
		for k := range node.children {
			delete(node.children, k)
		}
		if len(node.aggValues) > 0 {
			for i := range node.aggValues {
				node.aggValues[i].Reset()
			}
		}
	}
	stb.nodeCount = 0
}

func (stb *StarTreeBuilder) newNode() *Node {

	// pre-alloc on the first one to the size of MaxAgileTree,
	// and after that if the nodePool count exceeds then we can do the
	// one by one extension
	stbNodePoolLen := len(stb.nodePool)
	stb.nodePool = toputils.ResizeSlice(stb.nodePool, MaxAgileTreeNodeCountForAlloc)
	if len(stb.nodePool) > stbNodePoolLen {
		for i := stbNodePoolLen; i < len(stb.nodePool); i++ {
			stb.nodePool[i] = &Node{}
		}
	}
	if stb.nodeCount >= len(stb.nodePool) {
		stb.nodePool = append(stb.nodePool, &Node{})
	}
	ans := stb.nodePool[stb.nodeCount]
	stb.nodeCount += 1

	if ans.children == nil {
		ans.children = make(map[uint32]*Node)
	}

	lenAggValues := len(stb.mColNames) * TotalMeasFns
	ans.aggValues = toputils.ResizeSlice(ans.aggValues, lenAggValues)
	for i := range ans.aggValues {
		ans.aggValues[i] = &utils.Number{}
	}

	return ans
}

func (stb *StarTreeBuilder) Aggregate(cur *Node) error {

	first := true

	lenAggValues := len(stb.mColNames) * TotalMeasFns
	if len(cur.aggValues) != lenAggValues {
		return fmt.Errorf("Aggregate: aggValues length mismatch expected: %v, got: %v", lenAggValues, len(cur.aggValues))
	}

	var err error
	for _, child := range cur.children {
		err = stb.Aggregate(child)
		if err != nil {
			return err
		}

		if first {
			for i, aggVal := range child.aggValues {
				cur.aggValues[i].Copy(aggVal)
			}
			first = false
			continue
		}

		for mcNum := range stb.mColNames {
			midx := mcNum * TotalMeasFns
			agidx := midx + MeasFnMinIdx
			err = cur.aggValues[agidx].ReduceFast(child.aggValues[agidx], utils.Min)
			if err != nil {
				log.Errorf("Aggregate: error in aggregating min err:%v", err)
				return err
			}
			agidx = midx + MeasFnMaxIdx
			err = cur.aggValues[agidx].ReduceFast(child.aggValues[agidx], utils.Max)
			if err != nil {
				log.Errorf("Aggregate: error in aggregating max err:%v", err)
				return err
			}
			agidx = midx + MeasFnSumIdx
			err = cur.aggValues[agidx].ReduceFast(child.aggValues[agidx], utils.Sum)
			if err != nil {
				log.Errorf("Aggregate: error in aggregating sum err:%v", err)
				return err
			}
			agidx = midx + MeasFnCountIdx
			err = cur.aggValues[agidx].ReduceFast(child.aggValues[agidx], utils.Count)
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

func (stb *StarTreeBuilder) updateAggVals(node *Node, nodeToMerge *Node) error {
	if nodeToMerge.aggValues == nil {
		return nil
	}
	if node.aggValues == nil {
		node.aggValues = make([]*utils.Number, len(stb.mColNames)*TotalMeasFns)
		for i := range node.aggValues {
			node.aggValues[i] = &utils.Number{}
		}
	}

	var err error
	for mcNum := range stb.mColNames {
		midx := mcNum * TotalMeasFns
		agvidx := midx + MeasFnMinIdx
		err = node.aggValues[agvidx].ReduceFast(nodeToMerge.aggValues[agvidx], utils.Min)
		if err != nil {
			log.Errorf("updateAggVals: error in min err:%v", err)
			return err
		}

		agvidx = midx + MeasFnMaxIdx
		err = node.aggValues[agvidx].ReduceFast(nodeToMerge.aggValues[agvidx], utils.Max)
		if err != nil {
			log.Errorf("updateAggVals: error in max err:%v", err)
			return err
		}

		agvidx = midx + MeasFnSumIdx
		err = node.aggValues[agvidx].ReduceFast(nodeToMerge.aggValues[agvidx], utils.Sum)
		if err != nil {
			log.Errorf("updateAggVals: error in sum err:%v", err)
			return err
		}

		agvidx = midx + MeasFnCountIdx
		err = node.aggValues[agvidx].ReduceFast(nodeToMerge.aggValues[agvidx], utils.Count)
		if err != nil {
			log.Errorf("updateAggVals: error in count err:%v", err)
			return err
		}
	}

	return nil
}

func (stb *StarTreeBuilder) mergeChildNodes(currNode *Node) error {
	var err error

	// delink the children from the current nodes and accumulate them as commonChildren
	for _, nodes := range currNode.commonChildren {
		fixedNode := nodes[0]
		commonChildren := make(map[uint32][]*Node)
		for _, node := range nodes {
			for key, child := range node.children {
				if commonChildren[key] == nil {
					commonChildren[key] = []*Node{child}
				} else {
					commonChildren[key] = append(commonChildren[key], child)
					err = stb.updateAggVals(commonChildren[key][0], child)
					if err != nil {
						return err
					}
				}
				child.parent = fixedNode
				delete(node.children, key)
			}
		}
		fixedNode.commonChildren = commonChildren
	}

	// link parent and children properly and cleanup commonChildren
	for _, children := range currNode.commonChildren {
		err = stb.mergeChildNodes(children[0])
		if err != nil {
			return err
		}
		currNode.children[children[0].myKey] = children[0]
	}
	currNode.commonChildren = nil

	return nil
}

func (stb *StarTreeBuilder) updateLastLevel(node *Node) error {
	for _, child := range node.children {
		err := stb.updateAggVals(node, child)
		if err != nil {
			return err
		}
		delete(node.children, child.myKey)
	}

	return nil
}

func (stb *StarTreeBuilder) removeLevelFromTree(node *Node, currColIdx uint, colIdxToRemove uint) error {
	if currColIdx == colIdxToRemove {
		if currColIdx == uint(stb.numGroupByCols-1) {
			// if last column needs to be removed, accumulation of children is not required as they will be unique.
			// just combine the aggs at parent.
			return stb.updateLastLevel(node)
		}
		commonChildren := make(map[uint32][]*Node)

		// accumulate grandchildren as commonChildren
		for childKey, childNode := range node.children {
			for key, grandchild := range childNode.children {
				grandchild.parent = node
				if commonChildren[key] == nil {
					commonChildren[key] = []*Node{grandchild}
				} else {
					commonChildren[key] = append(commonChildren[key], grandchild)
					err := stb.updateAggVals(commonChildren[key][0], grandchild)
					if err != nil {
						return err
					}
				}
				delete(childNode.children, key)
			}

			// add child aggs to parent
			err := stb.updateAggVals(node, childNode)
			if err != nil {
				return err
			}

			// remove children
			childNode.parent = nil
			delete(node.children, childKey)
		}

		node.commonChildren = commonChildren

		return stb.mergeChildNodes(node)
	}

	for _, child := range node.children {
		err := stb.removeLevelFromTree(child, currColIdx+1, colIdxToRemove)
		if err != nil {
			return err
		}
	}

	return nil
}

func (stb *StarTreeBuilder) creatEnc(wip *WipBlock) error {

	numRecs := wip.blockSummary.RecCount

	for colNum, colName := range stb.groupByKeys {
		stb.wipRecNumToColEnc[colNum] = toputils.ResizeSlice(stb.wipRecNumToColEnc[colNum], int(numRecs))

		cwip := wip.colWips[colName]
		deData := cwip.deData
		if deData.deCount < wipCardLimit {
			for rawKey, indices := range deData.deMap {

				enc := stb.setColValEnc(colNum, []byte(rawKey))
				for _, recNum := range indices {
					stb.wipRecNumToColEnc[colNum][recNum] = enc
				}
			}
			continue // done with this dict encoded column
		}

		// read the non-dict way
		idx := uint32(0)
		for recNum := uint16(0); recNum < numRecs; recNum++ {
			cValBytes, endIdx, err := getColByteSlice(cwip.cbuf, int(idx), 0) // todo pass qid here
			if err != nil {
				log.Errorf("populateLeafsWithMeasVals: Could not extract val for cname: %v, idx: %v",
					colName, idx)
				return err
			}
			idx += uint32(endIdx)
			enc := stb.setColValEnc(colNum, cValBytes)
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

	curColValues := make([]uint32, stb.numGroupByCols)
	lenAggValues := len(stb.mColNames) * TotalMeasFns
	measCidx := make([]uint32, len(stb.mColNames))

	num := &utils.Number{}

	for recNum := uint16(0); recNum < numRecs; recNum += 1 {
		for colNum := range stb.groupByKeys {
			curColValues[colNum] = stb.wipRecNumToColEnc[colNum][recNum]
		}
		node := stb.insertIntoTree(stb.tree.Root, curColValues[:stb.numGroupByCols], recNum, 0)
		for mcNum, mcName := range stb.mColNames {
			cwip := wip.colWips[mcName]
			midx := mcNum * TotalMeasFns
			err := getMeasCval(cwip, recNum, measCidx, mcNum, mcName, num)
			if err != nil {
				log.Errorf("buildTreeStructure: Could not get measure for cname: %v, err: %v",
					mcName, err)
				continue
			}
			err = stb.addMeasures(num, lenAggValues, midx, node)
			if err != nil {
				log.Errorf("buildTreeStructure: Could not add measure for cname: %v", mcName)
				return err
			}
		}
	}
	return nil
}

func (stb *StarTreeBuilder) addMeasures(val *utils.Number,
	lenAggValues int, midx int, node *Node) error {

	if len(node.aggValues) != lenAggValues {
		return fmt.Errorf("addMeasures: aggValues length mismatch expected: %v, got: %v", lenAggValues, len(node.aggValues))
	}

	var err error
	// always calculate all meas Fns
	agvidx := midx + MeasFnMinIdx
	err = node.aggValues[agvidx].ReduceFast(val, utils.Min)
	if err != nil {
		log.Errorf("addMeasures: error in min err:%v", err)
		return err
	}
	agvidx = midx + MeasFnMaxIdx
	err = node.aggValues[agvidx].ReduceFast(val, utils.Max)
	if err != nil {
		log.Errorf("addMeasures: error in max err:%v", err)
		return err
	}
	agvidx = midx + MeasFnSumIdx
	err = node.aggValues[agvidx].ReduceFast(val, utils.Sum)
	if err != nil {
		log.Errorf("addMeasures: error in sum err:%v", err)
		return err
	}

	one := &utils.Number{}
	one.SetInt64(1)

	agvidx = midx + MeasFnCountIdx
	// for count we always use 1 instead of val
	err = node.aggValues[agvidx].ReduceFast(one, utils.Count)
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
	colName string, num *utils.Number) error {

	deData := cwip.deData
	if deData.deCount < wipCardLimit {
		for dword, recNumsArr := range deData.deMap {

			if toputils.BinarySearchUint16(recNum, recNumsArr) {
				buffer := &toputils.Buffer{}
				buffer.Append([]byte(dword))
				_, err := GetNumValFromRec(buffer, 0, 0, num)
				if err != nil {
					log.Errorf("getMeasCval: Could not extract val for cname: %v, dword: %v",
						colName, dword)
					return err
				}
				return nil
			}
		}
		return fmt.Errorf("could not find recNum: %v", recNum)
	}

	endIdx, err := GetNumValFromRec(cwip.cbuf, int(cIdx[colNum]), 0, num) // todo pass qid
	if err != nil {
		log.Errorf("getMeasCval: Could not extract val for cname: %v, idx: %v",
			colName, cIdx[colNum])
		return err
	}
	cIdx[colNum] += uint32(endIdx)
	return nil
}

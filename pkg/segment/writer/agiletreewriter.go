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
	"fmt"
	"os"

	. "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

func (stb *StarTreeBuilder) encodeDictEnc(colName string, colNum uint16,
	buf []byte) uint32 {

	idx := uint32(0)

	// copy colname strlen
	l1 := uint16(len(colName))
	copy(buf[idx:], utils.Uint16ToBytesLittleEndian(l1))
	idx += 2

	// copy the colname str
	copy(buf[idx:], colName)
	idx += uint32(l1)

	numKeysForCol := stb.segDictLastNum[colNum]
	copy(buf[idx:], utils.Uint32ToBytesLittleEndian(numKeysForCol))
	idx += 4

	for i := uint32(0); i < numKeysForCol; i++ {

		curString := stb.segDictEncRev[colNum][i]

		// copy enc col val strlen
		l1 := uint16(len(curString))
		copy(buf[idx:], utils.Uint16ToBytesLittleEndian(l1))
		idx += 2

		// copy the enc col val str
		copy(buf[idx:], curString)
		idx += uint32(l1)
	}
	return idx
}

func (stb *StarTreeBuilder) encodeMetadata(strMFd *os.File) (uint32, error) {

	sizeNeeded := stb.estimateMetaSize()
	sizeToAdd := sizeNeeded - len(stb.buf)
	if sizeToAdd > 0 {
		newArr := make([]byte, sizeToAdd)
		stb.buf = append(stb.buf, newArr...)
	}

	idx := uint32(0)
	idx += 4 // reserve for metabyteslen

	// Len of groupByKeys
	copy(stb.buf[idx:], utils.Uint16ToBytesLittleEndian(stb.numGroupByCols))
	idx += 2

	// each groupbyKey
	for i := uint16(0); i < stb.numGroupByCols; i++ {
		// copy strlen
		l1 := uint16(len(stb.groupByKeys[i]))
		copy(stb.buf[idx:], utils.Uint16ToBytesLittleEndian(l1))
		idx += 2

		// copy the str
		copy(stb.buf[idx:], stb.groupByKeys[i])
		idx += uint32(l1)
	}

	// Len of MeasureColNames
	copy(stb.buf[idx:], utils.Uint16ToBytesLittleEndian(uint16(len(stb.mColNames))))
	idx += 2

	// each aggFunc
	for _, mCname := range stb.mColNames {

		// Mcol len
		l1 := uint16(len(mCname))
		copy(stb.buf[idx:], utils.Uint16ToBytesLittleEndian(l1))
		idx += 2

		// copy the Mcol strname
		copy(stb.buf[idx:], mCname)
		idx += uint32(l1)
	}

	for colNum, cName := range stb.groupByKeys {
		size := stb.encodeDictEnc(cName, uint16(colNum), stb.buf[idx:])
		idx += size
	}

	// metaDataLen
	copy(stb.buf[0:], utils.Uint32ToBytesLittleEndian(idx-4))

	_, err := strMFd.Write(stb.buf[:idx])
	if err != nil {
		log.Errorf("encodeMetadata: meta write failed fname=%v, err=%v", strMFd.Name(), err)
		return idx, err
	}

	return idx, nil
}

func (stb *StarTreeBuilder) encodeNddWrapper(segKey string, levsOffsets []int64,
	levsSizes []uint32) (uint32, error) {

	strLevFname := fmt.Sprintf("%s.strl", segKey)
	strLevFd, err := os.OpenFile(strLevFname, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("encodeNddWrapper: open failed fname=%v, err=%v", strLevFname, err)
		return 0, err
	}
	defer strLevFd.Close()

	size, err := stb.encodeNodeDetails(strLevFd, []*Node{stb.tree.Root}, 0, 0, levsOffsets,
		levsSizes)
	if err != nil {
		return 0, err
	}

	return size, nil
}

func (stb *StarTreeBuilder) encodeNodeDetails(strLevFd *os.File, curLevNodes []*Node,
	level int, strLevFileOff int64, levsOffsets []int64, levsSizes []uint32) (uint32, error) {

	// save current level offset
	levsOffsets[level] = strLevFileOff

	sizeNeeded := stb.estimateNodeSize(len(curLevNodes))
	sizeToAdd := sizeNeeded - len(stb.buf)
	if sizeToAdd > 0 {
		newArr := make([]byte, sizeToAdd)
		stb.buf = append(stb.buf, newArr...)
	}

	idx := uint32(0)
	// encode levelNum
	copy(stb.buf[idx:], utils.Uint16ToBytesLittleEndian(uint16(level)))
	idx += 2

	// numOfNodes at this level
	copy(stb.buf[idx:], utils.Uint32ToBytesLittleEndian(uint32(len(curLevNodes))))
	idx += 4

	nextLevelNodes := []*Node{}
	for _, n := range curLevNodes {

		// save nextlevel children
		for _, child := range n.children {
			nextLevelNodes = append(nextLevelNodes, child)
		}
		// encode curr nodes details

		// mapKey
		copy(stb.buf[idx:], utils.Uint32ToBytesLittleEndian(n.myKey))
		idx += 4

		// add Parent keys, don't add parents for root (level-0) and level-1 (since their parent is root)
		ancestor := n.parent
		for i := 1; i < level; i++ {
			if ancestor == nil {
				log.Errorf("encodeNodeDetails: ancestor is nil, level: %v, nodeKey: %+v", level, n.myKey)
				break
			}

			copy(stb.buf[idx:], utils.Uint32ToBytesLittleEndian(ancestor.myKey))
			idx += 4
			ancestor = ancestor.parent
		}

		// We should have reached the root.
		if level > 0 && ancestor != stb.tree.Root {
			log.Errorf("encodeNodeDetails: ancestor is not the root, level: %v, nodeKey: %+v", level, n.myKey)
		}

		for agIdx, e := range n.aggValues {
			copy(stb.buf[idx:], []byte{uint8(e.Dtype)})
			idx += 1

			switch e.Dtype {
			case SS_DT_UNSIGNED_NUM:
				copy(stb.buf[idx:], utils.Uint64ToBytesLittleEndian(e.CVal.(uint64)))
			case SS_DT_SIGNED_NUM:
				copy(stb.buf[idx:], utils.Int64ToBytesLittleEndian(e.CVal.(int64)))
			case SS_DT_FLOAT:
				copy(stb.buf[idx:], utils.Float64ToBytesLittleEndian(e.CVal.(float64)))
			case SS_DT_BACKFILL: // even for backfill we will have empty bytes in to keep things uniform
			default:
				return 0, fmt.Errorf("encodeNodeDetails: unsupported Dtype: %v, agIdx: %v, nodeKey: %+v, e: %+v",
					e.Dtype, agIdx, n.myKey, e)
			}
			idx += 8
		}
	}
	_, err := strLevFd.WriteAt(stb.buf[:idx], strLevFileOff)
	if err != nil {
		log.Errorf("encodeNodeDetails: nnd write failed, level: %v fname=%v, err=%v", level, strLevFd.Name(), err)
		return idx, err
	}
	strLevFileOff += int64(idx)
	levsSizes[level] = idx

	if len(nextLevelNodes) > 0 {
		nSize, err := stb.encodeNodeDetails(strLevFd, nextLevelNodes, level+1, strLevFileOff, levsOffsets, levsSizes)
		if err != nil {
			return 0, err
		}
		idx += nSize
	}

	return idx, nil
}

/*
	   *************** StarTree Encoding Format *****************************

	   [FileType 1B] [LenMetaData 4B] [MetaData] [NodeDataDetails]

	   [MetaData] :
		  [GroupbyKeys] [MeasureColNames] [DictEncCol-1] [DictEncCol-2] ...[DictEncCol-N]
			[GroupbyKeys] : [LenGrpKeys 2B] [GPK-1] [GPK-2]...
			   [GPK] : [StrLen 2B] [ActualStr xB]

		  [MeasureColNames] : [LenMeasureColNames 2B] [MeasureColName-1] [MeasureColNames-2] ...
			   [MeasureColNames-1] : [StrLen 2B] [McolName xB]

		  [DictEncCol-1] : [ColStrLen 2B] [ColName xB] [NumKeys 4B] [Enc-1] {Enc-2] ...
			   [Enc-1] : [EncStrLen 2B] [EncStr xB]

	   [NodeDataDetails]: [NddLen 4B] [LevOffMeta xB] [LevelDetails-1 xB] [LevelDetails-2 xB].... in BFS
		  [LevOffMetas] : [levOff-0 8B] [levSize-0 4B] [levOff-1 8B] [levSize-1 4B] ....
		  [LevelDetails-1] : [LevelNum 2B] [numNodesAtLevel 4B] [NodeAgInfo...]
			  [NodeAgInfo-1] : [nodeKey 4B] [parentKeys xB] [aggValue-1] [aggValue-2] ...
				[parentKeys] : [parKey-0 4B] [parKey-1 4B].... // numOfParents depends on level
				[aggValue]: [dType 1B] [val 8B]
*/
func (stb *StarTreeBuilder) EncodeStarTree(segKey string) (uint32, error) {

	strMetaFname := fmt.Sprintf("%s.strm", segKey)

	err := stb.Aggregate(stb.tree.Root)
	if err != nil {
		return 0, err
	}

	strMFd, err := os.OpenFile(strMetaFname, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("EncodeStarTree: open failed fname=%v, err=%v", strMetaFname, err)
		return 0, err
	}

	_, err = strMFd.Write(STAR_TREE_BLOCK)
	if err != nil {
		log.Errorf("EncodeStarTree: compression Type write failed fname=%v, err=%v", strMetaFname, err)
		strMFd.Close()
		_ = os.Remove(strMetaFname) //we don't want half encoded agileTree file
		return 0, err
	}

	metaSize, err := stb.encodeMetadata(strMFd)
	if err != nil {
		strMFd.Close()
		_ = os.Remove(strMetaFname)
		return 0, err
	}

	levsOffsets := make([]int64, stb.numGroupByCols+1)
	levsSizes := make([]uint32, stb.numGroupByCols+1)

	nddSize, err := stb.encodeNddWrapper(segKey, levsOffsets, levsSizes)
	if err != nil {
		log.Errorf("EncodeStarTree: failed to encode nodeDetails Err: %+v", err)
		strMFd.Close()
		_ = os.Remove(strMetaFname)
		return 0, err
	}

	err = stb.writeLevsInfo(strMFd, levsOffsets, levsSizes)
	if err != nil {
		log.Errorf("EncodeStarTree: failed to write levvsoff Err: %+v", err)
		strMFd.Close()
		_ = os.Remove(strMetaFname)
		return 0, err
	}

	strMFd.Close()
	return nddSize + metaSize, nil
}

func (stb *StarTreeBuilder) estimateNodeSize(numNodes int) int {

	// 9 for CvalEnc
	lenAggVals := len(stb.mColNames) * TotalMeasFns * 9
	// 4 (for curNode mapkey) + 4 per parent path to root + 1000 for buffer
	return numNodes*(lenAggVals+4+4*int(stb.numGroupByCols)) + 1000

}

func (stb *StarTreeBuilder) writeLevsInfo(strMFd *os.File, levsOffsets []int64,
	levsSizes []uint32) error {

	idx := uint32(0)

	// encode level offsets and sizes
	for i := range levsOffsets {
		copy(stb.buf[idx:], utils.Int64ToBytesLittleEndian(levsOffsets[i]))
		idx += 8
		copy(stb.buf[idx:], utils.Uint32ToBytesLittleEndian(levsSizes[i]))
		idx += 4
	}

	_, err := strMFd.Write(stb.buf[:idx])
	if err != nil {
		log.Errorf("writeLevsInfo: failed levOff writing, err: %v", err)
		return err
	}
	return nil
}

func (stb *StarTreeBuilder) estimateMetaSize() int {

	// 55: estimate for width of colNames
	colsMeta := (int(stb.numGroupByCols) + len(stb.mColNames)) * 55

	deSize := int(0)
	for colNum := range stb.groupByKeys {
		// 60 : estimate for colnamelen, columnname, 55: for enc len
		deSize += 60 + int(stb.segDictLastNum[colNum])*55
	}

	return colsMeta + deSize + 1000
}

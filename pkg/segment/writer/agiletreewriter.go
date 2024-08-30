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
	"bufio"
	"fmt"
	"os"

	. "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

func (stb *StarTreeBuilder) encodeDictEnc(colName string, colNum uint16,
	writer *bufio.Writer) (uint32, error) {

	size := uint32(0)

	// copy colname strlen
	l1 := uint16(len(colName))
	_, err := writer.Write(utils.Uint16ToBytesLittleEndian(l1))
	if err != nil {
		log.Errorf("StarTreeBuilder.encodeDictEnc: failed to write length of colName; err=%v", err)
		return 0, err
	}
	size += 2

	// copy the colname str
	_, err = writer.WriteString(colName)
	if err != nil {
		log.Errorf("StarTreeBuilder.encodeDictEnc: failed to write colName; err=%v", err)
		return 0, err
	}
	size += uint32(l1)

	numKeysForCol := stb.segDictLastNum[colNum]
	_, err = writer.Write(utils.Uint32ToBytesLittleEndian(numKeysForCol))
	if err != nil {
		log.Errorf("StarTreeBuilder.encodeDictEnc: failed to write numKeysForCol; err=%v", err)
		return 0, err
	}
	size += 4

	for i := uint32(0); i < numKeysForCol; i++ {

		curString := stb.segDictEncRev[colNum][i]

		// copy enc col val strlen
		l1 := uint16(len(curString))
		_, err = writer.Write(utils.Uint16ToBytesLittleEndian(l1))
		if err != nil {
			log.Errorf("StarTreeBuilder.encodeDictEnc: failed to write length of column value; err=%v", err)
			return 0, err
		}
		size += 2

		// copy the enc col val str
		_, err = writer.WriteString(curString)
		if err != nil {
			log.Errorf("StarTreeBuilder.encodeDictEnc: failed to write column value; err=%v", err)
			return 0, err
		}
		size += uint32(l1)
	}
	return size, nil
}

func (stb *StarTreeBuilder) encodeMetadata(strMFd *os.File) (uint32, error) {
	writer := bufio.NewWriter(strMFd)
	if writer == nil {
		err := fmt.Errorf("StarTreeBuilder.encodeMetadata: failed to create writer for %v", strMFd.Name())
		log.Errorf(err.Error())
		return 0, err
	}

	metadataSize := uint32(0)

	// The first 4 bytes specify the length of the metadata, which we don't know yet.
	_, err := writer.Write([]byte{0, 0, 0, 0})
	if err != nil {
		log.Errorf("StarTreeBuilder.encodeMetadata: failed to skip bytes for metadata length; err=%v", err)
		return 0, err
	}
	metadataSize += 4

	// Len of groupByKeys
	_, err = writer.Write(utils.Uint16ToBytesLittleEndian(stb.numGroupByCols))
	if err != nil {
		log.Errorf("StarTreeBuilder.encodeMetadata: failed to write number of groupby columns; err=%v", err)
		return 0, err
	}
	metadataSize += 2

	// each groupbyKey
	for i := uint16(0); i < stb.numGroupByCols; i++ {
		// copy strlen
		l1 := uint16(len(stb.groupByKeys[i]))
		_, err = writer.Write(utils.Uint16ToBytesLittleEndian(l1))
		if err != nil {
			log.Errorf("StarTreeBuilder.encodeMetadata: failed to write length of groupby column; err=%v", err)
			return 0, err
		}
		metadataSize += 2

		// copy the str
		_, err = writer.WriteString(stb.groupByKeys[i])
		if err != nil {
			log.Errorf("StarTreeBuilder.encodeMetadata: failed to write groupby column; err=%v", err)
			return 0, err
		}
		metadataSize += uint32(l1)
	}

	// Len of MeasureColNames
	_, err = writer.Write(utils.Uint16ToBytesLittleEndian(uint16(len(stb.mColNames))))
	if err != nil {
		log.Errorf("StarTreeBuilder.encodeMetadata: failed to write number of measure columns; err=%v", err)
		return 0, err
	}
	metadataSize += 2

	// each aggFunc
	for _, mCname := range stb.mColNames {
		// Mcol len
		l1 := uint16(len(mCname))
		_, err = writer.Write(utils.Uint16ToBytesLittleEndian(l1))
		if err != nil {
			log.Errorf("StarTreeBuilder.encodeMetadata: failed to write length of measure column; err=%v", err)
			return 0, err
		}
		metadataSize += 2

		// copy the Mcol strname
		_, err = writer.WriteString(mCname)
		if err != nil {
			log.Errorf("StarTreeBuilder.encodeMetadata: failed to write measure column; err=%v", err)
			return 0, err
		}
		metadataSize += uint32(l1)
	}

	for colNum, cName := range stb.groupByKeys {
		size, err := stb.encodeDictEnc(cName, uint16(colNum), writer)
		if err != nil {
			log.Errorf("StarTreeBuilder.encodeMetadata: failed to encodeDictEnc; err=%v", err)
			return 0, err
		}
		metadataSize += size
	}

	writer.Flush()

	// Now we know the size of the metadata, so we can write it. The value we
	// write doesn't include the 4 bytes we use to store the value.
	// We need to write from the 2nd byte, since the first byte is the file type.
	_, err = strMFd.WriteAt(utils.Uint32ToBytesLittleEndian(metadataSize-4), 1)
	if err != nil {
		log.Errorf("StarTreeBuilder.encodeMetadata: failed to write metadata length; err=%v", err)
		return 0, err
	}

	return metadataSize, nil
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
	stb.buf = utils.ResizeSlice(stb.buf, sizeNeeded)

	idx := uint32(0)
	// encode levelNum
	copy(stb.buf[idx:], utils.Uint16ToBytesLittleEndian(uint16(level)))
	idx += 2

	// numOfNodes at this level
	copy(stb.buf[idx:], utils.Uint32ToBytesLittleEndian(uint32(len(curLevNodes))))
	idx += 4

	numNodesNeeded := 0
	for _, n := range curLevNodes {
		numNodesNeeded += len(n.children)
	}

	nextLevelNodes := make([]*Node, numNodesNeeded)
	nlIdx := 0
	for _, n := range curLevNodes {

		// save nextlevel children
		for _, child := range n.children {
			nextLevelNodes[nlIdx] = child
			nlIdx++
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

		for _, e := range n.aggValues {
			e.CopyToBuffer(stb.buf[idx:])
			idx += 9
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

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

package sortindex

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/siglens/siglens/pkg/segment/reader/segread/segreader"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
)

type Block struct {
	BlockNum uint16   `json:"blockNum"`
	RecNums  []uint16 `json:"recNums"`
}

var VERSION_SORT_INDEX = []byte{0}

func getFilename(segkey string, cname string) string {
	return filepath.Join(segkey, cname+".srt") // srt means "sort", not an acronym
}

func WriteSortIndex(segkey string, cname string) error {
	blockToRecords, err := segreader.ReadAllRecords(segkey, cname)
	if err != nil {
		return fmt.Errorf("WriteSortIndex: failed reading all records for segkey=%v, cname=%v; err=%v", segkey, cname, err)
	}

	valToBlockToRecords := make(map[string]map[uint16][]uint16)
	for blockNum, records := range blockToRecords {
		for recNum, recBytes := range records {
			if len(recBytes) == 0 {
				return fmt.Errorf("WriteSortIndex: empty record for segkey=%v, cname=%v, blockNum=%v, recNum=%v",
					segkey, cname, blockNum, recNum)
			}

			idx := 1
			switch dtype := recBytes[0]; dtype {
			case segutils.VALTYPE_ENC_SMALL_STRING[0]:
				length := toputils.BytesToUint16LittleEndian(recBytes[idx : idx+2])
				idx += 2
				value := string(recBytes[idx : idx+int(length)])

				if _, ok := valToBlockToRecords[value]; !ok {
					valToBlockToRecords[value] = make(map[uint16][]uint16)
				}

				if _, ok := valToBlockToRecords[value][blockNum]; !ok {
					valToBlockToRecords[value][blockNum] = make([]uint16, 0)
				}

				valToBlockToRecords[value][blockNum] = append(valToBlockToRecords[value][blockNum], uint16(recNum))
			case segutils.VALTYPE_ENC_BACKFILL[0]:
				// TODO: handle it
			default:
				return fmt.Errorf("WriteSortIndex: unsupported dtype=%v for segkey=%v, cname=%v, blockNum=%v, recNum=%v",
					dtype, segkey, cname, blockNum, recNum)
			}
		}
	}

	err = writeSortIndex(segkey, cname, valToBlockToRecords)
	if err != nil {
		return fmt.Errorf("WriteSortIndex: failed writing sort index for segkey=%v, cname=%v; err=%v", segkey, cname, err)
	}

	return nil
}

// Version Number
// NumOfUniqueColValues
// [If numeric]
// DType ColValue1 NumBlocks BlockNum1 NumRecords Rec1, Rec2, … BlockNum2 NumRecords Rec1, Rec2
// [If string]
// DType NumBytes ColValue1 NumBlocks BlockNum3 NumRecords Rec1, Rec2, … BlockNum4 NumRecords Rec1, Rec2
func writeSortIndex(segkey string, cname string, valToBlockToRecords map[string]map[uint16][]uint16) error {
	filename := getFilename(segkey, cname)
	dir := filepath.Dir(filename)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}

	file, err := os.Create(getFilename(segkey, cname))
	if err != nil {
		return err
	}
	defer file.Close()

	sortedValues := utils.GetKeysOfMap(valToBlockToRecords)
	sort.Strings(sortedValues)

	writer := bufio.NewWriter(file)

	// Write version
	_, err = writer.Write(VERSION_SORT_INDEX)
	if err != nil {
		return fmt.Errorf("failed writing version: %v", err)
	}

	// Write number of unique column values
	_, err = writer.Write(utils.Uint64ToBytesLittleEndian(uint64(len(sortedValues))))
	if err != nil {
		return fmt.Errorf("failed writing number of unique column values: %v", err)
	}

	for _, value := range sortedValues {
		// Write column value
		err = writeCVal(writer, segutils.CValueEnclosure{
			Dtype: segutils.SS_DT_STRING,
			CVal:  value,
		})
		if err != nil {
			return fmt.Errorf("failed writing value: %v", err)
		}
		
		sortedBlockNums := utils.GetKeysOfMap(valToBlockToRecords[value])
		sort.Slice(sortedBlockNums, func(i, j int) bool { return sortedBlockNums[i] < sortedBlockNums[j] })

		// Write number of blocks
		_, err = writer.Write(utils.Uint32ToBytesLittleEndian(uint32(len(sortedBlockNums))))
		if err != nil {
			return fmt.Errorf("failed writing number of blocks for cname: %v, colValue: %v, len(sortedBlockNums): %v, err: %v", cname, value, len(sortedBlockNums), err)
		}
		
		for _, blockNum := range sortedBlockNums {
			sortedRecords, ok := valToBlockToRecords[value][blockNum]
			if !ok {
				return fmt.Errorf("missing records for value %s block %d", value, blockNum)
			}

			sort.Slice(sortedRecords, func(i, j int) bool { return sortedRecords[i] < sortedRecords[j] })
			block := Block{
				BlockNum: blockNum,
				RecNums:  sortedRecords,
			}

			// Write blockNum
			_, err = writer.Write(utils.Uint16ToBytesLittleEndian(block.BlockNum))
			if err != nil {
				return fmt.Errorf("failed writing blockNum: %v", err)
			}
			

			// Write number of records
			_, err = writer.Write(utils.Uint32ToBytesLittleEndian(uint32(len(block.RecNums))))
			if err != nil {
				return fmt.Errorf("failed writing number of records: %v", err)
			}

			// Write sortedRecords
			for _, recNum := range block.RecNums {
				// Write recNum
				_, err = writer.Write(utils.Uint16ToBytesLittleEndian(recNum))
				if err != nil {
					return fmt.Errorf("failed writing recNum: %v", err)
				}
			}
		}
	}

	return nil
}

type Line struct {
	Value  string
	Blocks []Block
}

func AsRRCs(lines []Line, segKeyEncoding uint32) ([]*segutils.RecordResultContainer, []segutils.CValueEnclosure) {
	rrcs := make([]*segutils.RecordResultContainer, 0)
	values := make([]segutils.CValueEnclosure, 0)

	for _, line := range lines {
		for _, block := range line.Blocks {
			for _, recordNum := range block.RecNums {
				rrcs = append(rrcs, &segutils.RecordResultContainer{
					BlockNum:  block.BlockNum,
					RecordNum: recordNum,
					SegKeyInfo: segutils.SegKeyInfo{
						SegKeyEnc: segKeyEncoding,
					},
				})
				values = append(values, segutils.CValueEnclosure{
					Dtype: segutils.SS_DT_STRING,
					CVal:  line.Value,
				})
			}
		}
	}

	return rrcs, values
}

func ReadSortIndex(segkey string, cname string, maxRecords int) ([]Line, error) {
	file, err := os.Open(getFilename(segkey, cname))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read version
	version := make([]byte, 1)
	err = binary.Read(file, binary.LittleEndian, &version)
	if err != nil {
		return nil, fmt.Errorf("failed reading version: %v", err)
	}
	if version[0] != VERSION_SORT_INDEX[0] {
		return nil, fmt.Errorf("unsupported version: %v", version)
	}

	// Read Number of unique column values
	var totalUniqueColValues uint64
	err = binary.Read(file, binary.LittleEndian, &totalUniqueColValues)
	if err != nil {
		return nil, fmt.Errorf("failed reading number of unique column values: %v", err)
	}

	lines := make([]Line, 0)

	numColValues := uint64(0)
	for numColValues < totalUniqueColValues && err == nil {
		// Read DType
		var Dtype segutils.SS_DTYPE
		err = binary.Read(file, binary.LittleEndian, &Dtype)
		if err != nil {
			return nil, fmt.Errorf("failed reading DType: %v", err)
		}

		// Read len of value
		var valueLen uint16
		err = binary.Read(file, binary.LittleEndian, &valueLen)
		if err != nil {
			return nil, fmt.Errorf("failed reading len of value: %v", err)
		}

		valueBytes := make([]byte, valueLen)
		_, err = file.Read(valueBytes)
		if err != nil {
			return nil, err
		}
		value := string(valueBytes)


		// Read total blocks
		var totalBlocks uint32
		err = binary.Read(file, binary.LittleEndian, &totalBlocks)
		if err != nil {
			return nil, fmt.Errorf("failed reading totalBlocks: %v", err)
		}

		numBlocks := uint32(0)
		blocks := make([]Block, 0)

		for numBlocks < totalBlocks || err == io.EOF {
			// Read blockNum
			var blockNum uint16
			err = binary.Read(file, binary.LittleEndian, &blockNum)
			if err != nil {
				return nil, fmt.Errorf("failed reading blockNum: %v", err)
			}

			// Read total records
			var totalRecords uint32
			err = binary.Read(file, binary.LittleEndian, &totalRecords)
			if err != nil {
				return nil, fmt.Errorf("failed reading totalRecords: %v", err)
			}

			numRecords := uint32(0)
			recNums := make([]uint16, 0)
			for numRecords < totalRecords && err != io.EOF{
				// Read recNum
				var recNum uint16
				err = binary.Read(file, binary.LittleEndian, &recNum)
				if err != io.EOF {
					return nil, fmt.Errorf("failed reading recNum: %v", err)
				}

				numRecords++
				recNums = append(recNums, recNum)
			}

			if numRecords != totalRecords {
				return nil, fmt.Errorf("sort file seems to be corrupted, numRecords(%v) != totalRecords(%v)", numRecords, totalRecords)
			}

			numBlocks++
			blocks = append(blocks, Block{
				BlockNum: blockNum,
				RecNums:  recNums,
			})
		}

		if numBlocks != totalBlocks {
			return nil, fmt.Errorf("sort file seems to be corrupted, numBlocks(%v) != totalBlocks(%v)", numBlocks, totalBlocks)
		}

		lines = append(lines, Line{
			Value:  value,
			Blocks: blocks,
		})
	}

	if err != io.EOF {
		return nil, fmt.Errorf("Error while reading sort index: %v", err)
	}

	return lines, nil
}

func writeCVal(writer *bufio.Writer, CVal segutils.CValueEnclosure) error {
	if writer == nil {
		return fmt.Errorf("writeCVal: writer is nil")
	}

	if CVal.Dtype != segutils.SS_DT_STRING {
		return fmt.Errorf("writeCVal: Unsupported Dtype: %v", CVal.Dtype)
	}

	// Write dtype
	writer.Write([]byte{byte(CVal.Dtype)})

	switch CVal.Dtype {
	case segutils.SS_DT_STRING:
		// Write len of string
		writer.Write(utils.Uint16ToBytesLittleEndian(uint16(len(CVal.CVal.(string)))))
		// Write string
		writer.Write([]byte(CVal.CVal.(string)))
	default:
		return fmt.Errorf("writeCVal: Unsupported Dtype: %v", CVal.Dtype)
	}

	return nil
}
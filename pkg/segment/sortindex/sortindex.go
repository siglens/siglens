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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
)

type block struct {
	BlockNum uint16
	Records  []uint16
}

func getFilename(segkey string, cname string) string {
	return filepath.Join(segkey, cname+".sort")
}

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

	bytes := make([]byte, 0)
	for _, value := range sortedValues {
		bytes = bytes[:0]
		bytes = append(bytes, utils.Uint16ToBytesLittleEndian(uint16(len(value)))...)
		bytes = append(bytes, []byte(value)...)

		sortedBlockNums := utils.GetKeysOfMap(valToBlockToRecords[value])
		sort.Slice(sortedBlockNums, func(i, j int) bool { return sortedBlockNums[i] < sortedBlockNums[j] })

		allBlocks := make([]block, 0, len(sortedBlockNums))
		for _, blockNum := range sortedBlockNums {
			sortedRecords, ok := valToBlockToRecords[value][blockNum]
			if !ok {
				return fmt.Errorf("missing records for value %s block %d", value, blockNum)
			}

			sort.Slice(sortedRecords, func(i, j int) bool { return sortedRecords[i] < sortedRecords[j] })
			block := block{
				BlockNum: blockNum,
				Records:  sortedRecords,
			}

			allBlocks = append(allBlocks, block)
		}

		jsonBytes, err := json.Marshal(allBlocks)
		if err != nil {
			return err
		}

		bytes = append(bytes, utils.Uint64ToBytesLittleEndian(uint64(len(jsonBytes)))...)
		bytes = append(bytes, jsonBytes...)
		_, err = file.Write(bytes)
		if err != nil {
			return err
		}
	}

	return nil
}

type line struct {
	Value  string
	Blocks []block
}

func AsRRCs(lines []line, segKeyEncoding uint32) ([]*segutils.RecordResultContainer, []segutils.CValueEnclosure) {
	rrcs := make([]*segutils.RecordResultContainer, 0)
	values := make([]segutils.CValueEnclosure, 0)

	for _, line := range lines {
		for _, block := range line.Blocks {
			for _, recordNum := range block.Records {
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

func ReadSortIndex(segkey string, cname string, maxRecords int) ([]line, error) {
	file, err := os.Open(getFilename(segkey, cname))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	lines := make([]line, 0)

	numRecords := 0
	for numRecords < maxRecords {
		var valueLen uint16
		err := binary.Read(file, binary.LittleEndian, &valueLen)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		valueBytes := make([]byte, valueLen)
		_, err = file.Read(valueBytes)
		if err != nil {
			return nil, err
		}
		value := string(valueBytes)

		var blocksLen uint64
		err = binary.Read(file, binary.LittleEndian, &blocksLen)
		if err != nil {
			return nil, err
		}

		blocksBytes := make([]byte, blocksLen)
		_, err = file.Read(blocksBytes)
		if err != nil {
			return nil, err
		}

		var blocks []block
		err = json.Unmarshal(blocksBytes, &blocks)
		if err != nil {
			return nil, err
		}

		for _, block := range blocks {
			numRecords += len(block.Records)
		}

		lines = append(lines, line{
			Value:  value,
			Blocks: blocks,
		})
	}

	return lines, nil
}

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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/siglens/siglens/pkg/segment/reader/segread/segreader"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

type Block struct {
	BlockNum uint16   `json:"blockNum"`
	RecNums  []uint16 `json:"recNums"`
}

type SortColumnConfig struct {
	columns []string
	mu      sync.RWMutex
}

var sortConfig = &SortColumnConfig{
	columns: make([]string, 0),
}

var VERSION_SORT_INDEX = []byte{0}

type SortMode int

const (
	InvalidSortMode SortMode = iota
	SortAsAuto
	SortAsNumeric
	SortAsString
)

var AllSortModes = []SortMode{SortAsAuto, SortAsNumeric, SortAsString}

func getFilename(segkey string, cname string, sortMode SortMode) (string, error) {
	suffix := ""
	switch sortMode {
	case SortAsAuto:
		suffix = "_auto"
	case SortAsNumeric:
		suffix = "_num"
	case SortAsString:
		suffix = "_str"
	default:
		return "", fmt.Errorf("getFilename: invalid sort mode: %v", sortMode)
	}

	return filepath.Join(segkey, cname+suffix+".srt"), nil // srt means "sort", not an acronym
}

func getTempFilename(segkey string, cname string, sortMode SortMode) (string, error) {
	filename, err := getFilename(segkey, cname, sortMode)
	if err != nil {
		return "", fmt.Errorf("getTempFilename: failed getting filename: %v", err)
	}

	return filename + ".tmp", nil
}

func WriteSortIndex(segkey string, cname string, sortModes []SortMode) error {
	blockToRecords, err := segreader.ReadAllRecords(segkey, cname)
	if err != nil {
		return fmt.Errorf("WriteSortIndex: failed reading all records for segkey=%v, cname=%v; err=%v", segkey, cname, err)
	}

	valToBlockToRecords := make(map[segutils.CValueEnclosure]map[uint16][]uint16)
	enclosure := segutils.CValueEnclosure{}
	for blockNum, records := range blockToRecords {
		for recNum, recBytes := range records {
			if len(recBytes) == 0 {
				return fmt.Errorf("WriteSortIndex: empty record for segkey=%v, cname=%v, blockNum=%v, recNum=%v",
					segkey, cname, blockNum, recNum)
			}

			_, err := enclosure.FromBytes(recBytes)
			if err != nil {
				return fmt.Errorf("WriteSortIndex: failed to decode CValueEnclosure for segkey=%v, cname=%v, blockNum=%v, recNum=%v; err=%v",
					segkey, cname, blockNum, recNum, err)
			}

			if _, ok := valToBlockToRecords[enclosure]; !ok {
				valToBlockToRecords[enclosure] = make(map[uint16][]uint16)
			}

			if _, ok := valToBlockToRecords[enclosure][blockNum]; !ok {
				valToBlockToRecords[enclosure][blockNum] = make([]uint16, 0)
			}

			valToBlockToRecords[enclosure][blockNum] = append(valToBlockToRecords[enclosure][blockNum], uint16(recNum))
		}
	}

	for _, mode := range sortModes {
		err = writeSortIndex(segkey, cname, mode, valToBlockToRecords)
		if err != nil {
			return fmt.Errorf("WriteSortIndex: failed writing sort index for segkey=%v, cname=%v, mode=%v; err=%v",
				segkey, cname, mode, err)
		}
	}

	return nil
}

func sortEnclosures(values []segutils.CValueEnclosure, sortMode SortMode) error {
	switch sortMode {
	case SortAsAuto:
		// TODO: when IP sorting is handled, don't fall through.
		fallthrough
	case SortAsNumeric: // TODO:
		sort.Slice(values, func(i, j int) bool {
			v1, ok1 := values[i].GetFloatValueIfPossible()
			v2, ok2 := values[j].GetFloatValueIfPossible()

			if ok1 && ok2 {
				return v1 < v2
			} else if ok1 && !ok2 {
				return true
			} else if !ok1 && ok2 {
				return false
			}

			// Neither are numeric, so sort as strings.
			s1, err := values[i].GetString()
			if err != nil {
				return false
			}

			s2, err := values[j].GetString()
			if err != nil {
				return true
			}

			return s1 < s2
		})
	case SortAsString:
		sort.Slice(values, func(i, j int) bool {
			s1, err := values[i].GetString()
			if err != nil {
				return false
			}

			s2, err := values[j].GetString()
			if err != nil {
				return true
			}

			return s1 < s2
		})
	default:
		return fmt.Errorf("sortEnclosures: invalid sort mode: %v", sortMode)
	}

	return nil
}

// Version Number
// NumOfUniqueColValues
// [If numeric]
// DType ColValue1 NumBlocks BlockNum1 NumRecords Rec1, Rec2, … BlockNum2 NumRecords Rec1, Rec2
// [If string]
// DType NumBytes ColValue1 NumBlocks BlockNum3 NumRecords Rec1, Rec2, … BlockNum4 NumRecords Rec1, Rec2
func writeSortIndex(segkey string, cname string, sortMode SortMode,
	valToBlockToRecords map[segutils.CValueEnclosure]map[uint16][]uint16) error {

	switch sortMode {
	case SortAsAuto, SortAsNumeric, SortAsString: // Do nothing.
	default:
		return fmt.Errorf("writeSortIndex: invalid sort mode: %v", sortMode)
	}

	filename, err := getFilename(segkey, cname, sortMode)
	if err != nil {
		return fmt.Errorf("writeSortIndex: failed getting filename: %v", err)
	}

	dir := filepath.Dir(filename)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}

	finalName, err := getFilename(segkey, cname, sortMode)
	if err != nil {
		return fmt.Errorf("writeSortIndex: failed getting filename: %v", err)
	}

	tmpFileName, err := getTempFilename(segkey, cname, sortMode)
	if err != nil {
		return fmt.Errorf("writeSortIndex: failed getting temp filename: %v", err)
	}

	file, err := os.Create(tmpFileName)
	if err != nil {
		return err
	}
	defer file.Close()

	sortedValues := utils.GetKeysOfMap(valToBlockToRecords)
	err = sortEnclosures(sortedValues, sortMode)
	if err != nil {
		return fmt.Errorf("writeSortIndex: failed to sort column values: %v", err)
	}

	writer := bufio.NewWriter(file)

	// Write version
	_, err = writer.Write(VERSION_SORT_INDEX)
	if err != nil {
		return fmt.Errorf("writeSortIndex: failed writing version: %v", err)
	}

	// Write number of unique column values
	_, err = writer.Write(utils.Uint64ToBytesLittleEndian(uint64(len(sortedValues))))
	if err != nil {
		return fmt.Errorf("writeSortIndex: failed writing number of unique column values: %v", err)
	}

	for _, value := range sortedValues {
		// Write column value
		err = value.WriteBytes(writer)
		if err != nil {
			return fmt.Errorf("writeSortIndex: failed writing CValEnc: %v", err)
		}

		sortedBlockNums := utils.GetKeysOfMap(valToBlockToRecords[value])
		sort.Slice(sortedBlockNums, func(i, j int) bool { return sortedBlockNums[i] < sortedBlockNums[j] })

		// Write number of blocks
		_, err = writer.Write(utils.Uint32ToBytesLittleEndian(uint32(len(sortedBlockNums))))
		if err != nil {
			return fmt.Errorf("writeSortIndex: failed writing number of blocks for cname: %v, colValue: %v, len(sortedBlockNums): %v, err: %v", cname, value, len(sortedBlockNums), err)
		}

		for _, blockNum := range sortedBlockNums {
			sortedRecords, ok := valToBlockToRecords[value][blockNum]
			if !ok {
				return fmt.Errorf("writeSortIndex: missing records for value %v block %d", value, blockNum)
			}

			sort.Slice(sortedRecords, func(i, j int) bool { return sortedRecords[i] < sortedRecords[j] })
			block := Block{
				BlockNum: blockNum,
				RecNums:  sortedRecords,
			}

			// Write blockNum
			_, err = writer.Write(utils.Uint16ToBytesLittleEndian(block.BlockNum))
			if err != nil {
				return fmt.Errorf("writeSortIndex: failed writing blockNum, blockNum: %v, err: %v", block.BlockNum, err)
			}

			// Write number of records
			_, err = writer.Write(utils.Uint32ToBytesLittleEndian(uint32(len(block.RecNums))))
			if err != nil {
				return fmt.Errorf("writeSortIndex: failed writing number of records: %v, err: %v", len(block.RecNums), err)
			}

			// Write sortedRecords
			for _, recNum := range block.RecNums {
				// Write recNum
				_, err = writer.Write(utils.Uint16ToBytesLittleEndian(recNum))
				if err != nil {
					return fmt.Errorf("writeSortIndex: failed writing recNum: %v, err: %v", recNum, err)
				}
			}
		}
	}

	err = writer.Flush()
	if err != nil {
		return fmt.Errorf("writeSortIndex: failed flushing writer: %v", err)
	}

	err = os.Rename(tmpFileName, finalName)
	if err != nil {
		return fmt.Errorf("writeSortIndex: error while migrating %v to %v, err: %v", tmpFileName, finalName, err)
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

type Checkpoint struct {
	offsetToStartOfLine uint64
	totalOffset         uint64
	eof                 bool
}

func ReadSortIndex(segkey string, cname string, sortMode SortMode, maxRecordsToRead int,
	fromCheckpoint *Checkpoint) ([]Line, *Checkpoint, error) {

	if IsEOF(fromCheckpoint) {
		return nil, fromCheckpoint, nil
	}

	switch sortMode {
	case SortAsAuto, SortAsNumeric, SortAsString: // Do nothing.
	default:
		return nil, nil, fmt.Errorf("ReadSortIndex: invalid sort mode: %v", sortMode)
	}

	filename, err := getFilename(segkey, cname, sortMode)
	if err != nil {
		return nil, nil, fmt.Errorf("ReadSortIndex: failed getting filename: %v", err)
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	// Read version
	version := make([]byte, 1)
	err = binary.Read(file, binary.LittleEndian, &version)
	if err != nil {
		return nil, nil, fmt.Errorf("ReadSortIndex: failed reading version: %v", err)
	}
	if version[0] != VERSION_SORT_INDEX[0] {
		return nil, nil, fmt.Errorf("ReadSortIndex: unsupported version: %v", version)
	}

	// Read Number of unique column values
	var totalUniqueColValues uint64
	err = binary.Read(file, binary.LittleEndian, &totalUniqueColValues)
	if err != nil {
		return nil, nil, fmt.Errorf("ReadSortIndex: failed reading number of unique column values: %v", err)
	}

	lines := make([]Line, 0)

	numColValues := uint64(0)
	totalRecordsRead := uint64(0)
	done := false

	// Skip to the checkpoint.
	if fromCheckpoint != nil {
		_, err = file.Seek(int64(fromCheckpoint.offsetToStartOfLine), io.SeekStart)
		if err != nil {
			return nil, nil, fmt.Errorf("ReadSortIndex: failed seeking to start of line (position %v): %v",
				fromCheckpoint.offsetToStartOfLine, err)
		}
	}

	finalCheckpoint := &Checkpoint{}
	for numColValues < totalUniqueColValues && !done {
		filePos, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, nil, fmt.Errorf("ReadSortIndex: failed to get current file position: %v", err)
		}
		finalCheckpoint.offsetToStartOfLine = uint64(filePos)

		// Read DType
		var Dtype segutils.SS_DTYPE
		err = binary.Read(file, binary.LittleEndian, &Dtype)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("ReadSortIndex: failed reading DType: %v", err)
		}

		// Read len of value
		var valueLen uint16
		err = binary.Read(file, binary.LittleEndian, &valueLen)
		if err != nil {
			return nil, nil, fmt.Errorf("ReadSortIndex: failed reading len of value: %v", err)
		}

		valueBytes := make([]byte, valueLen)
		_, err = file.Read(valueBytes)
		if err != nil {
			return nil, nil, fmt.Errorf("ReadSortIndex: failed reading value: %v", err)
		}
		value := string(valueBytes)

		// Read total blocks
		var totalBlocks uint32
		err = binary.Read(file, binary.LittleEndian, &totalBlocks)
		if err != nil {
			return nil, nil, fmt.Errorf("ReadSortIndex: failed reading totalBlocks: %v", err)
		}

		numBlocks := uint32(0)
		blocks := make([]Block, 0)

		for numBlocks < totalBlocks && !done {
			// Read blockNum
			var blockNum uint16
			err = binary.Read(file, binary.LittleEndian, &blockNum)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, nil, fmt.Errorf("ReadSortIndex: failed reading blockNum: %v", err)
			}

			// Read total records
			var totalRecords uint32
			err = binary.Read(file, binary.LittleEndian, &totalRecords)
			if err != nil {
				return nil, nil, fmt.Errorf("ReadSortIndex: failed reading totalRecords: %v", err)
			}

			numRecords := uint32(0)
			recNums := make([]uint16, 0)
			for numRecords < totalRecords && totalRecordsRead < uint64(maxRecordsToRead) {
				// Read recNum
				var recNum uint16
				err = binary.Read(file, binary.LittleEndian, &recNum)
				if err == io.EOF {
					break
				}
				if err != nil {
					return nil, nil, fmt.Errorf("ReadSortIndex: failed reading recNum: %v", err)
				}

				numRecords++

				if pastCheckpoint(fromCheckpoint, file) {
					totalRecordsRead++
					recNums = append(recNums, recNum)
				}
			}

			if totalRecordsRead >= uint64(maxRecordsToRead) {
				done = true

				filePos, err = file.Seek(0, io.SeekCurrent)
				if err != nil {
					return nil, nil, fmt.Errorf("ReadSortIndex: failed to get current file position: %v", err)
				}
				finalCheckpoint.totalOffset = uint64(filePos)
			} else if numRecords != totalRecords {
				return nil, nil, fmt.Errorf("ReadSortIndex: sort file seems to be corrupted, numRecords(%v) != totalRecords(%v)", numRecords, totalRecords)
			}

			numBlocks++
			if pastCheckpoint(fromCheckpoint, file) {
				blocks = append(blocks, Block{
					BlockNum: blockNum,
					RecNums:  recNums,
				})
			}
		}

		if !done && numBlocks != totalBlocks {
			return nil, nil, fmt.Errorf("ReadSortIndex: sort file seems to be corrupted, numBlocks(%v) != totalBlocks(%v)", numBlocks, totalBlocks)
		}

		if pastCheckpoint(fromCheckpoint, file) {
			lines = append(lines, Line{
				Value:  value,
				Blocks: blocks,
			})
		}
	}

	if err != nil && err != io.EOF {
		return nil, nil, fmt.Errorf("ReadSortIndex: Error while reading sort index: %v", err)
	}

	filePos, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, nil, fmt.Errorf("ReadSortIndex: failed to get current file position: %v", err)
	}
	finalCheckpoint.totalOffset = uint64(filePos)

	// Check if we reached the end of the file.
	fileStat, err := file.Stat()
	if err != nil {
		return nil, nil, fmt.Errorf("ReadSortIndex: failed to get file stat: %v", err)
	}

	if finalCheckpoint.totalOffset == uint64(fileStat.Size()) {
		finalCheckpoint.eof = true
	}

	return lines, finalCheckpoint, nil
}

func pastCheckpoint(fromCheckpoint *Checkpoint, file *os.File) bool {
	if fromCheckpoint == nil {
		return true
	}

	filePos, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Errorf("pastCheckpoint: failed to get current file position: %v", err)
		return false
	}

	return uint64(filePos) > fromCheckpoint.totalOffset
}

func IsEOF(checkpoint *Checkpoint) bool {
	if checkpoint == nil {
		return false
	}

	return checkpoint.eof
}

func SetSortColumns(columnNames []string) error {
	sortConfig.mu.Lock()
	defer sortConfig.mu.Unlock()

	for _, col := range columnNames {
		if col == "" {
			return fmt.Errorf("SetSortColumns: column names must be non-empty strings")
		}
	}

	sortConfig.columns = columnNames

	return nil
}

func GetSortColumns() []string {
	sortConfig.mu.RLock()
	defer sortConfig.mu.RUnlock()
	return sortConfig.columns
}

func SetSortColumnsAPI(ctx *fasthttp.RequestCtx) {
	var columns []string
	if err := json.Unmarshal(ctx.PostBody(), &columns); err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBodyString("Invalid request body: " + err.Error())
		return
	}

	if err := SetSortColumns(columns); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBodyString("Failed to set sort columns: " + err.Error())
		return
	}

	ctx.SetContentType("application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetBodyString("Sort columns updated successfully")
}

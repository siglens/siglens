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
	"slices"
	"sort"
	"sync"

	"github.com/siglens/siglens/pkg/config"
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

var sortConfigMutex sync.RWMutex

type SortColumnsConfig struct {
	Indexes map[string][]string `json:"indexes"`
}

var VERSION_SORT_INDEX = []byte{1}

type metadata struct {
	version      uint8
	valueOffsets []uint64
}

type SortMode int

const (
	SortAsAuto SortMode = iota + 1
	SortAsNumeric
	SortAsString
)

var AllSortModes = []SortMode{SortAsAuto, SortAsNumeric, SortAsString}

func getSortColumnsConfigPath() string {
	return filepath.Join(config.GetDataPath(), "common", "sort_columns.json")
}

func (sm SortMode) String() string {
	switch sm {
	case SortAsAuto:
		return "auto"
	case SortAsNumeric:
		return "num"
	case SortAsString:
		return "str"
	}

	return "unknown"
}

func ModeFromString(mode string) (SortMode, error) {
	switch mode {
	case "", "auto":
		return SortAsAuto, nil
	case "num":
		return SortAsNumeric, nil
	case "str":
		return SortAsString, nil
	}

	return -1, fmt.Errorf("invalid sort mode: %v", mode)
}

func getFilename(segkey string, cname string, sortMode SortMode) string {
	suffix := ""
	switch sortMode {
	case SortAsAuto:
		suffix = "_auto"
	case SortAsNumeric:
		suffix = "_num"
	case SortAsString:
		suffix = "_str"
	}

	return filepath.Join(segkey, cname+suffix+".srt") // srt means "sort", not an acronym
}

func getTempFilename(segkey string, cname string, sortMode SortMode) string {
	return getFilename(segkey, cname, sortMode) + ".tmp"
}

func Exists(segkey string, cname string, sortMode SortMode) bool {
	filename := getFilename(segkey, cname, sortMode)

	_, err := os.Stat(filename)
	return err == nil
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

// File format:
//
//	 Metadata
//	   Version Number
//	     Number of unique column values
//		 List of offsets in the file where each unique column value starts
//	 Data
//	   [CValueEnclosure encoding] NumBlocks BlockNum1 NumRecords Rec1, Rec2, … BlockNum2 NumRecords Rec1, Rec2
func writeSortIndex(segkey string, cname string, sortMode SortMode,
	valToBlockToRecords map[segutils.CValueEnclosure]map[uint16][]uint16) error {

	switch sortMode {
	case SortAsAuto, SortAsNumeric, SortAsString: // Do nothing.
	default:
		return fmt.Errorf("writeSortIndex: invalid sort mode: %v", sortMode)
	}

	filename := getFilename(segkey, cname, sortMode)

	dir := filepath.Dir(filename)
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}

	finalName := getFilename(segkey, cname, sortMode)
	tmpFileName := getTempFilename(segkey, cname, sortMode)

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

	// Skip the space for the offsets; we'll write them later.
	offsets := make([]uint64, 0, len(sortedValues))
	_, err = writer.Write(make([]byte, 8*len(sortedValues)))
	if err != nil {
		return fmt.Errorf("writeSortIndex: failed skipping space for offsets: %v", err)
	}

	offset := uint64(9 + len(sortedValues)*8) // 1 (version) + 8 (numUniqueColValues) + offsets
	for _, value := range sortedValues {
		offsets = append(offsets, offset)

		// Write column value
		size, err := value.WriteBytes(writer)
		if err != nil {
			return fmt.Errorf("writeSortIndex: failed writing CValEnc: %v", err)
		}

		offset += uint64(size)
		sortedBlockNums := utils.GetKeysOfMap(valToBlockToRecords[value])
		slices.Sort(sortedBlockNums)

		// Write number of blocks
		_, err = writer.Write(utils.Uint32ToBytesLittleEndian(uint32(len(sortedBlockNums))))
		if err != nil {
			return fmt.Errorf("writeSortIndex: failed writing number of blocks for cname: %v, colValue: %v, len(sortedBlockNums): %v, err: %v", cname, value, len(sortedBlockNums), err)
		}
		offset += 4

		for _, blockNum := range sortedBlockNums {
			sortedRecords, ok := valToBlockToRecords[value][blockNum]
			if !ok {
				return fmt.Errorf("writeSortIndex: missing records for value %v block %d", value, blockNum)
			}

			slices.Sort(sortedRecords)
			block := Block{
				BlockNum: blockNum,
				RecNums:  sortedRecords,
			}

			// Write blockNum
			_, err = writer.Write(utils.Uint16ToBytesLittleEndian(block.BlockNum))
			if err != nil {
				return fmt.Errorf("writeSortIndex: failed writing blockNum, blockNum: %v, err: %v", block.BlockNum, err)
			}
			offset += 2

			// Write number of records
			_, err = writer.Write(utils.Uint32ToBytesLittleEndian(uint32(len(block.RecNums))))
			if err != nil {
				return fmt.Errorf("writeSortIndex: failed writing number of records: %v, err: %v", len(block.RecNums), err)
			}
			offset += 4

			// Write sortedRecords
			for _, recNum := range block.RecNums {
				// Write recNum
				_, err = writer.Write(utils.Uint16ToBytesLittleEndian(recNum))
				if err != nil {
					return fmt.Errorf("writeSortIndex: failed writing recNum: %v, err: %v", recNum, err)
				}
				offset += 2
			}
		}
	}

	err = writer.Flush()
	if err != nil {
		return fmt.Errorf("writeSortIndex: failed flushing writer: %v", err)
	}

	// Go back and write the offsets.
	offsetsAsBytes := make([]byte, 8*len(offsets))
	for i, offset := range offsets {
		copy(offsetsAsBytes[i*8:], utils.Uint64ToBytesLittleEndian(offset))
	}
	offset = 9 // 1 (version) + 8 (numUniqueColValues)
	_, err = file.WriteAt(offsetsAsBytes, int64(offset))
	if err != nil {
		return fmt.Errorf("writeSortIndex: failed writing offsets: %v", err)
	}

	err = os.Rename(tmpFileName, finalName)
	if err != nil {
		return fmt.Errorf("writeSortIndex: error while migrating %v to %v, err: %v", tmpFileName, finalName, err)
	}

	return nil
}

type Line struct {
	Value  segutils.CValueEnclosure
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
				values = append(values, line.Value)
			}
		}
	}

	return rrcs, values
}

type Checkpoint struct {
	lineNum     int64
	totalOffset uint64
	eof         bool
}

//   - If fromCheckpoint is nil, it reads from the start of the file;
//     otherwise it picks up from the checkpoint.
//   - If readFullLine is false, it will exit after reading maxRecordsToRead
//     records (or reaching the end of the file); otherwise, once it reads
//     maxRecordsToRead, it will continue reading the rest of the line and then
//     return.
func ReadSortIndex(segkey string, cname string, sortMode SortMode, reverse bool,
	maxRecordsToRead int, readFullLine bool, fromCheckpoint *Checkpoint) ([]Line, *Checkpoint, error) {

	if IsEOF(fromCheckpoint) {
		return nil, fromCheckpoint, nil
	}

	switch sortMode {
	case SortAsAuto, SortAsNumeric, SortAsString: // Do nothing.
	default:
		return nil, nil, fmt.Errorf("ReadSortIndex: invalid sort mode: %v", sortMode)
	}

	filename := getFilename(segkey, cname, sortMode)
	file, err := os.Open(filename)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	metadata, err := readMetadata(file)
	if err != nil {
		return nil, nil, fmt.Errorf("ReadSortIndex: failed reading metadata: %v", err)
	}

	lines := make([]Line, 0)

	if reverse && fromCheckpoint == nil {
		fromCheckpoint = &Checkpoint{
			lineNum:     int64(len(metadata.valueOffsets)) - 1,
			totalOffset: metadata.valueOffsets[len(metadata.valueOffsets)-1],
		}
	}

	// Skip to the checkpoint.
	if fromCheckpoint != nil {
		offsetToStartOfLine := metadata.valueOffsets[fromCheckpoint.lineNum]
		_, err = file.Seek(int64(offsetToStartOfLine), io.SeekStart)
		if err != nil {
			return nil, nil, fmt.Errorf("ReadSortIndex: failed seeking to start of line (position %v): %v",
				offsetToStartOfLine, err)
		}
	}

	finalCheckpoint := &Checkpoint{}
	totalUniqueColValues := int64(len(metadata.valueOffsets))
	for maxRecordsToRead > 0 && finalCheckpoint.lineNum < totalUniqueColValues {
		numRecords, line, checkpoint, err := readLine(file, maxRecordsToRead, readFullLine, fromCheckpoint, reverse, metadata)
		if err != nil {
			return nil, nil, fmt.Errorf("ReadSortIndex: failed reading line: %v", err)
		}

		maxRecordsToRead -= numRecords

		if line != nil {
			lines = append(lines, *line)
		}

		if checkpoint != nil {
			finalCheckpoint = checkpoint
			fromCheckpoint = checkpoint
		}

		if reverse {
			if checkpoint.lineNum < 0 {
				break
			}

			offsetToStartOfLine := metadata.valueOffsets[finalCheckpoint.lineNum]
			_, err = file.Seek(int64(offsetToStartOfLine), io.SeekStart)
			if err != nil {
				return nil, nil, fmt.Errorf("ReadSortIndex: failed seeking to start of line (position %v): %v",
					offsetToStartOfLine, err)
			}
		}
	}

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

// The file should already be positioned at the start of the line specified by
// the checkpoint.
func readLine(file *os.File, maxRecordsToRead int, readFullLine bool, fromCheckpoint *Checkpoint,
	reverse bool, meta *metadata) (int, *Line, *Checkpoint, error) {

	gotToEndOfLine := true
	line := Line{}
	finalCheckpoint := &Checkpoint{}
	if fromCheckpoint != nil {
		finalCheckpoint.lineNum = fromCheckpoint.lineNum
	}

	var enclosure segutils.CValueEnclosure
	_, err := enclosure.FromReader(file)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("ReadSortIndex: failed reading enclosure: %v", err)
	}

	// Read total blocks
	var totalBlocks uint32
	err = binary.Read(file, binary.LittleEndian, &totalBlocks)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("ReadSortIndex: failed reading totalBlocks: %v", err)
	}

	numBlocks := uint32(0)
	blocks := make([]Block, 0)
	done := false
	totalRecordsRead := uint64(0)

	for numBlocks < totalBlocks && !done {
		// Read blockNum
		var blockNum uint16
		err = binary.Read(file, binary.LittleEndian, &blockNum)
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, nil, nil, fmt.Errorf("ReadSortIndex: failed reading blockNum: %v", err)
		}

		// Read total records
		var totalRecordsInBlock uint32
		err = binary.Read(file, binary.LittleEndian, &totalRecordsInBlock)
		if err != nil {
			return 0, nil, nil, fmt.Errorf("ReadSortIndex: failed reading totalRecords: %v", err)
		}

		numRecords := uint32(0)
		recNums := make([]uint16, 0)
		for numRecords < totalRecordsInBlock && (readFullLine || totalRecordsRead < uint64(maxRecordsToRead)) {
			// Read recNum
			var recNum uint16
			err = binary.Read(file, binary.LittleEndian, &recNum)
			if err == io.EOF {
				break
			}
			if err != nil {
				return 0, nil, nil, fmt.Errorf("ReadSortIndex: failed reading recNum: %v", err)
			}

			numRecords++

			if pastCheckpoint(fromCheckpoint, file) {
				totalRecordsRead++
				recNums = append(recNums, recNum)
			}
		}

		numBlocks++

		if totalRecordsRead >= uint64(maxRecordsToRead) {
			gotToEndOfLine = (numBlocks == totalBlocks && numRecords == totalRecordsInBlock)
			if gotToEndOfLine || !readFullLine {
				done = true
			}
		} else if numRecords != totalRecordsInBlock {
			return 0, nil, nil, fmt.Errorf("ReadSortIndex: sort file seems to be corrupted, numRecords(%v) != totalRecords(%v)", numRecords, totalRecordsInBlock)
		}

		if pastCheckpoint(fromCheckpoint, file) {
			blocks = append(blocks, Block{
				BlockNum: blockNum,
				RecNums:  recNums,
			})
		}
	}

	if !done && numBlocks != totalBlocks {
		return 0, nil, nil, fmt.Errorf("ReadSortIndex: sort file seems to be corrupted, numBlocks(%v) != totalBlocks(%v)", numBlocks, totalBlocks)
	}

	if gotToEndOfLine {
		if reverse {
			finalCheckpoint.lineNum--
			if finalCheckpoint.lineNum < 0 {
				finalCheckpoint.eof = true
			} else {
				finalCheckpoint.totalOffset = meta.valueOffsets[finalCheckpoint.lineNum]
			}
		} else {
			finalCheckpoint.lineNum++
			if finalCheckpoint.lineNum >= int64(len(meta.valueOffsets)) {
				finalCheckpoint.eof = true
			} else {
				finalCheckpoint.totalOffset = meta.valueOffsets[finalCheckpoint.lineNum]
			}
		}
	} else {
		filePos, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, nil, nil, fmt.Errorf("ReadSortIndex: failed to get current file position: %v", err)
		}
		finalCheckpoint.totalOffset = uint64(filePos)
	}

	line.Value = enclosure
	line.Blocks = blocks

	return int(totalRecordsRead), &line, finalCheckpoint, nil
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

func readMetadata(file *os.File) (*metadata, error) {
	meta := &metadata{}

	// Read version
	version := make([]byte, 1)
	err := binary.Read(file, binary.LittleEndian, &version)
	if err != nil {
		return nil, fmt.Errorf("readMetadata: failed reading version: %v", err)
	}
	if version[0] != VERSION_SORT_INDEX[0] {
		return nil, fmt.Errorf("readMetadata: unsupported version: %v", version)
	}

	meta.version = version[0]

	// Read Number of unique column values
	var totalUniqueColValues uint64
	err = binary.Read(file, binary.LittleEndian, &totalUniqueColValues)
	if err != nil {
		return nil, fmt.Errorf("readMetadata: failed reading number of unique column values: %v", err)
	}

	meta.valueOffsets = make([]uint64, totalUniqueColValues)

	// Read the offsets.
	for i := uint64(0); i < totalUniqueColValues; i++ {
		err = binary.Read(file, binary.LittleEndian, &meta.valueOffsets[i])
		if err != nil {
			return nil, fmt.Errorf("readMetadata: failed reading offset: %v", err)
		}
	}

	return meta, nil
}

func SetSortColumns(indexName string, columnNames []string) error {
	configPath := getSortColumnsConfigPath()

	sortConfigMutex.Lock()
	defer sortConfigMutex.Unlock()

	dir := filepath.Dir(configPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("SetSortColumns: failed to create config directory: %v", err)
	}

	for _, col := range columnNames {
		if col == "" {
			return fmt.Errorf("SetSortColumns: column names must be non-empty strings")
		}
	}

	config := SortColumnsConfig{
		Indexes: make(map[string][]string),
	}

	data, err := os.ReadFile(configPath)
	if err == nil {
		if err := json.Unmarshal(data, &config); err != nil {
			return fmt.Errorf("SetSortColumns: failed to parse sort columns config: %v", err)
		}
	}

	config.Indexes[indexName] = columnNames

	data, err = json.MarshalIndent(config, "", "    ")
	if err != nil {
		return fmt.Errorf("SetSortColumns: failed to marshal sort columns config: %v", err)
	}

	err = os.WriteFile(configPath, data, 0644)
	if err != nil {
		return fmt.Errorf("SetSortColumns: failed to write sort columns config: %v", err)
	}

	return nil
}

func GetSortColumnNamesForIndex(indexName string) []string {
	configPath := getSortColumnsConfigPath()

	sortConfigMutex.RLock()
	defer sortConfigMutex.RUnlock()

	data, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Debugf("GetSortColumnNamesForIndex: no sort columns config file found: %v", err)
		} else {
			log.Errorf("GetSortColumnNamesForIndex: error reading config file: %v", err)
		}
		return make([]string, 0)
	}

	var config SortColumnsConfig
	if err := json.Unmarshal(data, &config); err != nil {
		log.Errorf("GetSortColumnNamesForIndex: failed to parse sort columns config: %v", err)
		return make([]string, 0)
	}

	return config.Indexes[indexName]
}

func SetSortColumnsAPI(ctx *fasthttp.RequestCtx) {
	var request struct {
		IndexName string   `json:"indexName"`
		Columns   []string `json:"columns"`
	}

	if err := json.Unmarshal(ctx.PostBody(), &request); err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBodyString("Invalid request body: " + err.Error())
		return
	}

	if request.IndexName == "" {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBodyString("Missing index name")
		return
	}

	if err := SetSortColumns(request.IndexName, request.Columns); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBodyString("Failed to set sort columns: " + err.Error())
		return
	}

	ctx.SetContentType("application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetBodyString("Sort columns updated successfully")
}

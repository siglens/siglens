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

package segread

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/siglens/siglens/pkg/segment/results/blockresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

const MAX_NODE_PTRS = 80_000

type AgileTreeReader struct {
	segKey         string
	metaFd         *os.File // meta file descriptor
	levDataFd      *os.File // level data file descriptor
	isMetaLoaded   bool
	metaFileBuffer []byte // buffer re-used for file reads values
	metaBuf        []byte // meta buff block
	treeMeta       *StarTreeMetadata
	buckets        aggsTreeBuckets
}

type aggsTreeBuckets struct {
	bucketLimit uint64
	saveBuckets bool
	rawVals     map[string]struct{}
}

type StarTreeMetadata struct {
	groupByKeys     []string
	numGroupByCols  uint16
	measureColNames []string // store only index of mcol, and calculate all stats for them

	// allDictEncodings[colName] has information about the ith groupby column.
	// allDictEncodings[colName][num] will give the raw encoding that num references in the agileTree
	allDictEncodings map[string]map[uint32][]byte
	levsOffsets      []int64  // stores where each level starts in the file, uses fileOffsetFromStart
	levsSizes        []uint32 // stores the size of each level
}

// returns a new AgileTreeReader and any errors encountered
// The returned AgileTreeReader must call .Close() when finished using it to close the fd
func InitNewAgileTreeReader(segKey string, qid uint64) (*AgileTreeReader, error) {

	// Open the FD for AgileTree
	// todo add download code for agileTree file
	// fName, err := blob.DownloadSegmentBlobAsInUse(segKey, colName, structs.Str)
	fName := segKey + ".strm"
	fd, err := os.OpenFile(fName, os.O_RDONLY, 0644)
	if err != nil {
		log.Infof("qid=%d, InitNewAgileTreeReader: failed to open STR %s for Error: %v.",
			qid, fName, err)
		return nil, err
	}

	return &AgileTreeReader{
		segKey:         segKey,
		metaFd:         fd,
		metaFileBuffer: *fileReadBufferPool.Get().(*[]byte),
		isMetaLoaded:   false,
		buckets:        aggsTreeBuckets{},
	}, nil
}

func (str *AgileTreeReader) GetBuckets() map[string]struct{} {
	return str.buckets.rawVals
}

func (str *AgileTreeReader) SetBuckets(buckets map[string]struct{}) {
	str.buckets.rawVals = buckets
}

func (str *AgileTreeReader) SetBucketLimit(bucketLimit uint64) {
	str.buckets.bucketLimit = bucketLimit

	// If the bucketLimit is 0, then there is no limit. If there is a limit, we
	// need to save the buckets between each segment so if we hit the limit,
	// we make sure to read all the same buckets between all the segments.
	str.buckets.saveBuckets = bucketLimit > 0
}

func (str *AgileTreeReader) Close() error {
	if str.metaFd != nil {
		str.metaFd.Close()
	}
	if str.levDataFd != nil {
		str.levDataFd.Close()
	}

	str.returnBuffers()
	return nil
}

func (str *AgileTreeReader) returnBuffers() {
	fileReadBufferPool.Put(&str.metaFileBuffer)
}

func (str *AgileTreeReader) resetBlkVars() {

	str.treeMeta = nil
	str.isMetaLoaded = false
}

/*
parameters:

	none

returns:

	err
*/
func (str *AgileTreeReader) ReadTreeMeta() error {

	if str.isMetaLoaded {
		return nil
	}

	str.resetBlkVars()

	finfo, err := os.Stat(str.metaFd.Name())
	if err != nil {
		log.Errorf("ReadTreeMeta could not get file size error: %+v", err)
		return err
	}
	fileSize := uint32(finfo.Size())

	if uint32(len(str.metaFileBuffer)) < fileSize {
		newArr := make([]byte, fileSize-uint32(len(str.metaFileBuffer)))
		str.metaFileBuffer = append(str.metaFileBuffer, newArr...)
	}

	_, err = str.metaFd.ReadAt(str.metaFileBuffer[:fileSize], 0)
	if err != nil {
		log.Errorf("ReadTreeMeta read file error: %+v", err)
		return err
	}

	if str.metaFileBuffer[0] != utils.STAR_TREE_BLOCK[0] {
		log.Errorf("ReadTreeMeta: received an unknown encoding type for agileTree: %v",
			str.metaFileBuffer[0])
		return errors.New("received non-agileTree encoding")
	}

	idx := uint32(0)
	str.metaBuf = str.metaFileBuffer[0:fileSize]
	idx += 1

	// LenMetaData
	lenMeta := toputils.BytesToUint32LittleEndian(str.metaBuf[idx : idx+4])
	idx += 4

	// MetaData
	meta, err := str.decodeMetadata(str.metaBuf[idx : idx+lenMeta])
	if err != nil {
		return err
	}
	idx += lenMeta

	// read levsOffsets and levsSizes
	meta.levsOffsets = make([]int64, meta.numGroupByCols+1)
	meta.levsSizes = make([]uint32, meta.numGroupByCols+1)
	for i := range meta.levsOffsets {
		meta.levsOffsets[i] = toputils.BytesToInt64LittleEndian(str.metaBuf[idx : idx+8])
		idx += 8
		meta.levsSizes[i] = toputils.BytesToUint32LittleEndian(str.metaBuf[idx : idx+4])
		idx += 4
	}

	str.treeMeta = meta
	str.isMetaLoaded = true

	return nil
}

/*
parameters:

	grpColNames: Names of GroupByColNames
	mColNames: Names of MeasureColumns

returns:

	bool: if grp and mcol are present and query is fully answerable by AgileTree
	error: error if any

Func: If any colname either in grp or measure is not present will return false
*/
func (str *AgileTreeReader) CanUseAgileTree(grpReq *structs.GroupByRequest) (bool, error) {

	if len(grpReq.GroupByColumns) == 0 && len(grpReq.MeasureOperations) == 0 {
		return false, nil
	}

	if !str.isMetaLoaded {
		err := str.ReadTreeMeta()
		if err != nil {
			return false, err
		}
	}

	// walk through grpColnames
	for _, cname := range grpReq.GroupByColumns {
		ok := toputils.SearchStr(cname, str.treeMeta.groupByKeys)
		if !ok {
			return false, nil
		}
	}

	// walk through measure colname
	for _, m := range grpReq.MeasureOperations {
		if m.MeasureCol == "*" && m.MeasureFunc == utils.Count {
			continue // we treat count(*) as just as a bucket count
		}
		found := false
		for _, treeMCname := range str.treeMeta.measureColNames {
			if m.MeasureCol == treeMCname {
				found = true
				break
			}
		}
		if !found {
			return false, nil
		}
	}
	return true, nil
}

func (str *AgileTreeReader) decodeMetadata(buf []byte) (*StarTreeMetadata, error) {

	tmeta := StarTreeMetadata{}

	idx := uint32(0)

	// Len of groupByKeys
	tmeta.numGroupByCols = toputils.BytesToUint16LittleEndian(buf[idx : idx+2])
	idx += 2

	tmeta.groupByKeys = make([]string, tmeta.numGroupByCols)
	for i := uint16(0); i < tmeta.numGroupByCols; i++ {
		// grp str len
		l1 := toputils.BytesToUint16LittleEndian(buf[idx : idx+2])
		idx += 2

		// grp actual str
		tmeta.groupByKeys[i] = string(buf[idx : idx+uint32(l1)])
		idx += uint32(l1)
	}

	// Len of MeasureColNames
	lenMcolNames := toputils.BytesToUint16LittleEndian(buf[idx : idx+2])
	idx += 2

	tmeta.measureColNames = make([]string, lenMcolNames)

	for i := uint16(0); i < lenMcolNames; i++ {
		// Mcol Len
		l1 := toputils.BytesToUint16LittleEndian(buf[idx : idx+2])
		idx += 2

		// Mcol strname
		tmeta.measureColNames[i] = string(buf[idx : idx+uint32(l1)])
		idx += uint32(l1)
	}
	tmeta.allDictEncodings = make(map[string]map[uint32][]byte, tmeta.numGroupByCols)

	var soff, eoff uint32
	for j := uint16(0); j < tmeta.numGroupByCols; j++ {

		// colname strlen
		l1 := toputils.BytesToUint16LittleEndian(buf[idx : idx+2])
		idx += 2

		// colname str : we only store the offsets to save on string copy
		soff = idx
		idx += uint32(l1)
		eoff = idx

		// numKeys
		numDictEncodings := toputils.BytesToUint32LittleEndian(buf[idx : idx+4])
		idx += 4

		if numDictEncodings == 0 {
			log.Errorf("decodeMetadata: numDictEncodings was 0 for cname: %v", string(buf[soff:eoff]))
			continue
		}

		dictEncoding := make(map[uint32][]byte, numDictEncodings)

		for i := uint32(0); i < numDictEncodings; i += 1 {
			// enc col val strlen
			l1 := toputils.BytesToUint16LittleEndian(buf[idx : idx+2])
			idx += 2

			// enc col val str
			dictEncoding[i] = buf[idx : idx+uint32(l1)]
			idx += uint32(l1)
		}
		tmeta.allDictEncodings[string(buf[soff:eoff])] = dictEncoding

	}

	return &tmeta, nil
}

// returns the level that the column name will exist in tree.
// This assumes that level -1 is the root node
func (str *AgileTreeReader) getLevelForColumn(colName string) (int, error) {
	for idx, name := range str.treeMeta.groupByKeys {
		if name == colName {
			return idx + 1, nil // root is at level 0 so add 1
		}
	}
	return 0, fmt.Errorf("column %+v not found in tree", colName)
}

func (str *AgileTreeReader) getRawVal(key uint32, dictEncoding map[uint32][]byte) ([]byte, error) {
	rawVal, ok := dictEncoding[key]
	if !ok {
		return []byte{}, fmt.Errorf("failed to find raw value for idx %+v which has %+v keys", key, len(dictEncoding))
	}
	return rawVal, nil
}

func (str *AgileTreeReader) decodeNodeDetailsJit(buf []byte, numAggValues int,
	desiredLevel uint16, combiner map[string][]utils.NumTypeEnclosure,
	measResIndices []int, lenMri int, grpTreeLevels []uint16, grpColNames []string) error {

	var wvInt64 int64
	var wvFloat64 float64
	idx := uint32(0)

	// level
	curLevel := toputils.BytesToUint16LittleEndian(buf[idx : idx+2])
	idx += 2

	// numNodes at this level
	numNodes := toputils.BytesToUint32LittleEndian(buf[idx : idx+4])
	idx += 4

	if curLevel != desiredLevel {
		log.Errorf("decodeNodeDetailsJit wanted level: %v, but read level: %v", desiredLevel, curLevel)
		return fmt.Errorf("decodeNodeDetailsJit wanted level: %v, but read level: %v", desiredLevel, curLevel)
	}

	usedDictEncodings := make([]map[uint32][]byte, len(grpTreeLevels))
	for i, grpCol := range grpColNames {
		usedDictEncodings[i] = str.treeMeta.allDictEncodings[grpCol]
	}

	// Allocate all the memory we need for the group by keys upfront to avoid
	// many small allocations. This also allows us to convert a byte slice to
	// a string without copying; this uses the unsafe package, but we never
	// change that region of the byte slice, so it's safe.
	wvBuf := make([]byte, len(grpTreeLevels)*4*int(numNodes))
	wvIdx := uint32(0)

	newBuckets := 0

	for i := uint32(0); i < numNodes; i++ {
		// get mapkey

		myKey := buf[idx : idx+4]
		idx += 4

		kidx := uint32(0)
		for _, grpLev := range grpTreeLevels {
			if grpLev == desiredLevel {
				copy(wvBuf[wvIdx+kidx:], myKey)
			} else {
				// The next four bytes of buf is the parent's node key, the
				// next four after that is the grandparent's node key, etc.
				ancestorLevel := desiredLevel - grpLev
				offset := uint32(ancestorLevel-1) * 4
				copy(wvBuf[wvIdx+kidx:], buf[idx+offset:idx+offset+4])
			}
			kidx += 4
		}
		wvNodeKey := toputils.UnsafeByteSliceToString(wvBuf[wvIdx : wvIdx+kidx])
		wvIdx += kidx
		idx += uint32(desiredLevel-1) * 4

		aggVal, ok := combiner[wvNodeKey]
		if !ok {
			// Check if we hit the bucket limit. bucketLimit == 0 is a special
			// case and means there is no limit.
			if str.buckets.bucketLimit > 0 {
				rawVal, _ := str.decodeRawValBytes(wvNodeKey, usedDictEncodings, grpColNames)
				_, existingBucket := str.buckets.rawVals[rawVal]
				if !existingBucket {
					if uint64(len(str.buckets.rawVals))+uint64(newBuckets) >= str.buckets.bucketLimit {
						// We've reached the bucket limit, so we shouldn't add another.
						// However, we need to continue reading the AgileTree because
						// we might reach another node that has data for a bucket we've
						// already added.
						idx += uint32(numAggValues) * 9
						continue
					} else {
						newBuckets += 1
					}
				}
			}

			aggVal = make([]utils.NumTypeEnclosure, lenMri)
			combiner[wvNodeKey] = aggVal
		}

		if aggVal == nil {
			aggVal = make([]utils.NumTypeEnclosure, lenMri)
		}

		for j := 0; j < lenMri; j++ {
			agIdx := idx                           // set to the start of aggValue for this node's data
			agIdx += uint32(measResIndices[j]) * 9 // jump to the AgValue for this meas's index

			dtype := utils.SS_DTYPE(buf[agIdx])
			agIdx += 1

			switch dtype {
			case utils.SS_DT_UNSIGNED_NUM, utils.SS_DT_SIGNED_NUM:
				wvInt64 = toputils.BytesToInt64LittleEndian(buf[agIdx : agIdx+8])
			case utils.SS_DT_FLOAT:
				wvFloat64 = toputils.BytesToFloat64LittleEndian(buf[agIdx : agIdx+8])
			case utils.SS_DT_BACKFILL:
			default:
				return fmt.Errorf("decodeNodeDetailsJit: unsupported Dtype: %v", dtype)
			}

			// remainder will give us MeasFnIdx
			fn := writer.IdxToAgFn[measResIndices[j]%writer.TotalMeasFns]
			err := aggVal[j].ReduceFast(dtype, wvInt64, wvFloat64, fn)
			if err != nil {
				log.Errorf("decodeNodeDetailsJit: Failed to reduce aggregation for err: %v", err)
			}
		}
		idx += uint32(numAggValues) * 9
	}

	return nil
}

// applies groupby results and returns requested measure operations
// first applies the first groupby column. For all returned nodes, apply second & so on until no more groupby exists
func (str *AgileTreeReader) ApplyGroupByJit(grpColNames []string,
	internalMops []*structs.MeasureAggregator, blkResults *blockresults.BlockResults,
	qid uint64, agileTreeBuf []byte) error {

	// make sure meta is loaded
	_ = str.ReadTreeMeta()

	var maxGrpLevel uint16
	grpTreeLevels := make([]uint16, len(grpColNames))
	for i, grpByCol := range grpColNames {
		level, err := str.getLevelForColumn(grpByCol)
		if err != nil {
			log.Errorf("qid=%v, ApplyGroupByJit: failed to get level in tree for column %s: %v", qid,
				grpByCol, err)
			return err
		}
		maxGrpLevel = utils.MaxUint16(maxGrpLevel, uint16(level))
		grpTreeLevels[i] = uint16(level)
	}

	measResIndices := make([]int, 0)

	// Always retrieve count.
	// If count is asked we return count twice, but thats a small price to pay for simpler code
	measResIndices = append(measResIndices, writer.MeasFnCountIdx)

	for _, mops := range internalMops {
		found := false
		tcidx := 0 // var for tree's column name index
		for i, treeMCname := range str.treeMeta.measureColNames {
			if mops.MeasureCol == treeMCname {
				found = true
				tcidx = i
				break
			}
		}
		if !found {
			log.Errorf("qid=%v, ApplyGroupByJit: Tree could not find mcol: %v", qid, mops.MeasureCol)
			return fmt.Errorf("qid=%v, ApplyGroupByJit: Tree could not find mcol: %v",
				qid, mops.MeasureCol)
		}
		fnidx := writer.AgFnToIdx(mops.MeasureFunc)                              // What MeasFn idx this translates to
		measResIndices = append(measResIndices, tcidx*writer.TotalMeasFns+fnidx) // see where it is in agileTree
	}

	combiner := make(map[string][]utils.NumTypeEnclosure)

	err := str.computeAggsJit(combiner, maxGrpLevel, measResIndices, agileTreeBuf,
		grpTreeLevels, grpColNames)
	if err != nil {
		log.Errorf("qid=%v, ApplyGroupByJit: failed to apply aggs-jit: %v", qid, err)
		return err
	}

	usedDictEncodings := make([]map[uint32][]byte, len(grpColNames))
	for i, grpCol := range grpColNames {
		usedDictEncodings[i] = str.treeMeta.allDictEncodings[grpCol]
	}

	for mkey, ntAgvals := range combiner {
		if len(ntAgvals) == 0 {
			continue
		}
		rawVal, err := str.decodeRawValBytes(mkey, usedDictEncodings, grpColNames)
		if err != nil {
			log.Errorf("qid=%v, ApplyGroupByJit: Failed to get raw value for a agileTree key! %+v", qid, err)
			return err
		}

		if str.buckets.saveBuckets {
			if str.buckets.rawVals == nil {
				str.buckets.rawVals = make(map[string]struct{})
			}

			str.buckets.rawVals[rawVal] = struct{}{}
		}

		cvaggvalues := make([]utils.CValueEnclosure, len(internalMops))
		resCvIdx := 0
		var colCntVal uint64
		extVal := utils.CValueEnclosure{}
		for i := 0; i < len(ntAgvals); i++ {
			switch ntAgvals[i].Ntype {
			case utils.SS_DT_SIGNED_NUM, utils.SS_DT_UNSIGNED_NUM:
				extVal.Dtype = utils.SS_DT_SIGNED_NUM
				extVal.CVal = ntAgvals[i].IntgrVal
			case utils.SS_DT_FLOAT:
				extVal.Dtype = utils.SS_DT_FLOAT
				extVal.CVal = ntAgvals[i].FloatVal
			}
			// todo count is stored multiple times in the nodeAggvalue (per measCol), store only once
			if i == 0 { // count is always at index 0
				colCntVal = uint64(extVal.CVal.(int64))
			} else {
				cvaggvalues[resCvIdx] = extVal
				resCvIdx++
			}
		}
		blkResults.AddMeasureResultsToKeyAgileTree(string(rawVal), cvaggvalues, qid, colCntVal)
	}

	return nil
}

func (str *AgileTreeReader) computeAggsJit(combiner map[string][]utils.NumTypeEnclosure,
	desiredLevel uint16, measResIndices []int, agileTreeBuf []byte, grpTreeLevels []uint16,
	grpColNames []string) error {

	numAggValues := len(str.treeMeta.measureColNames) * writer.TotalMeasFns

	fName := str.segKey + ".strl"
	fd, err := os.OpenFile(fName, os.O_RDONLY, 0644)
	if err != nil {
		log.Infof("computeAggsJit: failed to open STRLev %v  Error: %v.",
			fName, err)
		return err
	}
	str.levDataFd = fd

	myLevsOff := str.treeMeta.levsOffsets[desiredLevel]
	myLevsSize := int64(str.treeMeta.levsSizes[desiredLevel])

	sizeToAdd := myLevsSize - int64(len(agileTreeBuf))
	if sizeToAdd > 0 {
		newArr := make([]byte, sizeToAdd)
		agileTreeBuf = append(agileTreeBuf, newArr...)
	}

	_, err = str.levDataFd.ReadAt(agileTreeBuf[:myLevsSize], myLevsOff)
	if err != nil {
		log.Errorf("computeAggsJit read file error: %+v", err)
		return err
	}

	// assumes root is at level -1
	err = str.decodeNodeDetailsJit(agileTreeBuf[0:myLevsSize], numAggValues, desiredLevel,
		combiner, measResIndices, len(measResIndices), grpTreeLevels, grpColNames)
	return err
}

func (str *AgileTreeReader) decodeRawValBytes(mkey string, usedGrpDictEncodings []map[uint32][]byte,
	grpColNames []string) (string, error) {

	// Estimate how much space we need for the string builder to avoid
	// reallocations. An int or float groupby column will take 9 bytes, and
	// a string groupby column could take more or less space.
	var sb strings.Builder
	sb.Grow(len(usedGrpDictEncodings) * 16)

	buf := []byte(mkey)
	idx := uint32(0)
	for i, dictEncoding := range usedGrpDictEncodings {
		nk := toputils.BytesToUint32LittleEndian(buf[idx : idx+4])
		idx += 4

		cname := grpColNames[i]
		rawVal, err := str.getRawVal(nk, dictEncoding)
		if err != nil {
			log.Errorf("decodeRawValBytes: Failed to get raw value for nk:%v, came: %v, err: %+v",
				nk, cname, err)
			return "", err
		}
		sb.Write(rawVal)
	}
	return sb.String(), nil
}

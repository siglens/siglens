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

package tagstree

import (
	"fmt"
	"os"

	tsidtracker "github.com/siglens/siglens/pkg/segment/results/mresults/tsid"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

const STAR = "*"

var TAG_VALUE_DELIMITER_BYTE = []byte("`")

/*
Holder struct for all indiviual tagTreeReaders
*/
type AllTagTreeReaders struct {
	baseDir  string
	tagTrees map[string]*TagTreeReader // maps tagKey to its specific TagTreeReader
}

/*
Reader struct for a single tagTree.

Should expose functions that will return a list of tsids given a metric name and tagValue
*/
type TagTreeReader struct {
	fd          *os.File // file having all the tagstree info for a tag key
	metadataBuf []byte   // consists of the meta data info for a given tag key
}

func (ttr *TagTreeReader) Close() error {
	return ttr.fd.Close()
}

/*
Iterator to get all tag values for a given metric name & tagKey
*/
type TagValueIterator struct {
	tagTreeBuf    []byte
	treeOffset    uint32
	matchingTSIDs map[uint64]struct{}
}

func InitAllTagsTreeReader(tagsTreeBaseDir string) (*AllTagTreeReaders, error) {
	return &AllTagTreeReaders{
		baseDir:  tagsTreeBaseDir,
		tagTrees: make(map[string]*TagTreeReader),
	}, nil
}

func (attr *AllTagTreeReaders) InitTagsTreeReader(tagKey string) (*TagTreeReader, error) {
	fName := attr.baseDir + tagKey
	finfo, err := os.Stat(fName)
	if err != nil {
		// This can happen when we don't know whether a metric has this key or not, so we try to open
		// the tagstree file. So just warn.
		log.Warnf("InitTagsTreeReader: error when trying to stat file=%+v. Error=%+v", fName, err)
		return nil, err
	}

	fileSize := finfo.Size()
	if fileSize == 0 {
		log.Errorf("InitTagsTreeReader: file is empty %s", fName)
		return nil, fmt.Errorf("InitTagsTreeReader: file is empty %s", fName)
	}
	fd, err := os.OpenFile(fName, os.O_RDONLY, 0644)
	if err != nil {
		log.Errorf("InitTagsTreeReader: failed to open file %s. Error: %v.", fName, err)
		return nil, err
	}
	metadataSizeBuf := make([]byte, 5)
	_, err = fd.ReadAt(metadataSizeBuf[:5], 0)
	if err != nil {
		log.Errorf("InitTagsTreeReader: Error reading file: %v. Error: %v", fName, err)
		return nil, err
	}
	versionTagsTree := make([]byte, 1)
	copy(versionTagsTree, metadataSizeBuf[:1])
	if versionTagsTree[0] != segutils.VERSION_TAGSTREE[0] {
		return nil, fmt.Errorf("InitTagsTreeReader: the file version doesn't match")
	}
	metadataSize := utils.BytesToUint32LittleEndian(metadataSizeBuf[1:5])
	rbuf := make([]byte, 0)
	// initializing buffer of 16 bytes because: [mName1-uint64][msOff-uint32][meOff-uint32]
	newArr := make([]byte, 16)
	id := uint32(5)
	for id < metadataSize {
		_, err = fd.ReadAt(newArr[:16], int64(id))
		if err != nil {
			log.Errorf("InitTagsTreeReader: cannot read file: %v. Error: %v", fName, err)
			return nil, err
		}
		rbuf = append(rbuf, newArr...)
		id += 16
	}
	ttr := &TagTreeReader{
		fd:          fd,
		metadataBuf: rbuf,
	}
	attr.tagTrees[tagKey] = ttr
	return ttr, nil
}

/*
Wrapper function that applies all tags filters

# Returns a map of groupid to all tsids that are in that group

# It is assumed that mQuery.ReorderTagFilters() has been called before this function

Due to how we add tag filters (see ApplyMetricsQuery()), we can have queries where we add the filter
someKey=* for a key someKey that does not exist for this metric (but does exist for a different
metric). We want to detect and remove such keys before returning our final results to the user. We
do that here with the following logic.

This makes the assumption that for this metric M being queried, and for each tag key K, if M has one
TSID in the tag tree for key K, then that tag tree has all of M's TSIDs. From this assumption we can
say that for any of M's TSIDs, if any tag tree does not contain that TSID, then that tag tree does
not contain any of M's TSIDs. Thus, if we have a tag filter like myKey=* and don't get any returned
TSIDs, we know that none of M's time series have the key so we can remove it. Moreover, if we get
any results for the search myKey=*, we got all of M's TSIDs because they all have a value for that
key.
*/
func (attr *AllTagTreeReaders) FindTSIDS(mQuery *structs.MetricsQuery) (*tsidtracker.AllMatchedTSIDs, error) {

	defer func() {
		for _, ttr := range attr.tagTrees {
			ttr.Close()
		}
	}()
	// for each filter, somehow keep track of the running group for each TSID?
	tracker, err := tsidtracker.InitTSIDTracker(len(mQuery.TagsFilters))
	if err != nil {
		log.Errorf("FindTSIDS: failed to initialize the TSID tracker. Error: %v", err)
		return nil, err
	}

	tagIndicesToRemove := make(map[int]struct{})
	for i := 0; i < len(mQuery.TagsFilters); i++ {
		tf := mQuery.TagsFilters[i]
		if tagVal, ok := tf.RawTagValue.(string); ok && tagVal == "*" {
			itr, mNameExists, err := attr.GetValueIteratorForMetric(mQuery.HashedMName, tf.TagKey)
			if err != nil {
				log.Infof("FindTSIDS: failed to get the value iterator for metric name %v and tag key %v. Error: %v. TagVAlH %+v", mQuery.MetricName, tf.TagKey, err, tf.HashTagValue)
				tagIndicesToRemove[i] = struct{}{}
				continue
			}

			if !mNameExists {
				tagIndicesToRemove[i] = struct{}{}
				continue
			}

			rawTagValueToTSIDs := make(map[string]map[uint64]struct{})
			for {
				_, grpID, tsids, more := itr.Next()
				if !more {
					break
				}
				grpIDStr := string(grpID)
				rawTagValueToTSIDs[grpIDStr] = make(map[uint64]struct{})
				for tsid := range tsids {
					rawTagValueToTSIDs[grpIDStr][tsid] = struct{}{}
				}
			}
			err = tracker.BulkAddStar(rawTagValueToTSIDs)
			if err != nil {
				log.Errorf("FindTSIDS: failed to build add tsids to tracker! Error %+v", err)
				return nil, err
			}
			err = tracker.FinishBlock()
			if err != nil {
				log.Errorf("FindTSIDS: failed to execute finish on block! Error %+v", err)
				return nil, err
			}
		} else {
			mNameExists, rawTagValueToTSIDs, _, err := attr.GetMatchingTSIDs(mQuery.HashedMName, tf.TagKey, tf.HashTagValue, tf.TagOperator)
			if err != nil {
				log.Infof("FindTSIDS: failed to get matching tsids for mNAme %v and tag key %v. Error: %v. TagVAlH %+v tagVal %+v", mQuery.MetricName, tf.TagKey, err, tf.HashTagValue, tf.RawTagValue)
				return nil, err
			}
			if !mNameExists {
				tagIndicesToRemove[i] = struct{}{}
				continue
			}
			err = tracker.BulkAdd(rawTagValueToTSIDs)
			if err != nil {
				log.Errorf("FindTSIDS: failed to build add tsids to tracker! Error %+v", err)
				return nil, err
			}

			err = tracker.FinishBlock()
			if err != nil {
				log.Errorf("FindTSIDS: failed to execute finish on block! Error %+v", err)
				return nil, err
			}
		}
	}
	tracker.FinishAllMatches()

	// Remove tags where this metric doesn't have the specified keys.
	if len(tagIndicesToRemove) < len(mQuery.TagsFilters) {
		newTags := make([]*structs.TagsFilter, 0)
		for i, tag := range mQuery.TagsFilters {
			if _, ok := tagIndicesToRemove[i]; !ok {
				newTags = append(newTags, tag)
			}
		}
		mQuery.TagsFilters = newTags
	}

	return tracker, nil
}

/*
Returns:
- map[uint64]struct{}, mapping with tsid as key
- bool, indicating whether metric name existed
- map[string]map[uint64]struct{}, map matching raw tag values for this tag key to set of all tsids with that value
- error, any errors encountered
*/
func (attr *AllTagTreeReaders) GetMatchingTSIDs(mName uint64, tagKey string, tagValue uint64, tagOperator segutils.TagOperator) (bool, map[string]map[uint64]struct{}, uint64, error) {
	ttr, ok := attr.tagTrees[tagKey]
	if !ok {
		ttr, err := attr.InitTagsTreeReader(tagKey)
		if err != nil {
			return false, nil, 0, fmt.Errorf("GetMatchingTSIDs: failed to initialize tags tree reader for key %s, error: %v", tagKey, err)
		} else {
			return ttr.GetMatchingTSIDs(mName, tagValue, tagOperator)
		}
	}
	return ttr.GetMatchingTSIDs(mName, tagValue, tagOperator)
}

// See encodeTagsTree() for how the file for a TagTree is laid out.
func (ttr *TagTreeReader) GetMatchingTSIDs(mName uint64, tagValue uint64, tagOperator segutils.TagOperator) (bool, map[string]map[uint64]struct{}, uint64, error) {
	if tagOperator != segutils.Equal && tagOperator != segutils.NotEqual {
		log.Errorf("TagTreeReader.GetMatchingTSIDs: tagOperator %v is not supported; only Equal and NotEqual are currently implemented", tagOperator)
		return false, nil, tagValue, fmt.Errorf("TagTreeReader.GetMatchingTSIDs: tagOperator not supported")
	}

	var hashedMName uint64
	var startOff, endOff uint32
	matchedSomething := false
	var tagHashValue uint64
	var rawTagValueToTSIDs = make(map[string]map[uint64]struct{})
	var rawTagValue []byte
	id := uint32(0)
	for id < uint32(len(ttr.metadataBuf)) {
		hashedMName = utils.BytesToUint64LittleEndian(ttr.metadataBuf[id : id+8])
		id += 8
		if hashedMName != mName {
			id += 8
			continue
		}
		startOff = utils.BytesToUint32LittleEndian(ttr.metadataBuf[id : id+4])
		id += 4
		endOff = utils.BytesToUint32LittleEndian(ttr.metadataBuf[id : id+4])
		tagTreeBuf := make([]byte, endOff-startOff)
		_, err := ttr.fd.ReadAt(tagTreeBuf, int64(startOff))
		if err != nil {
			return false, nil, 0, err
		}
		treeOffset := uint32(0)
		for treeOffset < uint32(len(tagTreeBuf)) {
			// a tagtree entry comprises a minimum of tag hash (8 bytes) + tag value type (1 byte) + tsid count (2 bytes)
			if uint32(len(tagTreeBuf))-treeOffset < 11 {
				// not enough bytes left in tagTreeBuf for a full tag tree entry
				log.Errorf("GetMatchingTSIDs: unexpected lack of space for tag tree entry")
				break
			}
			tagHashValue = utils.BytesToUint64LittleEndian(tagTreeBuf[treeOffset : treeOffset+8])
			treeOffset += 8
			tagRawValueType := tagTreeBuf[treeOffset : treeOffset+1]
			treeOffset += 1
			if tagRawValueType[0] == segutils.VALTYPE_ENC_SMALL_STRING[0] {
				tagValueLen := utils.BytesToUint16LittleEndian(tagTreeBuf[treeOffset : treeOffset+2])
				treeOffset += 2
				rawTagValue = tagTreeBuf[treeOffset : treeOffset+uint32(tagValueLen)]
				treeOffset += uint32(tagValueLen)
			} else if tagRawValueType[0] == segutils.VALTYPE_ENC_FLOAT64[0] {
				rawTagValue = tagTreeBuf[treeOffset : treeOffset+8]
				treeOffset += 8
			} else {
				log.Errorf("TagTreeReader.GetMatchingTSIDs: unknown value type: %v, (treeOffset, len(tagTreeBuf)): (%v, %v)", tagRawValueType, treeOffset, len(tagTreeBuf))
				return false, nil, 0, fmt.Errorf("unknown value type: %v", tagRawValueType)
			}
			tsidCount := utils.BytesToUint16LittleEndian(tagTreeBuf[treeOffset : treeOffset+2])
			treeOffset += 2
			if uint32(len(tagTreeBuf))-treeOffset < uint32(tsidCount*8) {
				// not enough bytes left in tagTreeBuf for tsidCount TSIDs
				log.Errorf("GetMatchingTSIDs: unexpected lack of space for %v TSIDs", tsidCount)
				break
			}
			matchesThis, mightMatchOtherValue := tagValueMatches(tagHashValue, tagValue, tagOperator)
			if matchesThis {
				matchedSomething = true
				valueAsStr := string(rawTagValue)
				rawTagValueToTSIDs[valueAsStr] = make(map[uint64]struct{})

				for i := uint32(0); i < uint32(tsidCount); i++ {
					tsid := utils.BytesToUint64LittleEndian(tagTreeBuf[treeOffset : treeOffset+8])
					rawTagValueToTSIDs[valueAsStr][tsid] = struct{}{}

					treeOffset += 8
				}
			}
			if mightMatchOtherValue && !matchesThis {
				treeOffset += uint32(tsidCount * 8)
			} else if !mightMatchOtherValue {
				break
			}
		}
		if matchedSomething {
			break
		} else {
			return true, rawTagValueToTSIDs, tagHashValue, fmt.Errorf("GetMatchingTSIDs: tag hash value doesn't exist")
		}
	}
	if !matchedSomething {
		return false, rawTagValueToTSIDs, tagHashValue, nil
	}
	return true, rawTagValueToTSIDs, tagHashValue, nil
}

/*
Returns *TagValueIterator a boolean indicating if the metric name was found, or any errors encountered
*/
func (attr *AllTagTreeReaders) GetValueIteratorForMetric(mName uint64, tagKey string) (*TagValueIterator, bool, error) {
	ttr, ok := attr.tagTrees[tagKey]
	if !ok {
		ttr, err := attr.InitTagsTreeReader(tagKey)
		if err != nil {
			return nil, false, fmt.Errorf("GetMatchingTSIDs: failed to initialize tags tree reader for key %s, error: %v", tagKey, err)
		} else {
			return ttr.GetValueIteratorForMetric(mName)
		}
	}
	return ttr.GetValueIteratorForMetric(mName)
}

func (ttr *TagTreeReader) GetValueIteratorForMetric(mName uint64) (*TagValueIterator, bool, error) {
	var hashedMName uint64
	var startOff, endOff uint32
	id := uint32(0)
	for id < uint32(len(ttr.metadataBuf)) {
		hashedMName = utils.BytesToUint64LittleEndian(ttr.metadataBuf[id : id+8])
		id += 8
		if hashedMName != mName {
			id += 8
			continue
		}
		startOff = utils.BytesToUint32LittleEndian(ttr.metadataBuf[id : id+4])
		id += 4
		endOff = utils.BytesToUint32LittleEndian(ttr.metadataBuf[id : id+4])
		tagTreeBuf := make([]byte, endOff-startOff)
		_, err := ttr.fd.ReadAt(tagTreeBuf, int64(startOff))
		if err != nil {
			log.Errorf("GetValueIteratorForMetric: failed to read tagtree buffer at %d! Err %+v", startOff, err)
			return nil, false, err
		}
		return &TagValueIterator{
			tagTreeBuf:    tagTreeBuf,
			treeOffset:    0,
			matchingTSIDs: make(map[uint64]struct{}),
		}, true, nil
	}
	return nil, false, nil
}

/*
Returns next tag value, all matching tsids, and bool indicating if more values exist
If bool=false, the returned tagvalue/rawvalue/matching tsids will be empty
*/
func (tvi *TagValueIterator) Next() (uint64, []byte, map[uint64]struct{}, bool) {
	var tagValue []byte
	var matchingTSIDs map[uint64]struct{} = map[uint64]struct{}{}
	for tvi.treeOffset < uint32(len(tvi.tagTreeBuf)) {
		if uint32(len(tvi.tagTreeBuf))-tvi.treeOffset < 10 {
			// not enough bytes left in tagTreeBuf for a full tag tree entry
			return 0, nil, nil, false
		}
		tagHashValue := utils.BytesToUint64LittleEndian(tvi.tagTreeBuf[tvi.treeOffset : tvi.treeOffset+8])
		tvi.treeOffset += 8
		tagRawValueType := tvi.tagTreeBuf[tvi.treeOffset : tvi.treeOffset+1]
		tvi.treeOffset += 1
		if tagRawValueType[0] == segutils.VALTYPE_ENC_SMALL_STRING[0] {
			tagValueLen := utils.BytesToUint16LittleEndian(tvi.tagTreeBuf[tvi.treeOffset : tvi.treeOffset+2])
			tvi.treeOffset += 2
			tagValue = tvi.tagTreeBuf[tvi.treeOffset : tvi.treeOffset+uint32(tagValueLen)]
			tvi.treeOffset += uint32(tagValueLen)
		} else if tagRawValueType[0] == segutils.VALTYPE_ENC_FLOAT64[0] {
			tagValue = tvi.tagTreeBuf[tvi.treeOffset : tvi.treeOffset+8]
			tvi.treeOffset += 8
		}
		tsidCount := utils.BytesToUint16LittleEndian(tvi.tagTreeBuf[tvi.treeOffset : tvi.treeOffset+2])
		tvi.treeOffset += 2
		if uint32(len(tvi.tagTreeBuf))-tvi.treeOffset < uint32(tsidCount*8) {
			// not enough bytes left in tagTreeBuf for all TSIDs
			return 0, nil, nil, false
		}
		for i := uint16(0); i < tsidCount; i++ {
			tsid := utils.BytesToUint64LittleEndian(tvi.tagTreeBuf[tvi.treeOffset : tvi.treeOffset+8])
			tvi.treeOffset += 8
			matchingTSIDs[tsid] = struct{}{}
			tvi.matchingTSIDs[tsid] = struct{}{}
		}
		if len(matchingTSIDs) > 0 {
			return tagHashValue, tagValue, matchingTSIDs, true
		}
	}
	return 0, nil, nil, false
}

// Returns two bools; first is true if it matches this value, second is true if it might match a different value.
func tagValueMatches(actualValue uint64, pattern uint64, tagOperator segutils.TagOperator) (matchesThis bool, mightMatchOtherValue bool) {
	switch tagOperator {
	case segutils.Equal:
		matchesThis = (actualValue == pattern)
		mightMatchOtherValue = !matchesThis
	case segutils.NotEqual:
		matchesThis = (actualValue != pattern)
		mightMatchOtherValue = true
	default:
		log.Errorf("tagValueMatches: unsupported tagOperator: %v", tagOperator)
		matchesThis = false
		mightMatchOtherValue = false
	}

	return matchesThis, mightMatchOtherValue
}

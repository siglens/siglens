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

package tagstree

import (
	"fmt"
	"io/fs"
	"os"

	"github.com/cespare/xxhash"
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
	metadataBuf []byte   // consists of the meta data info for a given tag key; excludes the first 5 bytes (version and size)
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

func (attr *AllTagTreeReaders) getTagTreeFileInfoForTagKey(tagKey string) (bool, fs.FileInfo) {
	fName := attr.baseDir + tagKey
	finfo, err := os.Stat(fName)
	if err != nil {
		return false, nil
	}
	return true, finfo
}

func InitAllTagsTreeReader(tagsTreeBaseDir string) (*AllTagTreeReaders, error) {
	// Each file in the base directory is a tag tree file. The file name is the
	// tag key.
	filesInDir, err := os.ReadDir(tagsTreeBaseDir)
	if err != nil {
		err = fmt.Errorf("InitAllTagsTreeReader: failed to read the base directory %s; err=%v", tagsTreeBaseDir, err)
		log.Errorf(err.Error())
		return nil, err
	}

	attr := &AllTagTreeReaders{
		baseDir:  tagsTreeBaseDir,
		tagTrees: make(map[string]*TagTreeReader),
	}

	for _, file := range filesInDir {
		if file.IsDir() {
			log.Warnf("InitAllTagsTreeReader: found a directory %v in the base directory %s; skipping it", file.Name(), tagsTreeBaseDir)
			continue
		}

		tagKey := file.Name()
		fInfo, err := file.Info()
		if err != nil {
			err = fmt.Errorf("InitAllTagsTreeReader: failed to get file info for file %s; err=%v", tagKey, err)
			log.Errorf(err.Error())
			return nil, err
		}

		// This also inserts the tagTreeReader into the tagTrees map.
		_, err = attr.InitTagsTreeReader(tagKey, fInfo)
		if err != nil {
			err = fmt.Errorf("InitAllTagsTreeReader: failed to initialize tag tree reader for tag key %s in base dir %v; err=%v", tagKey, tagsTreeBaseDir, err)
			log.Errorf(err.Error())
			return nil, err
		}
	}

	return attr, nil
}

func (attr *AllTagTreeReaders) InitTagsTreeReader(tagKey string, fInfo fs.FileInfo) (*TagTreeReader, error) {
	fName := attr.baseDir + tagKey

	if fInfo.Size() == 0 {
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

	for i := 0; i < len(mQuery.TagsFilters); i++ {
		tf := mQuery.TagsFilters[i]
		//  Check if the tag key exists in the tag tree
		fileExists, fInfo := attr.getTagTreeFileInfoForTagKey(tf.TagKey)
		if !fileExists {
			continue
		}
		if tagVal, ok := tf.RawTagValue.(string); ok && tagVal == "*" {
			itr, mNameExists, err := attr.GetValueIteratorForMetric(mQuery.HashedMName, tf.TagKey, fInfo)
			if err != nil {
				log.Infof("FindTSIDS: failed to get the value iterator for metric name %v and tag key %v. Error: %v. TagVAlH %+v", mQuery.MetricName, tf.TagKey, err, tf.HashTagValue)
				continue
			}

			if !mNameExists {
				continue
			}

			rawTagValueToTSIDs := make(map[string]map[uint64]struct{})
			for {
				_, tagRawValue, tsids, tagRawValueType, more := itr.Next()
				if !more {
					if mQuery.GetAllLabels {
						numValueFiltersNonZero := mQuery.GetNumValueFilters() > 0
						initMetricName := fmt.Sprintf("%v{", mQuery.MetricName)
						err = tracker.BulkAddStar(rawTagValueToTSIDs, initMetricName, tf.TagKey, numValueFiltersNonZero)
					} else if !mQuery.SelectAllSeries || mQuery.ExitAfterTagsSearch {
						numValueFiltersNonZero := mQuery.GetNumValueFilters() > 0
						var initMetricName string
						if mQuery.ExitAfterTagsSearch {
							initMetricName = mQuery.MetricName
							// Update the tag indices to keep Map; This is only required in this case
							// Because for other cases and normal query flow we do not need to track the tag indices i.e. Tag Filters
							if _, indexExists := mQuery.TagIndicesToKeep[i]; !indexExists {
								mQuery.TagIndicesToKeep[i] = struct{}{}
							}
							err = tracker.BulkAddStarTagsOnly(rawTagValueToTSIDs, initMetricName, tf.TagKey, numValueFiltersNonZero)
						} else {
							initMetricName = fmt.Sprintf("%v{", mQuery.MetricName)
							err = tracker.BulkAddStar(rawTagValueToTSIDs, initMetricName, tf.TagKey, numValueFiltersNonZero)
						}
					}
					if err != nil {
						log.Errorf("FindTSIDS: failed to bulk add tsids to tracker for the tag Key: %v! Error %+v", tf.TagKey, err)
						return nil, err
					}
					break
				}
				var grpIDStr string
				if !mQuery.GetAllLabels && mQuery.SelectAllSeries && !mQuery.ExitAfterTagsSearch {
					for tsid := range tsids {
						err := tracker.AddTSID(tsid, mQuery.MetricName, tf.TagKey, false)
						if err != nil {
							log.Errorf("FindTSIDS: failed to add tsid %v to tracker for the tag key: %v! Error %+v", tsid, tf.TagKey, err)
							return nil, err
						}
					}
				} else {
					if tagRawValueType[0] == segutils.VALTYPE_ENC_FLOAT64[0] {
						grpIDStr = fmt.Sprintf("%f", utils.BytesToFloat64LittleEndian(tagRawValue))
					} else if tagRawValueType[0] == segutils.VALTYPE_ENC_INT64[0] {
						grpIDStr = fmt.Sprintf("%d", utils.BytesToInt64LittleEndian(tagRawValue))
					} else {
						grpIDStr = string(tagRawValue)
					}

					rawTagValueToTSIDs[grpIDStr] = make(map[uint64]struct{})
					for tsid := range tsids {
						rawTagValueToTSIDs[grpIDStr][tsid] = struct{}{}
					}
				}
			}
			err = tracker.FinishBlock()
			if err != nil {
				log.Errorf("FindTSIDS: failed to execute finish on block! Error %+v", err)
				return nil, err
			}
		} else {
			mNameExists, tagValueExists, rawTagValueToTSIDs, _, err := attr.GetMatchingTSIDs(mQuery.HashedMName, tf.TagKey, tf.HashTagValue, tf.TagOperator, fInfo)
			if err != nil {
				log.Infof("FindTSIDS: failed to get matching tsids for mNAme %v and tag key %v. Error: %v. TagVAlH %+v tagVal %+v", mQuery.MetricName, tf.TagKey, err, tf.HashTagValue, tf.RawTagValue)
				return nil, err
			}
			if !mNameExists {
				continue
			}
			if !tagValueExists {
				continue
			}
			if mQuery.ExitAfterTagsSearch {
				err = tracker.BulkAddTagsOnly(rawTagValueToTSIDs, mQuery.MetricName, tf.TagKey)
			} else {
				err = tracker.BulkAdd(rawTagValueToTSIDs, mQuery.MetricName, tf.TagKey)
			}
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

	return tracker, nil
}

/*
Returns:
- map[uint64]struct{}, mapping with tsid as key
- bool, indicating whether metric name existed
- bool, another bool indicates whether tag value existed
- map[string]map[uint64]struct{}, map matching raw tag values for this tag key to set of all tsids with that value
- error, any errors encountered
*/
func (attr *AllTagTreeReaders) GetMatchingTSIDs(mName uint64, tagKey string, tagValue uint64, tagOperator segutils.TagOperator, fInfo fs.FileInfo) (bool, bool, map[string]map[uint64]struct{}, uint64, error) {
	ttr, ok := attr.tagTrees[tagKey]
	if !ok {
		ttr, err := attr.InitTagsTreeReader(tagKey, fInfo)
		if err != nil {
			return false, false, nil, 0, fmt.Errorf("GetMatchingTSIDs: failed to initialize tags tree reader for key %s, error: %v", tagKey, err)
		} else {
			return ttr.GetMatchingTSIDs(mName, tagValue, tagOperator)
		}
	}
	return ttr.GetMatchingTSIDs(mName, tagValue, tagOperator)
}

// See encodeTagsTree() for how the file for a TagTree is laid out.
// The return values are (mNameFound, tagValueFound, rawTagValueToTSIDs, tagHashValue, error)
func (ttr *TagTreeReader) GetMatchingTSIDs(mName uint64, tagValue uint64, tagOperator segutils.TagOperator) (bool, bool, map[string]map[uint64]struct{}, uint64, error) {
	if tagOperator != segutils.Equal && tagOperator != segutils.NotEqual {
		log.Errorf("TagTreeReader.GetMatchingTSIDs: tagOperator %v is not supported; only Equal and NotEqual are currently implemented", tagOperator)
		return false, false, nil, tagValue, fmt.Errorf("TagTreeReader.GetMatchingTSIDs: tagOperator not supported")
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
			log.Errorf("TagTreeReader.GetMatchingTSIDs: failed to read tagtree buffer with startOffset %v and endOffset %v; err=%+v", startOff, endOff, err)
			return false, false, nil, 0, err
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
				log.Errorf("TagTreeReader.GetMatchingTSIDs: unknown value type: %v, (treeOffset, len(tagTreeBuf)): (%v, %v), file name: %v, startOffset: %v",
					tagRawValueType, treeOffset, len(tagTreeBuf), ttr.fd.Name(), startOff)
				return false, false, nil, 0, fmt.Errorf("unknown value type: %v", tagRawValueType)
			}

			tsidCount := uint32(utils.BytesToUint16LittleEndian(tagTreeBuf[treeOffset : treeOffset+2]))
			treeOffset += 2
			if uint32(len(tagTreeBuf))-treeOffset < tsidCount*8 {
				// not enough bytes left in tagTreeBuf for tsidCount TSIDs
				log.Errorf("GetMatchingTSIDs: unexpected lack of space for %v TSIDs", tsidCount)
				break
			}
			matchesThis, mightMatchOtherValue := tagValueMatches(tagHashValue, tagValue, tagOperator)
			if matchesThis {
				matchedSomething = true
				valueAsStr := string(rawTagValue)
				rawTagValueToTSIDs[valueAsStr] = make(map[uint64]struct{})

				for i := uint32(0); i < tsidCount; i++ {
					tsid := utils.BytesToUint64LittleEndian(tagTreeBuf[treeOffset : treeOffset+8])
					rawTagValueToTSIDs[valueAsStr][tsid] = struct{}{}

					treeOffset += 8
				}
			}
			if mightMatchOtherValue && !matchesThis {
				treeOffset += tsidCount * 8
			} else if !mightMatchOtherValue {
				break
			}
		}
		if matchedSomething {
			break
		} else {
			return true, false, rawTagValueToTSIDs, tagHashValue, nil
		}
	}
	if !matchedSomething {
		return false, false, rawTagValueToTSIDs, tagHashValue, nil
	}
	return true, true, rawTagValueToTSIDs, tagHashValue, nil
}

func (attr *AllTagTreeReaders) GetAllTagKeys() map[string]struct{} {
	tagKeys := make(map[string]struct{})
	for tagKey := range attr.tagTrees {
		tagKeys[tagKey] = struct{}{}
	}

	return tagKeys
}

// Returns a map: tagKey -> set of tagValues for that key
func (attr *AllTagTreeReaders) GetAllTagPairs() map[string]map[string]struct{} {
	tagPairs := make(map[string]map[string]struct{})
	for tagKey, ttr := range attr.tagTrees {
		err := ttr.readTagValuesOnly(tagKey, tagPairs)
		if err != nil {
			log.Errorf("AllTagTreeReaders.GetAllTagPairs: failed to get tag values for tag key %v. Error: %v", tagKey, err)
		}
	}

	return tagPairs
}

func (attr *AllTagTreeReaders) GetTSIDsForKey(tagKey string) (map[uint64]struct{}, error) {
	ttr, ok := attr.tagTrees[tagKey]
	if !ok {
		return nil, fmt.Errorf("AllTagTreeReaders.GetTSIDsForKey: tag key %v not found", tagKey)
	}

	allTSIDs := make(map[uint64]struct{})
	values := make(map[string]map[string]struct{})
	err := ttr.readTagValuesOnly(tagKey, values)
	if err != nil {
		log.Errorf("AllTagTreeReaders.GetTSIDsForKey: failed to get tag values for tag key %v. Error: %v", tagKey, err)
		return nil, err
	}

	valuesForKey, ok := values[tagKey]
	if !ok {
		err := fmt.Errorf("AllTagTreeReaders.GetTSIDsForKey: tag key %v not found in values map", tagKey)
		log.Errorf(err.Error())
		return nil, err
	}

	for value := range valuesForKey {
		tsids, err := ttr.GetTSIDsForTagValue(value)
		if err != nil {
			log.Errorf("AllTagTreeReaders.GetTSIDsForKey: failed to get TSIDs for tag key %v, tag value %v. Error: %v", tagKey, value, err)
			return nil, err
		}

		allTSIDs = utils.MergeMaps(allTSIDs, tsids)
	}

	return allTSIDs, nil
}

func (attr *AllTagTreeReaders) GetTSIDsForTagPair(tagKey string, tagValue string) (map[uint64]struct{}, error) {
	ttr, ok := attr.tagTrees[tagKey]
	if !ok {
		return nil, fmt.Errorf("AllTagTreeReaders.GetTSIDsForTagPair: tag key %v not found", tagKey)
	}

	return ttr.GetTSIDsForTagValue(tagValue)
}

func (ttr *TagTreeReader) GetTSIDsForTagValue(tagValue string) (map[uint64]struct{}, error) {
	hashedMetricNames, err := ttr.getHashedMetricNames()
	if err != nil {
		log.Errorf("TagTreeReader.GetTSIDsForTagValue: failed to get hashed metric names. Error: %v", err)
		return nil, err
	}

	allTSIDs := make(map[uint64]struct{})
	hashedTagValue := xxhash.Sum64String(tagValue) // The hash function that TagTree.AddTagValue uses.

	for hashedMetricName := range hashedMetricNames {
		_, _, rawTagValueToTSIDs, _, err := ttr.GetMatchingTSIDs(hashedMetricName, hashedTagValue, segutils.Equal)
		if err != nil {
			log.Errorf("AllTagTreeReaders.GetTSIDsForTagValue: failed to get matching TSIDs for tag value %v, metric hash %v. Error: %v",
				tagValue, hashedMetricName, err)
			return nil, err
		}

		// There should be 0 or 1 matching tag values, since we're looking for
		// a specific tag value.
		if len(rawTagValueToTSIDs) > 1 {
			err := fmt.Errorf("AllTagTreeReaders.GetTSIDsForTagValue: expected 0 or 1 tag value to match, got %v", len(rawTagValueToTSIDs))
			log.Errorf(err.Error())
			return nil, err
		}

		for _, tsids := range rawTagValueToTSIDs {
			allTSIDs = utils.MergeMaps(allTSIDs, tsids)
		}
	}

	return allTSIDs, nil
}

/*
Returns *TagValueIterator a boolean indicating if the metric name was found, or any errors encountered
*/
func (attr *AllTagTreeReaders) GetValueIteratorForMetric(mName uint64, tagKey string, fInfo fs.FileInfo) (*TagValueIterator, bool, error) {
	ttr, ok := attr.tagTrees[tagKey]
	if !ok {
		ttr, err := attr.InitTagsTreeReader(tagKey, fInfo)
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
func (tvi *TagValueIterator) Next() (uint64, []byte, map[uint64]struct{}, []byte, bool) {
	var tagValue []byte
	var matchingTSIDs map[uint64]struct{} = map[uint64]struct{}{}
	for tvi.treeOffset < uint32(len(tvi.tagTreeBuf)) {
		if uint32(len(tvi.tagTreeBuf))-tvi.treeOffset < 10 {
			// not enough bytes left in tagTreeBuf for a full tag tree entry
			return 0, nil, nil, nil, false
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
		} else if tagRawValueType[0] == segutils.VALTYPE_ENC_INT64[0] {
			tagValue = tvi.tagTreeBuf[tvi.treeOffset : tvi.treeOffset+8]
			tvi.treeOffset += 8
		} else {
			log.Errorf("TagValueIterator.Next: unknown value type: %v", tagRawValueType)
		}
		tsidCount := uint32(utils.BytesToUint16LittleEndian(tvi.tagTreeBuf[tvi.treeOffset : tvi.treeOffset+2]))
		tvi.treeOffset += 2
		if uint32(len(tvi.tagTreeBuf))-tvi.treeOffset < tsidCount*8 {
			// not enough bytes left in tagTreeBuf for all TSIDs
			return 0, nil, nil, nil, false
		}
		for i := uint32(0); i < tsidCount; i++ {
			tsid := utils.BytesToUint64LittleEndian(tvi.tagTreeBuf[tvi.treeOffset : tvi.treeOffset+8])
			tvi.treeOffset += 8
			matchingTSIDs[tsid] = struct{}{}
			tvi.matchingTSIDs[tsid] = struct{}{}
		}
		if len(matchingTSIDs) > 0 {
			return tagHashValue, tagValue, matchingTSIDs, tagRawValueType, true
		}
	}
	return 0, nil, nil, nil, false
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

func (attr *AllTagTreeReaders) FindTagValuesOnly(mQuery *structs.MetricsQuery,
	rawTagValues map[string]map[string]struct{}) error {

	defer func() {
		for _, ttr := range attr.tagTrees {
			ttr.Close()
		}
	}()

	for i := 0; i < len(mQuery.TagsFilters); i++ {
		tf := mQuery.TagsFilters[i]
		//  Check if the tag key exists in the tag tree
		fileExists, fInfo := attr.getTagTreeFileInfoForTagKey(tf.TagKey)
		if !fileExists {
			continue
		}

		err := attr.readTagValuesOnly(tf.TagKey, fInfo, rawTagValues)
		if err != nil {
			log.Infof("FindTagValuesOnly: failed to get the value iterator for tag key %v. Error: %v. TagVAlH %+v", tf.TagKey, err, tf.HashTagValue)
			continue
		}
	}
	return nil
}

/*
Returns *TagValueIterator a boolean indicating if the metric name was found, or any errors encountered
*/
func (attr *AllTagTreeReaders) readTagValuesOnly(tagKey string, fInfo fs.FileInfo,
	rawTagValues map[string]map[string]struct{}) error {

	ttr, ok := attr.tagTrees[tagKey]
	if !ok {
		var err error
		ttr, err = attr.InitTagsTreeReader(tagKey, fInfo)
		if err != nil {
			return fmt.Errorf("readTagValuesOnly: failed to initialize tags tree reader for key %s, error: %v", tagKey, err)
		}
	}
	return ttr.readTagValuesOnly(tagKey, rawTagValues)
}
func (ttr *TagTreeReader) readTagValuesOnly(tagKey string,
	rawTagValues map[string]map[string]struct{}) error {
	var startOff, endOff uint32
	id := uint32(0)

	currTvMap, ok := rawTagValues[tagKey]
	if !ok {
		currTvMap = make(map[string]struct{})
		rawTagValues[tagKey] = currTvMap
	}

	for id < uint32(len(ttr.metadataBuf)) {
		id += 8 // for hashedMetricName

		startOff = utils.BytesToUint32LittleEndian(ttr.metadataBuf[id : id+4])
		id += 4
		endOff = utils.BytesToUint32LittleEndian(ttr.metadataBuf[id : id+4])
		tagTreeBuf := make([]byte, endOff-startOff)
		_, err := ttr.fd.ReadAt(tagTreeBuf, int64(startOff))
		if err != nil {
			log.Errorf("GetValueIteratorForMetric: failed to read tagtree buffer at %d! Err %+v", startOff, err)
			return err
		}
		tvi := &TagValueIterator{
			tagTreeBuf:    tagTreeBuf,
			treeOffset:    0,
			matchingTSIDs: make(map[uint64]struct{}),
		}
		tvi.loopThroughTagValues(currTvMap)
		id = endOff
	}
	return nil
}

func (tvi *TagValueIterator) loopThroughTagValues(currTvMap map[string]struct{}) {

	for {
		tagRawValue, tagRawValueType, more := tvi.NextTagValue()
		if !more {
			break
		}
		var tagvStr string
		if tagRawValueType[0] == segutils.VALTYPE_ENC_FLOAT64[0] {
			tagvStr = fmt.Sprintf("%f", utils.BytesToFloat64LittleEndian(tagRawValue))
		} else if tagRawValueType[0] == segutils.VALTYPE_ENC_INT64[0] {
			tagvStr = fmt.Sprintf("%d", utils.BytesToInt64LittleEndian(tagRawValue))
		} else {
			tagvStr = string(tagRawValue)
		}
		currTvMap[tagvStr] = struct{}{}
	}
}

/*
Returns next tag value,  tag valueType, bool indicating if more values exist
*/
func (tvi *TagValueIterator) NextTagValue() ([]byte, []byte, bool) {
	var tagValue []byte
	for tvi.treeOffset < uint32(len(tvi.tagTreeBuf)) {
		if uint32(len(tvi.tagTreeBuf))-tvi.treeOffset < 10 {
			// not enough bytes left in tagTreeBuf for a full tag tree entry
			return nil, nil, false
		}
		tvi.treeOffset += 8 // for tagHashValue
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
		} else if tagRawValueType[0] == segutils.VALTYPE_ENC_INT64[0] {
			tagValue = tvi.tagTreeBuf[tvi.treeOffset : tvi.treeOffset+8]
			tvi.treeOffset += 8
		} else {
			log.Errorf("TagValueIterator.Next: unknown value type: %v", tagRawValueType)
			return nil, nil, false

		}
		tsidCount := uint32(utils.BytesToUint16LittleEndian(tvi.tagTreeBuf[tvi.treeOffset : tvi.treeOffset+2]))
		tvi.treeOffset += 2
		if uint32(len(tvi.tagTreeBuf))-tvi.treeOffset < tsidCount*8 {
			// not enough bytes left in tagTreeBuf for all TSIDs
			return nil, nil, false
		}

		// we don't need the tsids
		tvi.treeOffset += 8 * tsidCount
		if tsidCount > 0 {
			return tagValue, tagRawValueType, true
		}
	}
	return nil, nil, false
}

func (attr *AllTagTreeReaders) GetHashedMetricNames() (map[uint64]struct{}, error) {
	allHashedMetricNames := make(map[uint64]struct{})
	for _, ttr := range attr.tagTrees {
		hashedMetricNames, err := ttr.getHashedMetricNames()
		if err != nil {
			log.Errorf("AllTagTreeReaders.GetHashedMetricNames: failed to get hashed metric names. Error: %v", err)
			return nil, err
		}

		allHashedMetricNames = utils.MergeMaps(allHashedMetricNames, hashedMetricNames)
	}

	return allHashedMetricNames, nil
}

// Refer to the comment above TagTree.encodeTagsTree() for how the metadata is
// structured.
func (ttr *TagTreeReader) getHashedMetricNames() (map[uint64]struct{}, error) {
	hashedMetricNames := make(map[uint64]struct{})
	index := 0
	for index < len(ttr.metadataBuf) {
		hashedMetricName := utils.BytesToUint64LittleEndian(ttr.metadataBuf[index : index+8])
		hashedMetricNames[hashedMetricName] = struct{}{}
		index += 16 // 8 for the hashed metric name, 4 for the start offset, 4 for the end offset
	}

	return hashedMetricNames, nil
}

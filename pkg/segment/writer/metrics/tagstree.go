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

package metrics

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	jp "github.com/buger/jsonparser"
	"github.com/cespare/xxhash"
	"github.com/siglens/siglens/pkg/config"
	. "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer/suffix"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

/*
TagTree is a two level tree, containing metricName at level 1 and a tagValue at level 2
The leaf nodes stores the tsids that match certain tagValue

# The tags for a metrics will be inserted via xxhash to allow for O(log n) search

TODO: how to flushes to just write updates
*/
type TagTree struct {
	rawValues    map[uint64][]*tagInfo // maps metricNameHash to a list of tagInfo in sorted order
	dirty        bool                  // in memory has data that is not flushed to disk
	numMetrics   int                   // number of metric names in this tree
	numLeafNodes int                   // number of leaf nodes in this tree (i.e. number of tagValues)
	numTSIDs     int                   // number of tsids in this tree
	rwLock       *sync.RWMutex
}

/*
Holder struct for all tagTrees

Internally, will expose functions to check and add tags to the tree
*/
type TagsTreeHolder struct {
	tagstreeBase string // the base directory where the tags tree information is stored
	mid          string
	suffix       uint64
	allTrees     map[string]*TagTree // maps tagKey to the corresponding tagTree
	tagBloom     *bloom.BloomFilter  // bloom filter for all tsids that exist across allTrees
	rwLock       *sync.RWMutex
	createdTime  time.Time
}

// for a given tag value, store all tsids that match
type tagInfo struct {
	tagValue      []byte
	tagHashValue  uint64
	tagValueType  jp.ValueType
	matchingtsids []uint64
}

func InitTagsTree() *TagTree {
	return &TagTree{
		rawValues: make(map[uint64][]*tagInfo),
		dirty:     false,
		rwLock:    &sync.RWMutex{},
	}
}

func InitTagsTreeHolder(mid string) (*TagsTreeHolder, error) {
	suffix, err := suffix.GetSuffix(mid, "tth")
	if err != nil {
		return nil, err
	}
	tKey := GetFinalTagsTreeDir(mid, suffix)
	return &TagsTreeHolder{
		tagstreeBase: tKey,
		mid:          mid,
		suffix:       suffix,
		allTrees:     make(map[string]*TagTree),
		tagBloom:     bloom.NewWithEstimates(10_000, 0.001), // TODO: dynamic sizing
		rwLock:       &sync.RWMutex{},
		createdTime:  time.Now(),
	}, nil
}

/*
Returns a bool indicating if this tsid is new

# Adds the inputed tags into corresponding tagsTree

Internally, will use the internal bloom to check if the tsid has already been added or not
*/
func (tth *TagsTreeHolder) AddTagsForTSID(mName []byte, tags *TagsHolder, tsid uint64) error {
	rawTSID := utils.Uint64ToBytesLittleEndian(tsid)
	retVal := tth.tagBloom.Test(rawTSID)
	if !retVal {
		tth.rwLock.Lock()
		defer tth.rwLock.Unlock()
		err := tth.addTags(mName, tags, tsid)
		if err != nil {
			return err
		}
		tth.tagBloom.Add(rawTSID)
		return nil
	}
	return nil
}

// Add tag keys and values to the tree. If inserted into a tree, sets the updated flag.
func (tth *TagsTreeHolder) addTags(mName []byte, tags *TagsHolder, tsid uint64) error {
	finaltags := tags.getEntries()
	for _, tag := range finaltags {
		currKey := tag.tagKey
		currTree, ok := tth.allTrees[currKey]
		if !ok {
			currTree = InitTagsTree()
			tth.allTrees[currKey] = currTree
		}
		if err := currTree.AddTagValue(mName, tag.tagValue, tag.tagValueType, tsid); err != nil {
			return err
		}
	}
	return nil
}

func (tt *TagTree) AddTagValue(mName, val []byte, valueType jp.ValueType, tsid uint64) error {
	var hashVal uint64
	switch valueType {
	case jp.String:
		if value, err := jp.ParseString(val); err != nil {
			return fmt.Errorf("AddTagValue: Error in raw tag value conversion %T. Error: %v", val, err)
		} else {
			hashVal = xxhash.Sum64String(value)
		}
	case jp.Number:
		if value, err := jp.ParseFloat(val); err != nil {
			return fmt.Errorf("AddTagValue: Error in raw tag value conversion %T. Error: %v", val, err)
		} else {
			hashVal = uint64(value)
		}
	case jp.NotExist:
		// TODO: do we need special null handling?
		return fmt.Errorf("received null tag value")
	default:
		return fmt.Errorf("AddTagValue: Error in raw tag value conversion %T type %v", val, valueType.String())
	}
	hashedMName := xxhash.Sum64(mName)

	var ok bool
	var allTagInfo []*tagInfo
	tt.rwLock.Lock()
	defer tt.rwLock.Unlock()
	allTagInfo, ok = tt.rawValues[hashedMName]
	if !ok {
		tt.dirty = true
		tInfo := &tagInfo{
			tagValue:      make([]byte, len(val)),
			tagHashValue:  hashVal,
			tagValueType:  valueType,
			matchingtsids: []uint64{tsid},
		}
		copy(tInfo.tagValue, val)
		tt.rawValues[hashedMName] = []*tagInfo{tInfo}
		tt.numMetrics++
		tt.numLeafNodes++
		tt.numTSIDs++
		return nil
	}

	idx := sort.Search(len(allTagInfo), func(i int) bool { return allTagInfo[i].tagHashValue >= hashVal })

	if idx == len(allTagInfo) {
		tt.dirty = true
		toAddtag := &tagInfo{
			tagValue:      make([]byte, len(val)),
			tagHashValue:  hashVal,
			tagValueType:  valueType,
			matchingtsids: []uint64{tsid},
		}
		copy(toAddtag.tagValue, val)
		tt.numLeafNodes++
		tt.numTSIDs++
		allTagInfo = append(allTagInfo, toAddtag)
		tt.rawValues[hashedMName] = allTagInfo
		return nil
	}
	if allTagInfo[idx].tagHashValue == hashVal {
		added := allTagInfo[idx].insertTSID(tsid)
		if added {
			tt.dirty = true
			tt.numTSIDs++
		}
		tt.rawValues[hashedMName] = allTagInfo
		return nil
	}
	toAddtag := &tagInfo{
		tagValue:      make([]byte, len(val)),
		tagHashValue:  hashVal,
		tagValueType:  valueType,
		matchingtsids: []uint64{tsid},
	}
	copy(toAddtag.tagValue, val)
	tt.dirty = true
	tt.numLeafNodes++
	tt.numTSIDs++
	allTagInfo = append(allTagInfo, &tagInfo{})
	copy(allTagInfo[idx+1:], allTagInfo[idx:])
	allTagInfo[idx] = toAddtag
	tt.rawValues[hashedMName] = allTagInfo

	return nil
}

func (ti *tagInfo) insertTSID(tsid uint64) bool {
	idx := sort.Search(len(ti.matchingtsids), func(i int) bool { return ti.matchingtsids[i] <= tsid })
	if idx == len(ti.matchingtsids) {
		ti.matchingtsids = append(ti.matchingtsids, tsid)
		return true
	}

	if ti.matchingtsids[idx] == tsid {
		return false
	}
	ti.matchingtsids = append(ti.matchingtsids, 0)
	copy(ti.matchingtsids[idx+1:], ti.matchingtsids[idx:])
	ti.matchingtsids[idx] = tsid
	return true
}

func GetFinalTagsTreeDir(mid string, suffix uint64) string {
	var sb strings.Builder
	sb.WriteString(config.GetRunningConfig().DataPath)
	sb.WriteString(config.GetHostID())
	sb.WriteString("/final/tth/")
	sb.WriteString(mid + "/")
	sb.WriteString(strconv.FormatUint(suffix, 10) + "/")
	baseDir := sb.String()
	return baseDir
}

func getTagsTreeFileName(key string, ttBase string) string {
	var sb strings.Builder
	sb.WriteString(ttBase)
	sb.WriteString(key)
	fileName := sb.String()
	return fileName
}

func (tt *TagTree) flushSingleTagsTree(tagKey string, tagsTreeBase string) error {
	tt.rwLock.Lock()
	defer tt.rwLock.Unlock()
	err := createTagsTreeDirectory(tagsTreeBase)
	if err != nil {
		return err
	}
	fName := getTagsTreeFileName(tagKey, tagsTreeBase)
	encodedTT, err := tt.encodeTagsTree()
	if err != nil {
		log.Errorf("EncodeTagsTreeHolder: encode failed fname=%v. Error: %v", fName, err)
		return err
	}
	ttFd, err := os.OpenFile(fName, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("EncodeTagsTreeHolder: file open failed fname=%v. Error: %v", fName, err)
		_ = os.Remove(fName)
		return err
	}
	defer ttFd.Close()
	_, err = ttFd.Write(encodedTT)
	if err != nil {
		log.Errorf("EncodeTagsTreeHolder: failed to write encoded tags tree in file=%v. Error: %v", fName, err)
		return err
	}
	return nil
}

func (tt *TagsTreeHolder) flushTagsTree() {
	err := tt.EncodeTagsTreeHolder()
	if err != nil {
		log.Errorf("flushTagsTree: failed to write tagstree info to file. Error: %v", err)
	}
}

func (tt *TagsTreeHolder) rotateTagsTree(forceRotate bool) error {
	if !forceRotate {
		nextSuffix, err := suffix.GetSuffix(tt.mid, "tth")
		if err != nil {
			log.Errorf("rotateTagsTree: failed to get the next suffix for mid %s. Error: %+v", tt.mid, err)
			return err
		}
		tagsTreePath := GetFinalTagsTreeDir(tt.mid, nextSuffix)
		tt.tagstreeBase = tagsTreePath
		tt.suffix = nextSuffix
		tt.allTrees = make(map[string]*TagTree)
		tt.tagBloom = bloom.NewWithEstimates(10_000, 0.001) // TODO: dynamic sizing
		tt.createdTime = time.Now()
	}
	return nil
}

func createTagsTreeDirectory(ttBase string) error {
	if _, err := os.Stat(ttBase); err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(ttBase, 0764)
			if err != nil {
				log.Errorf("EncodeTagsTreeHolder: failed to create directory %s . Error %+v", ttBase, err)
				return err
			}
		}
	}
	return nil
}

func (tt *TagsTreeHolder) EncodeTagsTreeHolder() error {
	err := createTagsTreeDirectory(tt.tagstreeBase)
	if err != nil {
		return err
	}
	for tagKey, tree := range tt.allTrees {
		err := tree.flushSingleTagsTree(tagKey, tt.tagstreeBase)
		if err != nil {
			return err
		}
		tree.dirty = false
	}
	return nil
}

/*
The returned []byte consists of two sections: Metadata and Data.

In the Metadata section, the first byte is VERSION_TAGSTREE and the next four
bytes give the size of the Metadata section. The rest of the Metadata section
consists of 16-byte chunks (one chunk per metric with this tag key); within each
of these chunks, the first 8 bytes are the hashed metric name, the next 4 bytes
is the offset within the []byte where the corresponding data for this
(key, metric) combination starts; this offset will be somewhere in the Data
section (so it will be at least the size of the Metadata portion). The next (and
final) 4 bytes of the chunk is the end offset--similar to the start offset.

The Data section consists of several chunks, with one chunk per metric with this
tag key. Each of these chunks consists of several blocks, with one block per tag
value that this (key, metric) combination has a time series for. The format of
each block is:
  - 8 bytes for the hashed tag value
  - 1 byte for the type of the tag value (currently, either VALTYPE_ENC_SMALL_STRING or VALTYPE_ENC_FLOAT64)
  - The raw tag value
    -- If the type is VALTYPE_ENC_SMALL_STRING there's 2 bytes for the size of
    the string, and then N more bytes, where N is the size of the string.
    -- If the type is VALTYPE_ENC_FLOAT64 there's 8 bytes
  - 2 bytes for the number of matching TSIDs; call this numMatchingTSIDs
  - numMatchingTSIDs 8-byte numbers, each representing a TSID satisfying this (metric, key, value) combination
*/
func (tree *TagTree) encodeTagsTree() ([]byte, error) {
	// metaInfo bytes: [metricBytes-uint32][[mName1-uint64][msOff-uint32][meOff-uint32]..]
	metadataSize := uint32(5 + (tree.numMetrics * 16))
	metadataBuf := make([]byte, metadataSize)
	dataBuf := make([]byte, metadataSize)
	totalBytesWritten := 0
	startOff := metadataSize
	copy(metadataBuf[:1], VERSION_TAGSTREE) // Write version byte as 0x01
	copy(metadataBuf[1:5], utils.Uint32ToBytesLittleEndian(metadataSize))
	idx := uint32(5)
	for hashedMName, tagInfo := range tree.rawValues {
		tagBuf := new(bytes.Buffer)
		id := uint32(0)
		for _, tInfo := range tagInfo {
			if _, err := tagBuf.Write(utils.Uint64ToBytesLittleEndian(tInfo.tagHashValue)); err != nil {
				log.Errorf("encodeTagsTree: Cannot write to buffer. Error: %v", err)
				return nil, err
			}
			id += 8
			switch tInfo.tagValueType {
			case jp.String:
				if value, err := jp.ParseString(tInfo.tagValue); err != nil {
					return nil, fmt.Errorf("encodeTagsTree: Error in raw tag value conversion %T. Error: %v", tInfo.tagValue, err)
				} else {
					if _, err := tagBuf.Write(VALTYPE_ENC_SMALL_STRING[:]); err != nil {
						log.Errorf("encodeTagsTree: Cannot write to buffer. Error: %v", err)
						return nil, err
					}
					id += 1
					n := uint16(len(value))
					if _, err := tagBuf.Write(utils.Uint16ToBytesLittleEndian(n)); err != nil {
						log.Errorf("encodeTagsTree: Cannot write to buffer. Error: %v", err)
						return nil, err
					}
					id += 2
					if _, err := tagBuf.WriteString(value); err != nil {
						log.Errorf("encodeTagsTree: Cannot write to buffer. Error: %v", err)
						return nil, err
					}
					id += uint32(len(value))
				}
			case jp.Number:
				if value, err := jp.ParseFloat(tInfo.tagValue); err != nil {
					return nil, fmt.Errorf("encodeTagsTree: Error in raw tag value conversion %T. Error: %v", tInfo.tagValue, err)
				} else {
					if _, err := tagBuf.Write(VALTYPE_ENC_FLOAT64[:]); err != nil {
						log.Errorf("encodeTagsTree: Cannot write to buffer. Error: %v", err)
						return nil, err
					}
					id += 1
					if _, err := tagBuf.Write(utils.Float64ToBytesLittleEndian(value)); err != nil {
						log.Errorf("encodeTagsTree: Cannot write to buffer. Error: %v", err)
						return nil, err
					}
					id += 8
				}
			default:
				return nil, fmt.Errorf("encodeTagsTree: Incorrect tag value type %v", tInfo.tagValueType)
			}
			l1 := uint16(len(tInfo.matchingtsids))
			if _, err := tagBuf.Write(utils.Uint16ToBytesLittleEndian(l1)); err != nil {
				log.Errorf("encodeTagsTree: Cannot write to buffer. Error: %v", err)
				return nil, err
			}
			id += 2
			for _, tsid := range tInfo.matchingtsids {
				if _, err := tagBuf.Write(utils.Uint64ToBytesLittleEndian(tsid)); err != nil {
					log.Errorf("encodeTagsTree: Cannot write to buffer. Error: %v", err)
					return nil, err
				}
				id += 8
			}
		}
		copy(metadataBuf[idx:], utils.Uint64ToBytesLittleEndian(hashedMName))
		idx += 8
		copy(metadataBuf[idx:], utils.Uint32ToBytesLittleEndian(startOff))
		idx += 4
		copy(metadataBuf[idx:], utils.Uint32ToBytesLittleEndian(startOff+id))
		idx += 4
		sizeToAdd := int64(tagBuf.Len())
		if sizeToAdd > 0 {
			newArr := make([]byte, sizeToAdd)
			dataBuf = append(dataBuf, newArr...)
		}
		offset := int64(metadataSize) + int64(totalBytesWritten)
		copy(dataBuf[offset:offset+int64(tagBuf.Len())], tagBuf.Bytes())
		startOff += id
		totalBytesWritten += tagBuf.Len()
	}
	copy(dataBuf[0:metadataSize], metadataBuf)
	return dataBuf, nil
}

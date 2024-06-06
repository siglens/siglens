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
	"sort"

	jp "github.com/buger/jsonparser"
	"github.com/cespare/xxhash"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/bytebufferpool"
)

type tagEntry struct {
	tagKey       string
	tagValue     []byte
	tagValueType jp.ValueType
}

type TagsHolder struct {
	idx     int
	len     int
	done    bool
	entries []tagEntry
	buf     *bytes.Buffer
}

var initialTagCapacity int = 10

/*
Allocates and returns a TagsHolder

Caller is responsible for calling ReturnTagsHolder
*/
func GetTagsHolder() *TagsHolder {
	holder := &TagsHolder{}
	holder.buf = &bytes.Buffer{}

	holder.len = initialTagCapacity
	holder.idx = 0
	holder.entries = make([]tagEntry, initialTagCapacity)
	holder.done = false

	return holder
}

func (th *TagsHolder) Insert(key string, value []byte, vType jp.ValueType) {
	th.len = len(th.entries)
	th.entries[th.idx].tagKey = key
	th.entries[th.idx].tagValue = value
	th.entries[th.idx].tagValueType = vType
	th.idx++
	if th.idx >= th.len {
		newBuf := make([]tagEntry, initialTagCapacity)
		th.entries = append(th.entries, newBuf...)
	}
}

func (th *TagsHolder) finish() {
	if th.done {
		return
	}

	th.entries = th.entries[:th.idx]
	if th.idx == 0 {
		th.done = true
		return
	} else {
		sort.Slice(th.entries, func(i, j int) bool { return th.entries[i].tagKey > th.entries[j].tagKey })
	}
	th.done = true
}

/*
Gets the TSID given a metric name

Internally, will make sure the tags keys are sorted
*/
func (th *TagsHolder) GetTSID(mName []byte) (uint64, error) {
	th.finish()
	th.buf.Reset()
	_, err := th.buf.Write(mName)
	if err != nil {
		log.Errorf("TagsHolder.GetTSID: Error writing metric name: %v", err)
		return 0, err
	}

	_, err = th.buf.Write(tags_separator)
	if err != nil {
		log.Errorf("TagsHolder.GetTSID: Error writing tags separator %v, err=%v", tags_separator, err)
		return 0, err
	}
	for _, val := range th.entries {
		_, err := th.buf.WriteString(val.tagKey)
		if err != nil {
			log.Errorf("TagsHolder.GetTSID: Error writing tag key %v, err=%v", val.tagKey, err)
			return 0, err
		}
		_, err = th.buf.Write(tags_separator)
		if err != nil {
			log.Errorf("TagsHolder.GetTSID: Error writing tags separator %v, err=%v", tags_separator, err)
			return 0, err
		}
		if val.tagValue == nil {
			continue
		}
		_, err = th.buf.Write(val.tagValue)
		if err != nil {
			log.Errorf("TagsHolder.GetTSID: Error writing tag value %v, err=%v", val.tagValue, err)
			return 0, err
		}
	}
	retVal := xxhash.Sum64(th.buf.Bytes())
	return retVal, nil
}

func (th *TagsHolder) getEntries() []tagEntry {
	return th.entries[:th.idx]
}

func (th *TagsHolder) String() string {
	buf := bytebufferpool.Get()
	_, _ = buf.WriteString("{")
	for _, val := range th.entries {
		_, _ = buf.WriteString(val.tagKey)
		_, _ = buf.WriteString(":")
		_, _ = buf.Write(val.tagValue)
		_, _ = buf.WriteString(",")
	}
	_, _ = buf.WriteString("}")
	return buf.String()
}
